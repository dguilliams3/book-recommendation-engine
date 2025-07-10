"""
Enhanced Google Books API integration with comprehensive error handling and caching.

This module provides robust book metadata fetching from Google Books API with:
- Comprehensive error handling and retry logic
- Redis caching for performance optimization
- Structured logging for debugging and monitoring
- Graceful fallback handling for missing data
- Rate limiting and timeout management
"""

import aiohttp
import asyncio
import logging
import hashlib
import json
from typing import Dict, Optional, List
from datetime import datetime, timedelta

from src.common.redis_utils import get_redis_client
from src.common.structured_logging import get_logger

# Configure structured logging
logger = get_logger(__name__)

# Configuration constants
GOOGLE_BOOKS_BASE_URL = "https://www.googleapis.com/books/v1/volumes"
CACHE_TTL_SECONDS = 86400  # 24 hours
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1


class GoogleBooksEnrichmentError(Exception):
    """Custom exception for Google Books API errors."""
    pass


def _generate_cache_key(isbn: str) -> str:
    """Generate Redis cache key for Google Books metadata."""
    return f"google_books_meta:{isbn}"


def _extract_publication_year(published_date: str) -> Optional[str]:
    """Extract publication year from various date formats."""
    if not published_date:
        return None
    
    # Handle different date formats: "2023", "2023-01", "2023-01-15"
    try:
        year = published_date.split('-')[0]
        if len(year) == 4 and year.isdigit():
            return year
    except (IndexError, AttributeError):
        pass
    
    return None


def _clean_description(description: str) -> Optional[str]:
    """Clean and truncate book description."""
    if not description:
        return None
    
    # Remove HTML tags and excessive whitespace
    import re
    clean_desc = re.sub(r'<[^>]+>', '', description)
    clean_desc = ' '.join(clean_desc.split())
    
    # Truncate if too long (for database storage)
    if len(clean_desc) > 2000:
        clean_desc = clean_desc[:1997] + "..."
    
    return clean_desc if clean_desc else None


async def fetch_google_books_meta(isbn: str, use_cache: bool = True) -> Optional[Dict]:
    """
    Fetch comprehensive book metadata from Google Books API.
    
    Args:
        isbn: The ISBN-10 or ISBN-13 of the book
        use_cache: Whether to use Redis caching (default: True)
        
    Returns:
        Dictionary containing book metadata or None if not found
        
    Raises:
        GoogleBooksEnrichmentError: For API errors or network issues
        
    Example:
        >>> metadata = await fetch_google_books_meta("9780134685991")
        >>> print(metadata["title"])
        "Effective Java"
    """
    start_time = datetime.now()
    
    logger.info(
        "Starting Google Books metadata fetch",
        extra={
            "isbn": isbn,
            "use_cache": use_cache,
            "operation": "google_books_fetch"
        }
    )
    
    # Input validation
    if not isbn or not isinstance(isbn, str):
        logger.error("Invalid ISBN provided", extra={"isbn": isbn})
        raise GoogleBooksEnrichmentError("Invalid ISBN provided")
    
    # Clean ISBN (remove hyphens and spaces)
    clean_isbn = isbn.replace('-', '').replace(' ', '')
    cache_key = _generate_cache_key(clean_isbn)
    
    # Try cache first
    if use_cache:
        try:
            redis_client = get_redis_client()
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                logger.info(
                    "Google Books metadata found in cache",
                    extra={
                        "isbn": clean_isbn,
                        "cache_hit": True,
                        "duration_ms": int((datetime.now() - start_time).total_seconds() * 1000)
                    }
                )
                return json.loads(cached_data)
        except Exception as e:
            logger.warning(
                "Cache lookup failed, proceeding with API call",
                extra={"isbn": clean_isbn, "error": str(e)}
            )
    
    # Fetch from API with retry logic
    metadata = None
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            url = f"{GOOGLE_BOOKS_BASE_URL}?q=isbn:{clean_isbn}"
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("totalItems", 0) > 0 and data.get("items"):
                            volume_info = data["items"][0].get("volumeInfo", {})
                            
                            # Extract and clean metadata
                            metadata = {
                                "title": volume_info.get("title"),
                                "authors": volume_info.get("authors", []),
                                "page_count": volume_info.get("pageCount"),
                                "publication_year": _extract_publication_year(
                                    volume_info.get("publishedDate", "")
                                ),
                                "description": _clean_description(volume_info.get("description")),
                                "categories": volume_info.get("categories", []),
                                "average_rating": volume_info.get("averageRating"),
                                "ratings_count": volume_info.get("ratingsCount"),
                                "publisher": volume_info.get("publisher"),
                                "language": volume_info.get("language"),
                                "isbn_10": volume_info.get("industryIdentifiers", [{}])[0].get("identifier") 
                                    if volume_info.get("industryIdentifiers") else None,
                                "isbn_13": clean_isbn,
                                "thumbnail": volume_info.get("imageLinks", {}).get("thumbnail"),
                                "source": "google_books",
                                "fetched_at": datetime.now().isoformat()
                            }
                            
                            # Cache the result
                            if use_cache and metadata:
                                try:
                                    await redis_client.setex(
                                        cache_key,
                                        CACHE_TTL_SECONDS,
                                        json.dumps(metadata)
                                    )
                                except Exception as e:
                                    logger.warning(
                                        "Failed to cache Google Books metadata",
                                        extra={"isbn": clean_isbn, "error": str(e)}
                                    )
                            
                            break
                        else:
                            logger.info(
                                "No books found in Google Books API response",
                                extra={"isbn": clean_isbn, "total_items": data.get("totalItems", 0)}
                            )
                            
                    elif response.status == 429:  # Rate limited
                        logger.warning(
                            "Google Books API rate limit exceeded",
                            extra={"isbn": clean_isbn, "attempt": attempt + 1}
                        )
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAY_SECONDS * (2 ** attempt))  # Exponential backoff
                        last_error = GoogleBooksEnrichmentError(f"Rate limited (HTTP {response.status})")
                        
                    else:
                        error_msg = f"Google Books API error: HTTP {response.status}"
                        logger.error(
                            "Google Books API returned error status",
                            extra={"isbn": clean_isbn, "status_code": response.status, "attempt": attempt + 1}
                        )
                        last_error = GoogleBooksEnrichmentError(error_msg)
                        
        except aiohttp.ClientError as e:
            logger.error(
                "Network error fetching from Google Books API",
                extra={"isbn": clean_isbn, "attempt": attempt + 1, "error": str(e)}
            )
            last_error = GoogleBooksEnrichmentError(f"Network error: {str(e)}")
            
        except Exception as e:
            logger.error(
                "Unexpected error fetching from Google Books API",
                extra={"isbn": clean_isbn, "attempt": attempt + 1, "error": str(e)}
            )
            last_error = GoogleBooksEnrichmentError(f"Unexpected error: {str(e)}")
        
        # Wait before retry (except on last attempt)
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY_SECONDS)
    
    # Log final result
    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    
    if metadata:
        logger.info(
            "Google Books metadata fetch successful",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "has_title": bool(metadata.get("title")),
                "has_authors": bool(metadata.get("authors")),
                "has_description": bool(metadata.get("description")),
                "page_count": metadata.get("page_count"),
                "source": "google_books"
            }
        )
    else:
        logger.warning(
            "Google Books metadata fetch failed",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "final_error": str(last_error) if last_error else "No data found"
            }
        )
    
    return metadata 