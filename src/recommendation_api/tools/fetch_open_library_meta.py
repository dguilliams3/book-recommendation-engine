"""
Enhanced Open Library API integration with comprehensive error handling and caching.

This module provides robust book metadata fetching from Open Library API with:
- Comprehensive error handling and retry logic
- Redis caching for performance optimization
- Structured logging for debugging and monitoring
- Graceful fallback handling for missing data
- Rate limiting and timeout management
"""

import aiohttp
import asyncio
import logging
import json
from typing import Dict, Optional, List
from datetime import datetime, timedelta

from src.common.redis_utils import get_redis_client
from src.common.structured_logging import get_logger

# Configure structured logging
logger = get_logger(__name__)

# Configuration constants
OPEN_LIBRARY_BASE_URL = "https://openlibrary.org/isbn"
CACHE_TTL_SECONDS = 86400  # 24 hours
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1


class OpenLibraryEnrichmentError(Exception):
    """Custom exception for Open Library API errors."""
    pass


def _generate_cache_key(isbn: str) -> str:
    """Generate Redis cache key for Open Library metadata."""
    return f"open_library_meta:{isbn}"


def _extract_publication_year(publish_date: str) -> Optional[str]:
    """Extract publication year from various date formats."""
    if not publish_date:
        return None
    
    # Handle different date formats: "2023", "Jan 2023", "January 15, 2023"
    try:
        # Try to find a 4-digit year
        import re
        year_match = re.search(r'\b(19|20)\d{2}\b', publish_date)
        if year_match:
            return year_match.group(0)
    except (IndexError, AttributeError):
        pass
    
    return None


def _extract_description(description_data) -> Optional[str]:
    """Extract and clean description from Open Library format."""
    if not description_data:
        return None
    
    # Handle different description formats
    if isinstance(description_data, dict):
        description = description_data.get("value", "")
    elif isinstance(description_data, str):
        description = description_data
    else:
        return None
    
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


def _extract_authors(authors_data) -> List[str]:
    """Extract author names from Open Library format."""
    if not authors_data:
        return []
    
    author_names = []
    for author in authors_data:
        if isinstance(author, dict):
            name = author.get("name") or author.get("key", "").split("/")[-1]
            if name:
                author_names.append(name)
        elif isinstance(author, str):
            author_names.append(author)
    
    return author_names


async def fetch_open_library_meta(isbn: str, use_cache: bool = True) -> Optional[Dict]:
    """
    Fetch comprehensive book metadata from Open Library API.
    
    Args:
        isbn: The ISBN-10 or ISBN-13 of the book
        use_cache: Whether to use Redis caching (default: True)
        
    Returns:
        Dictionary containing book metadata or None if not found
        
    Raises:
        OpenLibraryEnrichmentError: For API errors or network issues
        
    Example:
        >>> metadata = await fetch_open_library_meta("9780134685991")
        >>> print(metadata["title"])
        "Effective Java"
    """
    start_time = datetime.now()
    
    logger.info(
        "Starting Open Library metadata fetch",
        extra={
            "isbn": isbn,
            "use_cache": use_cache,
            "operation": "open_library_fetch"
        }
    )
    
    # Input validation
    if not isbn or not isinstance(isbn, str):
        logger.error("Invalid ISBN provided", extra={"isbn": isbn})
        raise OpenLibraryEnrichmentError("Invalid ISBN provided")
    
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
                    "Open Library metadata found in cache",
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
            url = f"{OPEN_LIBRARY_BASE_URL}/{clean_isbn}.json"
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Extract and clean metadata
                        metadata = {
                            "title": data.get("title"),
                            "authors": _extract_authors(data.get("authors", [])),
                            "page_count": data.get("number_of_pages") or data.get("pagination"),
                            "publication_year": _extract_publication_year(
                                data.get("publish_date", "")
                            ),
                            "description": _extract_description(data.get("description")),
                            "subjects": data.get("subjects", []),
                            "publishers": data.get("publishers", []),
                            "languages": data.get("languages", []),
                            "isbn_10": data.get("isbn_10", [None])[0] if data.get("isbn_10") else None,
                            "isbn_13": clean_isbn,
                            "lc_classifications": data.get("lc_classifications", []),
                            "dewey_decimal_class": data.get("dewey_decimal_class", []),
                            "source": "open_library",
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
                                    "Failed to cache Open Library metadata",
                                    extra={"isbn": clean_isbn, "error": str(e)}
                                )
                        
                        break
                        
                    elif response.status == 404:
                        logger.info(
                            "Book not found in Open Library",
                            extra={"isbn": clean_isbn}
                        )
                        break  # No point retrying 404s
                        
                    elif response.status == 429:  # Rate limited
                        logger.warning(
                            "Open Library API rate limit exceeded",
                            extra={"isbn": clean_isbn, "attempt": attempt + 1}
                        )
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAY_SECONDS * (2 ** attempt))  # Exponential backoff
                        last_error = OpenLibraryEnrichmentError(f"Rate limited (HTTP {response.status})")
                        
                    else:
                        error_msg = f"Open Library API error: HTTP {response.status}"
                        logger.error(
                            "Open Library API returned error status",
                            extra={"isbn": clean_isbn, "status_code": response.status, "attempt": attempt + 1}
                        )
                        last_error = OpenLibraryEnrichmentError(error_msg)
                        
        except aiohttp.ClientError as e:
            logger.error(
                "Network error fetching from Open Library API",
                extra={"isbn": clean_isbn, "attempt": attempt + 1, "error": str(e)}
            )
            last_error = OpenLibraryEnrichmentError(f"Network error: {str(e)}")
            
        except Exception as e:
            logger.error(
                "Unexpected error fetching from Open Library API",
                extra={"isbn": clean_isbn, "attempt": attempt + 1, "error": str(e)}
            )
            last_error = OpenLibraryEnrichmentError(f"Unexpected error: {str(e)}")
        
        # Wait before retry (except on last attempt)
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY_SECONDS)
    
    # Log final result
    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    
    if metadata:
        logger.info(
            "Open Library metadata fetch successful",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "has_title": bool(metadata.get("title")),
                "has_authors": bool(metadata.get("authors")),
                "has_description": bool(metadata.get("description")),
                "page_count": metadata.get("page_count"),
                "source": "open_library"
            }
        )
    else:
        logger.warning(
            "Open Library metadata fetch failed",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "final_error": str(last_error) if last_error else "No data found"
            }
        )
    
    return metadata 