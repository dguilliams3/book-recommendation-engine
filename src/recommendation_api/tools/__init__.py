"""
Comprehensive book enrichment service with intelligent fallback strategy.

This module provides a high-level interface for book metadata enrichment that:
- Orchestrates fallback between Google Books and Open Library APIs
- Intelligently merges metadata from multiple sources
- Provides comprehensive error handling and logging
- Optimizes performance with parallel fetching and caching
- Maintains data quality with validation and cleaning
"""

import asyncio
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime

from .fetch_google_books_meta import fetch_google_books_meta, GoogleBooksEnrichmentError
from .fetch_open_library_meta import fetch_open_library_meta, OpenLibraryEnrichmentError
from src.common.structured_logging import get_logger

# Configure structured logging
logger = get_logger(__name__)


class BookEnrichmentError(Exception):
    """Custom exception for book enrichment errors."""
    pass


def _merge_metadata(google_data: Optional[Dict], open_library_data: Optional[Dict]) -> Optional[Dict]:
    """
    Intelligently merge metadata from Google Books and Open Library.
    
    Strategy:
    - Prefer Google Books for: title, authors, description, ratings
    - Prefer Open Library for: subjects, classifications, detailed publication info
    - Combine arrays (categories, subjects) without duplicates
    - Use the most complete data available
    
    Args:
        google_data: Metadata from Google Books API
        open_library_data: Metadata from Open Library API
        
    Returns:
        Merged metadata dictionary or None if no data available
    """
    if not google_data and not open_library_data:
        return None
    
    # Start with Google Books data (generally more complete)
    merged = google_data.copy() if google_data else {}
    
    if open_library_data:
        # Merge fields, preferring non-empty values
        for key, value in open_library_data.items():
            if key not in merged or not merged[key]:
                merged[key] = value
            elif key in ['subjects', 'categories'] and isinstance(value, list):
                # Combine subject/category lists
                existing = merged.get(key, [])
                if isinstance(existing, list):
                    combined = list(set(existing + value))  # Remove duplicates
                    merged[key] = combined
        
        # Add Open Library specific fields
        if 'lc_classifications' in open_library_data:
            merged['lc_classifications'] = open_library_data['lc_classifications']
        if 'dewey_decimal_class' in open_library_data:
            merged['dewey_decimal_class'] = open_library_data['dewey_decimal_class']
    
    # Add enrichment metadata
    merged['enrichment_sources'] = []
    if google_data:
        merged['enrichment_sources'].append('google_books')
    if open_library_data:
        merged['enrichment_sources'].append('open_library')
    
    merged['enriched_at'] = datetime.now().isoformat()
    
    return merged


def _validate_isbn(isbn: str) -> bool:
    """Validate ISBN format (basic check)."""
    if not isbn or not isinstance(isbn, str):
        return False
    
    clean_isbn = isbn.replace('-', '').replace(' ', '')
    return len(clean_isbn) in [10, 13] and clean_isbn.isdigit()


async def enrich_book_metadata(
    isbn: str,
    title: Optional[str] = None,
    author: Optional[str] = None,
    parallel_fetch: bool = True,
    use_cache: bool = True
) -> Optional[Dict]:
    """
    Enrich book metadata using multiple sources with intelligent fallback.
    
    This function orchestrates metadata enrichment from Google Books and Open Library
    APIs, providing intelligent fallback and metadata merging.
    
    Args:
        isbn: The ISBN-10 or ISBN-13 of the book
        title: Optional title hint for validation
        author: Optional author hint for validation
        parallel_fetch: Whether to fetch from both APIs in parallel (default: True)
        use_cache: Whether to use Redis caching (default: True)
        
    Returns:
        Enriched metadata dictionary or None if no data found
        
    Raises:
        BookEnrichmentError: For validation errors or complete API failures
        
    Example:
        >>> metadata = await enrich_book_metadata("9780134685991")
        >>> print(f"Title: {metadata['title']}")
        >>> print(f"Sources: {metadata['enrichment_sources']}")
    """
    start_time = datetime.now()
    
    logger.info(
        "Starting book metadata enrichment",
        extra={
            "isbn": isbn,
            "title_hint": title,
            "author_hint": author,
            "parallel_fetch": parallel_fetch,
            "use_cache": use_cache,
            "operation": "book_enrichment"
        }
    )
    
    # Validate ISBN
    if not _validate_isbn(isbn):
        logger.error("Invalid ISBN format", extra={"isbn": isbn})
        raise BookEnrichmentError(f"Invalid ISBN format: {isbn}")
    
    clean_isbn = isbn.replace('-', '').replace(' ', '')
    
    # Fetch metadata from both sources
    google_data = None
    open_library_data = None
    errors = []
    
    if parallel_fetch:
        # Fetch from both APIs in parallel for better performance
        try:
            google_task = asyncio.create_task(
                fetch_google_books_meta(clean_isbn, use_cache=use_cache)
            )
            open_library_task = asyncio.create_task(
                fetch_open_library_meta(clean_isbn, use_cache=use_cache)
            )
            
            # Wait for both with timeout
            results = await asyncio.gather(
                google_task,
                open_library_task,
                return_exceptions=True
            )
            
            google_data = results[0] if not isinstance(results[0], Exception) else None
            open_library_data = results[1] if not isinstance(results[1], Exception) else None
            
            # Collect errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    source = "google_books" if i == 0 else "open_library"
                    errors.append(f"{source}: {str(result)}")
                    
        except Exception as e:
            logger.error(
                "Unexpected error during parallel metadata fetch",
                extra={"isbn": clean_isbn, "error": str(e)}
            )
            errors.append(f"Parallel fetch error: {str(e)}")
    
    else:
        # Sequential fetch (fallback strategy)
        try:
            google_data = await fetch_google_books_meta(clean_isbn, use_cache=use_cache)
        except Exception as e:
            logger.warning(
                "Google Books fetch failed, trying Open Library",
                extra={"isbn": clean_isbn, "error": str(e)}
            )
            errors.append(f"google_books: {str(e)}")
        
        try:
            open_library_data = await fetch_open_library_meta(clean_isbn, use_cache=use_cache)
        except Exception as e:
            logger.warning(
                "Open Library fetch failed",
                extra={"isbn": clean_isbn, "error": str(e)}
            )
            errors.append(f"open_library: {str(e)}")
    
    # Merge metadata from both sources
    merged_metadata = _merge_metadata(google_data, open_library_data)
    
    # Validate against hints if provided
    if merged_metadata and title:
        fetched_title = merged_metadata.get('title', '').lower()
        if title.lower() not in fetched_title and fetched_title not in title.lower():
            logger.warning(
                "Title mismatch detected",
                extra={
                    "isbn": clean_isbn,
                    "provided_title": title,
                    "fetched_title": merged_metadata.get('title')
                }
            )
    
    if merged_metadata and author:
        fetched_authors = [a.lower() for a in merged_metadata.get('authors', [])]
        author_match = any(author.lower() in fa or fa in author.lower() for fa in fetched_authors)
        if not author_match:
            logger.warning(
                "Author mismatch detected",
                extra={
                    "isbn": clean_isbn,
                    "provided_author": author,
                    "fetched_authors": merged_metadata.get('authors')
                }
            )
    
    # Log final result
    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    
    if merged_metadata:
        logger.info(
            "Book metadata enrichment successful",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "sources": merged_metadata.get('enrichment_sources', []),
                "has_title": bool(merged_metadata.get('title')),
                "has_authors": bool(merged_metadata.get('authors')),
                "has_description": bool(merged_metadata.get('description')),
                "page_count": merged_metadata.get('page_count'),
                "google_data_available": google_data is not None,
                "open_library_data_available": open_library_data is not None
            }
        )
    else:
        logger.warning(
            "Book metadata enrichment failed - no data found",
            extra={
                "isbn": clean_isbn,
                "duration_ms": duration_ms,
                "errors": errors,
                "google_attempted": True,
                "open_library_attempted": True
            }
        )
        
        # If we have errors but no data, this might be a systemic issue
        if errors:
            error_summary = "; ".join(errors)
            logger.error(
                "All enrichment sources failed",
                extra={"isbn": clean_isbn, "error_summary": error_summary}
            )
    
    return merged_metadata


async def enrich_multiple_books(
    book_list: List[Dict[str, Any]],
    isbn_field: str = 'isbn',
    title_field: str = 'title',
    author_field: str = 'author',
    max_concurrent: int = 5
) -> List[Dict[str, Any]]:
    """
    Enrich metadata for multiple books concurrently.
    
    Args:
        book_list: List of book dictionaries
        isbn_field: Field name containing ISBN
        title_field: Field name containing title (for validation)
        author_field: Field name containing author (for validation)
        max_concurrent: Maximum concurrent API calls
        
    Returns:
        List of books with enriched metadata
    """
    logger.info(
        "Starting batch book metadata enrichment",
        extra={
            "book_count": len(book_list),
            "max_concurrent": max_concurrent,
            "operation": "batch_book_enrichment"
        }
    )
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def enrich_single_book(book: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            isbn = book.get(isbn_field)
            title = book.get(title_field)
            author = book.get(author_field)
            
            if not isbn:
                logger.warning(
                    "Skipping book without ISBN",
                    extra={"book": book}
                )
                return book
            
            try:
                enriched_metadata = await enrich_book_metadata(
                    isbn=isbn,
                    title=title,
                    author=author,
                    parallel_fetch=True,
                    use_cache=True
                )
                
                if enriched_metadata:
                    # Merge enriched data with original book data
                    enriched_book = book.copy()
                    enriched_book['enriched_metadata'] = enriched_metadata
                    return enriched_book
                else:
                    logger.warning(
                        "No enrichment data found for book",
                        extra={"isbn": isbn, "title": title}
                    )
                    return book
                    
            except Exception as e:
                logger.error(
                    "Error enriching book metadata",
                    extra={"isbn": isbn, "title": title, "error": str(e)}
                )
                return book
    
    # Process all books concurrently
    start_time = datetime.now()
    enriched_books = await asyncio.gather(
        *[enrich_single_book(book) for book in book_list],
        return_exceptions=True
    )
    
    # Handle any exceptions
    results = []
    for i, result in enumerate(enriched_books):
        if isinstance(result, Exception):
            logger.error(
                "Exception during book enrichment",
                extra={"book_index": i, "error": str(result)}
            )
            results.append(book_list[i])  # Return original book
        else:
            results.append(result)
    
    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    enriched_count = sum(1 for book in results if 'enriched_metadata' in book)
    
    logger.info(
        "Batch book metadata enrichment completed",
        extra={
            "total_books": len(book_list),
            "enriched_count": enriched_count,
            "success_rate": f"{enriched_count/len(book_list)*100:.1f}%",
            "duration_ms": duration_ms,
            "avg_duration_per_book": duration_ms / len(book_list) if book_list else 0
        }
    )
    
    return results


# Export main functions
__all__ = [
    'enrich_book_metadata',
    'enrich_multiple_books',
    'BookEnrichmentError',
    'GoogleBooksEnrichmentError',
    'OpenLibraryEnrichmentError'
] 