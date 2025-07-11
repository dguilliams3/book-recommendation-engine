"""
Comprehensive tests for the book enrichment system.

Tests cover:
- Individual API integrations (Google Books, Open Library)
- Orchestration service with fallback strategies
- Error handling and edge cases
- Caching behavior
- Parallel vs sequential fetching
- Metadata merging logic
- Batch processing
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.recommendation_api.tools import (
    enrich_book_metadata,
    enrich_multiple_books,
    BookEnrichmentError
)
from src.recommendation_api.tools.fetch_google_books_meta import (
    fetch_google_books_meta,
    GoogleBooksEnrichmentError
)
from src.recommendation_api.tools.fetch_open_library_meta import (
    fetch_open_library_meta,
    OpenLibraryEnrichmentError
)


class TestGoogleBooksAPI:
    """Test Google Books API integration."""
    
    @pytest.fixture
    def mock_google_response(self):
        """Mock successful Google Books API response."""
        return {
            "totalItems": 1,
            "items": [{
                "volumeInfo": {
                    "title": "Effective Java",
                    "authors": ["Joshua Bloch"],
                    "pageCount": 416,
                    "publishedDate": "2017-12-27",
                    "description": "The definitive guide to Java programming language best practices.",
                    "categories": ["Computers"],
                    "averageRating": 4.5,
                    "ratingsCount": 150,
                    "publisher": "Addison-Wesley Professional",
                    "language": "en",
                    "industryIdentifiers": [{"type": "ISBN_13", "identifier": "9780134685991"}],
                    "imageLinks": {"thumbnail": "http://books.google.com/books/content?id=test"}
                }
            }]
        }
    
    @pytest.mark.asyncio
    async def test_fetch_google_books_success(self, mock_google_response):
        """Test successful Google Books API fetch."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = mock_google_response
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with patch('src.recommendation_api.tools.fetch_google_books_meta.get_redis_client') as mock_redis:
                mock_redis.return_value.get.return_value = None
                mock_redis.return_value.setex = AsyncMock()
                
                result = await fetch_google_books_meta("9780134685991")
                
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["authors"] == ["Joshua Bloch"]
                assert result["publication_year"] == "2017"
                assert result["source"] == "google_books"
                assert "fetched_at" in result
    
    @pytest.mark.asyncio
    async def test_fetch_google_books_not_found(self):
        """Test Google Books API when book not found."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"totalItems": 0, "items": []}
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with patch('src.recommendation_api.tools.fetch_google_books_meta.get_redis_client') as mock_redis:
                mock_redis.return_value.get.return_value = None
                
                result = await fetch_google_books_meta("9999999999999")
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_fetch_google_books_rate_limited(self):
        """Test Google Books API rate limiting with retry."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            # First call: rate limited, second call: success
            mock_response_429 = AsyncMock()
            mock_response_429.status = 429
            
            mock_response_200 = AsyncMock()
            mock_response_200.status = 200
            mock_response_200.json.return_value = {
                "totalItems": 1,
                "items": [{"volumeInfo": {"title": "Test Book"}}]
            }
            
            mock_get.return_value.__aenter__.side_effect = [
                mock_response_429,
                mock_response_200
            ]
            
            with patch('src.recommendation_api.tools.fetch_google_books_meta.get_redis_client') as mock_redis:
                mock_redis.return_value.get.return_value = None
                mock_redis.return_value.setex = AsyncMock()
                
                with patch('asyncio.sleep', new_callable=AsyncMock):
                    result = await fetch_google_books_meta("9780134685991")
                    
                    assert result is not None
                    assert result["title"] == "Test Book"
    
    @pytest.mark.asyncio
    async def test_fetch_google_books_invalid_isbn(self):
        """Test Google Books API with invalid ISBN."""
        with pytest.raises(GoogleBooksEnrichmentError, match="Invalid ISBN provided"):
            await fetch_google_books_meta("")
        
        with pytest.raises(GoogleBooksEnrichmentError, match="Invalid ISBN provided"):
            await fetch_google_books_meta(None)


class TestOpenLibraryAPI:
    """Test Open Library API integration."""
    
    @pytest.fixture
    def mock_open_library_response(self):
        """Mock successful Open Library API response."""
        return {
            "title": "Effective Java",
            "authors": [{"name": "Joshua Bloch"}],
            "number_of_pages": 416,
            "publish_date": "December 27, 2017",
            "description": {"value": "The definitive guide to Java programming language best practices."},
            "subjects": ["Java (Computer program language)", "Programming"],
            "publishers": ["Addison-Wesley Professional"],
            "languages": ["eng"],
            "isbn_10": ["0134685997"],
            "lc_classifications": ["QA76.73.J38"],
            "dewey_decimal_class": ["005.133"]
        }
    
    @pytest.mark.asyncio
    async def test_fetch_open_library_success(self, mock_open_library_response):
        """Test successful Open Library API fetch."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = mock_open_library_response
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with patch('src.recommendation_api.tools.fetch_open_library_meta.get_redis_client') as mock_redis:
                mock_redis.return_value.get.return_value = None
                mock_redis.return_value.setex = AsyncMock()
                
                result = await fetch_open_library_meta("9780134685991")
                
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["authors"] == ["Joshua Bloch"]
                assert result["publication_year"] == "2017"
                assert result["source"] == "open_library"
                assert "fetched_at" in result
    
    @pytest.mark.asyncio
    async def test_fetch_open_library_not_found(self):
        """Test Open Library API when book not found."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with patch('src.recommendation_api.tools.fetch_open_library_meta.get_redis_client') as mock_redis:
                mock_redis.return_value.get.return_value = None
                
                result = await fetch_open_library_meta("9999999999999")
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_fetch_open_library_invalid_isbn(self):
        """Test Open Library API with invalid ISBN."""
        with pytest.raises(OpenLibraryEnrichmentError, match="Invalid ISBN provided"):
            await fetch_open_library_meta("")


class TestBookEnrichmentOrchestration:
    """Test the main book enrichment orchestration service."""
    
    @pytest.fixture
    def mock_google_data(self):
        """Mock Google Books data."""
        return {
            "title": "Effective Java",
            "authors": ["Joshua Bloch"],
            "description": "The definitive guide to Java programming.",
            "categories": ["Computers"],
            "average_rating": 4.5,
            "source": "google_books"
        }
    
    @pytest.fixture
    def mock_open_library_data(self):
        """Mock Open Library data."""
        return {
            "title": "Effective Java",
            "authors": ["Joshua Bloch"],
            "subjects": ["Java (Computer program language)", "Programming"],
            "lc_classifications": ["QA76.73.J38"],
            "dewey_decimal_class": ["005.133"],
            "source": "open_library"
        }
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_success_parallel(self, mock_google_data, mock_open_library_data):
        """Test successful book enrichment with parallel fetching."""
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = mock_google_data
                mock_open_library.return_value = mock_open_library_data
                
                result = await enrich_book_metadata("9780134685991", parallel_fetch=True)
                
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["authors"] == ["Joshua Bloch"]
                assert "enrichment_sources" in result
                assert "google_books" in result["enrichment_sources"]
                assert "open_library" in result["enrichment_sources"]
                assert "enriched_at" in result
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_success_sequential(self, mock_google_data):
        """Test successful book enrichment with sequential fetching."""
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = mock_google_data
                mock_open_library.return_value = None
                
                result = await enrich_book_metadata("9780134685991", parallel_fetch=False)
                
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["enrichment_sources"] == ["google_books"]
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_fallback_to_open_library(self, mock_open_library_data):
        """Test fallback to Open Library when Google Books fails."""
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.side_effect = GoogleBooksEnrichmentError("API Error")
                mock_open_library.return_value = mock_open_library_data
                
                result = await enrich_book_metadata("9780134685991", parallel_fetch=False)
                
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["enrichment_sources"] == ["open_library"]
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_no_data_found(self):
        """Test when no enrichment data is found."""
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = None
                mock_open_library.return_value = None
                
                result = await enrich_book_metadata("9999999999999")
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_invalid_isbn(self):
        """Test with invalid ISBN."""
        with pytest.raises(BookEnrichmentError, match="Invalid ISBN format"):
            await enrich_book_metadata("invalid-isbn")
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata_title_validation(self, mock_google_data):
        """Test title validation against provided hint."""
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = mock_google_data
                mock_open_library.return_value = None
                
                # Should succeed with matching title
                result = await enrich_book_metadata(
                    "9780134685991",
                    title="Effective Java",
                    author="Joshua Bloch"
                )
                
                assert result is not None
                assert result["title"] == "Effective Java"
    
    @pytest.mark.asyncio
    async def test_enrich_multiple_books_success(self, mock_google_data):
        """Test batch enrichment of multiple books."""
        books = [
            {"isbn": "9780134685991", "title": "Effective Java", "author": "Joshua Bloch"},
            {"isbn": "9780596009205", "title": "Head First Design Patterns", "author": "Eric Freeman"},
            {"title": "No ISBN Book"}  # Should be skipped
        ]
        
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = mock_google_data
                mock_open_library.return_value = None
                
                results = await enrich_multiple_books(books, max_concurrent=2)
                
                assert len(results) == 3
                assert "enriched_metadata" in results[0]
                assert "enriched_metadata" in results[1]
                assert "enriched_metadata" not in results[2]  # No ISBN
    
    @pytest.mark.asyncio
    async def test_enrich_multiple_books_with_errors(self):
        """Test batch enrichment with some failures."""
        books = [
            {"isbn": "9780134685991", "title": "Book 1"},
            {"isbn": "9780596009205", "title": "Book 2"}
        ]
        
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                # First book succeeds, second fails
                mock_google.side_effect = [
                    {"title": "Book 1", "source": "google_books"},
                    GoogleBooksEnrichmentError("API Error")
                ]
                mock_open_library.side_effect = [
                    None,
                    None
                ]
                
                results = await enrich_multiple_books(books)
                
                assert len(results) == 2
                assert "enriched_metadata" in results[0]
                assert "enriched_metadata" not in results[1]  # Failed
    
    def test_metadata_merging(self):
        """Test intelligent metadata merging logic."""
        from src.recommendation_api.tools import _merge_metadata
        
        google_data = {
            "title": "Effective Java",
            "authors": ["Joshua Bloch"],
            "description": "Great book",
            "categories": ["Computers"],
            "average_rating": 4.5
        }
        
        open_library_data = {
            "title": "Effective Java",
            "authors": ["Joshua Bloch"],
            "subjects": ["Java", "Programming"],
            "categories": ["Technology"],  # Will be merged
            "lc_classifications": ["QA76.73.J38"]
        }
        
        merged = _merge_metadata(google_data, open_library_data)
        
        assert merged["title"] == "Effective Java"
        assert merged["authors"] == ["Joshua Bloch"]
        assert merged["description"] == "Great book"  # From Google
        assert merged["average_rating"] == 4.5  # From Google
        assert merged["lc_classifications"] == ["QA76.73.J38"]  # From Open Library
        assert set(merged["categories"]) == {"Computers", "Technology"}  # Merged
        assert "enrichment_sources" in merged
        assert "enriched_at" in merged
    
    def test_isbn_validation(self):
        """Test ISBN validation logic."""
        from src.recommendation_api.tools import _validate_isbn
        
        # Valid ISBNs
        assert _validate_isbn("9780134685991") == True
        assert _validate_isbn("978-0-13-468599-1") == True
        assert _validate_isbn("0134685997") == True
        
        # Invalid ISBNs
        assert _validate_isbn("") == False
        assert _validate_isbn(None) == False
        assert _validate_isbn("123") == False
        assert _validate_isbn("not-a-number") == False


class TestBookEnrichmentIntegration:
    """Integration tests for the complete book enrichment system."""
    
    @pytest.mark.asyncio
    async def test_full_enrichment_pipeline(self):
        """Test the complete enrichment pipeline end-to-end."""
        # This would be a live test that could be run against real APIs
        # For now, we'll mock it but structure it like a real integration test
        
        isbn = "9780134685991"
        
        with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
            with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
                mock_google.return_value = {
                    "title": "Effective Java",
                    "authors": ["Joshua Bloch"],
                    "description": "The definitive guide to Java programming.",
                    "categories": ["Computers"],
                    "average_rating": 4.5,
                    "source": "google_books"
                }
                
                mock_open_library.return_value = {
                    "title": "Effective Java",
                    "subjects": ["Java (Computer program language)"],
                    "lc_classifications": ["QA76.73.J38"],
                    "source": "open_library"
                }
                
                # Test the complete pipeline
                result = await enrich_book_metadata(
                    isbn=isbn,
                    title="Effective Java",
                    author="Joshua Bloch",
                    parallel_fetch=True,
                    use_cache=True
                )
                
                # Verify complete enrichment
                assert result is not None
                assert result["title"] == "Effective Java"
                assert result["authors"] == ["Joshua Bloch"]
                assert result["description"] == "The definitive guide to Java programming."
                assert result["average_rating"] == 4.5
                assert "Java (Computer program language)" in result["subjects"]
                assert result["lc_classifications"] == ["QA76.73.J38"]
                assert len(result["enrichment_sources"]) == 2
                assert "google_books" in result["enrichment_sources"]
                assert "open_library" in result["enrichment_sources"]
                assert "enriched_at" in result


@pytest.mark.asyncio
async def test_book_enrichment_performance():
    """Test performance characteristics of book enrichment."""
    # This test would measure actual performance metrics
    # For now, we'll structure it as a performance test template
    
    books = [{"isbn": f"97801346859{i:02d}", "title": f"Book {i}"} for i in range(10)]
    
    with patch('src.recommendation_api.tools.fetch_google_books_meta') as mock_google:
        with patch('src.recommendation_api.tools.fetch_open_library_meta') as mock_open_library:
            mock_google.return_value = {"title": "Test Book", "source": "google_books"}
            mock_open_library.return_value = None
            
            start_time = datetime.now()
            results = await enrich_multiple_books(books, max_concurrent=5)
            duration = (datetime.now() - start_time).total_seconds()
            
            # Verify performance expectations
            assert len(results) == 10
            assert duration < 5.0  # Should complete within 5 seconds
            
            # Verify all books were processed
            enriched_count = sum(1 for book in results if 'enriched_metadata' in book)
            assert enriched_count == 10 