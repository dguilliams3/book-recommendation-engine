"""
Test recommendation deduplication filtering.

This module tests the enhanced filtering logic that prevents recommending
books that users have already uploaded as their reading history.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from typing import List, Dict, Any

from src.recommendation_api.service import (
    _filter_out_user_books,
    _normalize_text,
    _calculate_title_similarity,
    _calculate_author_similarity,
)


class TestTextNormalization:
    """Test text normalization functions."""
    
    def test_normalize_text_basic(self):
        """Test basic text normalization."""
        assert _normalize_text("Hello World!") == "hello world"
        assert _normalize_text("  Test  Book  ") == "test book"
        assert _normalize_text("") == ""
        assert _normalize_text(None) == ""
    
    def test_normalize_text_punctuation(self):
        """Test punctuation removal."""
        assert _normalize_text("The Cat's Meow!") == "the cats meow"
        assert _normalize_text("Book: A Novel") == "book a novel"
        assert _normalize_text("Author-Name") == "author name"


class TestSimilarityCalculations:
    """Test similarity calculation functions."""
    
    def test_title_similarity_exact_match(self):
        """Test exact title matches."""
        assert _calculate_title_similarity("The Hobbit", "The Hobbit") == 1.0
        assert _calculate_title_similarity("The Hobbit", "the hobbit") == 1.0
        assert _calculate_title_similarity("The Hobbit!", "The Hobbit") == 1.0
    
    def test_title_similarity_containment(self):
        """Test title containment."""
        assert _calculate_title_similarity("The Hobbit", "The Hobbit: An Unexpected Journey") == 0.9
        assert _calculate_title_similarity("Hobbit", "The Hobbit") == 0.9
    
    def test_title_similarity_word_overlap(self):
        """Test word overlap similarity."""
        # "The Hobbit" vs "The Lord of the Rings" - only "The" in common
        similarity = _calculate_title_similarity("The Hobbit", "The Lord of the Rings")
        assert 0.1 < similarity < 0.3  # Should be low but not zero
        
        # "Harry Potter" vs "Harry Potter and the Sorcerer's Stone"
        similarity = _calculate_title_similarity("Harry Potter", "Harry Potter and the Sorcerer's Stone")
        assert similarity > 0.5  # Should be high due to "Harry Potter" overlap
    
    def test_author_similarity_exact_match(self):
        """Test exact author matches."""
        assert _calculate_author_similarity("J.R.R. Tolkien", "J.R.R. Tolkien") == 1.0
        assert _calculate_author_similarity("J.R.R. Tolkien", "j.r.r. tolkien") == 1.0
    
    def test_author_similarity_containment(self):
        """Test author name containment."""
        assert _calculate_author_similarity("Tolkien", "J.R.R. Tolkien") == 0.8
        assert _calculate_author_similarity("J.K. Rowling", "Rowling") == 0.8
    
    def test_author_similarity_word_overlap(self):
        """Test author name word overlap."""
        # "John Smith" vs "Jane Smith" - "Smith" in common
        similarity = _calculate_author_similarity("John Smith", "Jane Smith")
        assert similarity > 0.3  # Should be moderate due to "Smith" overlap


class TestBookFiltering:
    """Test the main filtering function."""
    
    @pytest.fixture
    def sample_uploaded_books(self) -> List[Dict[str, Any]]:
        """Sample uploaded books for testing."""
        return [
            {
                "book_id": "B001",
                "title": "The Hobbit",
                "author": "J.R.R. Tolkien",
                "isbn": "9780547928242",
                "genre": "Fantasy",
                "reading_level": 6.0,
                "rating": 5
            },
            {
                "book_id": "B002", 
                "title": "Harry Potter and the Sorcerer's Stone",
                "author": "J.K. Rowling",
                "isbn": "9780590353427",
                "genre": "Fantasy",
                "reading_level": 5.5,
                "rating": 4
            },
            {
                "title": "The Cat in the Hat",
                "author": "Dr. Seuss",
                "genre": "Children's",
                "reading_level": 2.0,
                "rating": 3
            }
        ]
    
    @pytest.fixture
    def sample_candidates(self) -> List[Dict[str, Any]]:
        """Sample candidate books for testing."""
        return [
            {
                "book_id": "C001",
                "title": "The Lord of the Rings",
                "author": "J.R.R. Tolkien",
                "isbn": "9780547928211",
                "genre": "Fantasy",
                "reading_level": 8.0
            },
            {
                "book_id": "C002",
                "title": "The Hobbit",  # Exact match with uploaded book
                "author": "J.R.R. Tolkien",
                "isbn": "9780547928242",  # Same ISBN as uploaded book
                "genre": "Fantasy",
                "reading_level": 6.0
            },
            {
                "book_id": "C003",
                "title": "Harry Potter and the Chamber of Secrets",
                "author": "J.K. Rowling",
                "isbn": "9780439064873",
                "genre": "Fantasy",
                "reading_level": 5.5
            },
            {
                "book_id": "C004",
                "title": "The Cat in the Hat Comes Back",  # Similar title
                "author": "Dr. Seuss",
                "isbn": "9780394800028",
                "genre": "Children's",
                "reading_level": 2.0
            },
            {
                "book_id": "C005",
                "title": "The Great Gatsby",
                "author": "F. Scott Fitzgerald",
                "isbn": "9780743273565",
                "genre": "Classic",
                "reading_level": 7.0
            }
        ]
    
    @pytest.mark.asyncio
    async def test_filter_exact_book_id_match(self, sample_uploaded_books, sample_candidates):
        """Test filtering by exact book_id match."""
        # Add a candidate with matching book_id
        candidates_with_match = sample_candidates + [
            {
                "book_id": "B001",  # Matches uploaded book
                "title": "Different Title",
                "author": "Different Author",
                "isbn": "1234567890123",
                "genre": "Different",
                "reading_level": 5.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates_with_match, sample_uploaded_books, "test_user"
        )
        
        # Should filter out the book with matching book_id
        assert len(filtered) == len(candidates_with_match) - 1
        assert not any(c["book_id"] == "B001" for c in filtered)
    
    @pytest.mark.asyncio
    async def test_filter_exact_isbn_match(self, sample_uploaded_books, sample_candidates):
        """Test filtering by exact ISBN match."""
        # Add a candidate with matching ISBN but different book_id
        candidates_with_isbn_match = sample_candidates + [
            {
                "book_id": "C999",
                "title": "Different Title",
                "author": "Different Author", 
                "isbn": "9780547928242",  # Matches uploaded book ISBN
                "genre": "Different",
                "reading_level": 5.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates_with_isbn_match, sample_uploaded_books, "test_user"
        )
        
        # Should filter out the book with matching ISBN
        assert len(filtered) == len(candidates_with_isbn_match) - 1
        assert not any(c["isbn"] == "9780547928242" for c in filtered)
    
    @pytest.mark.asyncio
    async def test_filter_exact_title_author_match(self, sample_uploaded_books, sample_candidates):
        """Test filtering by exact title/author match."""
        # Add a candidate with matching title/author but different book_id and ISBN
        candidates_with_title_match = sample_candidates + [
            {
                "book_id": "C999",
                "title": "The Cat in the Hat",  # Matches uploaded book title
                "author": "Dr. Seuss",  # Matches uploaded book author
                "isbn": "1234567890123",  # Different ISBN
                "genre": "Children's",
                "reading_level": 2.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates_with_title_match, sample_uploaded_books, "test_user"
        )
        
        # Should filter out the book with matching title/author
        assert len(filtered) == len(candidates_with_title_match) - 1
        assert not any(
            c["title"] == "The Cat in the Hat" and c["author"] == "Dr. Seuss" 
            for c in filtered
        )
    
    @pytest.mark.asyncio
    async def test_filter_fuzzy_title_match(self, sample_uploaded_books, sample_candidates):
        """Test filtering by fuzzy title similarity."""
        # Add a candidate with very similar title
        candidates_with_fuzzy_match = sample_candidates + [
            {
                "book_id": "C999",
                "title": "The Cat in the Hat Comes Back",  # Very similar to uploaded book
                "author": "Dr. Seuss",
                "isbn": "1234567890123",
                "genre": "Children's",
                "reading_level": 2.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates_with_fuzzy_match, sample_uploaded_books, "test_user"
        )
        
        # Should filter out the book with similar title
        assert len(filtered) == len(candidates_with_fuzzy_match) - 1
        assert not any(
            c["title"] == "The Cat in the Hat Comes Back" 
            for c in filtered
        )
    
    @pytest.mark.asyncio
    async def test_filter_fuzzy_author_match(self, sample_uploaded_books, sample_candidates):
        """Test filtering by fuzzy author similarity."""
        # Add a candidate with very similar author name
        candidates_with_author_match = sample_candidates + [
            {
                "book_id": "C999",
                "title": "Different Book",
                "author": "J.R.R. Tolkien",  # Exact match with uploaded book author
                "isbn": "1234567890123",
                "genre": "Fantasy",
                "reading_level": 6.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates_with_author_match, sample_uploaded_books, "test_user"
        )
        
        # Should filter out the book with matching author
        assert len(filtered) == len(candidates_with_author_match) - 1
        assert not any(
            c["author"] == "J.R.R. Tolkien" 
            for c in filtered
        )
    
    @pytest.mark.asyncio
    async def test_filter_no_matches(self, sample_uploaded_books, sample_candidates):
        """Test that books with no matches are not filtered."""
        # All candidates should pass through
        filtered = await _filter_out_user_books(
            sample_candidates, sample_uploaded_books, "test_user"
        )
        
        # Should return all candidates since none match uploaded books
        assert len(filtered) == len(sample_candidates)
        assert all(c in filtered for c in sample_candidates)
    
    @pytest.mark.asyncio
    async def test_filter_empty_inputs(self):
        """Test filtering with empty inputs."""
        # Empty candidates
        filtered = await _filter_out_user_books([], [], "test_user")
        assert filtered == []
        
        # Empty uploaded books
        candidates = [{"book_id": "C001", "title": "Test", "author": "Author"}]
        filtered = await _filter_out_user_books(candidates, [], "test_user")
        assert filtered == candidates
        
        # Empty candidates, non-empty uploaded books
        uploaded_books = [{"book_id": "B001", "title": "Test", "author": "Author"}]
        filtered = await _filter_out_user_books([], uploaded_books, "test_user")
        assert filtered == []
    
    @pytest.mark.asyncio
    async def test_filter_case_insensitive(self, sample_uploaded_books):
        """Test that filtering is case insensitive."""
        candidates = [
            {
                "book_id": "C001",
                "title": "THE HOBBIT",  # Different case
                "author": "j.r.r. tolkien",  # Different case
                "isbn": "1234567890123",
                "genre": "Fantasy",
                "reading_level": 6.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates, sample_uploaded_books, "test_user"
        )
        
        # Should filter out due to case-insensitive match
        assert len(filtered) == 0
    
    @pytest.mark.asyncio
    async def test_filter_punctuation_insensitive(self, sample_uploaded_books):
        """Test that filtering handles punctuation differences."""
        candidates = [
            {
                "book_id": "C001",
                "title": "The Hobbit!",  # With punctuation
                "author": "J.R.R. Tolkien",  # With punctuation
                "isbn": "1234567890123",
                "genre": "Fantasy",
                "reading_level": 6.0
            }
        ]
        
        filtered = await _filter_out_user_books(
            candidates, sample_uploaded_books, "test_user"
        )
        
        # Should filter out due to punctuation-insensitive match
        assert len(filtered) == 0


class TestFilteringPerformance:
    """Test filtering performance characteristics."""
    
    @pytest.mark.asyncio
    async def test_filter_large_dataset(self):
        """Test filtering with large datasets."""
        # Create large uploaded books list
        uploaded_books = [
            {
                "book_id": f"B{i:03d}",
                "title": f"Book {i}",
                "author": f"Author {i}",
                "isbn": f"978{i:012d}",
                "genre": "Fantasy",
                "reading_level": 5.0,
                "rating": 4
            }
            for i in range(100)
        ]
        
        # Create large candidates list
        candidates = [
            {
                "book_id": f"C{i:03d}",
                "title": f"Candidate Book {i}",
                "author": f"Candidate Author {i}",
                "isbn": f"978{i+1000:012d}",
                "genre": "Fantasy",
                "reading_level": 5.0
            }
            for i in range(200)
        ]
        
        # Add some matches
        candidates.extend([
            {
                "book_id": "C999",
                "title": "Book 50",  # Matches uploaded book
                "author": "Author 50",
                "isbn": "978000000000050",
                "genre": "Fantasy",
                "reading_level": 5.0
            }
        ])
        
        import time
        start_time = time.time()
        
        filtered = await _filter_out_user_books(
            candidates, uploaded_books, "test_user"
        )
        
        duration = time.time() - start_time
        
        # Should complete in reasonable time (< 1 second)
        assert duration < 1.0
        
        # Should filter out the matching book
        assert len(filtered) == len(candidates) - 1
        assert not any(c["title"] == "Book 50" for c in filtered)


if __name__ == "__main__":
    pytest.main([__file__]) 