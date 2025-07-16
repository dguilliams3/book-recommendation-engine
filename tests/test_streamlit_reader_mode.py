"""
Test suite for Streamlit Reader Mode functionality.
Tests the new Reader Mode functions added to streamlit_ui/app.py
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import requests
import json

# Import functions from streamlit_ui
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from streamlit_ui.app import (
    hash_user_identifier,
    upload_books_to_reader_api,
    get_reader_recommendations,
    get_user_books,
    submit_feedback,
    parse_csv_books
)


class TestReaderModeFunctions:
    """Test the Reader Mode utility functions"""
    
    def test_hash_user_identifier(self):
        """Test user identifier hashing"""
        identifier = "test@example.com"
        hash1 = hash_user_identifier(identifier)
        hash2 = hash_user_identifier(identifier)
        
        # Same input should produce same hash
        assert hash1 == hash2
        
        # Different input should produce different hash
        hash3 = hash_user_identifier("different@example.com")
        assert hash1 != hash3
        
        # Hash should be proper length for SHA256
        assert len(hash1) == 64
        assert isinstance(hash1, str)
    
    def test_parse_csv_books_valid(self):
        """Test CSV parsing with valid data"""
        csv_content = """title,author,rating,notes
The Great Gatsby,F. Scott Fitzgerald,5,Classic novel
To Kill a Mockingbird,Harper Lee,4,Powerful story
1984,George Orwell,,Dystopian fiction"""
        
        books = parse_csv_books(csv_content)
        
        assert len(books) == 3
        assert books[0]["title"] == "The Great Gatsby"
        assert books[0]["author"] == "F. Scott Fitzgerald"
        assert books[0]["rating"] == 5
        assert books[0]["notes"] == "Classic novel"
        
        assert books[1]["rating"] == 4
        assert books[2]["rating"] is None  # Empty rating should be None
        assert books[2]["notes"] == "Dystopian fiction"
    
    def test_parse_csv_books_title_only(self):
        """Test CSV parsing with only title column"""
        csv_content = """title
The Great Gatsby
To Kill a Mockingbird"""
        
        books = parse_csv_books(csv_content)
        
        assert len(books) == 2
        assert books[0]["title"] == "The Great Gatsby"
        assert books[0]["author"] is None
        assert books[0]["rating"] is None
        assert books[0]["notes"] is None
    
    def test_parse_csv_books_invalid_cases(self):
        """Test CSV parsing error cases"""
        
        # Missing title column
        with pytest.raises(ValueError, match="CSV must have a 'title' column"):
            parse_csv_books("author,rating\nSome Author,5")
        
        # Empty CSV
        with pytest.raises(ValueError, match="No books found in CSV"):
            parse_csv_books("title\n")
        
        # Too many books (simulate)
        too_many_books = "title\n" + "\n".join([f"Book {i}" for i in range(101)])
        with pytest.raises(ValueError, match="Too many books"):
            parse_csv_books(too_many_books)
        
        # File too large (simulate)
        large_content = "title\n" + "x" * (1024 * 1024 + 1)
        with pytest.raises(ValueError, match="File too large"):
            parse_csv_books(large_content)
    
    def test_parse_csv_books_rating_validation(self):
        """Test rating parsing and validation"""
        csv_content = """title,rating
Book1,5
Book2,3.5
Book3,invalid
Book4,0
Book5,6
Book6,"""
        
        books = parse_csv_books(csv_content)
        
        assert books[0]["rating"] == 5  # Valid rating
        assert books[1]["rating"] == 3  # Float converted to int
        assert books[2]["rating"] is None  # Invalid rating
        assert books[3]["rating"] is None  # Out of range (too low)
        assert books[4]["rating"] is None  # Out of range (too high)
        assert books[5]["rating"] is None  # Empty rating


class TestReaderModeAPIFunctions:
    """Test the Reader Mode API integration functions"""
    
    @patch('streamlit_ui.app.requests.post')
    def test_upload_books_success(self, mock_post):
        """Test successful book upload"""
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "message": "Books uploaded successfully",
            "books_processed": 2,
            "user_hash_id": "test_hash"
        }
        mock_post.return_value = mock_response
        
        books = [
            {"title": "Test Book", "author": "Test Author", "rating": 5, "notes": "Great book"}
        ]
        
        result = upload_books_to_reader_api("test@example.com", books)
        
        assert "error" not in result
        assert result["message"] == "Books uploaded successfully"
        assert result["books_processed"] == 2
        
        # Verify the request was made correctly
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["user_identifier"] == "test@example.com"
        assert call_args[1]["json"]["books"] == books
    
    @patch('streamlit_ui.app.requests.post')
    def test_upload_books_connection_error(self, mock_post):
        """Test upload with connection error"""
        mock_post.side_effect = requests.exceptions.ConnectionError()
        
        result = upload_books_to_reader_api("test@example.com", [])
        
        assert "error" in result
        assert "not available" in result["error"]
    
    @patch('streamlit_ui.app.requests.post')
    def test_upload_books_timeout(self, mock_post):
        """Test upload with timeout"""
        mock_post.side_effect = requests.exceptions.Timeout()
        
        result = upload_books_to_reader_api("test@example.com", [])
        
        assert "error" in result
        assert "timed out" in result["error"]
    
    @patch('streamlit_ui.app.requests.get')
    def test_get_reader_recommendations_success(self, mock_get):
        """Test successful recommendation retrieval"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "recommendations": [
                {
                    "book_id": "b123",
                    "title": "Recommended Book",
                    "author": "Rec Author",
                    "reading_level": 3.5,
                    "librarian_blurb": "Great read!",
                    "justification": "Based on your reading history"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        result = get_reader_recommendations("test_hash_id", "adventure", 3)
        
        assert "error" not in result
        assert len(result["recommendations"]) == 1
        assert result["recommendations"][0]["title"] == "Recommended Book"
        
        # Verify the request
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "recommendations/test_hash_id" in str(call_args)
        assert call_args[1]["params"]["query"] == "adventure"
        assert call_args[1]["params"]["n"] == 3
    
    @patch('streamlit_ui.app.requests.get')
    def test_get_user_books_success(self, mock_get):
        """Test successful user books retrieval"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "books": [
                {
                    "title": "My Book",
                    "author": "Me",
                    "rating": 4,
                    "upload_date": "2025-01-01"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        result = get_user_books("test_hash_id")
        
        assert "error" not in result
        assert len(result["books"]) == 1
        assert result["books"][0]["title"] == "My Book"
    
    @patch('streamlit_ui.app.requests.post')
    def test_submit_feedback_success(self, mock_post):
        """Test successful feedback submission"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"message": "Feedback recorded"}
        mock_post.return_value = mock_response
        
        result = submit_feedback("test_hash_id", "book_123", 1)
        
        assert "error" not in result
        assert result["message"] == "Feedback recorded"
        
        # Verify the request
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["user_hash_id"] == "test_hash_id"
        assert call_args[1]["json"]["book_id"] == "book_123"
        assert call_args[1]["json"]["score"] == 1
    
    @patch('streamlit_ui.app.requests.post')
    def test_submit_feedback_negative_score(self, mock_post):
        """Test feedback submission with negative score (thumbs down)"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"message": "Feedback recorded"}
        mock_post.return_value = mock_response
        
        result = submit_feedback("test_hash_id", "book_123", -1)
        
        assert "error" not in result
        call_args = mock_post.call_args
        assert call_args[1]["json"]["score"] == -1


class TestCSVParsing:
    """Test CSV parsing edge cases and validation"""
    
    def test_parse_csv_empty_titles_filtered(self):
        """Test that empty titles are filtered out"""
        csv_content = """title,author
Valid Book,Valid Author
,Empty Title Author
   ,Whitespace Only
Another Book,Another Author"""
        
        books = parse_csv_books(csv_content)
        
        assert len(books) == 2
        assert books[0]["title"] == "Valid Book"
        assert books[1]["title"] == "Another Book"
    
    def test_parse_csv_whitespace_trimming(self):
        """Test that whitespace is properly trimmed"""
        csv_content = """title,author,notes
  Spaced Book  ,  Spaced Author  ,  Spaced Notes  """
        
        books = parse_csv_books(csv_content)
        
        assert len(books) == 1
        assert books[0]["title"] == "Spaced Book"
        assert books[0]["author"] == "Spaced Author"
        assert books[0]["notes"] == "Spaced Notes"
    
    def test_parse_csv_mixed_case_columns(self):
        """Test CSV with different column names and case"""
        csv_content = """Title,Author,Rating,Notes
Test Book,Test Author,3,Test notes"""
        
        # This should work since we check for 'title' (lowercase)
        # but the CSV has 'Title' - this tests case sensitivity
        with pytest.raises(ValueError, match="CSV must have a 'title' column"):
            parse_csv_books(csv_content)


if __name__ == "__main__":
    pytest.main([__file__]) 