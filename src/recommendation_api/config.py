"""Configuration settings for Reader Mode recommendations."""

from dataclasses import dataclass
from typing import Dict, Any
import os


@dataclass
class ReaderModeConfig:
    """Configuration for Reader Mode recommendation scoring."""
    
    # Similarity scoring weights
    GENRE_MATCH_WEIGHT: float = 2.0
    AUTHOR_MATCH_WEIGHT: float = 3.0
    READING_LEVEL_WEIGHT_HIGH: float = 1.5  # Within 1 level
    READING_LEVEL_WEIGHT_MEDIUM: float = 1.0  # Within 2 levels
    READING_LEVEL_WEIGHT_LOW: float = 0.5  # Within 3 levels
    FEEDBACK_WEIGHT: float = 0.5
    RATING_BOOST_WEIGHT: float = 1.0
    QUERY_MATCH_WEIGHT: float = 2.0
    FALLBACK_SCORE: float = 0.5
    
    # Query limits
    MAX_UPLOADED_BOOKS: int = 20
    MAX_SIMILAR_BOOKS: int = 100
    MIN_RATING_THRESHOLD: float = 3.0
    FALLBACK_RATING_THRESHOLD: float = 3.5
    
    # Feature flags
    ENABLE_BOOK_ENRICHMENT: bool = True
    ENABLE_FEEDBACK_SCORING: bool = True
    ENABLE_QUERY_MATCHING: bool = True
    
    @classmethod
    def from_env(cls) -> "ReaderModeConfig":
        """Create configuration from environment variables."""
        return cls(
            GENRE_MATCH_WEIGHT=float(os.getenv("READER_GENRE_MATCH_WEIGHT", "2.0")),
            AUTHOR_MATCH_WEIGHT=float(os.getenv("READER_AUTHOR_MATCH_WEIGHT", "3.0")),
            READING_LEVEL_WEIGHT_HIGH=float(os.getenv("READER_READING_LEVEL_WEIGHT_HIGH", "1.5")),
            READING_LEVEL_WEIGHT_MEDIUM=float(os.getenv("READER_READING_LEVEL_WEIGHT_MEDIUM", "1.0")),
            READING_LEVEL_WEIGHT_LOW=float(os.getenv("READER_READING_LEVEL_WEIGHT_LOW", "0.5")),
            FEEDBACK_WEIGHT=float(os.getenv("READER_FEEDBACK_WEIGHT", "0.5")),
            RATING_BOOST_WEIGHT=float(os.getenv("READER_RATING_BOOST_WEIGHT", "1.0")),
            QUERY_MATCH_WEIGHT=float(os.getenv("READER_QUERY_MATCH_WEIGHT", "2.0")),
            FALLBACK_SCORE=float(os.getenv("READER_FALLBACK_SCORE", "0.5")),
            MAX_UPLOADED_BOOKS=int(os.getenv("READER_MAX_UPLOADED_BOOKS", "20")),
            MAX_SIMILAR_BOOKS=int(os.getenv("READER_MAX_SIMILAR_BOOKS", "100")),
            MIN_RATING_THRESHOLD=float(os.getenv("READER_MIN_RATING_THRESHOLD", "3.0")),
            FALLBACK_RATING_THRESHOLD=float(os.getenv("READER_FALLBACK_RATING_THRESHOLD", "3.5")),
            ENABLE_BOOK_ENRICHMENT=os.getenv("READER_ENABLE_BOOK_ENRICHMENT", "true").lower() == "true",
            ENABLE_FEEDBACK_SCORING=os.getenv("READER_ENABLE_FEEDBACK_SCORING", "true").lower() == "true",
            ENABLE_QUERY_MATCHING=os.getenv("READER_ENABLE_QUERY_MATCHING", "true").lower() == "true",
        )


# Global configuration instance
reader_config = ReaderModeConfig.from_env() 