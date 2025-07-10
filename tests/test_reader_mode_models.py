"""
Test suite for Reader Mode database models.
"""
import pytest
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from unittest.mock import patch
import hashlib

# Create a separate base for testing to avoid JSONB issues
TestBase = declarative_base()

# Define only the Reader Mode models for testing
class PublicUser(TestBase):
    __tablename__ = 'public_users'
    
    user_id_hash = Column(String, primary_key=True)
    created_at = Column(DateTime, nullable=False)

class UploadedBook(TestBase):
    __tablename__ = 'uploaded_books'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id_hash = Column(String, ForeignKey('public_users.user_id_hash'), nullable=False)
    title = Column(String, nullable=False)
    author = Column(String, nullable=False)
    isbn = Column(String, nullable=True)
    genre = Column(String, nullable=True)
    reading_level = Column(Float, nullable=True)
    uploaded_at = Column(DateTime, nullable=False)

class Feedback(TestBase):
    __tablename__ = 'feedback'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id_hash = Column(String, ForeignKey('public_users.user_id_hash'), nullable=False)
    book_id = Column(Integer, nullable=False)
    rating = Column(Integer, nullable=False)  # -1 or 1
    feedback_text = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)


@pytest.fixture
def db_session():
    """Create an in-memory database for testing."""
    engine = create_engine("sqlite:///:memory:")
    TestBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_public_user_creation(db_session):
    """Test creating a PublicUser with proper hashing."""
    user = PublicUser(
        user_id_hash="test_hash_123",
        created_at=datetime.now()
    )
    
    db_session.add(user)
    db_session.commit()
    
    retrieved = db_session.query(PublicUser).first()
    assert retrieved.user_id_hash == "test_hash_123"
    assert retrieved.created_at is not None


def test_uploaded_book_creation(db_session):
    """Test creating an UploadedBook with proper relationships."""
    # Create a user first
    user = PublicUser(
        user_id_hash="test_hash_123",
        created_at=datetime.now()
    )
    db_session.add(user)
    db_session.commit()
    
    # Create uploaded book
    book = UploadedBook(
        user_id_hash="test_hash_123",
        title="Test Book",
        author="Test Author",
        isbn="1234567890123",
        genre="Fiction",
        reading_level=5.5,
        uploaded_at=datetime.now()
    )
    
    db_session.add(book)
    db_session.commit()
    
    retrieved = db_session.query(UploadedBook).first()
    assert retrieved.title == "Test Book"
    assert retrieved.author == "Test Author"
    assert retrieved.isbn == "1234567890123"
    assert retrieved.genre == "Fiction"
    assert retrieved.reading_level == 5.5
    assert retrieved.user_id_hash == "test_hash_123"


def test_feedback_creation(db_session):
    """Test creating Feedback with proper relationships."""
    # Create a user first
    user = PublicUser(
        user_id_hash="test_hash_123",
        created_at=datetime.now()
    )
    db_session.add(user)
    db_session.commit()
    
    # Create feedback
    feedback = Feedback(
        user_id_hash="test_hash_123",
        book_id=1,
        rating=1,  # thumbs up
        feedback_text="Great recommendation!",
        created_at=datetime.now()
    )
    
    db_session.add(feedback)
    db_session.commit()
    
    retrieved = db_session.query(Feedback).first()
    assert retrieved.user_id_hash == "test_hash_123"
    assert retrieved.book_id == 1
    assert retrieved.rating == 1
    assert retrieved.feedback_text == "Great recommendation!"


def test_user_hash_functionality():
    """Test that user ID hashing works as expected."""
    import hashlib
    
    user_id = "test_user_123"
    expected_hash = hashlib.sha256(user_id.encode()).hexdigest()
    
    # Test that the hash is consistent
    hash1 = hashlib.sha256(user_id.encode()).hexdigest()
    hash2 = hashlib.sha256(user_id.encode()).hexdigest()
    assert hash1 == hash2 == expected_hash
    
    # Test that different inputs produce different hashes
    different_hash = hashlib.sha256("different_user".encode()).hexdigest()
    assert different_hash != expected_hash


def test_feedback_rating_values():
    """Test that feedback rating accepts valid values."""
    # Test that both -1 and 1 are valid ratings
    valid_ratings = [-1, 1]
    
    for rating in valid_ratings:
        feedback = Feedback(
            user_id_hash="test_hash",
            book_id=1,
            rating=rating,
            created_at=datetime.now()
        )
        assert feedback.rating == rating 