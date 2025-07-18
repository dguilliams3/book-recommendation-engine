"""
Streamlit UI - Teacher/Librarian Dashboard

SERVICE PURPOSE:
    Comprehensive Streamlit-based web interface for educators and librarians
    to interact with the book recommendation system. Provides both traditional
    recommendation features and new Reader Mode for user-uploaded book lists.

KEY FEATURES:
    - Student recommendation interface with reading level matching
    - Reader Mode for user-uploaded personal book collections
    - Real-time recommendation generation and feedback collection
    - CSV data upload and management for librarians
    - System health monitoring and metrics visualization
    - Database exploration and analytics tools

READER MODE CAPABILITIES:
    - User book list upload (CSV/manual entry)
    - Privacy-preserving user identification via hashing
    - Personalized recommendations from user's own collection
    - Thumbs up/down feedback for recommendation learning
    - Recommendation history and preference tracking

TRADITIONAL MODE FEATURES:
    - Student ID-based recommendations from school catalog
    - Reading level and preference filtering
    - Group recommendation generation for classrooms
    - Checkout history analysis and insights

DEPENDENCIES:
    - Recommendation API: Core recommendation engine
    - User Ingest Service: Reader Mode book uploads
    - PostgreSQL: Data queries and analytics
    - Redis: Session management and caching
    - Kafka: Real-time metrics and system monitoring

USER EXPERIENCE:
    - Responsive design optimized for tablets and desktops
    - Real-time feedback with loading indicators
    - Error handling with graceful degradation
    - Session persistence for user preferences

⚠️  REMEMBER: Update this documentation block when modifying service functionality!
"""

import asyncio
import csv
import hashlib
import io
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
import streamlit as st
from aiokafka import AIOKafkaConsumer
from sqlalchemy import create_engine, text

from common.settings import settings as S
from common.redis_utils import get_redis_client
from common.structured_logging import get_logger

# Page configuration for better performance
st.set_page_config(
    page_title="Let's Find You a Book!",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="collapsed",
)

logger = get_logger(__name__)

# UI Configuration
API_TIMEOUT = S.ui_api_timeout_seconds
MAX_RETRIES = S.ui_max_retries
QUICK_TIMEOUT = S.ui_quick_timeout_seconds
HEALTH_TIMEOUT = S.ui_health_timeout_seconds
KAFKA_TIMEOUT = S.ui_kafka_timeout_seconds

# ====================================================================
# READER MODE FUNCTIONS
# ====================================================================


def hash_user_identifier(identifier: str) -> str:
    """Hash user identifier for privacy (same as user_ingest_service)"""
    return hashlib.sha256(identifier.encode()).hexdigest()


def upload_books_to_reader_api(
    user_identifier: str, books: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Upload books to Reader Mode API with comprehensive logging"""
    user_hash_id = hash_user_identifier(user_identifier)
    start_time = time.time()

    logger.info(
        "Starting book upload",
        extra={
            "user_hash_id": user_hash_id[:8] + "...",  # Truncated for privacy
            "book_count": len(books),
            "operation": "upload_books",
        },
    )

    try:
        upload_url = f"http://user_ingest_service:{S.user_ingest_port}/upload_books"
        payload = {"user_identifier": user_identifier, "books": books}

        response = requests.post(upload_url, json=payload, timeout=API_TIMEOUT)
        response.raise_for_status()
        result = response.json()

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Book upload completed",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "duration_ms": duration_ms,
                "books_processed": result.get("books_processed", 0),
                "success": True,
            },
        )

        return result

    except requests.exceptions.ConnectionError:
        logger.warning(
            "Upload service unavailable",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "service": "user_ingest_service",
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {
            "error": "Reader Mode upload service not available. Please ensure user_ingest_service is running."
        }
    except requests.exceptions.Timeout:
        logger.warning(
            "Upload request timeout",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "timeout_sec": API_TIMEOUT,
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": "Upload request timed out. Please try again."}
    except ValueError as e:
        logger.info(
            "Upload validation error",
            extra={"user_hash_id": user_hash_id[:8] + "...", "error": str(e)},
        )
        return {"error": f"Invalid data: {str(e)}"}
    except Exception as e:
        logger.error(
            "Upload failed unexpectedly",
            exc_info=True,
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": f"Upload failed: {str(e)}"}


def get_reader_recommendations(
    user_hash_id: str, query: str = "", n: int = 3
) -> Dict[str, Any]:
    """Get personalized recommendations for a reader with performance tracking"""
    start_time = time.time()

    logger.info(
        "Starting recommendation request",
        extra={
            "user_hash_id": user_hash_id[:8] + "...",
            "query": query[:50] + "..." if len(query) > 50 else query,
            "recommendation_count": n,
            "operation": "get_recommendations",
        },
    )

    try:
        rec_url = f"http://recommendation_api:{S.recommendation_api_port}/recommendations/{user_hash_id}"
        params = {"query": query or "", "n": n}

        response = requests.get(rec_url, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        result = response.json()

        duration_ms = int((time.time() - start_time) * 1000)
        recommendations_count = len(result.get("recommendations", []))

        logger.info(
            "Recommendations retrieved",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "duration_ms": duration_ms,
                "recommendations_returned": recommendations_count,
                "success": True,
            },
        )

        return result

    except requests.exceptions.ConnectionError:
        logger.warning(
            "Recommendation service unavailable",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "service": "recommendation_api",
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": "Reader Mode recommendation service not available."}
    except requests.exceptions.Timeout:
        logger.warning(
            "Recommendation request timeout",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "timeout_sec": API_TIMEOUT,
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": "Recommendation request timed out. Please try again."}
    except Exception as e:
        logger.error(
            "Recommendation request failed",
            exc_info=True,
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": f"Recommendation request failed: {str(e)}"}


def get_user_books(user_hash_id: str) -> Dict[str, Any]:
    """Get user's uploaded books"""
    try:
        books_url = f"http://recommendation_api:{S.recommendation_api_port}/user/{user_hash_id}/books"
        response = requests.get(books_url, timeout=QUICK_TIMEOUT)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.ConnectionError:
        return {"error": "Could not connect to book history service."}
    except Exception as e:
        return {"error": f"Failed to fetch books: {str(e)}"}


def submit_feedback(user_hash_id: str, book_id: str, score: int) -> Dict[str, Any]:
    """Submit thumbs up/down feedback with user tracking"""
    start_time = time.time()

    logger.info(
        "Submitting user feedback",
        extra={
            "user_hash_id": user_hash_id[:8] + "...",
            "book_id": book_id,
            "score": score,
            "operation": "submit_feedback",
        },
    )

    try:
        feedback_url = f"http://recommendation_api:{S.recommendation_api_port}/feedback"
        payload = {"user_hash_id": user_hash_id, "book_id": book_id, "score": score}

        response = requests.post(feedback_url, json=payload, timeout=QUICK_TIMEOUT)
        response.raise_for_status()
        result = response.json()

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Feedback submitted successfully",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "book_id": book_id,
                "score": score,
                "duration_ms": duration_ms,
                "success": True,
            },
        )

        return result

    except requests.exceptions.ConnectionError:
        logger.warning(
            "Feedback service unavailable",
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "service": "recommendation_api",
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": "Feedback service not available."}
    except Exception as e:
        logger.error(
            "Feedback submission failed",
            exc_info=True,
            extra={
                "user_hash_id": user_hash_id[:8] + "...",
                "book_id": book_id,
                "duration_ms": int((time.time() - start_time) * 1000),
            },
        )
        return {"error": f"Feedback submission failed: {str(e)}"}


def parse_csv_books(csv_content: str) -> List[Dict[str, Any]]:
    """Parse and validate CSV content into book records.

    Args:
        csv_content: Raw CSV file content as string

    Returns:
        List of validated book dictionaries with fields:
        - title (required): Book title, trimmed of whitespace
        - author (optional): Author name, None if empty
        - rating (optional): Integer 1-5 star rating, None if invalid
        - notes (optional): User notes, None if empty

    Raises:
        ValueError: If CSV is invalid, too large, missing required fields,
                   or contains no valid books

    Example:
        >>> books = parse_csv_books("title,author\\nGatsby,Fitzgerald")
        >>> len(books)
        1
        >>> books[0]['title']
        'Gatsby'
    """
    logger.debug(
        "Starting CSV parsing",
        extra={"content_size_bytes": len(csv_content.encode("utf-8"))},
    )

    try:
        # Basic validation
        if len(csv_content.encode("utf-8")) > 1024 * 1024:  # 1MB limit
            raise ValueError("File too large (max 1MB)")

        reader = csv.DictReader(io.StringIO(csv_content))
        books = list(reader)

        if len(books) > 100:
            raise ValueError("Too many books (max 100)")

        if len(books) == 0:
            raise ValueError("No books found in CSV")

        # Validate required columns
        if not books or "title" not in books[0]:
            raise ValueError("CSV must have a 'title' column")

        # Clean and validate books
        cleaned_books = []
        skipped_count = 0

        for i, book in enumerate(books):
            if not book.get("title", "").strip():
                skipped_count += 1
                continue  # Skip empty titles

            cleaned_book = {
                "title": book.get("title", "").strip(),
                "author": book.get("author", "").strip() or None,
                "rating": None,
                "notes": book.get("notes", "").strip() or None,
            }

            # Parse rating if present
            rating_str = book.get("rating", "").strip()
            if rating_str:
                try:
                    rating = int(float(rating_str))
                    if 1 <= rating <= 5:
                        cleaned_book["rating"] = rating
                except ValueError:
                    pass  # Invalid rating, keep as None

            cleaned_books.append(cleaned_book)

        if not cleaned_books:
            raise ValueError("No valid books found (all titles were empty)")

        logger.debug(
            "CSV parsing completed",
            extra={
                "total_rows": len(books),
                "valid_books": len(cleaned_books),
                "skipped_rows": skipped_count,
            },
        )

        return cleaned_books

    except ValueError:
        # Re-raise validation errors as-is
        raise
    except Exception as e:
        logger.error("Unexpected CSV parsing error", exc_info=True)
        raise ValueError(f"CSV parsing error: {str(e)}")


# ====================================================================
# EXISTING FUNCTIONS (Student Mode)
# ====================================================================


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_student_ids():
    try:
        engine = get_engine()
        logger.debug(f"Connecting to DB: {engine.url}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT student_id FROM students")).fetchall()
        ids = sorted([r[0] for r in rows])
        logger.debug(f"Fetched {len(ids)} student IDs: {ids[:5]} ...")
        return ids
    except Exception as e:
        logger.error("Failed to load student IDs", exc_info=True)
        return []


API_URL = f"http://recommendation_api:{S.recommendation_api_port}/recommend"


async def get_latest_metrics():
    """Fetch the latest metrics from Redis instead of Kafka for better reliability"""
    try:
        redis_client = get_redis_client()

        # Get recent metrics from Redis sorted set (if available)
        recent_metrics = []

        # Try to get ingestion metrics
        try:
            ingestion_key = "metrics:ingestion:recent"
            ingestion_data = await redis_client.lrange(
                ingestion_key, 0, 4
            )  # Last 5 metrics
            for data in ingestion_data:
                try:
                    metric = json.loads(data)
                    recent_metrics.append(metric)
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass

        # Try to get API metrics
        try:
            api_key = "metrics:api:recent"
            api_data = await redis_client.lrange(api_key, 0, 4)  # Last 5 metrics
            for data in api_data:
                try:
                    metric = json.loads(data)
                    recent_metrics.append(metric)
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass

        # If Redis doesn't have metrics, try Kafka as fallback
        if not recent_metrics:
            return await get_kafka_metrics()

        return recent_metrics

    except Exception as e:
        logger.warning(f"Redis metrics not available, trying Kafka: {e}")
        return await get_kafka_metrics()


async def get_kafka_metrics():
    """Fallback method to get metrics from Kafka"""
    try:
        # Try Docker Kafka first, then localhost for local development
        kafka_servers = [S.kafka_bootstrap, "localhost:9092"]

        for server in kafka_servers:
            try:
                consumer = AIOKafkaConsumer(
                    "ingestion_metrics",
                    "api_metrics",
                    "logs",
                    bootstrap_servers=server,
                    group_id="streamlit_dashboard",
                    auto_offset_reset="earliest",  # Changed from latest to get historical data
                    enable_auto_commit=False,
                )

                await consumer.start()

                # Get recent messages
                metrics = []
                try:
                    # Get all available messages
                    all_messages = await asyncio.wait_for(
                        consumer.getmany(timeout_ms=2000), timeout=KAFKA_TIMEOUT
                    )
                    for topic, msgs in all_messages.items():
                        for msg in msgs[-10:]:  # Take last 10 messages per topic
                            try:
                                data = json.loads(msg.value.decode())
                                metrics.append(data)
                            except Exception as e:
                                logger.warning(f"Failed to parse metric message: {e}")
                except asyncio.TimeoutError:
                    # No messages available
                    pass
                finally:
                    await consumer.stop()

                return sorted(metrics, key=lambda x: x.get("timestamp", 0))[
                    -10:
                ]  # Most recent 10

            except Exception as e:
                logger.debug(f"Failed to connect to Kafka at {server}: {e}")
                continue

        logger.warning("No Kafka servers available")
        return []

    except Exception as e:
        logger.warning(f"Kafka not available for metrics: {e}")
        return []


def get_recommendation(student_id: str, interests: str, n: int = 3):
    """Get book recommendation from API (api expects query params)."""
    params = {
        "student_id": student_id,
        "query": interests or "",
        "n": n,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                API_URL,
                params=params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning(
                "API request timed out",
                extra={"attempt": attempt, "timeout_sec": API_TIMEOUT},
            )
            if attempt == MAX_RETRIES:
                return {
                    "error": f"API request timed out after {MAX_RETRIES} attempts. Please try again."
                }
        except requests.exceptions.ConnectionError:
            logger.warning("API service not available")
            return {
                "error": "API service not running. Please start the recommendation API."
            }
        except Exception as e:
            logger.error("API request failed", exc_info=True)
            return {"error": f"API request failed: {str(e)}"}


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_table_df(table_name):
    try:
        # Use the same DB URL as the rest of the app
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch table {table_name}: {e}")
        return pd.DataFrame({"error": [str(e)]})


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_table_data(table_name):
    """Get table data with UUID serialization fix for Streamlit"""
    try:
        # Use the same DB URL as the rest of the app
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)
            
            # Convert UUID columns to strings to avoid PyArrow serialization issues
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Check if column contains UUID objects
                    sample_values = df[col].dropna().head(10)
                    if sample_values.any() and all(isinstance(v, uuid.UUID) for v in sample_values):
                        df[col] = df[col].astype(str)
            
            return df
    except Exception as e:
        logger.error(f"Failed to fetch table {table_name}: {e}")
        return pd.DataFrame({"error": [str(e)]})


# Add a singleton DB engine for connection reuse and faster loads
# Database engine singleton (compatible with Streamlit >=1.18 and <1.18)
if hasattr(st, "cache_resource"):
    # Streamlit 1.18+
    @st.cache_resource
    def get_engine():
        """Singleton SQLAlchemy engine (Streamlit >=1.18)."""
        return create_engine(str(S.db_url).replace("+asyncpg", ""))
else:
    # Older Streamlit versions
    @st.experimental_singleton
    def get_engine():
        """Singleton SQLAlchemy engine (Streamlit <1.18)."""
        return create_engine(str(S.db_url).replace("+asyncpg", ""))


def main():
    logger.info("Starting Streamlit UI")
    
    st.title("📚 Book Recommendation Engine")

    # Create tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📖 Book Recommendations",
            "📊 System Metrics",
            "🗄️ Database Explorer",
            "📜 Logs",
        ]
    )

    with tab1:
        st.header("📖 Book Recommendations")

        # Mode selection - Reader Mode gated by feature flag
        if S.enable_reader_mode:
            mode = st.radio(
                "**Recommendation Mode**",
                ["📚 Reader Mode", "🎓 Student Mode"],
                horizontal=True,
                index=0,  # Default to Reader Mode (first option)
                help="Reader Mode: Upload your books for personalized recommendations. Student Mode: Get recommendations by student ID.",
            )
        else:
            mode = "🎓 Student Mode"
            st.info("🎓 **Student Mode** - Get recommendations by student ID")
            st.caption(
                "💡 Reader Mode is currently disabled. Set ENABLE_READER_MODE=true to enable personal book recommendations."
            )

        st.divider()

        if mode == "🎓 Student Mode":
            # STUDENT MODE - existing functionality
            st.subheader("🎓 Student Recommendations")

            with st.form("student_recommendation_form"):
                # Student ID input with autocomplete - only load when form is submitted
                student_id = st.text_input(
                    "Student ID",
                    placeholder="Enter student ID (e.g., S001, S002)",
                    help="Enter a student ID to get personalized recommendations"
                )

                interests = st.text_area(
                    "Keywords/Interests (comma-separated)",
                    value="adventure, animals, space",
                    help="Enter keywords describing what the student likes to read about",
                )
                num_recommendations = st.slider("Number of recommendations", 1, 5, 3)

                submitted = st.form_submit_button("Get Student Recommendations")

                if submitted:
                    # Only load student IDs when form is submitted
                    with st.spinner("Loading student data..."):
                        student_ids = get_all_student_ids()
                    
                    if not student_ids:
                        st.warning(
                            "⚠️ No students found. Is the ingestion service finished?"
                        )
                    elif student_id not in student_ids:
                        st.error(f"⚠️ Student ID '{student_id}' not found. Available IDs: {', '.join(student_ids[:10])}{'...' if len(student_ids) > 10 else ''}")
                    else:
                        with st.spinner("Getting recommendation..."):
                            result = get_recommendation(
                                student_id, interests, num_recommendations
                            )

                            if "error" in result:
                                st.error(result["error"])
                                st.json(result)
                            else:
                                st.success("Recommendation received!")
                                st.json(result)

        else:
            # READER MODE - new functionality
            st.subheader("📚 Reader Mode - Personalized Recommendations")

            # User identifier input
            user_identifier = st.text_input(
                "**Your identifier** (email, username, etc.)",
                placeholder="your.email@example.com",
                help="This will be securely hashed for privacy. Use the same identifier to access your recommendations later.",
            )

            if user_identifier:
                user_hash_id = hash_user_identifier(user_identifier)

                # Tabs for different reader actions
                reader_tab1, reader_tab2, reader_tab3 = st.tabs(
                    ["📤 Upload Books", "🎯 Get Recommendations", "📖 My Books"]
                )

                with reader_tab1:
                    st.write(
                        "**Upload your reading history to get personalized recommendations**"
                    )

                    # Upload method selection
                    upload_method = st.radio(
                        "How would you like to add books?",
                        ["📝 Manual Entry", "📁 CSV Upload"],
                        horizontal=True,
                    )

                    if upload_method == "📝 Manual Entry":
                        st.write("**Add books manually:**")

                        with st.form("manual_book_entry"):
                            col1, col2 = st.columns(2)
                            with col1:
                                title = st.text_input(
                                    "Book Title*", placeholder="The Great Gatsby"
                                )
                                rating = st.selectbox(
                                    "Rating (optional)",
                                    [None, 1, 2, 3, 4, 5],
                                    format_func=lambda x: (
                                        "No rating" if x is None else f"{x} stars"
                                    ),
                                )
                            with col2:
                                author = st.text_input(
                                    "Author (optional)",
                                    placeholder="F. Scott Fitzgerald",
                                )
                                notes = st.text_area(
                                    "Notes (optional)",
                                    placeholder="Great classic novel...",
                                )

                            submitted_manual = st.form_submit_button("Add Book")

                            if submitted_manual:
                                if not title.strip():
                                    st.error("Book title is required!")
                                else:
                                    book = {
                                        "title": title.strip(),
                                        "author": author.strip() or None,
                                        "rating": rating,
                                        "notes": notes.strip() or None,
                                    }

                                    with st.spinner("Uploading book..."):
                                        result = upload_books_to_reader_api(
                                            user_identifier, [book]
                                        )

                                        if "error" in result:
                                            st.error(result["error"])
                                        else:
                                            st.success(
                                                f"✅ Added '{title}' to your reading list!"
                                            )
                                            st.info(
                                                "💡 **Tip:** Add more books for better recommendations, then go to the 'Get Recommendations' tab."
                                            )

                    else:  # CSV Upload
                        st.write("**Upload a CSV file with your books:**")
                        st.info(
                            "📋 **CSV Format:** Required column: `title`. Optional: `author`, `rating` (1-5), `notes`"
                        )

                        uploaded_file = st.file_uploader(
                            "Choose CSV file",
                            type="csv",
                            help="Max file size: 1MB, Max books: 100",
                        )

                        if uploaded_file is not None:
                            try:
                                # Read file content
                                csv_content = uploaded_file.read().decode("utf-8")

                                # Parse and validate
                                books = parse_csv_books(csv_content)

                                st.success(f"✅ Found {len(books)} valid books in CSV")

                                # Show preview
                                with st.expander("📋 Preview books to upload"):
                                    preview_df = pd.DataFrame(books)
                                    st.dataframe(preview_df)

                                # Upload button
                                if st.button("📤 Upload All Books", type="primary"):
                                    with st.spinner(f"Uploading {len(books)} books..."):
                                        result = upload_books_to_reader_api(
                                            user_identifier, books
                                        )

                                        if "error" in result:
                                            st.error(result["error"])
                                        else:
                                            st.success(
                                                f"🎉 Successfully uploaded {result.get('books_processed', len(books))} books!"
                                            )
                                            st.info(
                                                "🔄 Metadata enrichment is happening in the background - your books will be enhanced with additional details shortly."
                                            )
                                            st.info(
                                                "💡 Now go to the 'Get Recommendations' tab to see your personalized suggestions."
                                            )

                            except Exception as e:
                                st.error(f"CSV processing error: {str(e)}")

                with reader_tab2:
                    st.write(
                        "**Get personalized recommendations based on your uploaded books**"
                    )

                    with st.form("reader_recommendation_form"):
                        interests = st.text_area(
                            "What are you interested in reading? (optional)",
                            placeholder="mystery, adventure, historical fiction...",
                            help="Add keywords to get more targeted recommendations",
                        )
                        num_recs = st.slider("Number of recommendations", 1, 5, 3)
                        get_recs = st.form_submit_button(
                            "🎯 Get My Recommendations", type="primary"
                        )
                        if get_recs:
                            with st.spinner("Finding your perfect next reads..."):
                                result = get_reader_recommendations(
                                    user_hash_id, interests, num_recs
                                )
                                if "error" in result:
                                    st.error(result["error"])
                                    if "not found" in result["error"].lower():
                                        st.info(
                                            "💡 **Tip:** Upload some books first in the 'Upload Books' tab to get personalized recommendations."
                                        )
                                else:
                                    st.success(
                                        "🎉 Here are your personalized recommendations!"
                                    )
                                    # Use tab-specific session state to prevent bleeding across tabs
                                    st.session_state["reader_recommendations"] = result.get(
                                        "recommendations", []
                                    )
                                    # Clear any existing recommendations timestamp for freshness indicator
                                    st.session_state["reader_recommendations_time"] = time.time()

                # After the form (outside the form context)
                # Only show recommendations in this tab, not globally
                recommendations = st.session_state.get("reader_recommendations", [])
                recommendations_time = st.session_state.get("reader_recommendations_time", None)
                
                # Add clear recommendations button and freshness indicator
                if recommendations:
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        if recommendations_time:
                            time_ago = int(time.time() - recommendations_time)
                            if time_ago < 60:
                                st.success(f"✨ Fresh recommendations (generated {time_ago}s ago)")
                            else:
                                st.info(f"📋 Cached recommendations (generated {time_ago//60}m ago)")
                    with col2:
                        if st.button("🔄 New Recommendations", help="Get fresh recommendations"):
                            # Clear cached recommendations to force new ones
                            if "reader_recommendations" in st.session_state:
                                del st.session_state["reader_recommendations"]
                            if "reader_recommendations_time" in st.session_state:
                                del st.session_state["reader_recommendations_time"]
                            st.rerun()
                    with col3:
                        if st.button("🗑️ Clear", help="Clear recommendations"):
                            # Clear recommendations from view
                            if "reader_recommendations" in st.session_state:
                                del st.session_state["reader_recommendations"]
                            if "reader_recommendations_time" in st.session_state:
                                del st.session_state["reader_recommendations_time"]
                            st.rerun()
                    st.divider()
                
                if recommendations:
                    for i, rec in enumerate(recommendations, 1):
                        with st.container():
                            st.write(f"**{i}. {rec.get('title', 'Unknown Title')}**")
                            if rec.get("author"):
                                st.write(f"*by {rec['author']}*")
                            if rec.get("reading_level"):
                                st.write(f"📊 Reading Level: {rec['reading_level']}")
                            if rec.get("librarian_blurb"):
                                st.write(f"📝 {rec['librarian_blurb']}")
                            if rec.get("justification"):
                                st.write(
                                    f"🎯 **Why this book:** {rec['justification']}"
                                )
                            col1, col2, col3 = st.columns([1, 1, 6])
                            with col1:
                                if st.button(
                                    "👍", key=f"thumbs_up_{rec.get('book_id', i)}_{i}"
                                ):
                                    feedback_result = submit_feedback(
                                        user_hash_id, rec.get("book_id", ""), 1
                                    )
                                    if "error" in feedback_result:
                                        st.error(
                                            f"Feedback error: {feedback_result['error']}"
                                        )
                                    else:
                                        st.success("👍 Thanks for the feedback!")
                            with col2:
                                if st.button(
                                    "👎", key=f"thumbs_down_{rec.get('book_id', i)}_{i}"
                                ):
                                    feedback_result = submit_feedback(
                                        user_hash_id, rec.get("book_id", ""), -1
                                    )
                                    if "error" in feedback_result:
                                        st.error(
                                            f"Feedback error: {feedback_result['error']}"
                                        )
                                    else:
                                        st.success("👎 Thanks for the feedback!")
                            st.divider()
                else:
                    st.info(
                        "No recommendations available. Try uploading more books or adjusting your interests."
                    )

                with reader_tab3:
                    st.write("**Your uploaded reading history**")

                    if st.button("🔄 Refresh My Books"):
                        with st.spinner("Loading your books..."):
                            books_result = get_user_books(user_hash_id)

                            if "error" in books_result:
                                st.error(books_result["error"])
                            else:
                                books = books_result.get("books", [])
                                if books:
                                    st.success(
                                        f"📚 You have {len(books)} books in your reading history"
                                    )

                                    books_df = pd.DataFrame(books)
                                    # Reorder columns for better display
                                    display_cols = [
                                        "title",
                                        "author",
                                        "rating",
                                        "notes",
                                        "upload_date",
                                    ]
                                    available_cols = [
                                        col
                                        for col in display_cols
                                        if col in books_df.columns
                                    ]
                                    st.dataframe(
                                        books_df[available_cols],
                                        use_container_width=True,
                                    )
                                else:
                                    st.info(
                                        "📖 No books uploaded yet. Go to the 'Upload Books' tab to add your reading history!"
                                    )
            else:
                st.info(
                    "👆 Please enter your identifier above to access Reader Mode features."
                )

    with tab2:
        st.header("📊 System Metrics")

        # Check system status
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🔍 System Status")

            # Try to connect to Kafka
            try:
                import nest_asyncio

                nest_asyncio.apply()
                metrics = asyncio.get_event_loop().run_until_complete(
                    get_latest_metrics()
                )
                if metrics is not None:
                    st.success("✅ Kafka Connected")
                else:
                    st.warning("⚠️ Kafka Not Available")
            except Exception as e:
                st.error(f"❌ Kafka Error: {str(e)}")

            # Try to connect to API
            try:
                response = requests.get(
                    f"http://recommendation_api:{S.recommendation_api_port}/health",
                    timeout=HEALTH_TIMEOUT,
                )
                if response.status_code == 200:
                    st.success("✅ API Connected")
                else:
                    st.warning("⚠️ API Responding but not healthy")
            except:
                st.error("❌ API Not Available")

        with col2:
            st.subheader("🔄 Refresh")
            if st.button("Refresh Metrics"):
                with st.spinner("Refreshing system status..."):
                    # Force refresh by clearing any cached data
                    st.cache_data.clear()
                    time.sleep(0.5)  # Brief delay to show spinner
                st.success("✅ Metrics refreshed!")
                st.rerun()

        # Show recent metrics
        st.subheader("📈 Recent Activity")

        try:
            import nest_asyncio

            nest_asyncio.apply()
            metrics = asyncio.get_event_loop().run_until_complete(get_latest_metrics())

            if metrics:
                st.write(f"Found {len(metrics)} recent metrics:")
                for metric in metrics[-5:]:  # Show last 5 metrics
                    if metric.get("event") == "ingestion_complete":
                        st.info(
                            f"""
                        **Ingestion Complete** ({metric.get('timestamp', 'N/A')})
                        - Duration: {metric.get('duration', 'N/A')} seconds
                        - Books: {metric.get('books_processed', 'N/A')}
                        - Students: {metric.get('students_processed', 'N/A')}
                        - Checkouts: {metric.get('checkouts_processed', 'N/A')}
                        """
                        )
                    elif metric.get("event") == "recommendation_served":
                        st.success(
                            f"""
                        **Recommendation Served** ({metric.get('timestamp', 'N/A')})
                        - Student: {metric.get('student_id', 'N/A')}
                        - Duration: {metric.get('duration_sec', 'N/A')}s
                        - Tools Used: {metric.get('tool_count', 'N/A')}
                        """
                        )
                    elif metric.get("event") == "api_request":
                        st.success(
                            f"""
                        **API Request** ({metric.get('timestamp', 'N/A')})
                        - Endpoint: {metric.get('endpoint', 'N/A')}
                        - Duration: {metric.get('duration', 'N/A')}ms
                        """
                        )
                    elif metric.get("event") == "reader_recommendation_served":
                        st.success(
                            f"""
                        **Reader Recommendation Served** ({metric.get('timestamp', 'N/A')})
                        - User: {metric.get('user_hash_id', 'N/A')[:8]}...
                        - Duration: {metric.get('duration_sec', 'N/A')}s
                        - Agent Duration: {metric.get('agent_duration', 'N/A')}s
                        - Tools Used: {metric.get('tool_count', 'N/A')}
                        - Recommendations: {metric.get('n', 'N/A')}
                        - Mode: {metric.get('mode', 'N/A')}
                        - Cache: {'✅' if metric.get('cache_enabled') else '❌'}
                        - Optimized: {'✅' if metric.get('performance_optimized') else '❌'}
                        """
                        )
                    else:
                        # Show any other metrics
                        st.text(
                            f"**{metric.get('event', 'Unknown Event')}**: {json.dumps(metric, indent=2)}"
                        )
            else:
                st.info("No recent metrics available.")
                st.info("💡 **To see metrics:** Start Docker services and run:")
                st.code(
                    "docker-compose exec ingestion_service python -m ingestion_service.main"
                )

        except Exception as e:
            st.warning(f"Could not fetch metrics: {str(e)}")
            st.info(
                "💡 **To enable metrics:** Start Redis, Kafka, and run ingestion service"
            )

    with tab3:
        st.header("🗄️ Database Explorer")
        
        # Get all available tables
        try:
            engine = get_engine()
            with engine.connect() as conn:
                # Get list of all tables
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """))
                tables = [row[0] for row in result]
        except Exception as e:
            st.error(f"Failed to get table list: {e}")
            tables = ["students", "catalog", "checkout", "student_similarity"]  # Fallback
        
        if not tables:
            st.warning("No tables found in database")
            return
        
        # Table selector
        selected_table = st.selectbox(
            "Select a table to explore:",
            tables,
            help="Choose a table to view its schema and data"
        )
        
        if selected_table:
            st.subheader(f"📋 Table: {selected_table}")
            
            # Get table schema
            try:
                engine = get_engine()
                with engine.connect() as conn:
                    schema_result = conn.execute(text(f"""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = '{selected_table}'
                        ORDER BY ordinal_position
                    """))
                    schema_data = [{"Column": row[0], "Type": row[1], "Nullable": row[2], "Default": row[3]} for row in schema_result]
                    schema_df = pd.DataFrame(schema_data)
            except Exception as e:
                st.error(f"Failed to get schema: {e}")
                schema_df = pd.DataFrame()
            
            # Display schema
            if not schema_df.empty:
                st.write("**Schema:**")
                st.dataframe(schema_df, use_container_width=True)
                st.divider()
            
            # Get and display data
            try:
                df = get_table_data(selected_table)
                if not df.empty:
                    st.write(f"**Data Preview (showing up to 100 rows):**")
                    
                    # Add download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=f"{selected_table}_data.csv",
                        mime="text/csv"
                    )
                    
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info("Table is empty or no data available")
            except Exception as e:
                st.error(f"Failed to load table data: {e}")

    # ---------------------------------------------------------------------
    # Logs tab – lightweight viewer for service_logs.jsonl produced by
    # log_consumer. Shows last N lines with simple filters.
    # ---------------------------------------------------------------------

    with tab4:
        st.header("📜 Service Logs")

        LOG_PATH = Path("logs/service_logs.jsonl")

        if not LOG_PATH.exists():
            st.info(
                "No log file found yet. Ensure log_consumer is running and logs are being published to Kafka."
            )
        else:
            max_lines = st.number_input("Max lines to load", 1000, 50000, 5000, 1000)

            @st.cache_data(ttl=5, show_spinner=False)
            def _load_logs(lines: int):
                try:
                    with open(LOG_PATH, "r", encoding="utf-8") as f:
                        data = f.readlines()[-lines:]
                except FileNotFoundError:
                    st.info(
                        "No log file found yet. Ensure log_consumer is running and logs are being published to Kafka."
                    )
                    return pd.DataFrame()
                except UnicodeDecodeError:
                    st.error(
                        "Log file is not valid UTF-8. It may be corrupted or written by another process."
                    )
                    return pd.DataFrame()
                except Exception as e:
                    st.error(f"Error reading log file: {e}")
                    return pd.DataFrame()

                records = []
                for l in data:
                    try:
                        records.append(json.loads(l))
                    except json.JSONDecodeError:
                        # Skip lines that aren't valid JSON
                        continue
                if not records:
                    st.info(
                        "Log file is present but contains no valid JSON log entries."
                    )
                    return pd.DataFrame()
                return pd.json_normalize(records)

            df = _load_logs(max_lines)

            if df.empty:
                st.info("Log file is empty.")
            else:
                # Filters
                cols = st.columns(3)
                with cols[0]:
                    svcs = sorted(df["service"].dropna().unique())
                    svc_sel = st.multiselect("Service", svcs, default=svcs)
                with cols[1]:
                    lvls = ["DEBUG", "INFO", "WARNING", "ERROR"]
                    lvl_sel = st.multiselect(
                        "Level", lvls, default=["INFO", "WARNING", "ERROR"]
                    )
                with cols[2]:
                    search = st.text_input("Search text")

                mask = df["service"].isin(svc_sel) & df["level"].isin(lvl_sel)
                if search:
                    mask &= df["event"].str.contains(search, case=False, na=False)

                st.dataframe(
                    df[mask]
                    .sort_values("timestamp", ascending=False)
                    .reset_index(drop=True)
                )

    # Add a subtle help text at the bottom
    st.markdown(
        """
        <div style="text-align: center; color: #888; font-size: 0.75em; margin-top: 5px;">
            Found bugs? Have suggestions? Feel free to reach out!
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Render footer in the placeholder (appears early in DOM but at natural position)
    st.markdown(
        """
        ---
        <div style="text-align: center; color: #666; font-size: 0.85em; padding: 8px 0;">
            Built with ❤️ by <a href="https://danguilliams.com" target="_blank" style="color: #0066cc; text-decoration: none;">Dan Guilliams</a> | 
            <a href="https://github.com/dguilliams3/book-recommendation-engine" target="_blank" style="color: #0066cc; text-decoration: none;">🐙 GitHub</a> | 
            <a href="https://buymeacoffee.com/danguilliams" target="_blank" style="color: #0066cc; text-decoration: none;">☕ Buy me a coffee</a>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    logger.info("Streamlit UI starting")
    try:
        main()
    except Exception as e:
        logger.error("Streamlit UI failed", exc_info=True)
        st.error("An error occurred while starting the application.")
        raise
