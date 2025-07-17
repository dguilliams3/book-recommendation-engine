"""Core recommendation logic independent of FastAPI layer.

This module handles:
1.   Launching the Fast-MCP subprocess via `mcp.stdio_client`.
2.   Loading LangChain-wrapped MCP tools and running the ReAct agent.
3.   Returning a structured list of book recommendations plus useful
     metrics (duration, tools used, etc.).

It deliberately contains no FastAPI imports so it can be reused from a
worker, a CLI, or tests without bringing in the web stack.

Performance Optimizations:
- Intelligent caching for student context and book metadata
- Connection pooling for database operations
- Async batch processing for bulk operations
- Performance monitoring and profiling
"""

from __future__ import annotations

import asyncio, json, os, time, re
from datetime import timedelta, datetime
from pathlib import Path
from typing import Any, List, Tuple, Optional, Dict
from collections import Counter

import numpy as np
from fastapi import HTTPException
from langchain.callbacks.base import AsyncCallbackHandler
from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_community.vectorstores import FAISS
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage
from mcp import StdioServerParameters, stdio_client, ClientSession
from pydantic import BaseModel

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from common.settings import settings as S
from common.structured_logging import get_logger
from common.performance import (
    cached,
    performance_context,
    get_cache,
    get_connection_pool,
    get_performance_monitor,
    BatchProcessor,
)
from .prompts import build_student_prompt, build_reader_prompt, parser
from .scoring import score_candidates
from .candidate_builder import build_candidates
from common.redis_utils import mark_recommended
from common.reading_level_utils import get_student_reading_level_from_db
from .config import reader_config
from common.llm_client import (
    get_llm_client,
    enrich_recommendations_with_llm,
    enrich_book_metadata,
    LLMServiceError
)
from common.kafka_utils import publish_event

logger = get_logger(__name__)


def _normalize_text(text: str) -> str:
    """Normalize text for comparison by removing punctuation and converting to lowercase."""
    if not text:
        return ""
    # Remove punctuation and convert to lowercase
    normalized = re.sub(r'[^\w\s]', '', text.lower())
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def _calculate_title_similarity(title1: str, title2: str) -> float:
    """Calculate similarity between two book titles using normalized comparison."""
    if not title1 or not title2:
        return 0.0
    
    norm1 = _normalize_text(title1)
    norm2 = _normalize_text(title2)
    
    if norm1 == norm2:
        return 1.0
    
    # Check if one title is contained within the other
    if norm1 in norm2 or norm2 in norm1:
        return 0.9
    
    # Simple word overlap similarity
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    return len(intersection) / len(union) if union else 0.0


def _calculate_author_similarity(author1: str, author2: str) -> float:
    """Calculate similarity between two author names."""
    if not author1 or not author2:
        return 0.0
    
    norm1 = _normalize_text(author1)
    norm2 = _normalize_text(author2)
    
    if norm1 == norm2:
        return 1.0
    
    # Check for exact match after normalization
    if norm1 == norm2:
        return 1.0
    
    # Check if one author name is contained within the other
    if norm1 in norm2 or norm2 in norm1:
        return 0.8
    
    # Simple word overlap for author names
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    return len(intersection) / len(union) if union else 0.0


async def _filter_out_user_books(
    candidates: List[dict], 
    uploaded_books: List[dict], 
    user_hash_id: str
) -> List[dict]:
    """
    Filter out books that user has already uploaded.
    
    This function implements comprehensive filtering to prevent recommending
    books that the user has already read/uploaded. It uses multiple criteria:
    
    1. Exact book_id matches
    2. Exact title/author matches
    3. Fuzzy title similarity (high confidence)
    4. ISBN matches
    5. Author similarity (high confidence)
    
    Args:
        candidates: List of candidate books to filter
        uploaded_books: List of books uploaded by the user
        user_hash_id: User identifier for logging
        
    Returns:
        Filtered list of candidates with uploaded books removed
    """
    if not uploaded_books or not candidates:
        return candidates
    
    # Create lookup sets for efficient filtering
    uploaded_book_ids = {book.get("book_id") for book in uploaded_books if book.get("book_id")}
    uploaded_isbns = {book.get("isbn") for book in uploaded_books if book.get("isbn")}
    uploaded_titles_authors = {
        (_normalize_text(book.get("title", "")), _normalize_text(book.get("author", "")))
        for book in uploaded_books
        if book.get("title") and book.get("author")
    }
    
    # Create normalized title/author pairs for uploaded books
    uploaded_titles = [_normalize_text(book.get("title", "")) for book in uploaded_books if book.get("title")]
    uploaded_authors = [_normalize_text(book.get("author", "")) for book in uploaded_books if book.get("author")]
    
    filtered_candidates = []
    filtered_count = 0
    
    for candidate in candidates:
        should_filter = False
        filter_reason = None
        
        # 1. Check exact book_id match
        if candidate.get("book_id") in uploaded_book_ids:
            should_filter = True
            filter_reason = "exact_book_id_match"
        
        # 2. Check exact ISBN match
        elif candidate.get("isbn") and candidate.get("isbn") in uploaded_isbns:
            should_filter = True
            filter_reason = "exact_isbn_match"
        
        # 3. Check exact title/author match
        elif (candidate.get("title") and candidate.get("author")):
            candidate_title = _normalize_text(candidate.get("title", ""))
            candidate_author = _normalize_text(candidate.get("author", ""))
            
            if (candidate_title, candidate_author) in uploaded_titles_authors:
                should_filter = True
                filter_reason = "exact_title_author_match"
            
            # 4. Check fuzzy title similarity (high confidence)
            elif candidate_title in uploaded_titles:
                should_filter = True
                filter_reason = "exact_title_match"
            else:
                # Check for high similarity titles
                for uploaded_title in uploaded_titles:
                    if _calculate_title_similarity(candidate_title, uploaded_title) > 0.8:
                        should_filter = True
                        filter_reason = "fuzzy_title_match"
                        break
                
                # 5. Check author similarity (high confidence)
                if not should_filter and candidate_author:
                    for uploaded_author in uploaded_authors:
                        if _calculate_author_similarity(candidate_author, uploaded_author) > 0.8:
                            should_filter = True
                            filter_reason = "fuzzy_author_match"
                            break
        
        if should_filter:
            filtered_count += 1
            logger.debug(
                "Filtered out candidate book",
                extra={
                    "user_hash_id": user_hash_id,
                    "candidate_book_id": candidate.get("book_id"),
                    "candidate_title": candidate.get("title"),
                    "candidate_author": candidate.get("author"),
                    "filter_reason": filter_reason
                }
            )
        else:
            filtered_candidates.append(candidate)
    
    if filtered_count > 0:
        logger.info(
            "Filtered out user's uploaded books",
            extra={
                "user_hash_id": user_hash_id,
                "original_candidates": len(candidates),
                "filtered_candidates": len(filtered_candidates),
                "filtered_count": filtered_count,
                "uploaded_books_count": len(uploaded_books)
            }
        )
    
    return filtered_candidates


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BookRecommendation(BaseModel):
    """Schema for a single recommended book."""

    book_id: str
    title: str
    author: str
    reading_level: float
    librarian_blurb: str
    justification: str


class RecommendResponse(BaseModel):
    """API response model returned by FastAPI layer."""

    request_id: str
    duration_sec: float
    recommendations: List[BookRecommendation]
    student_avg_level: float | None = None
    recent_books: List[str] | None = None


# ---------------------------------------------------------------------------
# Internal helper classes
# ---------------------------------------------------------------------------


class MetricCallbackHandler(AsyncCallbackHandler):
    """Collect tool-usage stats during a LangChain agent run."""

    def __init__(self) -> None:
        self.tools_used: list[str] = []
        self.error_count: int = 0

    async def on_tool_start(self, serialized, _input_str, **_kw):  # type: ignore[override]
        name = (
            serialized.get("name", "unknown")
            if isinstance(serialized, dict)
            else "unknown"
        )
        self.tools_used.append(name)

    async def on_tool_error(self, _err, **_kw):  # type: ignore[override]
        self.error_count += 1


# ---------------------------------------------------------------------------
# One-time globals (vector store, model, etc.)
# ---------------------------------------------------------------------------

logger.info("Initialising ChatOpenAI model for recommendation agent")
chat_model = ChatOpenAI(
    model=S.model_name,
    api_key=S.openai_api_key,
    temperature=0.3,
    max_tokens=S.model_max_tokens,
)

logger.info("Preparing server parameters for MCP subprocess")
_mcp_path = Path(__file__).parent / "mcp_book_server.py"
server_params = StdioServerParameters(
    command="python",
    args=[str(_mcp_path)],
    env=dict(os.environ),
)

# Create database engine for recommendation history logging
logger.info("Initialising database engine for recommendation history")
async_db_url = str(S.db_url).replace("postgresql://", "postgresql+asyncpg://")
engine = create_async_engine(async_db_url, echo=False)

# Performance optimization: Initialize connection pool
connection_pool = None
batch_processor = BatchProcessor(batch_size=20, max_concurrent=5)

# Rating weights for semantic search (same as student system)
RATING_WEIGHTS = {
    5: 1.0,  # 5-star books get full weight
    4: 0.7,  # 4-star books get 70% weight
    3: 0.4,  # 3-star books get 40% weight
    2: 0.1,  # 2-star books get 10% weight
    1: 0.05,  # 1-star books get 5% weight
}

# Global cache for FAISS store to avoid reloading on every request
_faiss_store_cache: Optional[FAISS] = None
_book_id_to_index_cache: Optional[dict] = None


def _get_faiss_store() -> Tuple[Optional[FAISS], Optional[dict]]:
    """Load FAISS vector store with caching and build book_id lookup index."""
    global _faiss_store_cache, _book_id_to_index_cache
    
    # Return cached version if available
    if _faiss_store_cache is not None and _book_id_to_index_cache is not None:
        return _faiss_store_cache, _book_id_to_index_cache
    
    try:
        vector_dir = S.data_dir / "vector_store"
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

        if not (vector_dir / "index.faiss").exists():
            logger.info(
                "FAISS index not found, creating empty index",
                extra={"path": str(vector_dir)},
            )
            # Create empty index so system can work
            vector_dir.mkdir(parents=True, exist_ok=True)
            store = FAISS.from_texts(
                ["dummy"], embeddings, metadatas=[{"book_id": "dummy"}]
            )
            store.save_local(vector_dir)
            logger.info("Empty FAISS index created")
            
            # Cache the empty store
            _faiss_store_cache = store
            _book_id_to_index_cache = {"dummy": 0}
            return store, _book_id_to_index_cache

        # Load the FAISS store
        store = FAISS.load_local(
            vector_dir, embeddings, allow_dangerous_deserialization=True
        )
        logger.info("FAISS store loaded", extra={"index_size": store.index.ntotal})
        
        # Build book_id to index mapping for O(1) lookup
        logger.info("Building book_id to index mapping for fast lookup")
        book_id_to_index = {}
        
        try:
            for idx, doc_id in store.index_to_docstore_id.items():
                doc = store.docstore.search(doc_id)
                book_id = doc.metadata.get("book_id")
                if book_id:
                    book_id_to_index[book_id] = idx
        except Exception as e:
            logger.warning(f"Failed to build book_id index: {e}", exc_info=True)
            book_id_to_index = {}
        
        logger.info(
            "Book ID index built", 
            extra={"total_books": len(book_id_to_index)}
        )
        
        # Cache both store and index
        _faiss_store_cache = store
        _book_id_to_index_cache = book_id_to_index
        
        return store, book_id_to_index
        
    except ImportError as e:
        logger.error(f"Missing required dependencies for FAISS: {e}")
        return None, None
    except FileNotFoundError as e:
        logger.warning(f"FAISS vector store files not found: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Failed to load/create FAISS store: {e}", exc_info=True)
        return None, None


async def _semantic_book_candidates_reader(
    uploaded_books: List[dict], 
    feedback_scores: dict, 
    user_hash_id: str, 
    k: int = 20
) -> List[str]:
    """Find semantically similar books using FAISS search on user's uploaded books and feedback.
    
    This adapts the student semantic search logic for Reader Mode by:
    1. Using uploaded books instead of checkout history
    2. Weighting by user ratings and feedback scores
    3. Considering both positive and negative feedback
    4. Aggregating embeddings from multiple sources
    
    Algorithm:
    1. Fetch user's uploaded books with ratings
    2. Weight each book's embedding by its rating (5★=1.0, 4★=0.7, etc.)
    3. Apply feedback score adjustments (positive feedback boosts, negative reduces)
    4. Aggregate weighted embeddings into a single query vector
    5. FAISS search for top k similar unseen books
    
    Returns:
        List of book_ids from semantic search, or empty list if FAISS unavailable
    """
    logger.debug(
        "Starting semantic book search for reader mode",
        extra={"user_hash_id": user_hash_id, "target_candidates": k},
    )

    if not uploaded_books:
        logger.debug(
            "No uploaded books found for semantic search",
            extra={"user_hash_id": user_hash_id},
        )
        return []

    # Load FAISS store with caching
    store, book_id_to_index = _get_faiss_store()
    if not store or not book_id_to_index:
        logger.debug("FAISS store unavailable for semantic search")
        return []

    # Aggregate weighted embeddings
    weighted_embeddings = []
    total_weight = 0.0

    for uploaded_book in uploaded_books:
        book_id = uploaded_book.get("book_id")
        if not book_id:
            continue
            
        # Get rating weight (default to 3-star weight if no rating)
        rating = uploaded_book.get("rating", 3)
        weight = RATING_WEIGHTS.get(rating, 0.4)
        
        # Apply feedback score adjustment
        if book_id in feedback_scores:
            feedback_score = feedback_scores[book_id]
            # Positive feedback boosts weight, negative reduces it
            if feedback_score > 0:
                weight *= (1 + feedback_score * 0.5)  # Boost by up to 50%
            else:
                weight *= (1 + feedback_score * 0.3)  # Reduce by up to 30%
                weight = max(weight, 0.01)  # Don't go below 1%

        # Find book embedding in FAISS store using O(1) lookup
        try:
            doc_idx = book_id_to_index.get(book_id)
            if doc_idx is not None:
                embedding = store.index.reconstruct(doc_idx)
                weighted_embeddings.append(embedding * weight)
                total_weight += weight
                logger.debug(
                    "Found embedding for uploaded book",
                    extra={"book_id": book_id, "weight": weight, "rating": rating}
                )
            else:
                logger.debug(
                    "Book ID not found in vector store",
                    extra={"book_id": book_id}
                )

        except ValueError as e:
            logger.warning(
                f"Invalid vector index for book_id {book_id}: {e}",
                extra={"book_id": book_id, "index": doc_idx}
            )
            continue
        except Exception as e:
            logger.warning(
                f"Could not retrieve embedding for book_id {book_id}: {e}",
                extra={"book_id": book_id}
            )
            continue

    if not weighted_embeddings or total_weight == 0:
        logger.debug(
            "No valid embeddings found for aggregation",
            extra={"user_hash_id": user_hash_id},
        )
        return []

    # Create aggregated query vector
    query_vector = np.sum(weighted_embeddings, axis=0) / total_weight

    # Search for similar books
    docs_with_scores = store.similarity_search_by_vector(
        query_vector, k=k * 2
    )  # Oversample

    # Extract book_ids, filtering out already-uploaded books
    uploaded_book_ids = {book.get("book_id") for book in uploaded_books if book.get("book_id")}

    candidates = []
    for doc in docs_with_scores:
        book_id = doc.metadata.get("book_id")
        if book_id and book_id not in uploaded_book_ids:
            candidates.append(book_id)
            if len(candidates) >= k:
                break

    logger.info(
        "Semantic search completed for reader mode",
        extra={
            "user_hash_id": user_hash_id,
            "uploaded_books_used": len(uploaded_books),
            "semantic_candidates": len(candidates),
            "total_weight": total_weight,
        },
    )

    return candidates


async def _query_based_semantic_candidates_reader(
    query: str, 
    uploaded_books: List[dict], 
    feedback_scores: dict,
    user_hash_id: str, 
    k: int = 10
) -> List[str]:
    """Find books semantically similar to the user's query with reader context.
    
    Enhanced to consider user context:
    - User's reading level preferences from uploaded books
    - User's favorite genres from uploaded books
    - User's feedback history
    - Combines query with user profile for personalized search
    
    Algorithm:
    1. Enhance query with user context from uploaded books
    2. Embed the enhanced query text
    3. FAISS search for most similar books
    4. Return top k book_ids
    """
    logger.debug(
        "Starting query-based semantic search for reader mode",
        extra={"query": query, "user_hash_id": user_hash_id, "target_candidates": k},
    )

    try:
        # Load FAISS store with caching
        store, _ = _get_faiss_store()
        if not store:
            logger.debug("FAISS store unavailable for query search")
            return []

        # Enhance query with user context
        enhanced_query = query
        
        if uploaded_books:
            # Calculate preferred reading level
            levels = [
                book.get("reading_level", 5.0) 
                for book in uploaded_books 
                if book.get("reading_level")
            ]
            avg_level = round(sum(levels) / len(levels), 1) if levels else 5.0

            # Find most common genres
            genre_counts = {}
            for book in uploaded_books:
                if book.get("genre"):
                    genre_counts[book["genre"]] = genre_counts.get(book["genre"], 0) + 1

            top_genres = sorted(
                genre_counts.items(), key=lambda x: x[1], reverse=True
            )[:3]

            # Build context string
            context_parts = []
            if top_genres:
                context_parts.append(f"genres: {', '.join(g[0] for g in top_genres)}")
            if avg_level:
                context_parts.append(f"reading level: {avg_level}")
                
            if context_parts:
                enhanced_query = f"{query} (prefers: {', '.join(context_parts)})"

        # Embed the enhanced query
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        query_embedding = embeddings.embed_query(enhanced_query)

        # Search for similar books
        docs_with_scores = store.similarity_search_by_vector(
            query_embedding, k=k * 2
        )  # Oversample

        # Extract book_ids, filtering out already-uploaded books
        uploaded_book_ids = {book.get("book_id") for book in uploaded_books if book.get("book_id")}

        candidates = []
        for doc in docs_with_scores:
            book_id = doc.metadata.get("book_id")
            if book_id and book_id not in uploaded_book_ids:
                candidates.append(book_id)
                if len(candidates) >= k:
                    break

        logger.info(
            "Query-based semantic search completed for reader mode",
            extra={
                "user_hash_id": user_hash_id,
                "original_query": query,
                "enhanced_query": enhanced_query,
                "semantic_candidates": len(candidates),
            },
        )

        return candidates

    except Exception:
        logger.warning(
            "Query-based semantic search failed for reader mode", 
            exc_info=True, 
            extra={"user_hash_id": user_hash_id}
        )
        return []


async def get_db_connection():
    """Get database connection from pool."""
    global connection_pool
    if connection_pool is None:
        connection_pool = await get_connection_pool()
    return await connection_pool.get_db_pool(async_db_url)


async def execute_with_deadlock_retry(
    conn, query, params=None, max_retries=3, base_delay=0.1
):
    """Execute a query with deadlock retry logic and exponential backoff."""
    import asyncio

    for attempt in range(max_retries):
        try:
            if params:
                result = await conn.execute(query, params)
            else:
                result = await conn.execute(query)
            return result
        except Exception as e:
            error_msg = str(e).lower()
            if (
                "deadlock" in error_msg
                or "lock timeout" in error_msg
                or "lock wait timeout" in error_msg
            ):
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"Database deadlock detected, retrying in {delay}s",
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "error": str(e),
                        },
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(
                        "Max deadlock retries exceeded",
                        extra={"max_retries": max_retries, "error": str(e)},
                    )
                    raise
            else:
                # Not a deadlock, re-raise immediately
                raise


# ---------------------------------------------------------------------------
# Cached helper functions
# ---------------------------------------------------------------------------


@cached(ttl=300, cache_key_prefix="student_context")
async def _get_student_context_cached(
    student_id: str,
) -> Tuple[float, List[str], List[str], dict]:
    """Cached version of student context retrieval."""
    async with performance_context(f"student_context:{student_id}") as monitor:
        try:
            async with engine.begin() as conn:
                rows = await conn.execute(
                    text(
                        """
                     SELECT c.title, c.reading_level, c.genre
                       FROM checkout co
                       JOIN catalog c USING(book_id)
                      WHERE co.student_id = :sid
                   ORDER BY co.checkout_date DESC LIMIT 30"""
                    ),
                    {"sid": student_id},
                )

            titles: list[str] = []
            levels: list[float] = []
            genres: list[str] = []

            def _level_to_band(g: float | None):
                if g is None:
                    return "Unknown"
                if g < 3.0:
                    return "Elementary"
                elif g < 6.0:
                    return "Middle"
                elif g < 9.0:
                    return "High School"
                else:
                    return "College"

            for row in rows:
                titles.append(row.title)
                if row.reading_level is not None:
                    levels.append(row.reading_level)
                if row.genre:
                    genres.append(row.genre)

                avg_level = sum(levels) / len(levels) if levels else None
                recent_titles = titles[:10]
                top_genres = [genre for genre, _ in Counter(genres).most_common(5)]

                band_hist = dict(Counter(_level_to_band(lv) for lv in levels))

                return avg_level, recent_titles, top_genres, band_hist
        except Exception as e:
            logger.error(
                f"Database error in student context fetch: {e}",
                exc_info=True,
                extra={"student_id": student_id},
            )
            # Return sensible defaults on error
            return None, [], [], {}


@cached(ttl=600, cache_key_prefix="user_uploaded_books")
async def _fetch_user_uploaded_books_cached(user_hash_id: str) -> List[dict]:
    """Cached version of user uploaded books retrieval."""
    async with performance_context(f"user_books:{user_hash_id}") as monitor:
        try:
            async with engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT ub.id as book_id, ub.title, ub.author, ub.isbn, ub.genre, ub.reading_level, 
                           ub.rating as user_rating, ub.read_date, ub.notes, ub.created_at
                    FROM uploaded_books ub
                    JOIN public_users pu ON ub.user_id = pu.id
                    WHERE pu.hash_id = :user_hash_id 
                    ORDER BY ub.created_at DESC
                """
                    ),
                    {"user_hash_id": user_hash_id},
                )
            books = [
                {
                    "book_id": row.book_id,
                    "title": row.title,
                    "author": row.author,
                    "isbn": row.isbn,
                    "genre": row.genre,
                    "reading_level": row.reading_level,
                    "user_rating": row.user_rating,
                    "read_date": row.read_date,
                    "notes": row.notes,
                    "created_at": row.created_at,
                }
                for row in result
            ]
            return books
        except Exception as e:
            logger.error(
                "Database error in user books fetch: %s",
                e,
                extra={"user_hash_id": user_hash_id},
                exc_info=True,
            )
            return []


@cached(ttl=300, cache_key_prefix="user_feedback_scores")
async def _fetch_user_feedback_scores_cached(user_hash_id: str) -> dict:
    """Cached version of user feedback scores retrieval."""
    async with performance_context(f"user_feedback:{user_hash_id}") as monitor:
        try:
            async with engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT f.book_id, f.score, f.created_at
                      FROM feedback f
                      JOIN public_users pu ON f.user_id = pu.id
                     WHERE pu.hash_id = :user_hash_id
                """
                    ),
                    {"user_hash_id": user_hash_id},
                )
            feedback_scores: dict[str, int] = {}
            for row in result:
                feedback_scores[row.book_id] = (
                    feedback_scores.get(row.book_id, 0) + row.score
                )
            return feedback_scores
        except Exception as e:
            logger.error(
                "Database error in feedback scores fetch: %s",
                e,
                extra={"user_hash_id": user_hash_id},
                exc_info=True,
            )
            return {}


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------


async def generate_recommendations(
    student_id: str,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Main orchestration entry-point for the API layer.

    The responsibilities are deliberately *narrow*:

    1. Validate student exists in the database
    2. Fetch & score candidate books (cheap, deterministic).
    3. Build a structured prompt and call the LLM agent (expensive, stochastic).
    4. Parse / validate the agent's answer and persist side-effects.

    All heavy I/O (DB queries, Redis, FAISS) lives in helper modules so we can
    unit-test this function with minimal monkey-patching (see tests).

    Performance optimizations:
    - Cached student context retrieval
    - Async database operations
    - Performance monitoring
    """

    async with performance_context(f"generate_recommendations:{student_id}") as monitor:
        logger.debug(
            "Starting recommendation generation",
            extra={"request_id": request_id, "student_id": student_id},
        )

        # Validate student exists before doing expensive operations
        try:
            async with engine.begin() as conn:
                student_exists = await conn.execute(
                    text(
                        "SELECT 1 FROM students WHERE student_id = :student_id LIMIT 1"
                    ),
                    {"student_id": student_id},
                )
                if not student_exists.fetchone():
                    logger.warning(
                        "Student not found in database",
                        extra={"student_id": student_id, "request_id": request_id},
                    )
                    # Return error recommendation instead of failing
                    return [
                        BookRecommendation(
                            book_id="ERROR",
                            title="Student Not Found",
                            author="",
                            reading_level=0.0,
                            librarian_blurb=f"Student ID '{student_id}' was not found in the system. Please verify the student ID and try again. Available students in the database have IDs like 'S001', 'S002', etc.",
                            justification="",
                        )
                    ], {
                        "agent_duration": 0.0,
                        "tool_count": 0,
                        "tools": [],
                        "error_count": 1,
                        "error": "student_not_found",
                    }
        except Exception as e:
            logger.error(
                "Database error during student validation",
                exc_info=True,
                extra={"student_id": student_id, "request_id": request_id},
            )
            # Continue with recommendation generation even if validation fails

        logger.debug("Starting MCP session", extra={"request_id": request_id})
        start_ts = time.perf_counter()

        async with stdio_client(server_params) as (read, write):
            logger.debug(
                "MCP stdio tunnel established", extra={"request_id": request_id}
            )

            async with ClientSession(
                read, write, read_timeout_seconds=timedelta(seconds=300)
            ) as session:
                await session.initialize()
                logger.debug(
                    "MCP session initialised", extra={"request_id": request_id}
                )

                lc_tools = await load_mcp_tools(session)
                agent = create_react_agent(
                    chat_model, lc_tools, name="BookRecommenderAgent"
                )

                # --- Fetch student context (cached) -------------------------------------------------
                avg_level, recent_titles, top_genres, band_hist = (
                    await _get_student_context_cached(student_id)
                )

                # --- Build candidates & prompt ---------------------------------------------------
                candidates = await build_candidates(student_id, n)
                scored_candidates = await score_candidates(
                    candidates, student_id, query
                )

                prompt_messages = build_student_prompt(
                    student_id=student_id,
                    query=query,
                    candidates=scored_candidates,
                    avg_level=avg_level,
                    recent_titles=recent_titles,
                    top_genres=top_genres,
                    band_hist=band_hist,
                    n=n,
                )

                # --- Ask agent -------------------------------------------------------------------
                logger.debug("Calling LLM agent", extra={"request_id": request_id})
                callbacks = [MetricCallbackHandler()]
                agent_response = await _ask_agent(agent, prompt_messages, callbacks)

                # --- Parse & validate response ---------------------------------------------------
                try:
                    parsed_recs = parser.parse(agent_response)
                    valid_recs = [r for r in parsed_recs if r.book_id != "ERROR"]

                    if not valid_recs:
                        logger.warning(
                            "No valid recommendations parsed",
                            extra={"request_id": request_id},
                        )
                        # Return fallback recommendations
                        valid_recs = await _get_fallback_recommendations(n, engine)

                except Exception as e:
                    logger.error(
                        "Failed to parse agent response",
                        exc_info=True,
                        extra={"request_id": request_id},
                    )
                    valid_recs = await _get_fallback_recommendations(n, engine)

                # --- Mark as recommended (fire-and-forget) ---------------------------------------
                try:
                    book_ids = [r.book_id for r in valid_recs]
                    await mark_recommended(student_id, book_ids, ttl_hours=24)
                except Exception as e:
                    logger.warning(
                        "Failed to mark books as recommended",
                        exc_info=True,
                        extra={"request_id": request_id},
                    )

                # --- Collect metrics -------------------------------------------------------------
                total_duration = time.perf_counter() - start_ts
                cb = callbacks[0] if callbacks else MetricCallbackHandler()

                metrics = {
                    "agent_duration": total_duration,
                    "tool_count": len(cb.tools_used),
                    "tools": cb.tools_used,
                    "error_count": cb.error_count,
                    "cache_enabled": True,
                    "performance_optimized": True,
                }

                logger.info(
                    "Recommendation generation completed",
                    extra={
                        "request_id": request_id,
                        "student_id": student_id,
                        "duration_sec": total_duration,
                        "recommendations_count": len(valid_recs),
                        "tools_used": cb.tools_used,
                        "error_count": cb.error_count,
                    },
                )

                return valid_recs, metrics


# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------


async def _fetch_user_uploaded_books(user_hash_id: str, conn) -> List[dict]:
    """Fetch user's uploaded books from database."""
    uploaded_books_result = await conn.execute(
        text(
            """
            SELECT ub.title, ub.author, ub.rating, ub.notes, ub.raw_payload
            FROM uploaded_books ub
            JOIN public_users pu ON ub.user_id = pu.id
            WHERE pu.hash_id = :user_hash_id 
            ORDER BY ub.created_at DESC 
            LIMIT :limit
        """
        ),
        {"user_hash_id": user_hash_id, "limit": reader_config.MAX_UPLOADED_BOOKS},
    )

    uploaded_books = []
    for row in uploaded_books_result.fetchall():
        # Extract genre and reading_level from raw_payload if available
        raw_payload = row.raw_payload or {}
        uploaded_books.append(
            {
                "title": row.title,
                "author": row.author,
                "genre": raw_payload.get("genre"),
                "reading_level": raw_payload.get("reading_level"),
                "rating": row.rating,
                "notes": row.notes,
            }
        )

    return uploaded_books


async def _fetch_user_feedback_scores(user_hash_id: str, conn) -> dict:
    """Fetch user's feedback scores from database."""
    feedback_result = await conn.execute(
        text(
            """
            SELECT f.book_id, f.score 
            FROM feedback f
            JOIN public_users pu ON f.user_id = pu.id
            WHERE pu.hash_id = :user_hash_id
        """
        ),
        {"user_hash_id": user_hash_id},
    )

    feedback_scores = {}
    for row in feedback_result.fetchall():
        book_id = row.book_id
        score = row.score
        feedback_scores[book_id] = feedback_scores.get(book_id, 0) + score

    return feedback_scores


async def _get_recently_recommended_books(user_hash_id: str, conn, hours: int = 24) -> set:
    """Get books recommended to user within the last N hours to prevent immediate duplicates.
    
    This implements a recommendation cooldown period to improve diversity and avoid
    showing the same books repeatedly. The cooldown can be configured via the hours parameter.
    """
    try:
        recent_result = await conn.execute(
            text(
                """
                SELECT DISTINCT rh.book_id 
                FROM recommendation_history rh
                JOIN public_users pu ON rh.user_id = pu.id::text
                WHERE pu.hash_id = :user_hash_id 
                AND rh.created_at > NOW() - INTERVAL '24 hours'
                """
            ),
            {"user_hash_id": user_hash_id}
        )
        
        recently_recommended = {row.book_id for row in recent_result.fetchall()}
        logger.debug(
            "Retrieved recently recommended books",
            extra={
                "user_hash_id": user_hash_id,
                "count": len(recently_recommended),
                "cooldown_hours": hours
            }
        )
        return recently_recommended
        
    except Exception as e:
        # Don't fail recommendations if recent history lookup fails
        logger.warning(
            "Failed to get recently recommended books, proceeding without deduplication",
            extra={
                "user_hash_id": user_hash_id,
                "error": str(e)
            }
        )
        return set()


async def _fetch_similar_books(
    uploaded_books: List[dict], user_hash_id: str, conn
) -> List[dict]:
    """Fetch similar books from catalog based on user's uploaded books."""
    user_genres = [book["genre"] for book in uploaded_books if book.get("genre")]
    user_authors = [book["author"] for book in uploaded_books if book.get("author")]

    # Build dynamic query based on available data
    where_conditions = []
    query_params = {"user_hash_id": user_hash_id}

    if user_genres:
        where_conditions.append("c.genre = ANY(:user_genres)")
        query_params["user_genres"] = user_genres

    if user_authors:
        where_conditions.append("c.author = ANY(:user_authors)")
        query_params["user_authors"] = user_authors

    # If no specific criteria, get popular books
    if not where_conditions:
        where_conditions.append(
            f"c.average_rating > {reader_config.MIN_RATING_THRESHOLD}"
        )

    similar_books_result = await conn.execute(
        text(
            f"""
            SELECT DISTINCT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                   c.description as librarian_blurb, c.isbn, c.average_rating
            FROM catalog c
            WHERE {' OR '.join(where_conditions)}
            ORDER BY c.average_rating DESC NULLS LAST, c.book_id
            LIMIT :limit
        """
        ),
        {**query_params, "limit": reader_config.MAX_SIMILAR_BOOKS},
    )

    similar_books = []
    for row in similar_books_result.fetchall():
        similar_books.append(
            {
                "book_id": row.book_id,
                "title": row.title,
                "author": row.author,
                "genre": row.genre,
                "reading_level": row.reading_level or 5.0,
                "librarian_blurb": row.librarian_blurb,
                "isbn": row.isbn,
                "average_rating": row.average_rating,
            }
        )

    return similar_books


def _calculate_similarity_score(
    book: dict, uploaded_books: List[dict], feedback_scores: dict, query: str = ""
) -> float:
    """Calculate similarity score between a catalog book and user's uploaded books."""
    similarity_score = 0.0

    # Genre matching (configurable weight)
    book_genre = book["genre"]
    for uploaded_book in uploaded_books:
        if uploaded_book.get("genre") == book_genre:
            similarity_score += reader_config.GENRE_MATCH_WEIGHT

    # Author matching (configurable weight)
    book_author = book["author"]
    for uploaded_book in uploaded_books:
        if uploaded_book.get("author") == book_author:
            similarity_score += reader_config.AUTHOR_MATCH_WEIGHT

    # Reading level proximity (configurable weights based on closeness)
    book_level = book["reading_level"]
    user_avg_level = sum(
        (book.get("reading_level") or 5.0) for book in uploaded_books
    ) / len(uploaded_books)

    level_diff = abs(book_level - user_avg_level)
    if level_diff <= 1.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_HIGH
    elif level_diff <= 2.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_MEDIUM
    elif level_diff <= 3.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_LOW

    # Apply feedback score adjustment (configurable weight)
    book_id = book["book_id"]
    if reader_config.ENABLE_FEEDBACK_SCORING and book_id in feedback_scores:
        similarity_score += feedback_scores[book_id] * reader_config.FEEDBACK_WEIGHT

    # Boost highly rated uploaded books' similar titles (configurable weight)
    for uploaded_book in uploaded_books:
        if uploaded_book.get("rating", 0) >= 4:
            if (
                uploaded_book.get("genre") == book_genre
                or uploaded_book.get("author") == book_author
            ):
                similarity_score += reader_config.RATING_BOOST_WEIGHT

    # Query matching if provided (configurable weight)
    if reader_config.ENABLE_QUERY_MATCHING and query:
        query_lower = query.lower()
        if (
            query_lower in (book["title"] or "").lower()
            or query_lower in (book["author"] or "").lower()
            or query_lower in (book["genre"] or "").lower()
        ):
            similarity_score += reader_config.QUERY_MATCH_WEIGHT

    return similarity_score


def _create_justification(
    book: dict, uploaded_books: List[dict], feedback_scores: dict, semantic_boost: bool = False
) -> str:
    """Create justification text for why a book was recommended."""
    justification_parts = []

    # Check what made this book similar
    for uploaded_book in uploaded_books:
        if uploaded_book.get("genre") == book["genre"]:
            justification_parts.append(f"Similar genre ({book['genre']})")
            break

    for uploaded_book in uploaded_books:
        if uploaded_book.get("author") == book["author"]:
            justification_parts.append(f"Same author ({book['author']})")
            break

    book_id = book["book_id"]
    if book_id in feedback_scores and feedback_scores[book_id] > 0:
        justification_parts.append("Based on your positive feedback")

    # Add semantic search indication if this was found via semantic search
    if semantic_boost:
        justification_parts.append("Semantically similar to your books")

    if not justification_parts:
        justification_parts.append("Matches your reading preferences")

    return f"Recommended because: {', '.join(justification_parts)}"


async def _get_fallback_recommendations(n: int, conn) -> List[dict]:
    """Get popular books as fallback when not enough similar books found."""
    fallback_result = await conn.execute(
        text(
            """
            SELECT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                   c.description as librarian_blurb
            FROM catalog c
            WHERE c.average_rating > :threshold
            ORDER BY c.average_rating DESC NULLS LAST
            LIMIT :n
        """
        ),
        {"n": n, "threshold": reader_config.FALLBACK_RATING_THRESHOLD},
    )

    fallback_books = []
    for row in fallback_result.fetchall():
        fallback_books.append(
            {
                "book_id": row.book_id,
                "title": row.title,
                "author": row.author,
                "genre": row.genre,
                "reading_level": row.reading_level or 5.0,
                "librarian_blurb": row.librarian_blurb,
            }
        )

    return fallback_books


async def _get_fallback_book_recommendations(n: int, conn) -> List[BookRecommendation]:
    """Get popular books as fallback and convert to BookRecommendation objects."""
    fallback_result = await conn.execute(
        text(
            """
            SELECT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                   c.description as librarian_blurb
            FROM catalog c
            WHERE c.average_rating > :threshold
            ORDER BY c.average_rating DESC NULLS LAST
            LIMIT :n
        """
        ),
        {"n": n, "threshold": reader_config.FALLBACK_RATING_THRESHOLD},
    )

    fallback_books = []
    for row in fallback_result.fetchall():
        fallback_books.append(
            BookRecommendation(
                book_id=row.book_id,
                title=row.title or "Unknown Title",
                author=row.author or "Unknown Author",
                reading_level=row.reading_level or 5.0,
                librarian_blurb=row.librarian_blurb or "No description available",
                justification="Popular book recommendation based on high ratings",
            )
        )

    return fallback_books


async def generate_reader_recommendations(
    user_hash_id: str,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Generate personalized book recommendations for a reader based on their uploaded books."""

    logger.info(
        "Starting reader recommendation generation",
        extra={
            "request_id": request_id,
            "user_hash_id": user_hash_id,
            "n": n,
            "query": query,
        },
    )

    start_ts = time.perf_counter()

    try:
        async with engine.begin() as conn:
            # Fetch user's uploaded books
            uploaded_books = await _fetch_user_uploaded_books(user_hash_id, conn)

            if not uploaded_books:
                logger.warning(
                    "No uploaded books found for user",
                    extra={"user_hash_id": user_hash_id},
                )
                return [], {
                    "agent_duration": 0.0,
                    "tool_count": 0,
                    "tools": [],
                    "error_count": 1,
                    "error": "no_uploaded_books",
                    "based_on_books": [],
                }

            # Fetch user's feedback history
            feedback_scores = await _fetch_user_feedback_scores(user_hash_id, conn)

            # Get recently recommended books to avoid immediate duplicates
            # This implements a cooldown period to improve recommendation diversity
            recently_recommended = await _get_recently_recommended_books(user_hash_id, conn)
            logger.info(
                "Recent recommendation filter applied",
                extra={
                    "user_hash_id": user_hash_id,
                    "recently_recommended_count": len(recently_recommended),
                    "cooldown_hours": 24  # Current cooldown period
                }
            )

            # Get semantic candidates using FAISS vector search
            semantic_candidates = await _semantic_book_candidates_reader(
                uploaded_books, feedback_scores, user_hash_id, k=30
            )
            
            # Get query-based semantic candidates if query provided
            query_candidates = []
            if query.strip():
                query_candidates = await _query_based_semantic_candidates_reader(
                    query, uploaded_books, feedback_scores, user_hash_id, k=15
                )
            
            # Combine all candidate book IDs and filter out recently recommended books
            all_candidate_ids = list(set(semantic_candidates + query_candidates))
            
            # Apply cooldown filter to prevent immediate duplicates
            if recently_recommended:
                original_count = len(all_candidate_ids)
                all_candidate_ids = [book_id for book_id in all_candidate_ids 
                                   if book_id not in recently_recommended]
                filtered_count = original_count - len(all_candidate_ids)
                if filtered_count > 0:
                    logger.info(
                        "Filtered out recently recommended books",
                        extra={
                            "user_hash_id": user_hash_id,
                            "filtered_count": filtered_count,
                            "remaining_candidates": len(all_candidate_ids)
                        }
                    )
            
            # Fetch full book details for candidates first
            candidates = []
            if all_candidate_ids:
                # Secure parameterized query construction
                if len(all_candidate_ids) > 1000:  # Prevent oversized queries
                    logger.warning(
                        "Too many candidate IDs, truncating to 1000",
                        extra={"user_hash_id": user_hash_id, "count": len(all_candidate_ids)}
                    )
                    all_candidate_ids = all_candidate_ids[:1000]
                
                # Use ANY() for secure parameterized query
                candidate_result = await conn.execute(
                    text(
                        """
                        SELECT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                               c.description as librarian_blurb, c.isbn, c.average_rating
                        FROM catalog c
                        WHERE c.book_id = ANY(:book_ids)
                        """
                    ),
                    {"book_ids": all_candidate_ids}
                )
                
                for row in candidate_result.fetchall():
                    book = {
                        "book_id": row.book_id,
                        "title": row.title,
                        "author": row.author,
                        "genre": row.genre,
                        "reading_level": row.reading_level or 5.0,
                        "librarian_blurb": row.librarian_blurb,
                        "isbn": row.isbn,
                        "average_rating": row.average_rating,
                    }
                    candidates.append(book)
            
            # Now filter out user's uploaded books from the full candidate list
            original_candidate_count = len(candidates)
            candidates = await _filter_out_user_books(candidates, uploaded_books, user_hash_id)
            filtered_count = original_candidate_count - len(candidates)
            
            if filtered_count > 0:
                logger.info(
                    "Applied user book filtering",
                    extra={
                        "user_hash_id": user_hash_id,
                        "original_candidates": original_candidate_count,
                        "filtered_candidates": len(candidates),
                        "filtered_count": filtered_count
                    }
                )
                
                for row in candidate_result.fetchall():
                    book = {
                        "book_id": row.book_id,
                        "title": row.title,
                        "author": row.author,
                        "genre": row.genre,
                        "reading_level": row.reading_level or 5.0,
                        "librarian_blurb": row.librarian_blurb,
                        "isbn": row.isbn,
                        "average_rating": row.average_rating,
                    }
                    
                    # Calculate semantic similarity score
                    similarity_score = _calculate_similarity_score(
                        book, uploaded_books, feedback_scores, query
                    )
                    
                    # Boost score for semantic candidates
                    if book["book_id"] in semantic_candidates:
                        similarity_score += 2.0  # Semantic boost
                    if book["book_id"] in query_candidates:
                        similarity_score += 1.5  # Query boost
                        
                    book["similarity_score"] = similarity_score
                    candidates.append(book)
            
            # If no semantic candidates, fall back to traditional similarity
            if not candidates:
                logger.info(
                    "No semantic candidates found, falling back to traditional similarity",
                    extra={"user_hash_id": user_hash_id}
                )
                similar_books = await _fetch_similar_books(
                    uploaded_books, user_hash_id, conn
                )
                
                for book in similar_books:
                    similarity_score = _calculate_similarity_score(
                        book, uploaded_books, feedback_scores, query
                    )
                    book["similarity_score"] = similarity_score
                    candidates.append(book)

            # Sort by similarity score and take top N
            candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
            top_candidates = candidates[:n]

            # If we don't have enough recommendations, add popular books
            if len(top_candidates) < n:
                needed = n - len(top_candidates)
                fallback_books = await _get_fallback_recommendations(needed, conn)

                # Add fallback books with default similarity score
                for book in fallback_books:
                    book["similarity_score"] = reader_config.FALLBACK_SCORE
                    top_candidates.append(book)

            # Build final recommendations
            recommendations = []
            seen_book_ids = set()
            for candidate in top_candidates:
                if candidate["book_id"] in seen_book_ids:
                    continue  # Skip duplicates
                
                seen_book_ids.add(candidate["book_id"])
                
                # Check if this was a semantic candidate
                semantic_boost = candidate["book_id"] in semantic_candidates
                
                justification = _create_justification(
                    candidate, uploaded_books, feedback_scores, semantic_boost
                )

                recommendation = BookRecommendation(
                    book_id=candidate["book_id"],
                    title=candidate["title"] or "Unknown Title",
                    author=candidate["author"] or "Unknown Author",
                    reading_level=candidate["reading_level"],
                    librarian_blurb=candidate["librarian_blurb"]
                    or "No description available",
                    justification=justification,
                )
                recommendations.append(recommendation)

            # Calculate metrics
            duration = time.perf_counter() - start_ts

            # Based on books for metadata
            based_on_books = [
                f"{book['title']} by {book['author']}" for book in uploaded_books[:3]
            ]

            logger.info(
                "Reader recommendation generation completed",
                extra={
                    "request_id": request_id,
                    "user_hash_id": user_hash_id,
                    "recommendations_count": len(recommendations),
                    "duration_sec": duration,
                    "based_on_books_count": len(uploaded_books),
                },
            )

            # Try to enhance recommendations with LLM microservice
            final_recommendations = recommendations
            llm_enhanced = False
            
            if S.llm_service_enabled and recommendations:
                try:
                    # Prepare context for LLM enrichment
                    user_context = {
                        "user_hash_id": user_hash_id,
                        "uploaded_books": uploaded_books,
                        "feedback_scores": feedback_scores
                    }
                    
                    # Convert BookRecommendation objects to dictionaries for LLM processing
                    rec_dicts = []
                    for rec in recommendations:
                        rec_dict = {
                            "book_id": rec.book_id,
                            "title": rec.title,
                            "author": rec.author,
                            "reading_level": rec.reading_level,
                            "genre": getattr(rec, 'genre', 'Unknown'),
                            "justification": rec.justification
                        }
                        rec_dicts.append(rec_dict)
                    
                    # Enhance with LLM
                    enhanced_rec_dicts = await enrich_recommendations_with_llm(
                        rec_dicts, user_context, query, request_id
                    )
                    
                    # Convert back to BookRecommendation objects
                    final_recommendations = []
                    seen_book_ids = set()
                    for enhanced_dict in enhanced_rec_dicts:
                        if enhanced_dict["book_id"] in seen_book_ids:
                            continue  # Skip duplicates
                        
                        seen_book_ids.add(enhanced_dict["book_id"])
                        
                        # Check if this was a semantic candidate
                        semantic_boost = enhanced_dict["book_id"] in semantic_candidates
                        
                        # Recreate justification with semantic boost info
                        book_dict = {
                            "book_id": enhanced_dict["book_id"],
                            "title": enhanced_dict["title"],
                            "author": enhanced_dict["author"],
                            "genre": enhanced_dict.get("genre", "Unknown"),
                        }
                        justification = _create_justification(
                            book_dict, uploaded_books, feedback_scores, semantic_boost
                        )
                        
                        enhanced_rec = BookRecommendation(
                            book_id=enhanced_dict["book_id"],
                            title=enhanced_dict["title"],
                            author=enhanced_dict["author"],
                            reading_level=enhanced_dict["reading_level"],
                            librarian_blurb=enhanced_dict.get("librarian_blurb", "No description available"),
                            justification=justification
                        )
                        final_recommendations.append(enhanced_rec)
                    
                    llm_enhanced = True
                    logger.info(
                        "Successfully enhanced reader recommendations with LLM",
                        extra={"request_id": request_id, "user_hash_id": user_hash_id}
                    )
                    
                except LLMServiceError as e:
                    logger.warning(
                        f"LLM service failed, using base recommendations: {e}",
                        extra={"request_id": request_id, "user_hash_id": user_hash_id}
                    )
                except Exception as e:
                    logger.error(
                        f"Unexpected error during LLM enhancement: {e}",
                        extra={"request_id": request_id, "user_hash_id": user_hash_id},
                        exc_info=True
                    )

            return final_recommendations, {
                "agent_duration": duration,
                "tool_count": 1,
                "tools": ["semantic_search"] + (["llm_enhancement"] if llm_enhanced else []),
                "error_count": 0,
                "based_on_books": based_on_books,
                "llm_enhanced": llm_enhanced,
                "semantic_candidates": len(semantic_candidates),
                "query_candidates": len(query_candidates),
                "total_candidates": len(candidates),
            }

    except Exception as exc:
        duration = time.perf_counter() - start_ts
        logger.error(
            "Reader recommendation generation failed",
            extra={
                "request_id": request_id,
                "user_hash_id": user_hash_id,
                "duration_sec": duration,
                "error": str(exc),
            },
            exc_info=True,
        )

        return [], {
            "agent_duration": duration,
            "tool_count": 0,
            "tools": [],
            "error_count": 1,
            "error": str(exc),
            "based_on_books": [],
        }


async def _ask_agent(agent, prompt_messages, callbacks=None):
    """Run the LangChain agent and return **only** the final AI message."""
    cfg = {"callbacks": callbacks} if callbacks else None
    response = await agent.ainvoke({"messages": prompt_messages}, config=cfg)
    final_msg = next(
        m for m in reversed(response["messages"]) if isinstance(m, AIMessage)
    )
    return final_msg


async def generate_agent_recommendations(
    mode: str,  # "student" or "reader"
    context: dict,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Unified agent-based recommendation function for both Student and Reader modes."""
    logger.info(
        "Starting agent-based recommendation",
        extra={"request_id": request_id, "mode": mode, "n": n, "query": query},
    )
    start_ts = time.perf_counter()

    try:
        # Setup MCP session and tools (same as existing student function)
        async with stdio_client(server_params) as (read, write):
            logger.debug(
                "MCP stdio tunnel established", extra={"request_id": request_id}
            )

            async with ClientSession(
                read, write, read_timeout_seconds=timedelta(seconds=300)
            ) as session:
                await session.initialize()
                logger.debug(
                    "MCP session initialised", extra={"request_id": request_id}
                )

                lc_tools = await load_mcp_tools(session)
                agent = create_react_agent(
                    chat_model, lc_tools, name="BookRecommenderAgent"
                )

                # Build prompt based on mode
                if mode == "student":
                    prompt_messages = build_student_prompt(
                        student_id=context["student_id"],
                        query=query,
                        candidates=context["candidates"],
                        avg_level=context["avg_level"],
                        recent_titles=context["recent_titles"],
                        top_genres=context["top_genres"],
                        band_hist=context["band_hist"],
                        n=n,
                    )
                elif mode == "reader":
                    prompt_messages = build_reader_prompt(
                        user_hash_id=context["user_hash_id"],
                        query=query,
                        uploaded_books=context["uploaded_books"],
                        feedback_scores=context["feedback_scores"],
                        candidates=context["candidates"],
                        n=n,
                    )
                else:
                    raise ValueError(f"Unknown mode: {mode}")

                # Ask agent
                logger.debug("Calling LLM agent", extra={"request_id": request_id})
                callbacks = [MetricCallbackHandler()]
                final_msg = await _ask_agent(agent, prompt_messages, callbacks)

                # Parse agent response text
                try:
                    parsed = parser.parse(final_msg.content)
                    valid_recs = [
                        BookRecommendation(**r.model_dump())
                        for r in parsed.recommendations
                    ]

                    # Deduplicate recommendations by book_id
                    seen_book_ids = set()
                    deduplicated_recs = []
                    for rec in valid_recs:
                        if rec.book_id not in seen_book_ids:
                            seen_book_ids.add(rec.book_id)
                            deduplicated_recs.append(rec)
                    
                    valid_recs = deduplicated_recs

                    if not valid_recs:
                        logger.warning(
                            "No valid recommendations parsed",
                            extra={"request_id": request_id},
                        )
                        async with engine.begin() as conn:
                            valid_recs = await _get_fallback_book_recommendations(
                                n, conn
                            )
                except Exception:
                    logger.error(
                        "Failed to parse agent response",
                        exc_info=True,
                        extra={"request_id": request_id},
                    )
                    async with engine.begin() as conn:
                        valid_recs = await _get_fallback_book_recommendations(n, conn)

                # Mark as recommended (fire-and-forget) for student mode
                if mode == "student":
                    try:
                        book_ids = [r.book_id for r in valid_recs]
                        await mark_recommended(
                            context["student_id"], book_ids, ttl_hours=24
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to mark books as recommended",
                            exc_info=True,
                            extra={"request_id": request_id},
                        )

                # Collect metrics
                duration = time.perf_counter() - start_ts
                cb = callbacks[0] if callbacks else MetricCallbackHandler()

                metrics = {
                    "agent_duration": duration,
                    "tool_count": len(cb.tools_used),
                    "tools": cb.tools_used,
                    "error_count": cb.error_count,
                    "mode": mode,
                    "cache_enabled": True,
                    "performance_optimized": True,
                }

                logger.info(
                    "Agent-based recommendation completed",
                    extra={
                        "request_id": request_id,
                        "mode": mode,
                        "duration_sec": duration,
                        "recommendations_count": len(valid_recs),
                        "tools_used": cb.tools_used,
                        "error_count": cb.error_count,
                    },
                )

                return valid_recs, metrics

    except Exception as exc:
        duration = time.perf_counter() - start_ts
        logger.error(
            "Agent-based recommendation failed",
            extra={
                "request_id": request_id,
                "mode": mode,
                "duration_sec": duration,
                "error": str(exc),
            },
            exc_info=True,
        )

        # Return fallback recommendations on error
        try:
            async with engine.begin() as conn:
                fallback_recs = await _get_fallback_book_recommendations(n, conn)
                return fallback_recs, {
                    "agent_duration": duration,
                    "tool_count": 0,
                    "tools": [],
                    "error_count": 1,
                    "error": str(exc),
                    "mode": mode,
                }
        except Exception as fallback_error:
            logger.error(
                "Failed to get fallback recommendations",
                exc_info=True,
                extra={"request_id": request_id},
            )
            return [], {
                "agent_duration": duration,
                "tool_count": 0,
                "tools": [],
                "error_count": 1,
                "error": str(exc),
                "mode": mode,
            }
