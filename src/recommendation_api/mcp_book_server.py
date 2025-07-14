"""
FastMCP registry for book-domain tooling.

Exposes:
• search_catalog                         – vector keyword search (unchanged)
• enrich_book_metadata(isbn)             – Google Books → Open Library → readability fallback
• get_student_reading_level(student_id)  – average reading level + confidence
• find_similar_students(student_id, k)   – top-k neighbours from student_similarity
• get_book_recommendations_for_group(student_ids, n) – simple group-based recs

All tools are **read-only** w.r.t. external world and safe for parallel calls.
"""

import asyncio, asyncpg, sys, logging
from pathlib import Path
from typing import List

from fastmcp import FastMCP
from asyncio_throttle import Throttler

from common.settings import settings as S
from common.structured_logging import get_logger
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS

# --------------------------------------------------------------------- #
mcp = FastMCP("book")

logger = get_logger(__name__)

# vector store
emb = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)
# Vector store cache and load failure tracking
vec_store = None
# Timestamp of the most-recent failed load attempt (epoch seconds). None until first failure.
_last_failed_load: float | None = None
# Cool-down (in seconds) before we attempt to load the FAISS index again after a failure.
_LOAD_RETRY_INTERVAL_SEC = 60  # 1 minute – adjust via env var later if needed.


def _store():
    """Return the cached FAISS store.

    To avoid hammering the filesystem (or spamming logs) if the index is
    missing/corrupt, we back-off after a failure for a short interval.
    """
    global vec_store, _last_failed_load

    # Short-circuit if we've already cached the store.
    if vec_store is not None:
        return vec_store

    # Respect cool-down after a previous failure.
    import time

    if _last_failed_load and time.time() - _last_failed_load < _LOAD_RETRY_INTERVAL_SEC:
        # Quickly raise rather than re-attempting disk I/O.
        raise RuntimeError(
            "Vector store unavailable – last load failure still within cool-down window"
        )

    logger.info("Loading FAISS vector store")
    try:
        vec_store = FAISS.load_local(
            Path(S.data_dir) / "vector_store", emb, allow_dangerous_deserialization=True
        )
        logger.info("FAISS vector store loaded successfully")
        return vec_store

    except Exception:
        # Record failure timestamp and propagate.
        import time as _time

        _last_failed_load = _time.time()
        logger.error("Failed to load FAISS vector store", exc_info=True)
        raise


# pg connection pool (lazy)
_pg_pool: asyncpg.Pool | None = None


async def _pg() -> asyncpg.pool.Pool:
    global _pg_pool
    if _pg_pool is None:
        logger.info("Creating database connection pool")
        try:
            url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
            _pg_pool = await asyncpg.create_pool(url, min_size=1, max_size=10)
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error("Failed to create database connection pool", exc_info=True)
            raise
    return _pg_pool


# rate-limiters for external APIs
gb_throttler = Throttler(rate_limit=2, period=60)  # 2 rpm
ol_throttler = Throttler(rate_limit=4, period=60)  # 4 rpm
logger.info(
    "Rate limiters initialized", extra={"google_books_rpm": 2, "open_library_rpm": 4}
)


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Search Catalog by Keyword",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def search_catalog(keyword: str, k: int = 5):
    """
    Vector keyword search across catalog title+description.

    Performs semantic search across book titles and descriptions using embeddings.
    Returns the most relevant books based on keyword similarity.

    Args:
        keyword (str): Search terms to find relevant books
        k (int): Maximum number of results to return (default: 5)

    Returns:
        List[dict]: Book search results with format:
            [{"book_id": "B001", "snippet": "Title. Description preview..."}]

    Usage:
        - Use this for finding books matching specific topics or themes
        - Results are ranked by semantic similarity, not exact keyword matching
        - Snippets are truncated to 200 characters for context

    Examples:
        search_catalog("space adventure", 3) -> books about space adventures
        search_catalog("friendship animals", 5) -> books about friendship with animals
    """
    logger.info("Catalog search requested", extra={"keyword": keyword, "k": k})

    try:
        docs = _store().similarity_search(keyword, k=k)
        results = [
            {"book_id": d.metadata["book_id"], "snippet": d.page_content[:200]}
            for d in docs
        ]

        logger.info(
            "Catalog search completed",
            extra={"keyword": keyword, "k": k, "results_count": len(results)},
        )

        return results

    except Exception as e:
        logger.error(
            "Catalog search failed", exc_info=True, extra={"keyword": keyword, "k": k}
        )
        raise


# --------------------------------------------------------------------- #
# @mcp.tool(annotations={"title": "Enrich Book Metadata", "readOnlyHint": True})
# async def enrich_book_metadata(isbn: str):
#     """
#     Fetch page_count & publication_year using Google Books (primary) or
#     Open Library (fallback). If still missing, estimate difficulty_band
#     via readability on the stored description.
#     """
#     logger.info("Book metadata enrichment requested", extra={"isbn": isbn})

#     try:
#         # Try Google Books first
#         logger.debug("Attempting Google Books enrichment", extra={"isbn": isbn})
#         async with gb_throttler:
#             meta = await fetch_google_books_meta(isbn)

#             # Handle None result from Google Books API
#             if meta is None:
#                 meta = {}

#             logger.debug("Google Books enrichment result", extra={
#                 "isbn": isbn,
#                 "has_page_count": bool(meta.get("page_count")),
#                 "has_publication_year": bool(meta.get("publication_year"))
#             })

#             # Fallback to Open Library if needed
#             if not meta.get("page_count") or not meta.get("publication_year"):
#                 logger.debug("Attempting Open Library enrichment", extra={"isbn": isbn})
#                 async with ol_throttler:
#                     ol_meta = await fetch_open_library_meta(isbn)
#                     # Handle None result from Open Library API
#                     if ol_meta is None:
#                         ol_meta = {}
#                     meta |= ol_meta

#                 logger.debug("Open Library enrichment result", extra={
#                     "isbn": isbn,
#                     "has_page_count": bool(meta.get("page_count")),
#                     "has_publication_year": bool(meta.get("publication_year"))
#                 })

#             # Readability fallback if still missing
#             if not meta.get("page_count") or not meta.get("publication_year"):
#                 logger.debug("Using readability formula fallback", extra={"isbn": isbn})
#                 pool = await _pg()
#                 async with pool.acquire() as con:
#                     desc = await con.fetchval("SELECT description FROM catalog WHERE isbn=$1", isbn)
#                     readability_meta = readability_formula_estimator(desc or "")
#                     meta |= readability_meta

#                     logger.debug("Readability fallback result", extra={
#                         "isbn": isbn,
#                         "has_difficulty_band": bool(readability_meta.get("difficulty_band"))
#                     })

#             logger.info("Book metadata enrichment completed", extra={
#                 "isbn": isbn,
#                 "page_count": meta.get("page_count"),
#                 "publication_year": meta.get("publication_year"),
#                 "difficulty_band": meta.get("difficulty_band")
#             })

#         return meta

#     except Exception as e:
#         logger.error("Book metadata enrichment failed", exc_info=True, extra={"isbn": isbn})
#         raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Compute Student Reading Level",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def get_student_reading_level(student_id: str):
    """
    Compute student reading level using checkout history and academic data.

    This tool analyzes how well a student reads by:
    1. PRIMARY: Averaging reading levels from their recent book checkouts
    2. FALLBACK: Using their grade level ± EOG test score adjustment
    3. SAFETY: Never returning impossible values (negative levels)

    Parameters:
        student_id (str): Student identifier (e.g., 'S001'). Must exist in students table.

    Returns:
        dict: Reading level analysis with fields:
            - avg_reading_level (float): Computed reading level (e.g., 4.2 for grade 4.2)
            - confidence (float): Confidence score 0.0-1.0 (higher = more reliable)
            - method (str): How level was computed: 'checkout_history', 'eog_fallback', or 'error_fallback'
            - books_used (int): Number of books used for checkout_history method
            - eog_score (float): EOG score used for eog_fallback method
            - error (str): Error code if computation failed

    Usage Instructions:
        - Use this to understand a student's current reading capability
        - Higher confidence scores (>0.5) indicate more reliable estimates
        - checkout_history method is most accurate, eog_fallback is less reliable
        - If method='error_fallback', investigate the error field and try alternative approaches

    Error Handling:
        - If student_not_found: Verify student_id exists in database
        - If database_error: Check database connectivity and retry
        - If confidence < 0.3: Consider using grade-level books as safe default
        - For any errors: Fall back to grade-appropriate books (grade 4 = reading level 4.0)

    Agent Guidelines:
        - Use this to select appropriately challenging books for recommendations
        - Understand student reading capability and progress
        - Make informed decisions about book difficulty
        - If this tool returns an error, use the student's grade level as reading level
        - For grade 4 students, assume reading level 4.0 as safe default
        - Low confidence (<0.3) suggests limited data - recommend popular grade-level books
    """
    logger.info(
        "Student reading level computation requested", extra={"student_id": student_id}
    )

    try:
        from common.reading_level_utils import get_student_reading_level_from_db

        pool = await _pg()
        result = await get_student_reading_level_from_db(
            student_id=student_id,
            db_pool=pool,
            recent_limit=10,  # Consider last 10 books
        )

        logger.info(
            "Student reading level tool completed",
            extra={
                "student_id": student_id,
                "method": result.get("method"),
                "confidence": result.get("confidence"),
                "avg_reading_level": result.get("avg_reading_level"),
            },
        )

        return result

    except ImportError as e:
        logger.error(
            "Reading level utils import failed",
            exc_info=True,
            extra={"student_id": student_id},
        )
        return {
            "avg_reading_level": 4.0,
            "confidence": 0.0,
            "method": "import_error_fallback",
            "error": "module_import_failed",
            "error_details": str(e),
            "student_id": student_id,
        }
    except Exception as e:
        logger.error(
            "Unexpected error in reading level tool",
            exc_info=True,
            extra={"student_id": student_id},
        )
        return {
            "avg_reading_level": 4.0,
            "confidence": 0.0,
            "method": "unexpected_error_fallback",
            "error": "unexpected_error",
            "error_details": str(e),
            "student_id": student_id,
        }


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Find Similar Students",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def find_similar_students(student_id: str, limit: int = 5):
    """
    Return up to `limit` other student_ids with highest similarity score.

    Finds students with similar reading preferences based on cosine similarity
    of their reading history embeddings. Higher similarity scores indicate
    more similar reading patterns.

    Args:
        student_id (str): Target student to find similarities for
        limit (int): Maximum number of similar students to return (default: 5)

    Returns:
        List[dict]: Similar students with format:
            [{"student_id": "S002", "similarity": 0.85}]
            Sorted by similarity score (descending)

    Usage:
        - Use for collaborative filtering recommendations
        - Find students who might like similar books
        - Understand reading preference clusters
        - Higher similarity scores (>0.7) indicate very similar preferences

    Agent Guidelines:
        - Use similar students' favorite books for recommendations
        - Weight recommendations by similarity scores
        - If no similar students found, fall back to popular books for grade level
    """
    logger.info(
        "Similar students search requested",
        extra={"student_id": student_id, "limit": limit},
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """SELECT b, sim FROM student_similarity
                    WHERE a=$1 ORDER BY sim DESC LIMIT $2""",
                student_id,
                limit,
            )

            results = [
                {"student_id": r["b"], "similarity": float(r["sim"])} for r in rows
            ]

            logger.info(
                "Similar students search completed",
                extra={
                    "student_id": student_id,
                    "limit": limit,
                    "results_count": len(results),
                    "top_similarity": results[0]["similarity"] if results else None,
                },
            )

            return results

    except Exception as e:
        logger.error(
            "Similar students search failed",
            exc_info=True,
            extra={"student_id": student_id, "limit": limit},
        )
        raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Group-Based Book Recommendations",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def get_book_recommendations_for_group(student_ids: List[str], n: int = 3):
    """
    Generate book recommendations for a group of students.

    Very simple heuristic: pick top-rated books among the group that no
    member has checked out yet. Useful for classroom reading assignments
    or book clubs.

    Args:
        student_ids (List[str]): List of student IDs in the group
        n (int): Number of book recommendations to return (default: 3)

    Returns:
        List[dict]: Book recommendations with format:
            [{"book_id": "B001", "title": "Book Title", "average_rating": 4.5}]
            Ordered by average rating descending

    Usage:
        - Classroom recommendations: get_book_recommendations_for_group(["S001", "S002", "S003"])
        - Reading group suggestions: get_book_recommendations_for_group(["S001", "S005"], 5)

    Algorithm:
        1. Find all books already checked out by group members
        2. Get highest-rated books from catalog (top 50)
        3. Filter out books already seen by group
        4. Return top N remaining books
    """
    logger.info(
        "Group book recommendations requested",
        extra={"student_ids": student_ids, "n": n, "group_size": len(student_ids)},
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # books already seen by group
            seen = await con.fetch(
                "SELECT DISTINCT book_id FROM checkout WHERE student_id = ANY($1::text[])",
                student_ids,
            )
            seen_ids = {r["book_id"] for r in seen}

            logger.debug(
                "Found books already seen by group",
                extra={"student_ids": student_ids, "seen_books_count": len(seen_ids)},
            )

            # candidate pool: highest rated books
            rows = await con.fetch(
                """SELECT book_id, title, average_rating
                     FROM catalog
                    WHERE average_rating IS NOT NULL
                 ORDER BY average_rating DESC LIMIT 50"""
            )

            recs = [dict(r) for r in rows if r["book_id"] not in seen_ids][:n]

            logger.info(
                "Group book recommendations completed",
                extra={
                    "student_ids": student_ids,
                    "n": n,
                    "recommendations_count": len(recs),
                    "candidate_pool_size": len(rows),
                    "seen_books_excluded": len(seen_ids),
                },
            )

            return recs

    except Exception as e:
        logger.error(
            "Group book recommendations failed",
            exc_info=True,
            extra={"student_ids": student_ids, "n": n},
        )
        raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Query Students Table",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def query_students(
    student_id: str = None,
    grade_level: int = None,
    homeroom_teacher: str = None,
    limit: int = 10,
):
    """
    Query the students table with optional filters.

    Table Schema:
        - student_id (TEXT): Primary key, student identifier
        - grade_level (INT): Grade level (typically 4 for elementary)
        - age (INT): Student age
        - homeroom_teacher (TEXT): Teacher name
        - prior_year_reading_score (REAL): EOG score 1.0-5.0
        - lunch_period (INT): Lunch period 1 or 2

    Args:
        student_id (str, optional): Specific student to look up
        grade_level (int, optional): Filter by grade level
        homeroom_teacher (str, optional): Filter by teacher name
        limit (int): Maximum results to return (default: 10)

    Returns:
        List[dict]: Student records matching the criteria

    Usage:
        - Look up specific student details: query_students(student_id="S001")
        - Find students by teacher: query_students(homeroom_teacher="Ms. Patel")
        - Get all grade 4 students: query_students(grade_level=4)
        - Browse students: query_students(limit=20)
    """
    logger.info(
        "Students table query requested",
        extra={
            "student_id": student_id,
            "grade_level": grade_level,
            "homeroom_teacher": homeroom_teacher,
            "limit": limit,
        },
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # Build dynamic query based on provided filters
            where_clauses = []
            params = []
            param_idx = 1

            if student_id:
                where_clauses.append(f"student_id = ${param_idx}")
                params.append(student_id)
                param_idx += 1

            if grade_level is not None:
                where_clauses.append(f"grade_level = ${param_idx}")
                params.append(grade_level)
                param_idx += 1

            if homeroom_teacher:
                where_clauses.append(f"homeroom_teacher = ${param_idx}")
                params.append(homeroom_teacher)
                param_idx += 1

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            query = f"SELECT * FROM students {where_sql} ORDER BY student_id LIMIT ${param_idx}"
            params.append(limit)

            rows = await con.fetch(query, *params)
            results = [dict(row) for row in rows]

            logger.info(
                "Students table query completed",
                extra={
                    "filters_applied": len(where_clauses),
                    "results_count": len(results),
                },
            )

            return results

    except Exception as e:
        logger.error("Students table query failed", exc_info=True)
        raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Query Catalog Table",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def query_catalog(
    book_id: str = None,
    genre: str = None,
    difficulty_band: str = None,
    min_rating: float = None,
    limit: int = 10,
):
    """
    Query the catalog table with optional filters.

    Table Schema:
        - book_id (TEXT): Primary key, book identifier
        - isbn (TEXT): ISBN number for external lookups
        - title (TEXT): Book title
        - genre (JSONB): Array of genres like ["fiction", "adventure"]
        - keywords (JSONB): Array of keywords for search
        - description (TEXT): Book description/summary
        - page_count (INT): Number of pages
        - publication_year (INT): Year published
        - difficulty_band (TEXT): Reading difficulty: "beginner", "early_elementary", etc.
        - reading_level (REAL): Numeric reading level (e.g., 4.2)
        - average_rating (REAL): Average rating 1.0-5.0
    Args:
        book_id (str, optional): Specific book to look up
        genre (str, optional): Filter by genre (matches any genre in array)
        difficulty_band (str, optional): Filter by difficulty level
        min_rating (float, optional): Minimum average student rating
        limit (int): Maximum results to return (default: 10)

    Returns:
        List[dict]: Book records matching the criteria

    Usage:
        - Look up specific book: query_catalog(book_id="B001")
        - Find fiction books: query_catalog(genre="fiction")
        - Get high-rated books: query_catalog(min_rating=4.0)
        - Browse by difficulty: query_catalog(difficulty_band="early_elementary")
    """
    logger.info(
        "Catalog table query requested",
        extra={
            "book_id": book_id,
            "genre": genre,
            "difficulty_band": difficulty_band,
            "min_rating": min_rating,
            "limit": limit,
        },
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # Build dynamic query
            where_clauses = []
            params = []
            param_idx = 1

            if book_id:
                where_clauses.append(f"book_id = ${param_idx}")
                params.append(book_id)
                param_idx += 1

            if genre:
                where_clauses.append(f"genre ? ${param_idx}")  # JSONB contains operator
                params.append(genre)
                param_idx += 1

            if difficulty_band:
                where_clauses.append(f"difficulty_band = ${param_idx}")
                params.append(difficulty_band)
                param_idx += 1

            if min_rating is not None:
                where_clauses.append(f"average_rating >= ${param_idx}")
                params.append(min_rating)
                param_idx += 1

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            query = (
                f"SELECT * FROM catalog {where_sql} ORDER BY book_id LIMIT ${param_idx}"
            )
            params.append(limit)

            rows = await con.fetch(query, *params)
            results = [dict(row) for row in rows]

            logger.info(
                "Catalog table query completed",
                extra={
                    "filters_applied": len(where_clauses),
                    "results_count": len(results),
                },
            )

            return results

    except Exception as e:
        logger.error("Catalog table query failed", exc_info=True)
        raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Query Checkout History",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def query_checkout_history(
    student_id: str = None,
    book_id: str = None,
    include_returns_only: bool = False,
    limit: int = 20,
):
    """
    Query the checkout table with optional filters.

    Table Schema:
        - student_id (TEXT): References students.student_id
        - book_id (TEXT): References catalog.book_id
        - checkout_date (DATE): When book was checked out
        - return_date (DATE): When book was returned (NULL if still checked out)
        - student_rating (INT): Student rating 1-5 (NULL until returned)

    Args:
        student_id (str, optional): Filter by specific student
        book_id (str, optional): Filter by specific book
        include_returns_only (bool): If True, only include returned books (default: False)
        limit (int): Maximum results to return (default: 20)

    Returns:
        List[dict]: Checkout records matching the criteria, ordered by checkout_date DESC

    Usage:
        - Student's reading history: query_checkout_history(student_id="S001")
        - Book's popularity: query_checkout_history(book_id="B001")
        - Completed reads only: query_checkout_history(student_id="S001", include_returns_only=True)
        - Recent activity: query_checkout_history(limit=50)
    """
    logger.info(
        "Checkout history query requested",
        extra={
            "student_id": student_id,
            "book_id": book_id,
            "include_returns_only": include_returns_only,
            "limit": limit,
        },
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # Build dynamic query
            where_clauses = []
            params = []
            param_idx = 1

            if student_id:
                where_clauses.append(f"student_id = ${param_idx}")
                params.append(student_id)
                param_idx += 1

            if book_id:
                where_clauses.append(f"book_id = ${param_idx}")
                params.append(book_id)
                param_idx += 1

            if include_returns_only:
                where_clauses.append("return_date IS NOT NULL")

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            query = f"SELECT * FROM checkout {where_sql} ORDER BY checkout_date DESC LIMIT ${param_idx}"
            params.append(limit)

            rows = await con.fetch(query, *params)
            results = [dict(row) for row in rows]

            logger.info(
                "Checkout history query completed",
                extra={
                    "filters_applied": len(where_clauses),
                    "results_count": len(results),
                },
            )

            return results

    except Exception as e:
        logger.error("Checkout history query failed", exc_info=True)
        raise


# --------------------------------------------------------------------- #
@mcp.tool(
    annotations={
        "title": "Query Student Similarity Matrix",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def query_student_similarity(
    student_a: str = None,
    student_b: str = None,
    min_similarity: float = None,
    limit: int = 15,
):
    """
    Query the student_similarity table with optional filters.

    Table Schema:
        - a (TEXT): First student ID
        - b (TEXT): Second student ID
        - sim (REAL): Similarity score 0.0-1.0 (higher = more similar)
        - last_event (UUID): UUID of last computation event

    Args:
        student_a (str, optional): Filter by first student
        student_b (str, optional): Filter by second student
        min_similarity (float, optional): Minimum similarity threshold
        limit (int): Maximum results to return (default: 15)

    Returns:
        List[dict]: Similarity records matching criteria, ordered by similarity DESC

    Usage:
        - Find all similarities for student: query_student_similarity(student_a="S001")
        - Check specific pair: query_student_similarity(student_a="S001", student_b="S002")
        - High similarities only: query_student_similarity(min_similarity=0.7)
        - Browse top similarities: query_student_similarity(limit=30)
    """
    logger.info(
        "Student similarity query requested",
        extra={
            "student_a": student_a,
            "student_b": student_b,
            "min_similarity": min_similarity,
            "limit": limit,
        },
    )

    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # Build dynamic query
            where_clauses = []
            params = []
            param_idx = 1

            if student_a:
                where_clauses.append(f"a = ${param_idx}")
                params.append(student_a)
                param_idx += 1

            if student_b:
                where_clauses.append(f"b = ${param_idx}")
                params.append(student_b)
                param_idx += 1

            if min_similarity is not None:
                where_clauses.append(f"sim >= ${param_idx}")
                params.append(min_similarity)
                param_idx += 1

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            query = f"SELECT * FROM student_similarity {where_sql} ORDER BY sim DESC LIMIT ${param_idx}"
            params.append(limit)

            rows = await con.fetch(query, *params)
            results = [dict(row) for row in rows]

            logger.info(
                "Student similarity query completed",
                extra={
                    "filters_applied": len(where_clauses),
                    "results_count": len(results),
                },
            )

            return results

    except Exception as e:
        logger.error("Student similarity query failed", exc_info=True)
        raise


# --------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logger.info("Starting MCP book server")
    try:
        # Ensure logger does not write to stdout (stdin/stdout used by FastMCP JSON-RPC)
        for h in list(logger.handlers):
            if isinstance(h, logging.StreamHandler):
                h.stream = sys.stderr
        mcp.run(transport="stdio")
        logger.info("MCP book server completed successfully")
    except Exception as e:
        logger.error("MCP book server failed", exc_info=True)
        raise
