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

from common import SettingsInstance as S
from common.structured_logging import get_logger
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from recommendation_api.tools import (
    fetch_google_books_meta,
    fetch_open_library_meta,
    readability_formula_estimator,
    compute_student_reading_level,
)

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
gb_throttler = Throttler(rate_limit=2, period=60)   # 2 rpm
ol_throttler = Throttler(rate_limit=4, period=60)   # 4 rpm
logger.info("Rate limiters initialized", extra={
    "google_books_rpm": 2,
    "open_library_rpm": 4
})

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Search Catalog by Keyword", "readOnlyHint": True})
def search_catalog(keyword: str, k: int = 5):
    """Vector keyword search across catalog title+description."""
    logger.info("Catalog search requested", extra={"keyword": keyword, "k": k})
    
    try:
        docs = _store().similarity_search(keyword, k=k)
        results = [{"book_id": d.metadata["book_id"], "snippet": d.page_content[:200]} for d in docs]
        
        logger.info("Catalog search completed", extra={
            "keyword": keyword,
            "k": k,
            "results_count": len(results)
        })
        
        return results
        
    except Exception as e:
        logger.error("Catalog search failed", exc_info=True, extra={"keyword": keyword, "k": k})
        raise

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Enrich Book Metadata", "readOnlyHint": True})
async def enrich_book_metadata(isbn: str):
    """
    Fetch page_count & publication_year using Google Books (primary) or
    Open Library (fallback). If still missing, estimate difficulty_band
    via readability on the stored description.
    """
    logger.info("Book metadata enrichment requested", extra={"isbn": isbn})
    
    try:
        # Try Google Books first
        logger.debug("Attempting Google Books enrichment", extra={"isbn": isbn})
        async with gb_throttler:
            meta = await fetch_google_books_meta(isbn)
            
            logger.debug("Google Books enrichment result", extra={
                "isbn": isbn,
                "has_page_count": bool(meta.get("page_count")),
                "has_publication_year": bool(meta.get("publication_year"))
            })
            
            # Fallback to Open Library if needed
            if not meta.get("page_count") or not meta.get("publication_year"):
                logger.debug("Attempting Open Library enrichment", extra={"isbn": isbn})
                async with ol_throttler:
                    ol_meta = await fetch_open_library_meta(isbn)
                    meta |= ol_meta
                
                logger.debug("Open Library enrichment result", extra={
                    "isbn": isbn,
                    "has_page_count": bool(meta.get("page_count")),
                    "has_publication_year": bool(meta.get("publication_year"))
                })

            # Readability fallback if still missing
            if not meta.get("page_count") or not meta.get("publication_year"):
                logger.debug("Using readability formula fallback", extra={"isbn": isbn})
                pool = await _pg()
                async with pool.acquire() as con:
                    desc = await con.fetchval("SELECT description FROM catalog WHERE isbn=$1", isbn)
                    readability_meta = readability_formula_estimator(desc or "")
                    meta |= readability_meta
                    
                    logger.debug("Readability fallback result", extra={
                        "isbn": isbn,
                        "has_difficulty_band": bool(readability_meta.get("difficulty_band"))
                    })

            logger.info("Book metadata enrichment completed", extra={
                "isbn": isbn,
                "page_count": meta.get("page_count"),
                "publication_year": meta.get("publication_year"),
                "difficulty_band": meta.get("difficulty_band")
            })

        return meta
        
    except Exception as e:
        logger.error("Book metadata enrichment failed", exc_info=True, extra={"isbn": isbn})
        raise

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Compute Student Reading Level", "readOnlyHint": True})
async def get_student_reading_level(student_id: str):
    """
    Compute avg_reading_level and confidence from checkout history.
    """
    logger.info("Student reading level computation requested", extra={"student_id": student_id})
    
    try:
        pool = await _pg()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """SELECT difficulty_band, COALESCE(student_rating,3) AS rating
                     FROM checkout JOIN catalog USING(book_id)
                    WHERE student_id=$1 AND return_date IS NOT NULL""",
                student_id,
            )
            
            logger.debug("Fetched checkout history", extra={
                "student_id": student_id,
                "checkout_count": len(rows)
            })
            
            result = compute_student_reading_level([dict(r) for r in rows])
            
            logger.info("Student reading level computed", extra={
                "student_id": student_id,
                "avg_reading_level": result.get("avg_reading_level"),
                "confidence": result.get("confidence"),
                "checkout_count": len(rows)
            })
            
            return result
        
    except Exception as e:
        logger.error("Student reading level computation failed", exc_info=True, extra={"student_id": student_id})
        raise

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Find Similar Students", "readOnlyHint": True})
async def find_similar_students(student_id: str, limit: int = 5):
    """
    Return up to `limit` other student_ids with highest similarity score.
    """
    logger.info("Similar students search requested", extra={"student_id": student_id, "limit": limit})
    
    try:
        pool = await _pg()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """SELECT b, sim FROM student_similarity
                    WHERE a=$1 ORDER BY sim DESC LIMIT $2""",
                student_id,
                limit,
            )
            
            results = [{"student_id": r["b"], "similarity": float(r["sim"])} for r in rows]
            
            logger.info("Similar students search completed", extra={
                "student_id": student_id,
                "limit": limit,
                "results_count": len(results),
                "top_similarity": results[0]["similarity"] if results else None
            })
            
            return results
        
    except Exception as e:
        logger.error("Similar students search failed", exc_info=True, extra={"student_id": student_id, "limit": limit})
        raise

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Group-Based Book Recs", "readOnlyHint": True})
async def get_book_recommendations_for_group(student_ids: List[str], n: int = 3):
    """
    Very simple heuristic: pick top-rated books among the group that no
    member has checked out yet.
    """
    logger.info("Group book recommendations requested", extra={
        "student_ids": student_ids,
        "n": n,
        "group_size": len(student_ids)
    })
    
    try:
        pool = await _pg()
        async with pool.acquire() as con:
            # books already seen by group
            seen = await con.fetch(
                "SELECT DISTINCT book_id FROM checkout WHERE student_id = ANY($1::text[])", student_ids
            )
            seen_ids = {r["book_id"] for r in seen}
            
            logger.debug("Found books already seen by group", extra={
                "student_ids": student_ids,
                "seen_books_count": len(seen_ids)
            })

            # candidate pool: highest rated books
            rows = await con.fetch(
                """SELECT book_id, title, average_student_rating
                     FROM catalog
                    WHERE average_student_rating IS NOT NULL
                 ORDER BY average_student_rating DESC LIMIT 50"""
            )
            
            recs = [
                dict(r)
                for r in rows
                if r["book_id"] not in seen_ids
            ][: n]
            
            logger.info("Group book recommendations completed", extra={
                "student_ids": student_ids,
                "n": n,
                "recommendations_count": len(recs),
                "candidate_pool_size": len(rows),
                "seen_books_excluded": len(seen_ids)
            })
            
            return recs
        
    except Exception as e:
        logger.error("Group book recommendations failed", exc_info=True, extra={
            "student_ids": student_ids,
            "n": n
        })
        raise

# --------------------------------------------------------------------- #
if __name__ == "__main__":           # pragma: no cover
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