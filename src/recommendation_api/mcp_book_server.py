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

import asyncio, logging, asyncpg
from pathlib import Path
from typing import List

from fastmcp import FastMCP
from aio_limiter import AsyncLimiter

from common import SettingsInstance as S
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from recommendation_api.tools import (
    fetch_google_books_meta,
    fetch_open_library_meta,
    readability_formula_estimator,
    compute_student_reading_level,
)

# --------------------------------------------------------------------- #
mcp = FastMCP("book")

LOG = logging.getLogger("mcp_book_server")
logging.basicConfig(level=logging.INFO)

# vector store
emb = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.model_name)
vec_store = None

def _store():
    global vec_store
    if vec_store is None:
        vec_store = FAISS.load_local(Path(S.data_dir) / "vector_store")
    return vec_store

# pg connection pool (lazy)
_pg_pool: asyncpg.Pool | None = None
async def _pg() -> asyncpg.pool.Pool:
    global _pg_pool
    if _pg_pool is None:
        url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
        _pg_pool = await asyncpg.create_pool(url, min_size=1, max_size=10)
    return _pg_pool

# rate-limiters for external APIs
gb_limiter = AsyncLimiter(2, time_period=60)   # 2 rpm
ol_limiter = AsyncLimiter(4, time_period=60)   # 4 rpm

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Search Catalog by Keyword", "readOnlyHint": True})
def search_catalog(keyword: str, k: int = 5):
    """Vector keyword search across catalog title+description."""
    docs = _store().similarity_search(keyword, k=k)
    return [{"book_id": d.metadata["book_id"], "snippet": d.page_content[:200]} for d in docs]

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Enrich Book Metadata", "readOnlyHint": True})
async def enrich_book_metadata(isbn: str):
    """
    Fetch page_count & publication_year using Google Books (primary) or
    Open Library (fallback). If still missing, estimate difficulty_band
    via readability on the stored description.
    """
    async with gb_limiter:
        meta = await fetch_google_books_meta(isbn)
    # any missing? -> fallback
    if not meta.get("page_count") or not meta.get("publication_year"):
        async with ol_limiter:
            meta |= await fetch_open_library_meta(isbn)

    # readability fallback only if difficulty still None
    if not meta.get("page_count") or not meta.get("publication_year"):
        pool = await _pg()
        async with pool.acquire() as con:
            desc = await con.fetchval("SELECT description FROM catalog WHERE isbn=$1", isbn)
        meta |= readability_formula_estimator(desc or "")

    return meta

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Compute Student Reading Level", "readOnlyHint": True})
async def get_student_reading_level(student_id: str):
    """
    Compute avg_reading_level and confidence from checkout history.
    """
    pool = await _pg()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT difficulty_band, COALESCE(student_rating,3) AS rating
                 FROM checkout JOIN catalog USING(book_id)
                WHERE student_id=$1 AND return_date IS NOT NULL""",
            student_id,
        )
    return compute_student_reading_level([dict(r) for r in rows])

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Find Similar Students", "readOnlyHint": True})
async def find_similar_students(student_id: str, limit: int = 5):
    """
    Return up to `limit` other student_ids with highest similarity score.
    """
    pool = await _pg()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT b, sim FROM student_similarity
                WHERE a=$1 ORDER BY sim DESC LIMIT $2""",
            student_id,
            limit,
        )
    return [{"student_id": r["b"], "similarity": float(r["sim"])} for r in rows]

# --------------------------------------------------------------------- #
@mcp.tool(annotations={"title": "Group-Based Book Recs", "readOnlyHint": True})
async def get_book_recommendations_for_group(student_ids: List[str], n: int = 3):
    """
    Very simple heuristic: pick top-rated books among the group that no
    member has checked out yet.
    """
    pool = await _pg()
    async with pool.acquire() as con:
        # books already seen by group
        seen = await con.fetch(
            "SELECT DISTINCT book_id FROM checkout WHERE student_id = ANY($1::text[])", student_ids
        )
        seen_ids = {r["book_id"] for r in seen}

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
    return recs

# --------------------------------------------------------------------- #
if __name__ == "__main__":           # pragma: no cover
    mcp.run(transport="stdio") 