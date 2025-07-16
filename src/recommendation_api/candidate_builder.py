"""Build a *rough* list of book candidates for one student.

The purpose of this module is **not** to rank items perfectly—that job is
handled later in :pymod:`recommendation_api.scoring`.  Instead, we generate a
pool that is *likely* to contain good answers so we can keep the LLM prompt
short and cheap.

Current behaviour, in order of preference:

1.  Look at the *k* nearest neighbours in ``student_similarity`` (built by
    our nightly graph job) and collect the books they have checked-out in the
    last ``RECENCY_WINDOW_DAYS``.
2.  **Phase 2**: Use FAISS semantic search on student's rated books to find
    similar titles, weighted by rating (5★=1.0, 4★=0.7, 3★=0.4, 2★=0.2, 1★=0.1).
3.  Fill the remainder with random unseen titles from the catalogue so we
    always have enough material.
4.  If the student is brand-new (no history at all) fall back to the school-
    wide popularity list (top checkout counts).

The function is intentionally pure-Python + SQL so unit tests can run without
FAISS.  Hooks for future FAISS queries are noted inline.
"""

from __future__ import annotations

import os, asyncpg, numpy as np
from datetime import date, timedelta
from typing import List, Dict, Optional
from pathlib import Path

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

from common.settings import settings as S
from common.structured_logging import get_logger
from .scoring import CandidateBook
from common.redis_utils import was_recommended
from common.weights import get as get_weights

logger = get_logger(__name__)

RECENCY_WINDOW_DAYS = os.getenv("RECENCY_WINDOW_DAYS", 30)

# Phase 2: Rating-to-weight mapping for semantic search
RATING_WEIGHTS = {5: 1.0, 4: 0.7, 3: 0.4, 2: 0.2, 1: 0.1}


async def _pg_connect():
    url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    return await asyncpg.connect(url)


def _load_faiss_store() -> Optional[FAISS]:
    """Load FAISS vector store for semantic book search.

    Creates an empty index if none exists. Returns None only on hard failure.
    """
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
            return store

        store = FAISS.load_local(
            vector_dir, embeddings, allow_dangerous_deserialization=True
        )
        logger.debug("FAISS store loaded", extra={"index_size": store.index.ntotal})
        return store
    except Exception:
        logger.warning("Failed to load/create FAISS store", exc_info=True)
        return None


async def _semantic_book_candidates(student_id: str, k: int = 20) -> List[str]:
    """Phase 2: Find semantically similar books using FAISS search on student's rated books.

    This is the core of our semantic recommendation system. Instead of just looking
    at what similar students read, we look at the *content* of books the student
    actually liked and find more books like those.

    Algorithm:
    1. Fetch student's last m rated books (default m=10 from weights)
    2. Weight each book's embedding by its rating (5★=1.0, 4★=0.7, etc.)
    3. Aggregate weighted embeddings into a single query vector
    4. FAISS search for top k similar unseen books

    Returns:
        List of book_ids from semantic search, or empty list if FAISS unavailable
    """
    logger.debug(
        "Starting semantic book search",
        extra={"student_id": student_id, "target_candidates": k},
    )

    conn = await _pg_connect()
    weights = get_weights()
    m = weights.get("semantic_history_count", 10)  # How many rated books to aggregate

    try:
        # Fetch student's recent rated books
        rated_books = await conn.fetch(
            """
            SELECT book_id, student_rating 
            FROM checkout 
            WHERE student_id = $1 AND student_rating IS NOT NULL
            ORDER BY checkout_date DESC 
            LIMIT $2
            """,
            student_id,
            m,
        )

        if not rated_books:
            logger.debug(
                "No rated books found for semantic search",
                extra={"student_id": student_id},
            )
            return []

        # Load FAISS store
        store = _load_faiss_store()
        if not store:
            logger.debug("FAISS store unavailable for semantic search")
            return []

        # Aggregate weighted embeddings
        weighted_embeddings = []
        total_weight = 0.0

        for book_record in rated_books:
            book_id = book_record["book_id"]
            rating = book_record["student_rating"]
            weight = RATING_WEIGHTS.get(rating, 0.1)  # Default to lowest weight

            # Find book embedding in FAISS store
            try:
                # Search for exact book_id match to get its embedding
                docs = store.similarity_search_by_vector_with_score(
                    query_vector=None,  # We'll search by metadata instead
                    k=1,
                    filter={"book_id": book_id},  # Use metadata filter
                )

                if docs:
                    # Alternative: Use docstore to get embedding directly
                    doc_idx = None
                    for idx, doc_id in store.index_to_docstore_id.items():
                        doc = store.docstore.search(doc_id)
                        if doc.metadata.get("book_id") == book_id:
                            doc_idx = idx
                            break

                    if doc_idx is not None:
                        embedding = store.index.reconstruct(doc_idx)
                        weighted_embeddings.append(embedding * weight)
                        total_weight += weight

            except Exception:
                logger.debug(
                    "Could not find embedding for book", extra={"book_id": book_id}
                )
                continue

        if not weighted_embeddings or total_weight == 0:
            logger.debug(
                "No valid embeddings found for aggregation",
                extra={"student_id": student_id},
            )
            return []

        # Create aggregated query vector
        query_vector = np.sum(weighted_embeddings, axis=0) / total_weight

        # Search for similar books
        docs_with_scores = store.similarity_search_by_vector(
            query_vector, k=k * 2
        )  # Oversample

        # Extract book_ids, filtering out already-read books
        already_read = await conn.fetch(
            "SELECT book_id FROM checkout WHERE student_id=$1", student_id
        )
        already_read_set = {r["book_id"] for r in already_read}

        candidates = []
        for doc in docs_with_scores:
            book_id = doc.metadata.get("book_id")
            if book_id and book_id not in already_read_set:
                candidates.append(book_id)
                if len(candidates) >= k:
                    break

        logger.info(
            "Semantic search completed",
            extra={
                "student_id": student_id,
                "rated_books_used": len(rated_books),
                "semantic_candidates": len(candidates),
                "total_weight": total_weight,
            },
        )

        return candidates

    except Exception:
        logger.warning(
            "Semantic search failed", exc_info=True, extra={"student_id": student_id}
        )
        return []
    finally:
        await conn.close()


async def _query_based_semantic_candidates(
    query: str, student_id: str = None, k: int = 10, conn=None
) -> List[str]:
    """Phase 3: Find books semantically similar to the user's query.

    Enhanced to consider student context when available:
    - Student's reading level preferences
    - Student's favorite genres from reading history
    - Combines query with student profile for personalized search

    Algorithm:
    1. Enhance query with student context if available
    2. Embed the enhanced query text
    3. FAISS search for most similar books
    4. Return top k book_ids
    """
    logger.debug(
        "Starting query-based semantic search",
        extra={"query": query, "student_id": student_id, "target_candidates": k},
    )

    try:
        # Load FAISS store
        store = _load_faiss_store()
        if not store:
            logger.debug("FAISS store unavailable for query search")
            return []

        # Enhance query with student context
        enhanced_query = query
        if student_id and conn:
            try:
                # Get student's average reading level and recent genres
                context_rows = await conn.fetch(
                    """
                    SELECT c.genre, c.reading_level, co.student_rating
                    FROM checkout co
                    JOIN catalog c USING(book_id)
                    WHERE co.student_id = $1 AND co.student_rating >= 4
                    ORDER BY co.checkout_date DESC LIMIT 10
                """,
                    student_id,
                )

                if context_rows:
                    # Calculate preferred reading level
                    levels = [
                        r["reading_level"] for r in context_rows if r["reading_level"]
                    ]
                    avg_level = round(sum(levels) / len(levels), 1) if levels else None

                    # Find most common genres
                    genre_counts = {}
                    for r in context_rows:
                        if r["genre"]:
                            genre_counts[r["genre"]] = (
                                genre_counts.get(r["genre"], 0) + 1
                            )

                    top_genres = sorted(
                        genre_counts.items(), key=lambda x: x[1], reverse=True
                    )[:2]

                    # Enhance query with context
                    context_parts = []
                    if avg_level:
                        context_parts.append(f"reading level {avg_level}")
                    if top_genres:
                        genres = ", ".join([g[0] for g in top_genres])
                        context_parts.append(f"genres like {genres}")

                    if context_parts:
                        enhanced_query = f"{query} for student who likes {' and '.join(context_parts)}"
                        logger.debug(
                            "Enhanced query with student context",
                            extra={
                                "original_query": query,
                                "enhanced_query": enhanced_query,
                                "avg_level": avg_level,
                                "top_genres": [g[0] for g in top_genres],
                            },
                        )

            except Exception as e:
                logger.debug(
                    "Failed to enhance query with student context", exc_info=True
                )
                # Fall back to original query
                enhanced_query = query

        # Embed the (possibly enhanced) query
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        query_vector = embeddings.embed_query(enhanced_query)

        # Search for similar books
        docs_with_scores = store.similarity_search_by_vector(
            query_vector, k=k * 2
        )  # Oversample

        candidates = []
        for doc in docs_with_scores:
            book_id = doc.metadata.get("book_id")
            if book_id and book_id != "dummy":  # Skip dummy entries
                candidates.append(book_id)
                if len(candidates) >= k:
                    break

        logger.info(
            "Query-based semantic search completed",
            extra={
                "original_query": query,
                "enhanced_query": enhanced_query,
                "query_candidates": len(candidates),
                "candidates": candidates[:5],  # Log first 5 for debugging
            },
        )

        return candidates

    except Exception:
        logger.warning(
            "Query-based semantic search failed", exc_info=True, extra={"query": query}
        )
        return []


async def build_candidates(
    student_id: str, k: int = 30, query: str = None
) -> List[CandidateBook]:
    """Return **at most** *k* books the student hasn't seen.

    The algorithm tries neighbour-based suggestions first, then backs off to
    random catalogue picks, and finally to popularity if the student has no
    history at all (cold start).

    Phase 2: Also includes semantic book similarity via FAISS search.
    """
    conn = await _pg_connect()

    # Fetch books already checked out by the student
    read_rows = await conn.fetch(
        "SELECT book_id FROM checkout WHERE student_id=$1", student_id
    )
    already_read = {r["book_id"] for r in read_rows}

    # Quick sanity – unknown student?  Raise so the API can return 404
    if not read_rows:
        prof = await conn.fetchrow(
            "SELECT 1 FROM students WHERE student_id=$1", student_id
        )
        if prof is None:
            await conn.close()
            raise ValueError(f"Unknown student_id '{student_id}'")

    # ---------------- Phase 2: Semantic candidates via FAISS -----------------
    semantic_book_ids = await _semantic_book_candidates(student_id, k=min(k // 2, 15))

    # ---------------- Phase 3: Query-based semantic search -----------------
    query_book_ids = []
    if query and query.strip():
        query_book_ids = await _query_based_semantic_candidates(
            query.strip(), student_id=student_id, k=min(k // 3, 10), conn=conn
        )

    # ---------------- primary candidates via neighbours -----------------
    nbr_recent_counts: dict[str, int] = {}

    try:
        rows_sim = await conn.fetch(
            "SELECT b AS other, sim FROM student_similarity WHERE a=$1 ORDER BY sim DESC LIMIT 5",
            student_id,
        )
        nbr_ids = [r["other"] for r in rows_sim]
        logger.debug(
            "Nearest neighbours fetched",
            extra={"student_id": student_id, "neighbours": nbr_ids},
        )
        if nbr_ids:
            recent_dt = date.today() - timedelta(days=RECENCY_WINDOW_DAYS)
            chk_rows = await conn.fetch(
                "SELECT book_id FROM checkout WHERE student_id = ANY($1::text[]) AND checkout_date >= $2",
                nbr_ids,
                recent_dt,
            )
            for r in chk_rows:
                bid = r["book_id"]
                nbr_recent_counts[bid] = nbr_recent_counts.get(bid, 0) + 1
        logger.debug(
            "Neighbour recent checkout aggregation",
            extra={"counts": len(nbr_recent_counts)},
        )
    except Exception:
        logger.debug("student_similarity table missing or query failed", exc_info=True)

    # Pull random unseen books from catalogue as fallback / filler.
    # We oversample (*k*×3) because many titles will be filtered out for being
    # already-read or already-recommended.
    rows = await conn.fetch(
        "SELECT book_id, title, reading_level AS level, author FROM catalog WHERE book_id <> ALL($1::text[]) ORDER BY RANDOM() LIMIT $2",
        list(already_read),
        k * 3,
    )

    # Also fetch metadata for semantic candidates
    semantic_metadata = {}
    if semantic_book_ids:
        semantic_rows = await conn.fetch(
            "SELECT book_id, title, reading_level AS level, author FROM catalog WHERE book_id = ANY($1::text[])",
            semantic_book_ids,
        )
        semantic_metadata = {r["book_id"]: r for r in semantic_rows}

    await conn.close()

    out: List[CandidateBook] = []
    today = date.today()

    # Add query-based candidates first (highest priority when user searches)
    for book_id in query_book_ids:
        if book_id in already_read:
            continue
        if await was_recommended(student_id, book_id):
            continue

        # Fetch metadata for query candidates
        try:
            conn = await _pg_connect()
            meta_row = await conn.fetchrow(
                "SELECT title, reading_level AS level, author FROM catalog WHERE book_id = $1",
                book_id,
            )
            await conn.close()
            level = meta_row["level"] if meta_row else None
            author = meta_row["author"] if meta_row else None
            title = meta_row["title"] if meta_row else None
        except Exception:
            level = None
            author = None
            title = None

        cb: CandidateBook = {
            "book_id": book_id,
            "title": title,
            "level": level,
            "author": author,
            "days_since_checkout": None,
            "neighbour_recent": nbr_recent_counts.get(book_id, 0),
            "staff_pick": False,
            "student_level": None,
            "semantic_candidate": True,  # Flag for scoring
            "query_match": True,  # Highest priority flag
        }
        out.append(cb)
        if len(out) >= k // 2:  # Reserve half for query matches
            break

    # Add semantic candidates second (higher priority than random)
    for book_id in semantic_book_ids:
        if book_id in already_read:
            continue
        if await was_recommended(student_id, book_id):
            continue

        meta = semantic_metadata.get(book_id, {})
        cb: CandidateBook = {
            "book_id": book_id,
            "title": meta.get("title"),
            "level": meta.get("level"),
            "author": meta.get("author"),
            "days_since_checkout": None,
            "neighbour_recent": nbr_recent_counts.get(book_id, 0),
            "staff_pick": False,
            "student_level": None,
            "semantic_candidate": True,  # Flag for scoring
        }
        out.append(cb)
        if len(out) >= k:
            break

    # Fill remainder with random catalogue picks and neighbour suggestions
    for r in rows:
        bid = r["book_id"]
        if bid in already_read:
            continue
        if await was_recommended(student_id, bid):
            continue
        if any(c["book_id"] == bid for c in out):  # Skip duplicates
            continue

        # Package minimal metadata required by the scorer.  In a real system
        # we'd attach genre, keywords, etc., but that would blow up the JSON
        # payload size.
        cb: CandidateBook = {
            "book_id": bid,
            "title": r["title"],
            "level": r["level"],
            "author": r["author"],
            "days_since_checkout": None,
            "neighbour_recent": nbr_recent_counts.get(bid, 0),
            "staff_pick": False,
            "student_level": None,
            "semantic_candidate": False,
        }
        out.append(cb)
        if len(out) >= k:
            break

    # If we *still* have nothing we're in a cold-start scenario.
    # Grab the most-checked-out titles across the entire school so the LLM
    # has something to chew on.
    if not out:
        try:
            conn = await _pg_connect()
            popular = await conn.fetch(
                "SELECT book_id, COUNT(*) cnt FROM checkout GROUP BY book_id ORDER BY cnt DESC LIMIT $1",
                get_weights().get("cold_start_k", 20),
            )
            await conn.close()
            for p in popular:
                if p["book_id"] not in already_read and not await was_recommended(
                    student_id, p["book_id"]
                ):
                    out.append(
                        {
                            "book_id": p["book_id"],
                            "title": None,
                            "level": None,
                            "author": None,
                            "days_since_checkout": None,
                            "neighbour_recent": 0,
                            "staff_pick": False,
                            "student_level": None,
                            "semantic_candidate": False,
                        }
                    )
                    if len(out) >= k:
                        break
        except Exception:
            logger.warning("cold-start query failed", exc_info=True)

    # High-level audit log – helps during manual QA and demo walk-throughs.
    logger.info(
        "Candidate builder completed",
        extra={
            "student_id": student_id,
            "candidates": len(out),
            "semantic_candidates": len(semantic_book_ids),
            "neighbour_counts": len(nbr_recent_counts),
            "cold_start": not bool(read_rows),
        },
    )
    return out
