"""
Feedback Worker - Kafka Consumer

Consumes feedback events and updates database and Redis cache.
Handles feedback scoring and recommendation adjustment.
"""

import asyncio
import redis.asyncio as redis
from typing import Dict, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.settings import SettingsInstance as S
from common.structured_logging import get_logger
from common.kafka_utils import KafkaEventConsumer
from common.events import FeedbackEvent, FEEDBACK_EVENTS_TOPIC
from common.metrics import JOB_RUNS_TOTAL, JOB_DURATION_SECONDS
from common.models import Feedback, PublicUser

logger = get_logger(__name__)

# Database setup
engine = create_async_engine(
    str(S.db_url).replace("postgresql://", "postgresql+asyncpg://"),
    echo=False
)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Redis setup
redis_client = None

async def get_redis_client():
    """Get or create Redis client"""
    global redis_client
    if redis_client is None:
        redis_client = redis.from_url(S.redis_url)
    return redis_client

async def process_feedback_event(event_data: Dict[str, Any]):
    """Process a single feedback event"""
    try:
        # Parse event
        event = FeedbackEvent(**event_data)
        logger.info("Processing feedback event", extra={
            "user_hash_id": event.user_hash_id,
            "book_id": event.book_id,
            "score": event.score
        })
        
        # Get user ID from hash
        async with async_session() as session:
            result = await session.execute(
                select(PublicUser).where(PublicUser.hash_id == event.user_hash_id)
            )
            user = result.scalar_one_or_none()
            
            if user is None:
                logger.warning("User not found for feedback", extra={
                    "user_hash_id": event.user_hash_id
                })
                return
            
            # Store feedback in database
            feedback = Feedback(
                user_id=user.id,
                book_id=event.book_id,
                score=event.score
            )
            session.add(feedback)
            await session.commit()
            
            logger.info("Stored feedback in database", extra={
                "user_id": str(user.id),
                "book_id": event.book_id,
                "score": event.score
            })
        
        # Update Redis cache
        redis_client = await get_redis_client()
        cache_key = f"feedback:book:{event.book_id}"
        
        # Increment feedback score using Redis sorted set
        await redis_client.zincrby(cache_key, event.score, event.user_hash_id)
        
        # Set expiration (30 days)
        await redis_client.expire(cache_key, 30 * 24 * 60 * 60)
        
        logger.info("Updated Redis feedback cache", extra={
            "cache_key": cache_key,
            "score": event.score
        })
        
    except Exception as e:
        logger.error("Failed to process feedback event", exc_info=True, extra={
            "event_data": event_data
        })
        raise

async def get_book_feedback_score(book_id: str) -> float:
    """Get aggregated feedback score for a book"""
    redis_client = await get_redis_client()
    cache_key = f"feedback:book:{book_id}"
    
    # Get all scores for this book
    scores = await redis_client.zrange(cache_key, 0, -1, withscores=True)
    
    if not scores:
        return 0.0
    
    # Calculate weighted average (could be enhanced with more sophisticated scoring)
    total_score = sum(score for _, score in scores)
    return total_score / len(scores)

async def main():
    """Main worker loop"""
    logger.info("Starting Feedback Worker")
    
    # Create Kafka consumer
    consumer = KafkaEventConsumer(
        topic=FEEDBACK_EVENTS_TOPIC,
        group_id="feedback_worker"
    )
    
    try:
        # Start consuming events
        await consumer.start(process_feedback_event)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error("Feedback worker error", exc_info=True)
    finally:
        await consumer.stop()
        if redis_client:
            await redis_client.close()
        logger.info("Feedback Worker stopped")

if __name__ == "__main__":
    asyncio.run(main()) 