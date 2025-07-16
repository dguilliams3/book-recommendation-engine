#!/usr/bin/env python3
"""
Book Enrichment Worker

Enhanced asynchronous book data enrichment with priority escalation and retry logic.
Runs as a background service with priority-based processing:
- Priority 1 (Low): Background batch processing every 30 seconds  
- Priority 2 (High): Worker requests processed within 30 seconds
- Priority 3 (Critical): User requests processed within 5 seconds

Features:
- Priority queue system with database persistence
- Exponential backoff retry logic
- Rate limiting by priority level
- Comprehensive status tracking
- Backward compatibility with existing messages
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import aiohttp
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from aiokafka import AIOKafkaConsumer

from common.models import Base
from common.settings import settings
from common.structured_logging import get_logger
from common.kafka_utils import publish_event
from common.events import BOOK_EVENTS_TOPIC, BookAddedEvent

# Setup
logger = get_logger(__name__)

# Database setup
engine = create_engine(settings.db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Configuration
ENRICHMENT_CONFIG = {
    'batch_size': int(os.getenv('ENRICHMENT_BATCH_SIZE', '50')),
    'batch_interval': int(os.getenv('ENRICHMENT_BATCH_INTERVAL', '30')),
    'max_retries': {
        1: int(os.getenv('ENRICHMENT_MAX_RETRIES_LOW', '2')),
        2: int(os.getenv('ENRICHMENT_MAX_RETRIES_HIGH', '3')),
        3: int(os.getenv('ENRICHMENT_MAX_RETRIES_CRITICAL', '5'))
    },
    'rate_limits': {
        1: float(os.getenv('ENRICHMENT_RATE_LIMIT_LOW', '0.5')),
        2: float(os.getenv('ENRICHMENT_RATE_LIMIT_HIGH', '0.2')),
        3: float(os.getenv('ENRICHMENT_RATE_LIMIT_CRITICAL', '0.1'))
    },
    'api_timeout': int(os.getenv('ENRICHMENT_API_TIMEOUT', '10')),
    'openlibrary_base_url': os.getenv('OPENLIBRARY_BASE_URL', 'https://openlibrary.org')
}

class PriorityEnrichmentManager:
    """Manages priority-based enrichment processing with retry logic."""
    
    def __init__(self):
        self.priority_queues = {1: [], 2: [], 3: []}
        self.last_call_times = {}
        self.session = None
        self.last_batch_time = 0
        
    async def initialize(self):
        """Initialize HTTP session and database connections."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=ENRICHMENT_CONFIG['api_timeout'])
        )
        logger.info("Priority Enrichment Manager initialized")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.session:
            await self.session.close()
    
    async def add_to_queue(self, book_id: str, priority: int, reason: str = None, requester: str = None):
        """Add book to priority queue and database tracking."""
        if priority not in [1, 2, 3]:
            priority = 2  # Default to high priority for unknown values
            
        self.priority_queues[priority].append({
            'book_id': book_id,
            'priority': priority,
            'reason': reason or 'unknown',
            'requester': requester or 'system',
            'added_at': time.time()
        })
        
        # Add to database tracking
        await self.ensure_enrichment_tracking(book_id, priority, reason, requester)
        
        logger.info(f"Added book {book_id} to priority {priority} queue (reason: {reason})")
    
    async def ensure_enrichment_tracking(self, book_id: str, priority: int, reason: str = None, requester: str = None):
        """Ensure book has enrichment tracking record."""
        try:
            with SessionLocal() as db:
                # Check if tracking record exists
                result = db.execute(
                    text("SELECT book_id FROM book_metadata_enrichment WHERE book_id = :book_id"),
                    {"book_id": book_id}
                )
                existing = result.fetchone()
                
                if not existing:
                    # Create new tracking record
                    db.execute(
                        text("""
                            INSERT INTO book_metadata_enrichment 
                            (book_id, priority, enrichment_status, attempts, created_at, updated_at)
                            VALUES (:book_id, :priority, 'pending', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """),
                        {"book_id": book_id, "priority": priority}
                    )
                else:
                    # Update priority if higher
                    db.execute(
                        text("""
                            UPDATE book_metadata_enrichment 
                            SET priority = GREATEST(priority, :priority),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE book_id = :book_id
                        """),
                        {"book_id": book_id, "priority": priority}
                    )
                
                # Always create enrichment request record
                if reason:
                    request_id = str(uuid.uuid4())
                    db.execute(
                        text("""
                            INSERT INTO enrichment_requests 
                            (request_id, book_id, requester, priority, reason, status, created_at)
                            VALUES (:request_id, :book_id, :requester, :priority, :reason, 'pending', CURRENT_TIMESTAMP)
                        """),
                        {
                            "request_id": request_id,
                            "book_id": book_id,
                            "requester": requester or 'system',
                            "priority": priority,
                            "reason": reason
                        }
                    )
                
                db.commit()
                
        except Exception as e:
            logger.error(f"Error ensuring enrichment tracking for {book_id}: {e}")
    
    async def get_next_book(self) -> Optional[Dict[str, Any]]:
        """Get next book to process in priority order."""
        for priority in [3, 2, 1]:  # Critical first
            if self.priority_queues[priority]:
                return self.priority_queues[priority].pop(0)
        return None
    
    async def wait_for_rate_limit(self, priority: int):
        """Wait appropriate time based on priority rate limit."""
        rate_limit = ENRICHMENT_CONFIG['rate_limits'][priority]
        last_call = self.last_call_times.get(priority, 0)
        time_since_last = time.time() - last_call
        
        if time_since_last < rate_limit:
            sleep_time = rate_limit - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_call_times[priority] = time.time()
    
    async def fetch_work_details(self, work_key: str) -> Optional[Dict[str, Any]]:
        """Fetch detailed work information from OpenLibrary API or local cache."""
        # Check for local cache first
        # Handle double OL prefix: /works/OLOL17453W -> OL17453W.json
        cache_key = work_key
        if work_key.startswith("/works/OLOL"):
            # Extract the OL17453W part from /works/OLOL17453W
            cache_key = work_key.replace("/works/OLOL", "OL")
        elif work_key.startswith("OLOL"):
            cache_key = work_key[2:]  # Remove first "OL" prefix
        
        local_path = Path("data/raw_openlibrary/works") / (cache_key + ".json")
        if local_path.exists():
            try:
                with open(local_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded work data from local cache: {work_key}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load local cache for {work_key}: {e}")
        
        # Fallback to API call
        url = f"https://openlibrary.org{work_key}.json"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Save to local cache for future use
                    try:
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(local_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.info(f"Saved work data to local cache: {work_key}")
                    except Exception as e:
                        logger.warning(f"Failed to save local cache for {work_key}: {e}")
                    return data
                else:
                    logger.warning(f"OpenLibrary API returned status {response.status} for {work_key}")
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch work data for {work_key}: {e}")
            return None

    async def fetch_edition_details(self, edition_key: str) -> Optional[Dict[str, Any]]:
        """Fetch detailed edition information from OpenLibrary API or local cache."""
        # Check for local cache first
        # Handle double OL prefix: /works/OLOL17453W -> OL17453W.json
        cache_key = edition_key
        if edition_key.startswith("/works/OLOL"):
            # Extract the OL17453W part from /works/OLOL17453W
            cache_key = edition_key.replace("/works/OLOL", "OL")
        elif edition_key.startswith("OLOL"):
            cache_key = edition_key[2:]  # Remove first "OL" prefix
        
        local_path = Path("data/raw_openlibrary/works") / (cache_key + ".json")
        if local_path.exists():
            try:
                with open(local_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded edition data from local cache: {edition_key}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load local cache for {edition_key}: {e}")
        
        # Fallback to API call
        url = f"https://openlibrary.org{edition_key}.json"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Save to local cache for future use
                    try:
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(local_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.info(f"Saved edition data to local cache: {edition_key}")
                    except Exception as e:
                        logger.warning(f"Failed to save local cache for {edition_key}: {e}")
                    return data
                else:
                    logger.warning(f"OpenLibrary API returned status {response.status} for {edition_key}")
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch edition data for {edition_key}: {e}")
            return None
    
    def extract_publication_year(self, work_data: Dict[str, Any], edition_data: Optional[Dict[str, Any]] = None) -> Optional[int]:
        """Extract publication year from work or edition data."""
        # Try edition data first (more specific)
        if edition_data and 'publish_date' in edition_data:
            try:
                date_str = edition_data['publish_date']
                # Handle various date formats
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', str(date_str))
                if year_match:
                    year = int(year_match.group())
                    if 1800 <= year <= 2030:
                        return year
            except (ValueError, TypeError):
                pass
        
        # Try work data
        if 'first_publish_date' in work_data:
            try:
                date_str = work_data['first_publish_date']
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', str(date_str))
                if year_match:
                    year = int(year_match.group())
                    if 1800 <= year <= 2030:
                        return year
            except (ValueError, TypeError):
                pass
        
        return None
    
    def extract_page_count(self, work_data: Dict[str, Any], edition_data: Optional[Dict[str, Any]] = None) -> Optional[int]:
        """Extract page count from work or edition data."""
        # Try edition data first
        if edition_data:
            for field in ['number_of_pages', 'pages', 'page_count']:
                if field in edition_data and edition_data[field]:
                    try:
                        pages = int(edition_data[field])
                        if 1 <= pages <= 5000:
                            return pages
                    except (ValueError, TypeError):
                        continue
        
        # Try work data
        if 'number_of_pages_median' in work_data and work_data['number_of_pages_median']:
            try:
                pages = int(work_data['number_of_pages_median'])
                if 1 <= pages <= 5000:
                    return pages
            except (ValueError, TypeError):
                pass
        
        return None
    
    def extract_isbn(self, edition_data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Extract ISBN from edition data."""
        if not edition_data:
            return None
        
        # Try various ISBN fields
        for field in ['isbn_13', 'isbn_10', 'isbn']:
            if field in edition_data and edition_data[field]:
                isbns = edition_data[field]
                if isinstance(isbns, list) and isbns:
                    return str(isbns[0]).replace('-', '').replace(' ', '')
                elif isinstance(isbns, str):
                    return isbns.replace('-', '').replace(' ', '')
        
        return None
    
    async def enrich_book_metadata(self, book_id: str, priority: int) -> Dict[str, Any]:
        """Enrich a single book with detailed metadata."""
        start_time = time.time()
        enrichment_data = {
            'book_id': book_id,
            'publication_year': None,
            'page_count': None,
            'isbn': None,
            'enriched_at': time.time(),
            'success': False,
            'error_message': None
        }
        
        try:
            # Apply rate limiting
            await self.wait_for_rate_limit(priority)
            
            # Get book from database
            with SessionLocal() as db:
                result = db.execute(
                    text("SELECT book_id, title FROM catalog WHERE book_id = :book_id"),
                    {"book_id": book_id}
                )
                book = result.fetchone()
                
                if not book:
                    enrichment_data['error_message'] = f"Book {book_id} not found in database"
                    logger.warning(enrichment_data['error_message'])
                    return enrichment_data
            
            # Extract OpenLibrary work key from book_id
            if book_id.startswith('OL') and book_id.endswith('W'):
                work_key = f"/works/{book_id}"
            elif book_id.startswith('OL'):
                work_key = f"/works/{book_id[2:]}"  # Remove 'OL' prefix
            else:
                enrichment_data['error_message'] = f"Book {book_id} doesn't have OpenLibrary format"
                logger.warning(enrichment_data['error_message'])
                return enrichment_data
            
            # Fetch work details
            work_data = await self.fetch_work_details(work_key)
            if not work_data:
                enrichment_data['error_message'] = f"Failed to fetch work data for {work_key}"
                return enrichment_data
            
            # Try to get edition details for more specific data
            edition_data = None
            if 'editions' in work_data and work_data['editions']:
                edition_key = work_data['editions'][0]  # Use first edition
                if isinstance(edition_key, dict) and 'key' in edition_key:
                    edition_key = edition_key['key']
                edition_data = await self.fetch_edition_details(edition_key)
            
            # Extract metadata
            enrichment_data['publication_year'] = self.extract_publication_year(work_data, edition_data)
            enrichment_data['page_count'] = self.extract_page_count(work_data, edition_data)
            enrichment_data['isbn'] = self.extract_isbn(edition_data) if edition_data else None
            enrichment_data['success'] = True
            
            processing_time = time.time() - start_time
            logger.info(f"Enriched book {book_id} (priority {priority}) in {processing_time:.2f}s: "
                       f"year={enrichment_data['publication_year']}, "
                       f"pages={enrichment_data['page_count']}, "
                       f"isbn={enrichment_data['isbn']}")
            
            # Note: Metrics recording removed temporarily for import compatibility
            
            return enrichment_data
            
        except Exception as e:
            processing_time = time.time() - start_time
            enrichment_data['error_message'] = str(e)
            logger.error(f"Error enriching book {book_id} (priority {priority}): {e}")
            return enrichment_data
    
    async def update_book_in_database(self, enrichment_data: Dict[str, Any]) -> bool:
        """Update book record with enriched metadata and tracking status."""
        try:
            book_id = enrichment_data['book_id']
            
            with SessionLocal() as db:
                # Use the database function for atomic updates
                if enrichment_data['success']:
                    # Update with successful enrichment
                    db.execute(
                        text("""
                            SELECT update_enrichment_status(
                                :book_id, 'completed', :publication_year, :page_count, :isbn, NULL
                            )
                        """),
                        {
                            "book_id": book_id,
                            "publication_year": enrichment_data['publication_year'],
                            "page_count": enrichment_data['page_count'],
                            "isbn": enrichment_data['isbn']
                        }
                    )
                    logger.info(f"Successfully updated database for book {book_id}")
                else:
                    # Update with failure
                    db.execute(
                        text("""
                            SELECT update_enrichment_status(
                                :book_id, 'failed', NULL, NULL, NULL, :error_message
                            )
                        """),
                        {
                            "book_id": book_id,
                            "error_message": enrichment_data['error_message']
                        }
                    )
                    logger.warning(f"Marked book {book_id} as failed: {enrichment_data['error_message']}")
                
                db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error updating database for book {enrichment_data['book_id']}: {e}")
            return False
    
    async def should_retry(self, book_id: str, priority: int) -> bool:
        """Check if book should be retried based on attempt count and priority."""
        try:
            with SessionLocal() as db:
                result = db.execute(
                    text("""
                        SELECT attempts, enrichment_status, last_attempt 
                        FROM book_metadata_enrichment 
                        WHERE book_id = :book_id
                    """),
                    {"book_id": book_id}
                )
                record = result.fetchone()
                
                if not record:
                    return True  # No record, first attempt
                
                attempts, status, last_attempt = record
                max_retries = ENRICHMENT_CONFIG['max_retries'][priority]
                
                # Don't retry if already completed or max attempts reached
                if status == 'completed' or attempts >= max_retries:
                    return False
                
                # For failed attempts, check if enough time has passed (exponential backoff)
                if last_attempt and status == 'failed':
                    time_since_last = datetime.now() - last_attempt
                    min_delay = timedelta(seconds=2 ** min(attempts, 6))  # Cap at 64 seconds
                    return time_since_last >= min_delay
                
                return True
                
        except Exception as e:
            logger.error(f"Error checking retry status for {book_id}: {e}")
            return False
    
    async def process_book(self, book_item: Dict[str, Any]) -> bool:
        """Process a single book enrichment with retry logic."""
        book_id = book_item['book_id']
        priority = book_item['priority']
        
        # Check if we should retry this book
        if not await self.should_retry(book_id, priority):
            logger.debug(f"Skipping book {book_id} - no retry needed")
            return False
        
        # Mark as in progress
        try:
            with SessionLocal() as db:
                db.execute(
                    text("""
                        UPDATE book_metadata_enrichment 
                        SET enrichment_status = 'in_progress',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE book_id = :book_id
                    """),
                    {"book_id": book_id}
                )
                db.commit()
        except Exception as e:
            logger.error(f"Error marking book {book_id} as in progress: {e}")
        
        # Enrich the book
        enrichment_data = await self.enrich_book_metadata(book_id, priority)
        
        # Update database
        success = await self.update_book_in_database(enrichment_data)
        
        # Send completion event
        if success:
            try:
                completion_event = {
                    'event_type': 'book_enrichment_completed',
                    'book_id': book_id,
                    'priority': priority,
                    'success': enrichment_data['success'],
                    'enrichment_data': enrichment_data,
                    'timestamp': time.time()
                }
                await publish_event('book_events', completion_event)
                # Emit BookAddedEvent for re-embedding
                book_added_event = BookAddedEvent(
                    count=1,
                    book_ids=[book_id],
                    source="enrichment_worker"
                )
                await publish_event(BOOK_EVENTS_TOPIC, book_added_event.model_dump())
            except Exception as e:
                logger.error(f"Error sending completion event for {book_id}: {e}")
        
        return success
    
    async def scan_for_pending_enrichments(self):
        """Scan database for books needing enrichment and add to low priority queue."""
        try:
            with SessionLocal() as db:
                # Use the database function to get enrichment batch
                result = db.execute(
                    text("SELECT * FROM get_enrichment_batch(:batch_size, :priority)"),
                    {
                        "batch_size": ENRICHMENT_CONFIG['batch_size'],
                        "priority": 1  # Low priority for background processing
                    }
                )
                books = result.fetchall()
            
            for book in books:
                await self.add_to_queue(
                    book.book_id, 
                    priority=1, 
                    reason='background_scan',
                    requester='background_worker'
                )
            
            if books:
                logger.info(f"Added {len(books)} books to background enrichment queue")
                
        except Exception as e:
            logger.error(f"Error scanning for pending enrichments: {e}")
    
    async def process_priority_queues(self):
        """Process all priority queues in order."""
        processed_count = 0
        
        while True:
            book_item = await self.get_next_book()
            if not book_item:
                break
                
            try:
                success = await self.process_book(book_item)
                if success:
                    processed_count += 1
            except Exception as e:
                logger.error(f"Error processing book {book_item['book_id']}: {e}")
        
        if processed_count > 0:
            logger.info(f"Processed {processed_count} books from priority queues")
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get current health status and metrics."""
        try:
            with SessionLocal() as db:
                # Get queue depths by priority
                result = db.execute(
                    text("""
                        SELECT priority, enrichment_status, COUNT(*) as count
                        FROM book_metadata_enrichment 
                        WHERE enrichment_status IN ('pending', 'in_progress')
                        GROUP BY priority, enrichment_status
                    """)
                )
                queue_stats = {}
                for row in result.fetchall():
                    priority, status, count = row
                    if priority not in queue_stats:
                        queue_stats[priority] = {}
                    queue_stats[priority][status] = count
                
                # Get processing rate over last hour
                result = db.execute(
                    text("""
                        SELECT COUNT(*) as completed_last_hour
                        FROM book_metadata_enrichment 
                        WHERE enriched_at > NOW() - INTERVAL '1 hour'
                          AND enrichment_status = 'completed'
                    """)
                )
                completed_last_hour = result.fetchone()[0]
                
                return {
                    'status': 'healthy',
                    'queue_stats': queue_stats,
                    'completed_last_hour': completed_last_hour,
                    'in_memory_queues': {
                        priority: len(queue) for priority, queue in self.priority_queues.items()
                    },
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting health status: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Main processing functions
async def process_kafka_message(manager: PriorityEnrichmentManager, message: Dict[str, Any]):
    """Process enrichment request from Kafka."""
    try:
        event_type = message.get('event_type', 'book_enrichment_requested')
        book_id = message.get('book_id')
        
        if not book_id:
            logger.warning("Received Kafka message without book_id")
            return
        
        # Extract priority (default to 2 for backward compatibility)
        priority = message.get('priority', 2)
        reason = message.get('reason', 'kafka_request')
        requester = message.get('requester', 'unknown')
        
        logger.info(f"Processing Kafka enrichment request for book {book_id} "
                   f"(priority {priority}, reason: {reason})")
        
        await manager.add_to_queue(book_id, priority, reason, requester)
        
    except Exception as e:
        logger.error(f"Error processing Kafka message: {e}")

async def setup_kafka_consumer(manager: PriorityEnrichmentManager):
    """Setup and run Kafka consumer for enrichment requests."""
    consumer = AIOKafkaConsumer(
        'book_enrichment_requests',
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="book_enrichment_worker",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    logger.info("Started Kafka consumer for enrichment requests")
    
    try:
        async for message in consumer:
            try:
                await process_kafka_message(manager, message.value)
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
    finally:
        await consumer.stop()

async def main():
    """Main worker loop with priority-based enrichment processing."""
    logger.info("Starting Enhanced Book Enrichment Worker with Priority Processing")
    logger.info(f"Configuration: {ENRICHMENT_CONFIG}")
    
    manager = PriorityEnrichmentManager()
    await manager.initialize()
    
    try:
        # Start Kafka consumer in background
        kafka_task = asyncio.create_task(setup_kafka_consumer(manager))
        
        last_health_log = 0
        
        while True:
            try:
                # 1. Process priority queues
                await manager.process_priority_queues()
                
                # 2. Background batch processing (every 30 seconds)
                current_time = time.time()
                if current_time - manager.last_batch_time >= ENRICHMENT_CONFIG['batch_interval']:
                    await manager.scan_for_pending_enrichments()
                    manager.last_batch_time = current_time
                
                # 3. Log health status every 5 minutes
                if current_time - last_health_log >= 300:
                    health_status = await manager.get_health_status()
                    logger.info(f"Health status: {health_status}")
                    last_health_log = current_time
                
                # Short sleep to prevent busy waiting
                await asyncio.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                kafka_task.cancel()
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5)  # Longer delay on errors
                
    finally:
        await manager.cleanup()
        logger.info("Book Enrichment Worker shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
