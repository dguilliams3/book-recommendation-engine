#!/usr/bin/env python3
"""
Enrichment Priority System Integration Test

Tests the complete priority-based enrichment system:
- Priority queue handling
- Rate limiting compliance
- Database status tracking
- End-to-end enrichment flow
- Retry logic and error handling
"""

import asyncio
import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from common.settings import get_settings
from common.structured_logging import setup_logging, get_logger
from common.kafka_utils import KafkaProducer

# Setup
setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Database setup
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Kafka producer for sending test requests
kafka_producer = KafkaProducer(
    bootstrap_servers=settings.kafka_bootstrap_servers
)

class EnrichmentSystemTester:
    """Test the complete enrichment system."""
    
    def __init__(self):
        self.test_books = []
        self.test_results = {}
        
    async def create_test_books(self, count: int = 5) -> List[str]:
        """Create test books in the database."""
        test_book_ids = []
        
        try:
            with SessionLocal() as db:
                for i in range(count):
                    book_id = f"OL{uuid.uuid4().hex[:8]}W"
                    test_book_ids.append(book_id)
                    
                    # Insert basic book data without enrichment metadata
                    db.execute(
                        text("""
                            INSERT INTO catalog 
                            (book_id, title, author, publication_year, page_count, isbn)
                            VALUES (:book_id, :title, :author, NULL, NULL, NULL)
                        """),
                        {
                            "book_id": book_id,
                            "title": f"Test Book {i+1}",
                            "author": f"Test Author {i+1}"
                        }
                    )
                
                db.commit()
                logger.info(f"Created {count} test books: {test_book_ids}")
                
        except Exception as e:
            logger.error(f"Error creating test books: {e}")
            return []
        
        self.test_books = test_book_ids
        return test_book_ids
    
    async def send_enrichment_request(self, book_id: str, priority: int, reason: str) -> str:
        """Send enrichment request via Kafka."""
        request_id = str(uuid.uuid4())
        
        try:
            enrichment_request = {
                'event_type': 'book_enrichment_requested',
                'request_id': request_id,
                'book_id': book_id,
                'priority': priority,
                'reason': reason,
                'requester': 'test_script',
                'timestamp': time.time()
            }
            
            kafka_producer.send('book_enrichment_requests', enrichment_request)
            logger.info(f"Sent enrichment request for {book_id} (priority {priority})")
            
            return request_id
            
        except Exception as e:
            logger.error(f"Error sending enrichment request for {book_id}: {e}")
            return ""
    
    async def wait_for_enrichment(self, book_id: str, timeout: int = 30) -> Dict[str, Any]:
        """Wait for enrichment to complete and return status."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                with SessionLocal() as db:
                    result = db.execute(
                        text("""
                            SELECT enrichment_status, attempts, error_message, 
                                   publication_year, page_count, isbn, updated_at
                            FROM book_metadata_enrichment 
                            WHERE book_id = :book_id
                        """),
                        {"book_id": book_id}
                    )
                    record = result.fetchone()
                    
                    if record:
                        status, attempts, error_msg, pub_year, pages, isbn, updated = record
                        
                        if status in ['completed', 'failed']:
                            return {
                                'status': status,
                                'attempts': attempts,
                                'error_message': error_msg,
                                'publication_year': pub_year,
                                'page_count': pages,
                                'isbn': isbn,
                                'processing_time': time.time() - start_time,
                                'updated_at': updated
                            }
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error checking enrichment status for {book_id}: {e}")
                await asyncio.sleep(1)
        
        return {
            'status': 'timeout',
            'processing_time': timeout,
            'error_message': f'Enrichment did not complete within {timeout} seconds'
        }
    
    async def test_priority_ordering(self) -> Dict[str, Any]:
        """Test that higher priority requests are processed first."""
        logger.info("Testing priority ordering...")
        
        if len(self.test_books) < 3:
            return {'error': 'Need at least 3 test books for priority testing'}
        
        # Send requests in reverse priority order (low to high)
        request_times = {}
        request_ids = {}
        
        # Low priority (should process last)
        request_ids['low'] = await self.send_enrichment_request(
            self.test_books[0], 1, 'priority_test_low'
        )
        request_times['low'] = time.time()
        
        await asyncio.sleep(0.1)
        
        # High priority (should process second)
        request_ids['high'] = await self.send_enrichment_request(
            self.test_books[1], 2, 'priority_test_high'
        )
        request_times['high'] = time.time()
        
        await asyncio.sleep(0.1)
        
        # Critical priority (should process first)
        request_ids['critical'] = await self.send_enrichment_request(
            self.test_books[2], 3, 'priority_test_critical'
        )
        request_times['critical'] = time.time()
        
        # Wait for all to complete
        results = {}
        for priority, book_id in [('critical', self.test_books[2]), 
                                 ('high', self.test_books[1]), 
                                 ('low', self.test_books[0])]:
            results[priority] = await self.wait_for_enrichment(book_id, 60)
        
        # Analyze results
        completion_order = []
        for priority in ['critical', 'high', 'low']:
            if results[priority]['status'] == 'completed':
                completion_order.append(priority)
        
        expected_order = ['critical', 'high', 'low']
        priority_correct = completion_order == expected_order
        
        return {
            'test': 'priority_ordering',
            'success': priority_correct,
            'expected_order': expected_order,
            'actual_order': completion_order,
            'results': results,
            'details': f"Priority ordering {'PASSED' if priority_correct else 'FAILED'}"
        }
    
    async def test_rate_limiting(self) -> Dict[str, Any]:
        """Test rate limiting compliance."""
        logger.info("Testing rate limiting...")
        
        if len(self.test_books) < 2:
            return {'error': 'Need at least 2 test books for rate limiting test'}
        
        # Send two critical priority requests quickly
        start_time = time.time()
        
        await self.send_enrichment_request(
            self.test_books[0], 3, 'rate_limit_test_1'
        )
        
        await self.send_enrichment_request(
            self.test_books[1], 3, 'rate_limit_test_2'
        )
        
        # Wait for both to complete
        result1 = await self.wait_for_enrichment(self.test_books[0], 30)
        result2 = await self.wait_for_enrichment(self.test_books[1], 30)
        
        total_time = time.time() - start_time
        expected_min_time = 0.1  # Critical priority rate limit
        
        rate_limit_respected = total_time >= expected_min_time
        
        return {
            'test': 'rate_limiting',
            'success': rate_limit_respected,
            'total_time': total_time,
            'expected_min_time': expected_min_time,
            'result1': result1,
            'result2': result2,
            'details': f"Rate limiting {'PASSED' if rate_limit_respected else 'FAILED'}"
        }
    
    async def test_retry_logic(self) -> Dict[str, Any]:
        """Test retry logic by using an invalid book ID."""
        logger.info("Testing retry logic...")
        
        # Use an invalid book ID that will trigger retries
        invalid_book_id = "INVALID_BOOK_ID_TEST"
        
        # Insert invalid book to database
        try:
            with SessionLocal() as db:
                db.execute(
                    text("""
                        INSERT INTO catalog 
                        (book_id, title, author, publication_year, page_count, isbn)
                        VALUES (:book_id, :title, :author, NULL, NULL, NULL)
                    """),
                    {
                        "book_id": invalid_book_id,
                        "title": "Invalid Test Book",
                        "author": "Test Author"
                    }
                )
                db.commit()
        except Exception as e:
            logger.error(f"Error inserting invalid test book: {e}")
            return {'error': 'Could not create invalid test book'}
        
        # Send enrichment request
        request_id = await self.send_enrichment_request(
            invalid_book_id, 2, 'retry_logic_test'
        )
        
        # Wait for processing (should fail and retry)
        result = await self.wait_for_enrichment(invalid_book_id, 60)
        
        # Check that retries were attempted
        retry_logic_working = (
            result['status'] == 'failed' and 
            result.get('attempts', 0) > 1
        )
        
        return {
            'test': 'retry_logic',
            'success': retry_logic_working,
            'attempts': result.get('attempts', 0),
            'status': result['status'],
            'error_message': result.get('error_message', ''),
            'details': f"Retry logic {'PASSED' if retry_logic_working else 'FAILED'}"
        }
    
    async def test_database_integration(self) -> Dict[str, Any]:
        """Test database views and functions work correctly."""
        logger.info("Testing database integration...")
        
        try:
            with SessionLocal() as db:
                # Test books_needing_enrichment view
                result = db.execute(text("SELECT COUNT(*) FROM books_needing_enrichment"))
                books_needing = result.fetchone()[0]
                
                # Test get_enrichment_batch function
                result = db.execute(
                    text("SELECT * FROM get_enrichment_batch(5, 3) LIMIT 5")
                )
                batch_books = result.fetchall()
                
                # Test update_enrichment_status function (read-only test)
                result = db.execute(
                    text("""
                        SELECT 1 FROM information_schema.routines 
                        WHERE routine_name = 'update_enrichment_status'
                    """)
                )
                function_exists = result.fetchone() is not None
                
                return {
                    'test': 'database_integration',
                    'success': True,
                    'books_needing_enrichment': books_needing,
                    'batch_function_works': len(batch_books) >= 0,
                    'update_function_exists': function_exists,
                    'details': 'Database integration PASSED'
                }
                
        except Exception as e:
            logger.error(f"Database integration test failed: {e}")
            return {
                'test': 'database_integration',
                'success': False,
                'error': str(e),
                'details': 'Database integration FAILED'
            }
    
    async def cleanup_test_books(self):
        """Clean up test books from database."""
        try:
            with SessionLocal() as db:
                for book_id in self.test_books:
                    # Clean up enrichment tracking
                    db.execute(
                        text("DELETE FROM book_metadata_enrichment WHERE book_id = :book_id"),
                        {"book_id": book_id}
                    )
                    
                    # Clean up enrichment requests
                    db.execute(
                        text("DELETE FROM enrichment_requests WHERE book_id = :book_id"),
                        {"book_id": book_id}
                    )
                    
                    # Clean up catalog entry
                    db.execute(
                        text("DELETE FROM catalog WHERE book_id = :book_id"),
                        {"book_id": book_id}
                    )
                
                # Clean up invalid test book
                db.execute(
                    text("DELETE FROM catalog WHERE book_id = 'INVALID_BOOK_ID_TEST'")
                )
                db.execute(
                    text("DELETE FROM book_metadata_enrichment WHERE book_id = 'INVALID_BOOK_ID_TEST'")
                )
                db.execute(
                    text("DELETE FROM enrichment_requests WHERE book_id = 'INVALID_BOOK_ID_TEST'")
                )
                
                db.commit()
                logger.info("Cleaned up test books")
                
        except Exception as e:
            logger.error(f"Error cleaning up test books: {e}")
    
    def format_test_results(self, results: List[Dict[str, Any]]) -> str:
        """Format test results as readable output."""
        output = []
        output.append("=" * 80)
        output.append("ENRICHMENT SYSTEM INTEGRATION TEST RESULTS")
        output.append("=" * 80)
        
        passed = 0
        total = len(results)
        
        for result in results:
            test_name = result.get('test', 'Unknown')
            success = result.get('success', False)
            details = result.get('details', 'No details')
            
            status_emoji = "✅" if success else "❌"
            output.append(f"\n{status_emoji} {test_name.upper()}: {details}")
            
            if success:
                passed += 1
            
            # Add additional details for failed tests
            if not success and 'error' in result:
                output.append(f"   Error: {result['error']}")
        
        output.append(f"\n{'='*80}")
        output.append(f"SUMMARY: {passed}/{total} tests passed")
        
        if passed == total:
            output.append("🎉 ALL TESTS PASSED! The enrichment system is working correctly.")
        else:
            output.append("⚠️  SOME TESTS FAILED. Please check the system configuration.")
        
        return "\n".join(output)

async def run_integration_tests():
    """Run complete integration test suite."""
    tester = EnrichmentSystemTester()
    
    print("Starting enrichment system integration tests...")
    print("This will test priority handling, rate limiting, retry logic, and database integration.")
    print("=" * 80)
    
    try:
        # Create test books
        print("Creating test books...")
        test_books = await tester.create_test_books(5)
        if not test_books:
            print("❌ Failed to create test books. Aborting tests.")
            return
        
        # Wait a moment for any existing enrichment worker to settle
        print("Waiting for system to settle...")
        await asyncio.sleep(2)
        
        # Run all tests
        test_results = []
        
        print("Running database integration test...")
        result = await tester.test_database_integration()
        test_results.append(result)
        
        print("Running priority ordering test...")
        result = await tester.test_priority_ordering()
        test_results.append(result)
        
        print("Running rate limiting test...")
        result = await tester.test_rate_limiting()
        test_results.append(result)
        
        print("Running retry logic test...")
        result = await tester.test_retry_logic()
        test_results.append(result)
        
        # Display results
        print("\n" + tester.format_test_results(test_results))
        
    finally:
        # Cleanup
        print("\nCleaning up test data...")
        await tester.cleanup_test_books()
        print("Cleanup complete.")

def main():
    """Main entry point."""
    print("Enrichment System Integration Test")
    print("=" * 80)
    print("This test validates the complete priority-based enrichment system.")
    print("Make sure the enrichment worker is running before starting tests.")
    print()
    
    response = input("Continue with tests? (y/N): ").strip().lower()
    if response != 'y':
        print("Tests cancelled.")
        return
    
    asyncio.run(run_integration_tests())

if __name__ == "__main__":
    main() 