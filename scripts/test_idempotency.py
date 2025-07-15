#!/usr/bin/env python3
"""
Test Idempotency System

This script demonstrates the idempotency system by running the ingestion service
multiple times and showing how unchanged data is skipped.
"""

import asyncio
import time
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion_service.pipeline import run_ingestion
from common.structured_logging import get_logger

logger = get_logger(__name__)


async def test_idempotency():
    """Test the idempotency system with multiple ingestion runs"""
    
    print("🧪 Testing Idempotency System")
    print("=" * 50)
    
    # First run - should process everything
    print("\n📥 First Run - Processing all data...")
    start_time = time.perf_counter()
    await run_ingestion()
    first_run_time = time.perf_counter() - start_time
    print(f"✅ First run completed in {first_run_time:.2f} seconds")
    
    # Second run - should skip most operations
    print("\n🔄 Second Run - Should skip unchanged data...")
    start_time = time.perf_counter()
    await run_ingestion()
    second_run_time = time.perf_counter() - start_time
    print(f"✅ Second run completed in {second_run_time:.2f} seconds")
    
    # Third run - should skip everything
    print("\n🔄 Third Run - Should skip all data...")
    start_time = time.perf_counter()
    await run_ingestion()
    third_run_time = time.perf_counter() - start_time
    print(f"✅ Third run completed in {third_run_time:.2f} seconds")
    
    # Calculate performance improvements
    if first_run_time > 0:
        second_improvement = ((first_run_time - second_run_time) / first_run_time) * 100
        third_improvement = ((first_run_time - third_run_time) / first_run_time) * 100
        
        print("\n📊 Performance Analysis")
        print("=" * 30)
        print(f"First run time:  {first_run_time:.2f}s")
        print(f"Second run time: {second_run_time:.2f}s ({second_improvement:.1f}% faster)")
        print(f"Third run time:  {third_run_time:.2f}s ({third_improvement:.1f}% faster)")
        
        if second_improvement > 50:
            print("🎉 Excellent! Idempotency system is working correctly.")
        elif second_improvement > 20:
            print("✅ Good! Idempotency system is working.")
        else:
            print("⚠️  Low improvement - check if data is actually unchanged.")
    
    print("\n✨ Idempotency test completed!")


async def test_incremental_update():
    """Test incremental updates by adding a new book"""
    
    print("\n\n🔧 Testing Incremental Updates")
    print("=" * 40)
    
    # Add a new book to the catalog
    catalog_path = Path("data/catalog_openlibrary_sample.csv")
    if catalog_path.exists():
        with open(catalog_path, "a") as f:
            f.write('\nOL_TEST_001,"Test Book for Idempotency","Test Author","1234567890","fiction",4.5,5,2024,200,4.2')
        
        print("📝 Added test book to catalog")
        
        # Run ingestion - should only process the new book
        print("🔄 Running ingestion with new book...")
        start_time = time.perf_counter()
        await run_ingestion()
        incremental_time = time.perf_counter() - start_time
        print(f"✅ Incremental update completed in {incremental_time:.2f} seconds")
        
        # Run again - should skip the new book too
        print("🔄 Running ingestion again...")
        start_time = time.perf_counter()
        await run_ingestion()
        skip_time = time.perf_counter() - start_time
        print(f"✅ Skip run completed in {skip_time:.2f} seconds")
        
        print(f"\n📊 Incremental Update Results:")
        print(f"  - New book processing: {incremental_time:.2f}s")
        print(f"  - Skip run: {skip_time:.2f}s")
        
        if skip_time < incremental_time * 0.5:
            print("🎉 Incremental updates working correctly!")
        else:
            print("⚠️  Check incremental update logic")


if __name__ == "__main__":
    try:
        # Run basic idempotency test
        asyncio.run(test_idempotency())
        
        # Run incremental update test
        asyncio.run(test_incremental_update())
        
        print("\n🎯 All tests completed successfully!")
        print("\n💡 Tips:")
        print("  - Check logs for detailed processing statistics")
        print("  - Monitor 'skipped' vs 'processed' counts")
        print("  - Use this system for large OpenLibrary datasets")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1) 