#!/usr/bin/env python3
"""
Enrichment Management Script

This script helps manage book enrichment settings and retry failed enrichments.
"""

import asyncio
import httpx
import json
from typing import Dict, Any
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from common.settings import settings


async def get_enrichment_status() -> Dict[str, Any]:
    """Get current enrichment status from the service."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.user_ingest_service_url}/enrichment/status")
        response.raise_for_status()
        return response.json()


async def retry_failed_enrichments() -> Dict[str, Any]:
    """Retry failed enrichments."""
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{settings.user_ingest_service_url}/enrichment/retry")
        response.raise_for_status()
        return response.json()


async def cleanup_duplicate_books() -> Dict[str, Any]:
    """Clean up duplicate books."""
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{settings.user_ingest_service_url}/enrichment/cleanup-duplicates")
        response.raise_for_status()
        return response.json()


def print_enrichment_status(status: Dict[str, Any]):
    """Pretty print enrichment status."""
    print("\n" + "="*60)
    print("📊 ENRICHMENT STATUS")
    print("="*60)
    
    print(f"\n🔧 Current Settings:")
    print(f"   Max Attempts: {status['current_max_attempts']}")
    print(f"   Retry Delay Base: {status['retry_delay_base']}s")
    print(f"   Retry Delay Max: {status['retry_delay_max']}s")
    
    print(f"\n📈 Statistics:")
    for stat in status['enrichment_stats']:
        status_emoji = {
            'pending': '⏳',
            'in_progress': '🔄',
            'enriched': '✅',
            'failed': '❌',
            'max_attempts_reached': '🚫',
            'duplicate': '🔄'
        }.get(stat['status'], '❓')
        
        print(f"   {status_emoji} {stat['status']}: {stat['count']} books")
        if stat['avg_attempts'] > 0:
            print(f"      Avg attempts: {stat['avg_attempts']:.1f}")
    
    print(f"\n🔄 Retryable Books: {status['retryable_books']}")
    
    # Check for duplicates
    duplicate_count = next((stat['count'] for stat in status['enrichment_stats'] if stat['status'] == 'duplicate'), 0)
    if duplicate_count > 0:
        print(f"🔄 Duplicate Books: {duplicate_count}")
        print(f"\n💡 You can clean up {duplicate_count} duplicates with:")
        print(f"   python scripts/manage_enrichment.py cleanup")
    
    if status['retryable_books'] > 0:
        print(f"\n💡 You can retry {status['retryable_books']} books with:")
        print(f"   python scripts/manage_enrichment.py retry")


async def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/manage_enrichment.py [status|retry|cleanup]")
        print("\nCommands:")
        print("  status   - Show enrichment status and statistics")
        print("  retry    - Retry failed enrichments")
        print("  cleanup  - Remove duplicate books")
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == "status":
            status = await get_enrichment_status()
            print_enrichment_status(status)
            
        elif command == "retry":
            print("🔄 Retrying failed enrichments...")
            result = await retry_failed_enrichments()
            
            print(f"\n✅ {result['message']}")
            if result['retried_count'] > 0:
                print(f"📚 Books queued for retry: {result['retried_count']}")
                print(f"🆔 First few book IDs: {result['book_ids'][:3]}")
            
        elif command == "cleanup":
            print("🧹 Cleaning up duplicate books...")
            result = await cleanup_duplicate_books()
            
            print(f"\n✅ {result['message']}")
            if result['removed_count'] > 0:
                print(f"🗑️ Books removed: {result['removed_count']}")
                print(f"🆔 First few book IDs: {result['book_ids'][:3]}")
            
        else:
            print(f"❌ Unknown command: {command}")
            print("Available commands: status, retry, cleanup")
            
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP Error: {e.response.status_code}")
        print(f"Response: {e.response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 