#!/usr/bin/env python3
"""
Enrichment System Monitor

Monitors the continuous enrichment system status, including:
- Queue depths by priority
- Processing rates and times
- Success/failure rates
- Database health
- API performance metrics
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from common.settings import get_settings
from common.structured_logging import setup_logging, get_logger

# Setup
setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Database setup
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class EnrichmentSystemMonitor:
    """Monitor enrichment system health and performance."""
    
    def __init__(self):
        self.start_time = datetime.now()
    
    async def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get comprehensive enrichment system statistics."""
        try:
            with SessionLocal() as db:
                # Queue depths by priority and status
                result = db.execute(text("""
                    SELECT 
                        priority,
                        enrichment_status,
                        COUNT(*) as count
                    FROM book_metadata_enrichment 
                    GROUP BY priority, enrichment_status
                    ORDER BY priority DESC, enrichment_status
                """))
                
                queue_stats = {}
                for row in result.fetchall():
                    priority, status, count = row
                    if priority not in queue_stats:
                        queue_stats[priority] = {}
                    queue_stats[priority][status] = count
                
                # Processing rates over different time periods
                time_periods = {
                    '1_hour': 1,
                    '24_hours': 24,
                    '7_days': 168
                }
                
                processing_rates = {}
                for period_name, hours in time_periods.items():
                    result = db.execute(text("""
                        SELECT 
                            enrichment_status,
                            COUNT(*) as count
                        FROM book_metadata_enrichment 
                        WHERE updated_at > NOW() - INTERVAL '%s hours'
                        GROUP BY enrichment_status
                    """ % hours))
                    
                    processing_rates[period_name] = {}
                    for row in result.fetchall():
                        status, count = row
                        processing_rates[period_name][status] = count
                
                # Average processing times by priority
                result = db.execute(text("""
                    SELECT 
                        priority,
                        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_processing_time,
                        COUNT(*) as completed_count
                    FROM book_metadata_enrichment 
                    WHERE enrichment_status = 'completed'
                      AND updated_at > NOW() - INTERVAL '24 hours'
                    GROUP BY priority
                    ORDER BY priority DESC
                """))
                
                processing_times = {}
                for row in result.fetchall():
                    priority, avg_time, count = row
                    processing_times[priority] = {
                        'avg_seconds': float(avg_time or 0),
                        'completed_count': count
                    }
                
                # Retry statistics
                result = db.execute(text("""
                    SELECT 
                        priority,
                        AVG(attempts) as avg_attempts,
                        MAX(attempts) as max_attempts,
                        COUNT(CASE WHEN enrichment_status = 'failed' THEN 1 END) as failed_count,
                        COUNT(CASE WHEN enrichment_status = 'completed' THEN 1 END) as success_count
                    FROM book_metadata_enrichment 
                    GROUP BY priority
                    ORDER BY priority DESC
                """))
                
                retry_stats = {}
                for row in result.fetchall():
                    priority, avg_attempts, max_attempts, failed, success = row
                    total = failed + success
                    retry_stats[priority] = {
                        'avg_attempts': float(avg_attempts or 0),
                        'max_attempts': max_attempts or 0,
                        'success_rate': (success / total * 100) if total > 0 else 0,
                        'failure_rate': (failed / total * 100) if total > 0 else 0,
                        'total_processed': total
                    }
                
                # Books needing enrichment
                result = db.execute(text("""
                    SELECT COUNT(*) as count
                    FROM books_needing_enrichment
                """))
                books_needing_enrichment = result.fetchone()[0]
                
                # Recent enrichment requests
                result = db.execute(text("""
                    SELECT 
                        requester,
                        priority,
                        COUNT(*) as count
                    FROM enrichment_requests 
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                    GROUP BY requester, priority
                    ORDER BY priority DESC, count DESC
                """))
                
                recent_requests = {}
                for row in result.fetchall():
                    requester, priority, count = row
                    if requester not in recent_requests:
                        recent_requests[requester] = {}
                    recent_requests[requester][priority] = count
                
                return {
                    'timestamp': datetime.now().isoformat(),
                    'queue_stats': queue_stats,
                    'processing_rates': processing_rates,
                    'processing_times': processing_times,
                    'retry_stats': retry_stats,
                    'books_needing_enrichment': books_needing_enrichment,
                    'recent_requests': recent_requests
                }
                
        except Exception as e:
            logger.error(f"Error getting enrichment stats: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    async def check_system_health(self) -> Dict[str, Any]:
        """Check overall system health."""
        health_status = {
            'status': 'healthy',
            'checks': {},
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Database connectivity
            with SessionLocal() as db:
                result = db.execute(text("SELECT 1"))
                result.fetchone()
                health_status['checks']['database'] = {'status': 'healthy', 'message': 'Connected successfully'}
        except Exception as e:
            health_status['checks']['database'] = {'status': 'unhealthy', 'message': str(e)}
            health_status['status'] = 'unhealthy'
        
        try:
            # Check for stuck enrichments (in progress for > 30 minutes)
            with SessionLocal() as db:
                result = db.execute(text("""
                    SELECT COUNT(*) as stuck_count
                    FROM book_metadata_enrichment 
                    WHERE enrichment_status = 'in_progress'
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                """))
                stuck_count = result.fetchone()[0]
                
                if stuck_count > 0:
                    health_status['checks']['stuck_enrichments'] = {
                        'status': 'warning',
                        'message': f'{stuck_count} enrichments stuck in progress'
                    }
                else:
                    health_status['checks']['stuck_enrichments'] = {
                        'status': 'healthy',
                        'message': 'No stuck enrichments'
                    }
        except Exception as e:
            health_status['checks']['stuck_enrichments'] = {'status': 'error', 'message': str(e)}
        
        try:
            # Check processing rate (should have completed some in last hour)
            with SessionLocal() as db:
                result = db.execute(text("""
                    SELECT COUNT(*) as recent_completions
                    FROM book_metadata_enrichment 
                    WHERE enrichment_status = 'completed'
                      AND updated_at > NOW() - INTERVAL '1 hour'
                """))
                recent_completions = result.fetchone()[0]
                
                if recent_completions > 0:
                    health_status['checks']['processing_rate'] = {
                        'status': 'healthy',
                        'message': f'{recent_completions} completions in last hour'
                    }
                else:
                    health_status['checks']['processing_rate'] = {
                        'status': 'warning',
                        'message': 'No completions in last hour'
                    }
        except Exception as e:
            health_status['checks']['processing_rate'] = {'status': 'error', 'message': str(e)}
        
        return health_status
    
    def format_stats_table(self, stats: Dict[str, Any]) -> str:
        """Format statistics as a readable table."""
        if 'error' in stats:
            return f"ERROR: {stats['error']}"
        
        output = []
        output.append("=" * 80)
        output.append(f"ENRICHMENT SYSTEM STATUS - {stats['timestamp']}")
        output.append("=" * 80)
        
        # Queue Status
        output.append("\n📊 QUEUE STATUS BY PRIORITY:")
        output.append("-" * 40)
        queue_stats = stats.get('queue_stats', {})
        for priority in [3, 2, 1]:
            priority_name = {3: 'Critical', 2: 'High', 1: 'Low'}[priority]
            output.append(f"\nPriority {priority} ({priority_name}):")
            if priority in queue_stats:
                for status, count in queue_stats[priority].items():
                    output.append(f"  {status}: {count}")
            else:
                output.append("  No items")
        
        # Processing Rates
        output.append(f"\n⚡ PROCESSING RATES:")
        output.append("-" * 40)
        processing_rates = stats.get('processing_rates', {})
        for period, data in processing_rates.items():
            period_display = period.replace('_', ' ').title()
            output.append(f"\n{period_display}:")
            total = sum(data.values())
            for status, count in data.items():
                percentage = (count / total * 100) if total > 0 else 0
                output.append(f"  {status}: {count} ({percentage:.1f}%)")
        
        # Processing Times
        output.append(f"\n⏱️  PROCESSING TIMES (24h avg):")
        output.append("-" * 40)
        processing_times = stats.get('processing_times', {})
        for priority in [3, 2, 1]:
            priority_name = {3: 'Critical', 2: 'High', 1: 'Low'}[priority]
            if priority in processing_times:
                avg_time = processing_times[priority]['avg_seconds']
                count = processing_times[priority]['completed_count']
                output.append(f"Priority {priority} ({priority_name}): {avg_time:.1f}s avg ({count} completed)")
            else:
                output.append(f"Priority {priority} ({priority_name}): No data")
        
        # Retry Statistics
        output.append(f"\n🔄 RETRY STATISTICS:")
        output.append("-" * 40)
        retry_stats = stats.get('retry_stats', {})
        for priority in [3, 2, 1]:
            priority_name = {3: 'Critical', 2: 'High', 1: 'Low'}[priority]
            if priority in retry_stats:
                data = retry_stats[priority]
                output.append(f"Priority {priority} ({priority_name}):")
                output.append(f"  Success Rate: {data['success_rate']:.1f}%")
                output.append(f"  Avg Attempts: {data['avg_attempts']:.1f}")
                output.append(f"  Max Attempts: {data['max_attempts']}")
                output.append(f"  Total Processed: {data['total_processed']}")
            else:
                output.append(f"Priority {priority} ({priority_name}): No data")
        
        # Books needing enrichment
        books_needed = stats.get('books_needing_enrichment', 0)
        output.append(f"\n📚 BOOKS NEEDING ENRICHMENT: {books_needed}")
        
        # Recent requests
        output.append(f"\n📬 RECENT REQUESTS (Last hour):")
        output.append("-" * 40)
        recent_requests = stats.get('recent_requests', {})
        if recent_requests:
            for requester, priorities in recent_requests.items():
                output.append(f"{requester}:")
                for priority, count in priorities.items():
                    priority_name = {3: 'Critical', 2: 'High', 1: 'Low'}.get(priority, f'P{priority}')
                    output.append(f"  {priority_name}: {count}")
        else:
            output.append("No recent requests")
        
        return "\n".join(output)
    
    def format_health_status(self, health: Dict[str, Any]) -> str:
        """Format health status as readable output."""
        output = []
        output.append("=" * 80)
        output.append(f"SYSTEM HEALTH - {health['timestamp']}")
        output.append("=" * 80)
        
        # Overall status
        status_emoji = {'healthy': '✅', 'warning': '⚠️', 'unhealthy': '❌', 'error': '🔥'}
        overall_status = health['status']
        output.append(f"\nOverall Status: {status_emoji.get(overall_status, '❓')} {overall_status.upper()}")
        
        # Individual checks
        output.append(f"\nHealth Checks:")
        output.append("-" * 40)
        for check_name, check_data in health.get('checks', {}).items():
            status = check_data['status']
            message = check_data['message']
            emoji = status_emoji.get(status, '❓')
            output.append(f"{emoji} {check_name}: {message}")
        
        return "\n".join(output)

async def monitor_once():
    """Run monitoring once and display results."""
    monitor = EnrichmentSystemMonitor()
    
    print("Collecting enrichment system statistics...")
    stats = await monitor.get_enrichment_stats()
    health = await monitor.check_system_health()
    
    print(monitor.format_stats_table(stats))
    print(monitor.format_health_status(health))

async def monitor_continuous(interval: int = 60):
    """Run continuous monitoring with specified interval."""
    monitor = EnrichmentSystemMonitor()
    
    print(f"Starting continuous monitoring (interval: {interval}s)")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Collecting stats...")
            
            stats = await monitor.get_enrichment_stats()
            health = await monitor.check_system_health()
            
            # Clear screen for continuous monitoring
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(monitor.format_stats_table(stats))
            print(monitor.format_health_status(health))
            
            print(f"\nNext update in {interval} seconds... (Ctrl+C to stop)")
            await asyncio.sleep(interval)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor enrichment system")
    parser.add_argument('--continuous', '-c', action='store_true',
                        help='Run continuous monitoring')
    parser.add_argument('--interval', '-i', type=int, default=60,
                        help='Monitoring interval in seconds (default: 60)')
    
    args = parser.parse_args()
    
    if args.continuous:
        asyncio.run(monitor_continuous(args.interval))
    else:
        asyncio.run(monitor_once())

if __name__ == "__main__":
    main() 