# Enrichment System Scripts

This directory contains scripts for monitoring, testing, and managing the continuous enrichment system with priority escalation.

## Scripts Overview

### 📊 monitor_enrichment_system.py
**Purpose**: Monitor the health and performance of the enrichment system in real-time.

**Features**:
- Queue depths by priority level
- Processing rates and completion times
- Success/failure rates and retry statistics
- Health checks for stuck processes
- Database connectivity monitoring

**Usage**:
```bash
# One-time status check
python scripts/monitor_enrichment_system.py

# Continuous monitoring (updates every 60 seconds)
python scripts/monitor_enrichment_system.py --continuous

# Custom monitoring interval
python scripts/monitor_enrichment_system.py --continuous --interval 30
```

**Sample Output**:
```
📊 QUEUE STATUS BY PRIORITY:
Priority 3 (Critical):
  pending: 2
  in_progress: 1

⚡ PROCESSING RATES:
1 Hour:
  completed: 45 (85.5%)
  failed: 8 (14.5%)

⏱️ PROCESSING TIMES (24h avg):
Priority 3 (Critical): 3.2s avg (12 completed)
Priority 2 (High): 15.8s avg (28 completed)
Priority 1 (Low): 45.1s avg (156 completed)
```

### 🧪 test_enrichment_priority_system.py
**Purpose**: Integration testing for the complete priority-based enrichment system.

**Test Coverage**:
- Priority queue ordering (critical → high → low)
- Rate limiting compliance by priority level
- Retry logic with exponential backoff
- Database views and functions integration
- End-to-end enrichment flow

**Usage**:
```bash
# Run complete test suite
python scripts/test_enrichment_priority_system.py

# The script will:
# 1. Create test books in the database
# 2. Send enrichment requests with different priorities
# 3. Verify processing order and timing
# 4. Test error handling and retries
# 5. Clean up test data automatically
```

**Sample Output**:
```
✅ DATABASE_INTEGRATION: Database integration PASSED
✅ PRIORITY_ORDERING: Priority ordering PASSED
✅ RATE_LIMITING: Rate limiting PASSED  
✅ RETRY_LOGIC: Retry logic PASSED

SUMMARY: 4/4 tests passed
🎉 ALL TESTS PASSED! The enrichment system is working correctly.
```

## Requirements

### Environment Setup
All scripts require the enrichment system to be running:

```bash
# Start the complete system
docker-compose up

# Or just the essential services for testing
docker-compose up postgres kafka zookeeper book_enrichment_worker
```

### Python Dependencies
Scripts use the existing project dependencies. No additional packages required.

### Database Access
Scripts connect to the database using settings from:
- Environment variables (if set)
- `.env` file (if present)
- Default connection parameters from `common.settings`

## System Configuration

### Environment Variables (Optional)
You can customize the enrichment system behavior via environment variables:

```bash
# Batch processing
ENRICHMENT_BATCH_SIZE=50              # Books per background batch
ENRICHMENT_BATCH_INTERVAL=30          # Seconds between background scans

# Retry limits by priority
ENRICHMENT_MAX_RETRIES_CRITICAL=5     # Critical requests (user-facing)
ENRICHMENT_MAX_RETRIES_HIGH=3         # High priority (worker requests)
ENRICHMENT_MAX_RETRIES_LOW=2          # Low priority (background)

# Rate limiting by priority (seconds between API calls)
ENRICHMENT_RATE_LIMIT_CRITICAL=0.1    # 10 requests/second max
ENRICHMENT_RATE_LIMIT_HIGH=0.2        # 5 requests/second max  
ENRICHMENT_RATE_LIMIT_LOW=0.5         # 2 requests/second max

# API configuration
ENRICHMENT_API_TIMEOUT=10             # HTTP timeout in seconds
OPENLIBRARY_BASE_URL=https://openlibrary.org
```

### Docker Compose
These variables are pre-configured in `docker-compose.yml` with sensible defaults. You can override them in your `.env` file if needed.

## Troubleshooting

### Common Issues

**"No module named 'common'"**
- Run scripts from the project root directory
- Scripts automatically add `src/` to Python path

**"Database connection failed"**
- Ensure PostgreSQL container is running: `docker-compose ps postgres`
- Check database health: `docker-compose exec postgres pg_isready -U books`

**"Kafka connection failed"**
- Ensure Kafka container is running: `docker-compose ps kafka`
- Check Kafka logs: `docker-compose logs kafka`

**"No enrichment activity"**
- Verify enrichment worker is running: `docker-compose ps book_enrichment_worker`
- Check worker logs: `docker-compose logs book_enrichment_worker`
- Run integration tests to verify system health

### Performance Monitoring

**Queue Buildup**
- Monitor queue depths with `monitor_enrichment_system.py --continuous`
- Adjust `ENRICHMENT_BATCH_SIZE` if background processing is slow
- Scale worker instances: `docker-compose up --scale book_enrichment_worker=2`

**High Error Rates**
- Check OpenLibrary API availability
- Review error messages in enrichment tracking tables
- Adjust retry limits if experiencing temporary failures

**Slow Processing**
- Verify rate limiting settings aren't too conservative
- Check network connectivity to OpenLibrary API
- Monitor database performance during high load

### Database Queries

**Manual Status Check**:
```sql
-- Queue status by priority
SELECT priority, enrichment_status, COUNT(*)
FROM book_metadata_enrichment 
GROUP BY priority, enrichment_status
ORDER BY priority DESC;

-- Recent processing activity
SELECT enrichment_status, COUNT(*) 
FROM book_metadata_enrichment 
WHERE updated_at > NOW() - INTERVAL '1 hour'
GROUP BY enrichment_status;

-- Books still needing enrichment
SELECT COUNT(*) FROM books_needing_enrichment;
```

**Performance Analysis**:
```sql
-- Average processing times by priority
SELECT 
    priority,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_seconds,
    COUNT(*) as completed_count
FROM book_metadata_enrichment 
WHERE enrichment_status = 'completed'
  AND updated_at > NOW() - INTERVAL '24 hours'
GROUP BY priority;

-- Retry statistics
SELECT 
    priority,
    AVG(attempts) as avg_attempts,
    MAX(attempts) as max_attempts,
    COUNT(CASE WHEN enrichment_status = 'failed' THEN 1 END) as failed,
    COUNT(CASE WHEN enrichment_status = 'completed' THEN 1 END) as success
FROM book_metadata_enrichment 
GROUP BY priority;
```

## Development

### Adding New Tests
To add tests to the integration suite:

1. Create test method in `EnrichmentSystemTester` class
2. Add test call to `run_integration_tests()` function
3. Return result dictionary with standard format:
   ```python
   {
       'test': 'test_name',
       'success': bool,
       'details': 'Human readable result',
       # Additional test-specific data
   }
   ```

### Extending Monitoring
To add new monitoring metrics:

1. Add database query to `get_enrichment_stats()` method
2. Update `format_stats_table()` to display new data
3. Consider adding health checks in `check_system_health()`

### Custom Configuration
Environment variables are automatically loaded from:
1. System environment
2. `.env` file in project root
3. Docker Compose environment section

Priority: System > .env file > Docker defaults 