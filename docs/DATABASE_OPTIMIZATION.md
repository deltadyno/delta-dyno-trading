# Database Optimization Strategy

As the telemetry system collects data from multiple clients over time, the database tables will grow significantly. This document outlines strategies to avoid performance bottlenecks.

## Backend Database Strategies

### 1. **Database Indexing** (Critical)

Ensure proper indexes are in place on the telemetry tables:

```sql
-- dd_telemetry_metrics
-- Note: Basic indexes are auto-created, but additional composite indexes improve query performance

CREATE INDEX IF NOT EXISTS idx_metrics_profile_window ON dd_telemetry_metrics(profile_id, window_type, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_metrics_type_name ON dd_telemetry_metrics(metric_type, metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_window_range ON dd_telemetry_metrics(window_start, window_end);

-- dd_trade_performance
-- Note: Basic indexes are auto-created (profile_id, entry_time) and (symbol, entry_time)

CREATE INDEX IF NOT EXISTS idx_trades_profile_time ON dd_trade_performance(profile_id, entry_time);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON dd_trade_performance(symbol, entry_time);
CREATE INDEX IF NOT EXISTS idx_trades_type_time ON dd_trade_performance(trade_type, entry_time);
CREATE INDEX IF NOT EXISTS idx_trades_exit_reason ON dd_trade_performance(exit_reason);

-- dd_system_health
-- Note: Basic index is auto-created (profile_id, script_name, timestamp)

CREATE INDEX IF NOT EXISTS idx_health_profile_script_time ON dd_system_health(profile_id, script_name, timestamp);
```

**Current Status**: Basic indexes are auto-created by `TelemetryStorage._initialize_schema()`. Additional indexes can be added via migration script.

### 2. **Data Partitioning** (Recommended for High Volume)

Partition tables by time (monthly or quarterly) for very large datasets:

```sql
-- Partition dd_trade_performance by month
-- Note: MySQL requires table to be partitioned at creation time, or use ALTER TABLE

-- Example: Create partitioned table (for new installations)
CREATE TABLE dd_trade_performance_partitioned (
    -- same schema as dd_trade_performance
) PARTITION BY RANGE (YEAR(entry_time) * 100 + MONTH(entry_time));

-- Or use range partitioning by timestamp
ALTER TABLE dd_trade_performance
PARTITION BY RANGE (UNIX_TIMESTAMP(entry_time)) (
    PARTITION p2024_01 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01')),
    PARTITION p2024_02 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01')),
    -- ... continue for each month
);
```

**Benefits:**
- Faster queries (only scan relevant partitions)
- Easier data archival (drop old partitions)
- Better maintenance (vacuum/analyze individual partitions)

**Note**: Partitioning requires careful planning. Consider this for long-term scaling (100+ profiles with years of data).

### 3. **Data Retention Policies**

Implement automatic data archival/deletion:

```python
# Backend scheduled job (cron or APScheduler)
# See scripts/monitoring/ for implementation examples

def archive_old_telemetry_data():
    """
    Archive or delete data older than retention period:
    - Raw trade records: Keep 1 year, archive older
    - Aggregated metrics: Keep 2 years, archive older
    - System health: Keep 90 days, delete older
    """
    cutoff_date = datetime.now() - timedelta(days=365)
    
    # Move old trade data to archive table
    archive_old_trades(cutoff_date)
    
    # Delete old system health data
    delete_old_health_data(datetime.now() - timedelta(days=90))
```

**Implementation**: Create a new script in `scripts/monitoring/` for data archival.

### 4. **Aggregation Strategy**

Store pre-aggregated data to reduce query complexity:

- **Hourly aggregations** for real-time dashboards
- **Daily aggregations** for historical views
- **Weekly/Monthly aggregations** for long-term trends

The `dd_telemetry_metrics` table already uses windowing (hourly/daily/weekly/monthly), which is good.

**Current Status**: ✅ Implemented - The `store_aggregated_metric()` method supports window types.

### 5. **Query Optimization**

Backend API endpoints should:

1. **Always use LIMIT** (frontend already passes this)
2. **Use date range filters** (mandatory for large datasets)
3. **Return only necessary fields** (avoid SELECT *)
4. **Use prepared statements** (prevents SQL injection, allows query plan caching)

**Current Status**: 
- ✅ LIMIT is used in all endpoints (max 1000-10000 records)
- ✅ Date range filters are implemented
- ✅ Prepared statements are used (via mysql.connector)
- ⚠️ Need to add date range validation (max 1 year)

```python
# Good: Indexed, limited, filtered query
SELECT * FROM dd_trade_performance
WHERE profile_id = ?
  AND entry_time >= ?
  AND entry_time <= ?
ORDER BY entry_time DESC
LIMIT 1000;

# Bad: Full table scan
SELECT * FROM dd_trade_performance
ORDER BY entry_time DESC;
```

### 6. **Caching Strategy**

Use Redis (already implemented) for:

- **Real-time metrics**: Cache for 5-10 seconds
- **Historical aggregations**: Cache for 1-5 minutes
- **Trade summaries**: Cache for 1 minute

**Current Status**: ✅ Redis is implemented for real-time data with 1-hour TTL. Can add API-level caching for historical queries.

```python
# Future enhancement: Add API-level caching
@cache(ttl=60)  # Cache for 1 minute
async def get_trade_summary(profile_id: int, start_time: str, end_time: str):
    cache_key = f"trade_summary:{profile_id}:{start_time}:{end_time}"
    # ... fetch from cache or database
```

## Frontend Optimizations

### 1. **Default Date Ranges**

The dashboards default to 7 days, which is good. Consider:

- **Breakout Dashboard**: Default to last 7-30 days
- **Equity Dashboard**: Default to last 30 days for historical, real-time for current metrics

**Current Status**: ✅ API endpoints have default date ranges (7-30 days).

### 2. **Pagination**

For large datasets, implement pagination:

```typescript
// Future enhancement: Add pagination to trade lists
interface TradesQueryParams {
  start_time?: string;
  end_time?: string;
  limit?: number;  // Already implemented
  offset?: number; // Add for pagination
}
```

**Current Status**: ⚠️ `limit` is implemented, but `offset` pagination is not yet added.

### 3. **Debouncing Date Range Changes**

Ensure users can't query too frequently:

```typescript
// Debounce date range changes (500ms delay)
const debouncedFetch = useMemo(
  () => debounce(fetchData, 500),
  [selectedProfileId]
);
```

**Implementation**: This should be handled in the frontend repository.

### 4. **Lazy Loading Charts**

Only load chart data when the chart is visible (using Intersection Observer).

**Implementation**: This should be handled in the frontend repository.

### 5. **Data Sampling for Large Ranges**

For very large date ranges, request aggregated data instead of raw records:

```typescript
// For ranges > 90 days, use daily aggregations
if (daysDiff > 90) {
  return getAggregatedMetrics('daily');
} else {
  return getRawTrades();
}
```

**Implementation**: This should be handled in the frontend repository.

## Recommended Backend Implementation

### API Endpoint Design

```python
# Good: Filtered, limited, indexed query
@router.get("/api/trades/{profile_id}")
async def get_trades(
    profile_id: int,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = Query(1000, le=10000),  # Max 10k records
    offset: int = 0
):
    # Validate date range (max 1 year)
    if start_time and end_time:
        if (end_time - start_time).days > 365:
            raise HTTPException(400, "Date range cannot exceed 1 year")
    
    # Use indexed query
    query = (
        select(TradePerformance)
        .where(TradePerformance.profile_id == profile_id)
        .order_by(TradePerformance.entry_time.desc())
        .limit(limit)
        .offset(offset)
    )
    
    if start_time:
        query = query.where(TradePerformance.entry_time >= start_time)
    if end_time:
        query = query.where(TradePerformance.entry_time <= end_time)
    
    return await db.execute(query)
```

**Current Status**: 
- ✅ LIMIT is implemented
- ✅ Date range filtering is implemented
- ⚠️ Date range validation (max 1 year) needs to be added
- ⚠️ Offset pagination needs to be added

### Scheduled Jobs

Implement background jobs for:

1. **Data aggregation** (hourly/daily)
2. **Data archival** (monthly)
3. **Cache warming** (pre-load common queries)
4. **Index maintenance** (periodic ANALYZE/VACUUM)

```python
# Example using Celery or APScheduler
# Create in scripts/monitoring/

@celery.task
def aggregate_daily_metrics():
    """Aggregate hourly metrics into daily metrics"""
    # Run daily at 1 AM
    pass

@celery.task
def archive_old_trades():
    """Move trades older than 1 year to archive table"""
    # Run monthly
    pass
```

**Current Status**: ⚠️ Not yet implemented. Can be added as new scripts in `scripts/monitoring/`.

## Monitoring and Alerts

Set up monitoring for:

1. **Query execution time** (alert if > 2 seconds)
2. **Table sizes** (alert if growing too fast)
3. **Cache hit rates** (optimize if < 80%)
4. **API response times** (P95 < 500ms, P99 < 1s)

**Implementation**: This can be integrated into the existing telemetry system or external monitoring tools.

## Implementation Checklist

### Immediate Actions (Do Now)

- [x] Basic database indexes (auto-created by `TelemetryStorage`)
- [x] LIMIT in all queries
- [x] Date range filtering in API endpoints
- [x] Redis caching for real-time metrics
- [ ] Add date range validation (max 1 year) to API endpoints
- [ ] Add additional composite indexes (via migration script)

### Short-term (1-3 months)

- [ ] Add pagination support (offset parameter)
- [ ] Implement data retention policies (archival script)
- [ ] Add API-level caching for historical queries
- [ ] Set up monitoring/alerting for query performance

### Long-term (3-6 months)

- [ ] Implement table partitioning (if data volume requires it)
- [ ] Add data archival strategy (scheduled jobs)
- [ ] Optimize aggregation queries
- [ ] Add query performance monitoring

## Current Implementation Status

### ✅ Already Implemented

1. **Basic Database Indexes**: Auto-created in `TelemetryStorage._initialize_schema()`
   - `dd_telemetry_metrics`: `(profile_id, metric_type, metric_name)`, `(window_start, window_end)`
   - `dd_trade_performance`: `(profile_id, entry_time)`, `(symbol, entry_time)`
   - `dd_system_health`: `(profile_id, script_name, timestamp)`

2. **Query Optimization**:
   - LIMIT in all endpoints (default 1000, max 10000)
   - Date range filtering
   - Prepared statements (via mysql.connector)

3. **Caching**:
   - Redis for real-time metrics (1-hour TTL)
   - Connection pooling (MySQL and Redis)

4. **Aggregation**:
   - Windowed metrics (hourly/daily/weekly/monthly) supported

### ⚠️ Needs Implementation

1. **Additional Composite Indexes**: Add via migration script
2. **Date Range Validation**: Add max 1-year limit to API endpoints
3. **Pagination**: Add offset parameter for large result sets
4. **Data Retention**: Implement archival scripts
5. **Scheduled Jobs**: Add aggregation and archival jobs

## Migration Scripts

### Adding Additional Indexes

Create a migration script to add composite indexes without downtime:

```sql
-- scripts/database/add_telemetry_indexes.sql
-- Run this after reviewing current index usage

-- Only add indexes that will be used by queries
-- Monitor query performance before and after

CREATE INDEX IF NOT EXISTS idx_trades_type_time 
ON dd_trade_performance(trade_type, entry_time);

CREATE INDEX IF NOT EXISTS idx_trades_exit_reason 
ON dd_trade_performance(exit_reason);

-- Add other indexes as needed based on query patterns
```

### Data Archival Script

Create a script in `scripts/monitoring/archive_telemetry_data.py`:

```python
"""
Archive old telemetry data to reduce table sizes.

Run monthly via cron:
    0 2 1 * * /path/to/python /path/to/archive_telemetry_data.py
"""
```

## Summary

**Immediate Actions:**
1. ✅ Add database indexes (critical) - **Basic indexes already implemented**
2. ⚠️ Implement date range limits in API (max 1 year) - **Needs implementation**
3. ✅ Use LIMIT in all queries - **Already implemented**
4. ✅ Implement Redis caching for real-time metrics - **Already implemented**

**Short-term (1-3 months):**
5. Add pagination support
6. Implement data retention policies
7. Set up monitoring/alerting

**Long-term (3-6 months):**
8. Implement table partitioning
9. Add data archival strategy
10. Optimize aggregation queries

The current backend implementation already includes:
- ✅ Basic database indexes
- ✅ Query optimization (LIMIT, date filtering)
- ✅ Connection pooling
- ✅ Redis caching
- ✅ Bulk INSERT operations

The main work remaining:
- Add date range validation
- Add pagination support
- Implement data retention/archival
- Add additional indexes based on query patterns

