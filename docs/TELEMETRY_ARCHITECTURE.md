# Telemetry System Architecture

## Storage Strategy Recommendation

### Hybrid Approach: MySQL + Redis

**MySQL (Persistent Storage)** - For aggregated, queryable metrics:
- **Daily/Hourly Aggregates**: PnL, success rates, drawdowns
- **Trade History**: Individual trade records with outcomes
- **Profile Performance**: Historical performance per profile
- **Configuration Tuning Data**: Metrics that help optimize settings

**Redis (Real-time Cache)** - For high-frequency, real-time metrics:
- **Live PnL**: Current portfolio value updates
- **API Latency**: Recent API call performance
- **Order Status**: Active order tracking
- **System Health**: Recent errors, rate limits

### Rationale

1. **MySQL** provides:
   - SQL queryability for complex dashboards
   - Data persistence for historical analysis
   - Join capabilities with existing user/profile tables
   - ACID guarantees for financial data

2. **Redis** provides:
   - Sub-millisecond write latency (non-blocking for trading loops)
   - TTL for automatic cleanup of old data
   - Pub/Sub capabilities for real-time updates (future WebSocket)
   - High throughput for frequent metric updates

3. **Combined**:
   - Real-time dashboards read from Redis
   - Historical analysis reads from MySQL
   - Background job aggregates Redis → MySQL periodically

---

## Database Schema

### MySQL Tables

```sql
-- Telemetry aggregations table
CREATE TABLE dd_telemetry_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    metric_type VARCHAR(50) NOT NULL,  -- 'breakout', 'equity', 'order', 'system'
    metric_name VARCHAR(100) NOT NULL,  -- 'success_rate', 'avg_slippage', 'mdd', etc.
    metric_value DECIMAL(15, 4) NOT NULL,
    window_type VARCHAR(20) NOT NULL,  -- 'hour', 'day', 'week', 'month'
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    metadata JSON,  -- Additional context
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_profile_metric (profile_id, metric_type, metric_name),
    INDEX idx_window (window_start, window_end)
);

-- Trade performance records
CREATE TABLE dd_trade_performance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    trade_type VARCHAR(20) NOT NULL,  -- 'breakout', 'equity_close', 'stop_loss'
    entry_price DECIMAL(10, 4),
    exit_price DECIMAL(10, 4),
    quantity INT,
    pnl DECIMAL(10, 2),
    slippage DECIMAL(10, 4),
    entry_time TIMESTAMP,
    exit_time TIMESTAMP,
    duration_seconds INT,
    metadata JSON,  -- bar_strength, direction, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_profile_trade (profile_id, entry_time),
    INDEX idx_symbol (symbol, entry_time)
);

-- System health metrics
CREATE TABLE dd_system_health (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    script_name VARCHAR(50) NOT NULL,  -- 'breakout_detector', 'order_monitor', 'equity_monitor'
    metric_name VARCHAR(100) NOT NULL,  -- 'api_latency', 'rate_limit_remaining', 'error_count'
    metric_value DECIMAL(15, 4),
    status VARCHAR(20),  -- 'healthy', 'degraded', 'error'
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    INDEX idx_profile_script (profile_id, script_name, timestamp)
);
```

---

## Module Structure

```
deltadyno/
├── telemetry/
│   ├── __init__.py
│   ├── manager.py          # TelemetryManager class
│   ├── models.py           # Data models/Pydantic schemas
│   ├── storage.py          # MySQL and Redis storage backends
│   ├── collectors.py       # Collectors for each script type
│   └── aggregator.py       # Background job to aggregate Redis → MySQL
└── api/
    ├── __init__.py
    ├── server.py           # FastAPI/Flask server
    ├── routes/
    │   ├── __init__.py
    │   ├── metrics.py      # Metrics endpoints
    │   ├── trades.py       # Trade history endpoints
    │   └── health.py       # System health endpoints
    └── middleware/
        ├── __init__.py
        └── cors.py         # CORS configuration
```

---

## Data Collection Points

### Breakout Detector
- **Signal Generation**: Record breakout detection events
- **Order Placement**: Track order submission latency and outcomes
- **Slippage**: Calculate execution price vs signal price
- **Success Rate**: Track whether breakouts led to profitable trades

### Order Monitor
- **Order Status**: Track limit order conversions and cancellations
- **API Latency**: Measure Alpaca API response times
- **Rate Limits**: Monitor remaining API calls
- **Conversion Metrics**: Limit → Market conversion rates

### Equity Monitor
- **PnL Updates**: Real-time profit/loss calculations
- **Position Closes**: Track exit reasons (profit target, stop loss, choppy day)
- **Drawdown**: Calculate maximum drawdown over time windows
- **Margin Utilization**: Track position sizing relative to account equity

---

## API Endpoints Design

### REST API (FastAPI)

```
GET  /api/v1/metrics/breakout/{profile_id}
GET  /api/v1/metrics/equity/{profile_id}
GET  /api/v1/metrics/orders/{profile_id}
GET  /api/v1/metrics/system/{profile_id}

GET  /api/v1/trades/{profile_id}
GET  /api/v1/trades/{profile_id}/performance

GET  /api/v1/health/{profile_id}
GET  /api/v1/health/system
```

### WebSocket (Future Enhancement)
```
WS   /ws/metrics/{profile_id}  # Real-time metric updates
```

---

## Implementation Phases

1. **Phase 1**: Core TelemetryManager + MySQL storage
2. **Phase 2**: Redis real-time cache layer
3. **Phase 3**: API layer with REST endpoints
4. **Phase 4**: Integration hooks in existing scripts
5. **Phase 5**: Background aggregator job
6. **Phase 6**: WebSocket support for real-time dashboards

