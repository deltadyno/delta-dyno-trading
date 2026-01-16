# Telemetry Database Tables

## Overview

The telemetry system automatically creates **3 new MySQL tables** when `TelemetryStorage` is first initialized. These tables store all monitoring and performance data.

---

## Tables Created

### 1. `dd_telemetry_metrics`

**Purpose**: Stores aggregated metrics (hourly/daily summaries)

**Structure**:
```sql
CREATE TABLE dd_telemetry_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    metric_type VARCHAR(50) NOT NULL,        -- 'breakout', 'equity', 'order', 'system'
    metric_name VARCHAR(100) NOT NULL,       -- 'success_rate', 'avg_slippage', 'mdd', etc.
    metric_value DECIMAL(15, 4) NOT NULL,
    window_type VARCHAR(20) NOT NULL,        -- 'hour', 'day', 'week', 'month'
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_profile_metric (profile_id, metric_type, metric_name),
    INDEX idx_window (window_start, window_end)
);
```

**Example Data**:
- `profile_id=1, metric_type='breakout', metric_name='success_rate', metric_value=65.5, window_type='day'`
- `profile_id=1, metric_type='equity', metric_name='max_drawdown', metric_value=234.56, window_type='week'`

---

### 2. `dd_trade_performance`

**Purpose**: Stores individual trade records with detailed performance metrics

**Structure**:
```sql
CREATE TABLE dd_trade_performance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    trade_type VARCHAR(20) NOT NULL,         -- 'breakout', 'equity_close', 'stop_loss'
    entry_price DECIMAL(10, 4),
    exit_price DECIMAL(10, 4),
    quantity INT,
    pnl DECIMAL(10, 2),
    pnl_pct DECIMAL(6, 2),
    slippage DECIMAL(10, 4),
    entry_time TIMESTAMP,
    exit_time TIMESTAMP,
    duration_seconds INT,
    bar_strength DECIMAL(5, 3),
    direction VARCHAR(20),                   -- 'upward', 'downward', etc.
    exit_reason VARCHAR(50),                 -- 'profit_target', 'stop_loss', 'choppy_day'
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_profile_trade (profile_id, entry_time),
    INDEX idx_symbol (symbol, entry_time)
);
```

**Example Data**:
- `profile_id=1, symbol='SPY241218C00604000', trade_type='breakout', entry_price=604.50, exit_price=608.25, pnl=3.75, pnl_pct=0.62, entry_time='2024-01-12 10:30:00', exit_time='2024-01-12 14:45:00'`

---

### 3. `dd_system_health`

**Purpose**: Stores system health snapshots

**Structure**:
```sql
CREATE TABLE dd_system_health (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id INT NOT NULL,
    script_name VARCHAR(50) NOT NULL,        -- 'breakout_detector', 'order_monitor', 'equity_monitor'
    metric_name VARCHAR(100) NOT NULL,       -- 'api_latency', 'rate_limit_remaining', 'error_count'
    metric_value DECIMAL(15, 4),
    status VARCHAR(20),                      -- 'healthy', 'degraded', 'error'
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    INDEX idx_profile_script (profile_id, script_name, timestamp)
);
```

**Example Data**:
- `profile_id=1, script_name='breakout_detector', metric_name='api_latency_avg_ms', metric_value=45.2, status='healthy'`
- `profile_id=1, script_name='order_monitor', metric_name='error_count', metric_value=0, status='healthy'`

---

## When Are Tables Created?

Tables are **automatically created** when:
1. `TelemetryStorage` class is instantiated
2. The `_initialize_schema()` method runs
3. This happens on first use - no manual setup needed!

**Code Location**: `deltadyno/telemetry/storage.py`, `_initialize_schema()` method

---

## Data Flow

### Real-time Data (Redis)
- Recent equity updates
- Current API latency measurements
- Live order status

### Persistent Data (MySQL)
- Aggregated metrics → `dd_telemetry_metrics`
- Trade records → `dd_trade_performance`
- System health → `dd_system_health`

---

## Querying the Data

### Get Trade Performance
```sql
SELECT * FROM dd_trade_performance 
WHERE profile_id = 1 
  AND entry_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY entry_time DESC;
```

### Get Aggregated Metrics
```sql
SELECT * FROM dd_telemetry_metrics 
WHERE profile_id = 1 
  AND metric_type = 'breakout'
  AND window_type = 'day'
ORDER BY window_start DESC;
```

### Get System Health
```sql
SELECT * FROM dd_system_health 
WHERE profile_id = 1 
  AND script_name = 'breakout_detector'
ORDER BY timestamp DESC 
LIMIT 100;
```

---

## Summary

✅ **3 tables created automatically**  
✅ **No manual setup required**  
✅ **Tables are created on first use**  
✅ **Indexed for fast queries**  
✅ **Structured for dashboard queries**

All tables are ready to use once you integrate telemetry hooks into your scripts!

