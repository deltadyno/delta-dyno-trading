# Telemetry Integration - Implementation Notes

## Overview

I'm integrating telemetry into all three main scripts. This involves:

1. **Initializing TelemetryManager** in each script's entry point
2. **Adding telemetry hooks** at key points (breakout signals, order events, equity updates)
3. **All telemetry calls are non-blocking** (async writes, won't slow down trading)
4. **All telemetry calls are wrapped in try/except** (won't break trading if telemetry fails)

---

## Database Tables

✅ **3 MySQL tables are automatically created**:
- `dd_telemetry_metrics` - Aggregated metrics
- `dd_trade_performance` - Individual trade records  
- `dd_system_health` - System health snapshots

**No manual setup needed** - tables are created automatically on first use!

---

## Integration Points

### breakout_detector.py
- Initialize TelemetryManager in `main()`
- Pass to `handle_positions()`
- Record breakout signals when detected (line ~506)
- Record system health periodically

### order_monitor.py
- Initialize TelemetryManager in `run_order_monitor()`
- Pass to `monitor_limit_orders()`
- Record order metrics (cancelled, converted, filled)
- Record API latency

### equity_monitor.py
- Initialize TelemetryManager in `run_equity_monitor()`
- Pass to `monitor_market_equity()`
- Record equity updates
- Record position closes

---

## Next Steps

After integration, you can:
1. Start the API server: `python api_server.py`
2. Query metrics via API endpoints
3. View data in your frontend dashboard

