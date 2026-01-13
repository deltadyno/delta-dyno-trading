# Telemetry Integration Guide

This guide shows how to add telemetry collection to your existing trading scripts.

## Quick Start

### 1. Initialize TelemetryManager in Your Script

Add this at the beginning of your main entry point function:

```python
from deltadyno.config.loader import ConfigLoader
from deltadyno.telemetry.manager import TelemetryManager
from deltadyno.telemetry.storage import TelemetryStorage
from deltadyno.telemetry.integration import set_telemetry_manager

# In your main() or run_*_monitor() function:

# Load configuration
file_config = ConfigLoader(config_file='config/config.ini')

# Create telemetry storage and manager
telemetry_storage = TelemetryStorage(
    db_host=file_config.db_host,
    db_user=file_config.db_user,
    db_password=file_config.db_password,
    db_name=file_config.db_name,
    redis_host=file_config.redis_host,
    redis_port=file_config.redis_port,
    redis_password=file_config.redis_password
)

telemetry_manager = TelemetryManager(storage=telemetry_storage, enabled=True)

# Set global instance for integration helpers
set_telemetry_manager(telemetry_manager)
```

## Integration Examples

### Breakout Detector Integration

#### Record Breakout Signals

In `breakout_detector.py`, when a breakout signal is detected:

```python
from deltadyno.telemetry.integration import get_telemetry_manager
from decimal import Decimal

# After detecting a breakout signal
telemetry = get_telemetry_manager()
if telemetry:
    telemetry.record_breakout_signal(
        profile_id=profile_id,  # Your profile ID
        symbol=symbol,
        direction=direction,  # 'upward', 'downward', etc.
        bar_strength=bar_strength,
        close_price=Decimal(str(close_price)),
        candle_size=Decimal(str(candle_size)),
        volume=int(volume),
        timestamp=close_time,
        metadata={"choppy_day_count": choppy_day_count}
    )
```

#### Record Trade Outcomes

When a breakout trade is closed:

```python
telemetry.record_breakout_outcome(
    profile_id=profile_id,
    symbol=symbol,
    entry_price=Decimal(str(entry_price)),
    exit_price=Decimal(str(exit_price)),
    quantity=quantity,
    entry_time=entry_time,
    exit_time=exit_time,
    bar_strength=bar_strength,
    direction=direction,
    metadata={"exit_reason": "profit_target"}
)
```

#### Measure API Latency

Wrap Alpaca API calls:

```python
from deltadyno.telemetry.integration import measure_api_latency

# When placing orders
with measure_api_latency(profile_id, "breakout_detector", "place_order"):
    order = trading_client.submit_order(...)
```

### Order Monitor Integration

#### Track Order Status Changes

In `order_monitor.py`, when monitoring orders:

```python
# When converting limit order to market order
telemetry.record_order_metric(
    profile_id=profile_id,
    symbol=order.symbol,
    order_type="limit",
    side="buy",
    status="converted",
    quantity=int(order.qty),
    order_id=order.id,
    limit_price=Decimal(str(order.limit_price)),
    filled_price=None,
    api_latency_ms=latency_ms,
    metadata={"script_name": "order_monitor", "conversion_reason": "time_threshold"}
)

# When order is filled
telemetry.record_order_metric(
    profile_id=profile_id,
    symbol=order.symbol,
    order_type="market",
    side="buy",
    status="filled",
    quantity=int(order.filled_qty),
    order_id=order.id,
    filled_price=Decimal(str(order.filled_avg_price)),
    api_latency_ms=latency_ms,
    metadata={"script_name": "order_monitor"}
)
```

#### Record System Health

Periodically record system health:

```python
# Every few minutes in your monitoring loop
telemetry.record_system_health(
    profile_id=profile_id,
    script_name="order_monitor",
    status="healthy",  # or "degraded", "error"
    api_rate_limit_remaining=rate_limit_info.get("remaining", 0),
    api_rate_limit_limit=rate_limit_info.get("limit", 0),
    error_count=error_count,
    warning_count=warning_count
)
```

### Equity Monitor Integration

#### Record Equity Updates

In `equity_monitor.py`, when checking positions:

```python
# Get account information
account = trading_client.get_account()

telemetry.record_equity_update(
    profile_id=profile_id,
    account_equity=Decimal(str(account.equity)),
    unrealized_pnl=Decimal(str(account.unrealized_pl)),
    realized_pnl=Decimal(str(account.cash)),  # Adjust based on your calculation
    margin_used=Decimal(str(account.portfolio_value - account.cash)),
    margin_available=Decimal(str(account.buying_power)),
    open_positions_count=len(positions),
    metadata={"choppy_day_detected": is_choppy_day}
)
```

#### Record Trade Closes

When closing positions:

```python
telemetry.record_trade_performance(
    profile_id=profile_id,
    symbol=symbol,
    trade_type="equity_close",  # or "stop_loss", "choppy_day_close"
    entry_price=Decimal(str(entry_price)),
    exit_price=Decimal(str(exit_price)),
    quantity=quantity,
    entry_time=entry_time,
    exit_time=datetime.utcnow(),
    exit_reason="profit_target",  # or "stop_loss", "choppy_day"
    metadata={"profit_pct": profit_pct}
)
```

## Best Practices

1. **Non-blocking**: Telemetry writes are already async/non-blocking, but avoid heavy computations in telemetry calls
2. **Error handling**: Always wrap telemetry calls in try/except to avoid breaking trading logic
3. **Metadata**: Use metadata to store additional context that might be useful for analysis
4. **Profile ID**: Always pass the correct profile_id to associate metrics with the right client
5. **Timestamps**: Use UTC timestamps consistently

## Example: Minimal Integration

If you want minimal changes, just initialize telemetry and add a few key measurement points:

```python
# At script startup
telemetry_manager = TelemetryManager(...)
set_telemetry_manager(telemetry_manager)

# In main loop - periodic health check
try:
    telemetry = get_telemetry_manager()
    if telemetry:
        telemetry.record_system_health(
            profile_id=profile_id,
            script_name="your_script_name",
            status="healthy"
        )
except Exception:
    pass  # Don't break trading logic
```

## Performance Considerations

- Telemetry writes are batched and async, so they won't block trading loops
- Redis operations are very fast (<1ms typically)
- MySQL writes are queued and flushed in batches
- If telemetry is disabled (`enabled=False`), all calls return immediately with no overhead

