# DeltaDyno Telemetry & Monitoring Dashboard

## Overview

The telemetry system provides comprehensive monitoring and analytics for the DeltaDyno trading suite, enabling data-driven optimization of trading configurations.

## Features

### 📊 Dashboard Metrics

1. **Breakout Dashboard**
   - Success rate tracking
   - Average slippage per trade
   - Profit per trade analysis
   - Helps optimize Take Profit and Stop Loss settings

2. **Equity & Risk Dashboard**
   - Real-time PnL tracking
   - Maximum Drawdown (MDD) calculation
   - Margin utilization monitoring
   - Supports Position Sizing decisions

3. **System & Order Health**
   - API latency tracking (avg, p95, p99)
   - Alpaca rate-limit monitoring
   - Order execution status tracking
   - Debug connectivity and execution speed issues

## Architecture

### Storage Strategy

**Hybrid Approach:**
- **Redis**: Real-time metrics, high-frequency data (<1ms write latency)
- **MySQL**: Persistent storage, aggregated metrics, historical analysis

This design provides:
- Fast, non-blocking writes for trading loops
- Queryable historical data for dashboards
- Automatic cleanup of old Redis data via TTL

### Database Schema

Three main tables:
1. `dd_telemetry_metrics` - Aggregated metrics (hourly/daily)
2. `dd_trade_performance` - Individual trade records
3. `dd_system_health` - System health snapshots

See `docs/TELEMETRY_ARCHITECTURE.md` for full schema details.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize Database Tables

Tables are auto-created on first use, but you can also run:

```sql
-- See docs/TELEMETRY_ARCHITECTURE.md for full schema
```

### 3. Start API Server

```bash
# Development
python api_server.py

# Production (with uvicorn)
uvicorn api_server:app --host 0.0.0.0 --port 8000

# With auto-reload
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Access API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### 5. Integrate Telemetry into Scripts

See `docs/TELEMETRY_INTEGRATION.md` for detailed integration guide.

## API Endpoints

### Metrics Endpoints

```
GET /api/v1/metrics/breakout/{profile_id}?days=7
GET /api/v1/metrics/equity/{profile_id}
GET /api/v1/metrics/orders/{profile_id}?script_name=order_monitor
GET /api/v1/metrics/system/{profile_id}?script_name=breakout_detector
```

### Trade Performance

```
GET /api/v1/trades/{profile_id}?days=30&symbol=SPY&limit=100
GET /api/v1/trades/{profile_id}/performance?days=30
```

### Health Check

```
GET /health
GET /
```

## Example API Calls

### Get Breakout Metrics

```bash
curl http://localhost:8000/api/v1/metrics/breakout/1?days=7
```

Response:
```json
{
  "profile_id": 1,
  "success_rate": 65.5,
  "avg_slippage": 0.0023,
  "avg_profit_per_trade": 45.67,
  "total_signals": 120,
  "profitable_trades": 79,
  "losing_trades": 41
}
```

### Get Equity Metrics

```bash
curl http://localhost:8000/api/v1/metrics/equity/1
```

Response:
```json
{
  "profile_id": 1,
  "current_equity": 10000.00,
  "total_pnl": 234.56,
  "unrealized_pnl": 123.45,
  "realized_pnl": 111.11,
  "margin_utilization_pct": 45.2
}
```

## CORS Configuration

The API is configured to allow requests from:
- `http://localhost:3000` (React dev server)
- `http://localhost:3001`
- `https://deltadyno.github.io`
- Vercel/Netlify deployments

To add custom origins, edit `deltadyno/api/middleware/cors.py`.

## Frontend Integration

The frontend repository at `https://github.com/deltadyno/delta-dyno.git` can consume this API:

```javascript
// Example React hook
const useBreakoutMetrics = (profileId, days = 7) => {
  const [data, setData] = useState(null);
  
  useEffect(() => {
    fetch(`http://localhost:8000/api/v1/metrics/breakout/${profileId}?days=${days}`)
      .then(res => res.json())
      .then(setData);
  }, [profileId, days]);
  
  return data;
};
```

## Configuration

Telemetry is enabled by default. To disable:

```python
telemetry_manager = TelemetryManager(storage=storage, enabled=False)
```

Adjust batch size and flush interval:

```python
telemetry_manager = TelemetryManager(
    storage=storage,
    batch_size=20,  # Batch 20 metrics before writing
    flush_interval_seconds=10.0  # Flush every 10 seconds max
)
```

## Monitoring Scripts

After integration, your trading scripts will automatically:
- ✅ Record breakout signals
- ✅ Track trade outcomes
- ✅ Monitor API latency
- ✅ Update equity metrics
- ✅ Report system health

All without blocking the main trading loops!

## Troubleshooting

### API Not Responding

1. Check if server is running: `curl http://localhost:8000/health`
2. Check logs for errors
3. Verify database connection in `config/config.ini`
4. Verify Redis connection

### Metrics Not Appearing

1. Ensure telemetry is enabled: `TelemetryManager(..., enabled=True)`
2. Check that scripts are calling telemetry methods
3. Verify database tables exist (auto-created on first use)
4. Check Redis connectivity

### CORS Errors in Browser

1. Verify allowed origins in `deltadyno/api/middleware/cors.py`
2. Add your frontend URL to allowed origins
3. Check browser console for specific CORS error details

## Next Steps

1. **Integrate telemetry hooks** into your scripts (see integration guide)
2. **Deploy API server** to your AWS EC2 instance
3. **Connect frontend** to the API endpoints
4. **Build dashboards** using the metrics data
5. **Analyze and optimize** trading configurations based on insights

## Support

For questions or issues:
- Review `docs/TELEMETRY_ARCHITECTURE.md` for architecture details
- Check `docs/TELEMETRY_INTEGRATION.md` for integration examples
- Open an issue on GitHub

