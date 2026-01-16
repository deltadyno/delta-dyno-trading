# Prompt for Building Dashboards (Copy this to your other Cursor instance)

---

## Project Context: DeltaDyno Trading Telemetry Dashboard

I need help building client-facing dashboards for a trading system telemetry backend. The backend is a Python FastAPI application that collects and serves trading performance data from multiple trading bot scripts.

### Backend Repository
- **Repository**: `delta-dyno-trading` (Python backend)
- **Location**: `https://github.com/deltadyno/delta-dyno-trading`
- **Branch**: `feature/monitoring-dashboard`
- **Framework**: FastAPI (Python)
- **API Server**: `api_server.py` (runs on port 8000 by default)

### Frontend Repository (Separate Repo)
- **Repository**: `delta-dyno` (React/Next.js frontend)
- **Location**: `https://github.com/deltadyno/delta-dyno`
- **Note**: This is a separate repository that will consume the API

### System Overview

The backend runs 3 main trading scripts per profile:
1. **Breakout Detector** - Detects trading signals and places orders
2. **Order Monitor** - Monitors limit orders, converts to market orders
3. **Equity Monitor** - Monitors open positions, closes based on profit/loss

Each profile can have multiple scripts running simultaneously. The telemetry system collects metrics from all scripts.

### Database Schema

The backend uses MySQL with these telemetry tables:

1. **`dd_telemetry_metrics`** - Aggregated metrics (windowed data)
   - Columns: `profile_id`, `metric_type`, `metric_name`, `metric_value`, `window_type`, `window_start`, `window_end`, `metadata`
   - Indexes: `(profile_id, metric_type, metric_name)`, `(window_start, window_end)`

2. **`dd_trade_performance`** - Individual trade records
   - Columns: `profile_id`, `symbol`, `trade_type`, `entry_price`, `exit_price`, `quantity`, `pnl`, `pnl_pct`, `slippage`, `entry_time`, `exit_time`, `duration_seconds`, `bar_strength`, `direction`, `exit_reason`, `metadata`
   - Indexes: `(profile_id, entry_time)`, `(symbol, entry_time)`

3. **`dd_system_health`** - System health metrics
   - Columns: `profile_id`, `script_name`, `metric_name`, `metric_value`, `status`, `timestamp`, `metadata`
   - Indexes: `(profile_id, script_name, timestamp)`

Redis is used for real-time data caching (TTL: 1 hour).

### API Endpoints

The FastAPI server exposes these endpoints (see `deltadyno/api/routes/`):

#### Health Check
- `GET /health` - API health check

#### Metrics Endpoints (`/api/metrics/`)
- `GET /api/metrics/{profile_id}` - Get all metrics for a profile
  - Query params: `metric_type?`, `metric_name?`, `window_start?`, `window_end?`, `limit?`
  - Returns: List of aggregated metrics
- `GET /api/metrics/{profile_id}/equity` - Get real-time equity metrics
  - Returns: Latest equity, PnL, drawdown data
- `GET /api/metrics/{profile_id}/latency/{script_name}` - Get API latency statistics
  - Returns: Avg, min, max, p50, p95, p99 latency

#### Trade Performance Endpoints (`/api/trades/`)
- `GET /api/trades/{profile_id}` - Get trade performance records
  - Query params: `start_time?`, `end_time?`, `symbol?`, `limit?`
  - Returns: List of trade records with PnL, slippage, etc.
- `GET /api/trades/{profile_id}/summary` - Get trade summary statistics
  - Returns: Total trades, win rate, avg PnL, total PnL, etc.

#### System Health Endpoints (`/api/health/`)
- `GET /api/health/{profile_id}/{script_name}` - Get system health for a script
  - Returns: Latest health status, metrics

### CORS Configuration

The API has CORS enabled to allow requests from the frontend:
- Allowed origins: Configured in `deltadyno/api/middleware/cors.py`
- Methods: GET, POST, OPTIONS
- Headers: Content-Type, Authorization

### Dashboard Requirements

I need **3 main dashboards** to help clients optimize their trading configurations:

#### 1. Breakout Dashboard
**Purpose**: Help clients tune "Take Profit" and "Stop Loss" settings

**Key Metrics to Display**:
- Success rate (winning trades / total trades)
- Average slippage per trade
- Average profit per trade
- Profit distribution (histogram)
- Trade duration statistics
- Best/worst performing symbols
- Time-based performance (hourly/daily)

**Data Sources**:
- `dd_trade_performance` table (breakout trades)
- Filter by `trade_type = 'breakout'` or `direction = 'long'/'short'`
- Group by `exit_reason` to see stop loss vs take profit outcomes

#### 2. Equity & Risk Dashboard
**Purpose**: Help clients make "Position Sizing" decisions

**Key Metrics to Display**:
- Real-time PnL (current equity value)
- Maximum Drawdown (MDD) - peak to trough
- Margin utilization (% of available margin used)
- Daily/weekly PnL trends
- Position exposure by symbol
- Risk-adjusted returns (Sharpe ratio if possible)
- Cumulative equity curve

**Data Sources**:
- Real-time: Redis (`/api/metrics/{profile_id}/equity`)
- Historical: `dd_telemetry_metrics` (window_type = 'daily', 'hourly')
- `dd_trade_performance` for PnL calculations

#### 3. System & Order Health Dashboard
**Purpose**: Help debug connectivity and execution speed issues

**Key Metrics to Display**:
- API Latency (p50, p95, p99) per script
- Alpaca API rate limit usage
- Order execution status (pending, filled, cancelled)
- Script uptime/status
- Error rates
- Order fill rates
- Time-to-fill statistics

**Data Sources**:
- `/api/health/{profile_id}/{script_name}` - System health
- `/api/metrics/{profile_id}/latency/{script_name}` - API latency
- `dd_system_health` table
- Real-time order status (may need additional integration)

### Technical Requirements

1. **API Integration**:
   - Base URL: `http://localhost:8000` (development) or production URL
   - All endpoints are RESTful JSON APIs
   - Error handling for API failures
   - Polling intervals for real-time data (suggest 5-10 seconds)

2. **Data Visualization**:
   - Use charting libraries (Chart.js, Recharts, D3.js, etc.)
   - Responsive design for mobile/desktop
   - Real-time updates where applicable
   - Historical data with date range selectors

3. **Performance Considerations**:
   - The backend supports 100s of profiles (connection pooling enabled)
   - Use pagination for large datasets
   - Cache static data where appropriate
   - Optimize API calls (batch requests, debounce inputs)

4. **Client Experience**:
   - Clean, professional UI
   - Filter by profile_id, date ranges, symbols
   - Export data to CSV/PDF (optional)
   - Responsive design
   - Loading states and error messages

### Getting Started

1. **Clone the frontend repository**: `https://github.com/deltadyno/delta-dyno`
2. **Start the backend API** (if not already running):
   ```bash
   # In the backend repo
   python api_server.py
   # Or: uvicorn deltadyno.api.server:app --host 0.0.0.0 --port 8000
   ```
3. **Test API endpoints** using curl or Postman
4. **Build dashboard components** in the frontend
5. **Integrate with existing frontend structure**

### Questions to Consider

- What chart types work best for each metric?
- How to handle real-time updates efficiently?
- What date range defaults make sense?
- How to display data for multiple profiles?
- What filters/search functionality is needed?
- How to handle missing data gracefully?

### Next Steps

Please help me:
1. Review the API structure and understand available data
2. Design the dashboard layouts and components
3. Implement the three dashboards with proper data visualization
4. Integrate with the existing frontend repository
5. Add proper error handling, loading states, and responsive design
6. Test with sample data

Let me know what you need to get started!

