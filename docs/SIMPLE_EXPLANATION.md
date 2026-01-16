# Telemetry System - Simple Explanation

## What Did We Build? (In Simple Terms)

Think of your trading system like a car. Right now, you're driving it, but you don't have a dashboard that shows you:
- How fast you're going (current performance)
- How much fuel you've used (profit/loss)
- If there are any problems with the engine (system health)

**We built a monitoring dashboard system** that:
1. **Collects information** from your trading scripts (breakout detector, order monitor, equity monitor)
2. **Stores it safely** so you can look at it later
3. **Shows it to you** through a website/API that your frontend can display

---

## What Does It Actually Do?

### 1. **Watches Your Trading Scripts**
Every time your scripts:
- Detect a breakout signal
- Place an order
- Close a position
- Check account balance

...the system records what happened, when it happened, and how well it worked.

### 2. **Tracks Performance**
It calculates:
- **Success Rate**: "Out of 100 trades, how many made money?"
- **Average Profit**: "How much money did I make per trade on average?"
- **Slippage**: "Did I get the price I wanted, or did execution cost me?"
- **Drawdown**: "What's the biggest drop in my account balance?"

### 3. **Monitors System Health**
It checks:
- **API Speed**: "How fast are the Alpaca API calls?"
- **Errors**: "Are things breaking?"
- **Rate Limits**: "Am I close to hitting API limits?"

### 4. **Provides Data to Your Frontend**
Your React/Next.js frontend can ask for this data and display it in beautiful charts and graphs.

---

## Database Choice: Why MySQL + Redis?

Think of it like this:

### **MySQL = Your Filing Cabinet (Long-term Storage)**
- ✅ Stores everything permanently
- ✅ Easy to search and analyze later
- ✅ Can run complex queries like "Show me all profitable trades in the last month"
- ✅ Good for historical data and reports

**Used for**: Trade history, daily/weekly summaries, performance reports

### **Redis = Your Whiteboard (Fast, Temporary Notes)**
- ✅ SUPER fast (reads/writes in less than 1 millisecond!)
- ✅ Perfect for "what's happening right now?"
- ✅ Doesn't slow down your trading scripts
- ✅ Automatically deletes old data (like a self-cleaning whiteboard)

**Used for**: Real-time equity updates, current API latency, live order status

### **Why Both? (The Hybrid Approach)**

Imagine you're cooking in a busy restaurant:
- **Redis** = Your prep table (fast access to ingredients you're using NOW)
- **MySQL** = Your pantry (organized storage for everything you'll need later)

**Your trading scripts need speed** - they can't wait for slow database writes. So:
- Quick updates go to Redis (instant, non-blocking)
- Important data gets saved to MySQL (permanent record)
- Your frontend can read from both - Redis for live data, MySQL for historical analysis

---

## How Does It Work? (Simple Flow)

```
1. Your Trading Script Runs
   ↓
2. Script does something (places order, detects breakout)
   ↓
3. Script calls telemetry: "Hey, record this!"
   ↓
4. Telemetry system QUICKLY writes to Redis (doesn't slow down script)
   ↓
5. Background process periodically saves important stuff to MySQL
   ↓
6. Your Frontend asks API: "Show me recent metrics"
   ↓
7. API reads from Redis (for live data) or MySQL (for historical)
   ↓
8. Frontend displays beautiful charts and graphs
```

---

## What Files Were Created?

### Core System Files:
- **`deltadyno/telemetry/`** - The "brain" that collects and stores data
- **`deltadyno/api/`** - The "server" that gives data to your frontend

### Entry Points:
- **`api_server.py`** - Run this to start the API server (like starting a web server)

### Documentation:
- **`README_TELEMETRY.md`** - Quick start guide
- **`docs/TELEMETRY_ARCHITECTURE.md`** - Technical details
- **`docs/TELEMETRY_INTEGRATION.md`** - How to add telemetry to your scripts

---

## Next Steps

### Step 1: Test the API Server (See "How to Test" below)

### Step 2: Integrate Telemetry into Your Scripts
You need to add a few lines of code to your trading scripts to start collecting data.

**Files to modify:**
- `deltadyno/core/breakout_detector.py` - Add telemetry for breakout signals
- `deltadyno/trading/order_monitor.py` - Add telemetry for order tracking
- `deltadyno/trading/equity_monitor.py` - Add telemetry for equity updates

**See**: `docs/TELEMETRY_INTEGRATION.md` for detailed examples

### Step 3: Deploy API Server to AWS
Once testing works locally, deploy the API server to your AWS EC2 instance so your frontend can access it.

### Step 4: Connect Your Frontend
Update your React/Next.js app at `https://github.com/deltadyno/delta-dyno.git` to fetch data from the API endpoints.

### Step 5: Build Dashboards
Create beautiful dashboards showing:
- Breakout success rates
- Profit/loss charts
- System health indicators
- Trade performance analysis

---

## How to Test It

### Prerequisites
Make sure you have:
- ✅ Python 3.9+
- ✅ MySQL database running
- ✅ Redis server running
- ✅ Dependencies installed: `pip install -r requirements.txt`

### Test 1: Check if API Server Starts

```bash
# From your project directory
python api_server.py
```

You should see:
```
INFO:     Started server process
INFO:     Uvicorn running on http://0.0.0.0:8000
```

If you see this, the server is running! 🎉

### Test 2: Check API Documentation

1. Open your web browser
2. Go to: `http://localhost:8000/docs`
3. You should see the Swagger API documentation (like an interactive API guide)

This proves the API server is working and you can see all available endpoints.

### Test 3: Test Health Check Endpoint

```bash
# In a new terminal (keep the server running in the other terminal)
curl http://localhost:8000/health
```

You should see:
```json
{"status": "healthy", "service": "deltadyno-telemetry"}
```

### Test 4: Test Root Endpoint

```bash
curl http://localhost:8000/
```

You should see:
```json
{
  "service": "DeltaDyno Telemetry API",
  "version": "1.0.0",
  "docs": "/docs"
}
```

### Test 5: Test Metrics Endpoint (May Return Empty - That's OK!)

```bash
# Replace 1 with your actual profile_id
curl http://localhost:8000/api/v1/metrics/breakout/1?days=7
```

**Expected**: If you haven't integrated telemetry into your scripts yet, this might return empty data or an error. **That's normal!** Once you add telemetry hooks to your scripts, data will start appearing.

---

## Testing Database Connection

### Check MySQL Connection

The system will try to create database tables automatically when you first use it. To verify:

```bash
# Connect to your MySQL database
mysql -u your_user -p your_database

# Check if tables were created
SHOW TABLES LIKE 'dd_telemetry%';

# You should see:
# - dd_telemetry_metrics
# - dd_trade_performance  
# - dd_system_health
```

### Check Redis Connection

```bash
# Connect to Redis
redis-cli

# Test connection
PING
# Should return: PONG

# Check if Redis has any telemetry keys (will be empty until scripts are integrated)
KEYS telemetry:*
```

---

## Common Issues & Solutions

### Issue: "ModuleNotFoundError: No module named 'fastapi'"
**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Issue: "Error: Can't connect to MySQL"
**Solution**: Check your `config/config.ini` database settings
- Make sure MySQL is running
- Verify host, username, password are correct

### Issue: "Error: Can't connect to Redis"
**Solution**: Check your `config/config.ini` Redis settings
- Make sure Redis is running: `redis-server`
- Verify host, port, password are correct

### Issue: "API endpoints return empty data"
**Solution**: This is expected! You need to:
1. Integrate telemetry hooks into your trading scripts (see integration guide)
2. Run your trading scripts to generate data
3. Then API endpoints will return data

---

## Summary

**What we built:**
- A monitoring system that watches your trading scripts
- Stores data in MySQL (permanent) and Redis (fast/real-time)
- Provides an API that your frontend can use to display dashboards

**Why MySQL + Redis:**
- MySQL = Permanent storage (like a filing cabinet)
- Redis = Fast real-time storage (like a whiteboard)
- Together = Speed + Reliability

**Next steps:**
1. Test the API server (it should start successfully)
2. Add telemetry hooks to your trading scripts
3. Deploy to AWS
4. Connect your frontend
5. Build beautiful dashboards!

**Testing:**
- Start the API server: `python api_server.py`
- Visit: `http://localhost:8000/docs`
- Test endpoints with `curl` or your browser
- Empty data is normal until you integrate telemetry into scripts

