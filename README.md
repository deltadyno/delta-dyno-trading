# DeltaDyno

<div align="center">

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

**Automated Breakout Detection Trading System**

*A professional-grade Python application for detecting market breakouts and executing options trades via Alpaca.*

</div>

---

## 📋 Overview

DeltaDyno is an automated trading system that:

- **Monitors market data** in real-time using Alpaca's API
- **Detects breakout signals** using pivot points and slope analysis
- **Validates signals** with Kalman filter smoothing
- **Publishes trade signals** to Redis for downstream execution
- **Supports backtesting** with historical data

## 🏗️ Project Structure

```
DeltaDyno/
├── deltadyno/                  # Main package
│   ├── __init__.py
│   ├── constants.py            # Application constants
│   ├── core/                   # Core business logic
│   │   ├── __init__.py
│   │   ├── breakout_detector.py
│   │   └── position_manager.py
│   ├── data/                   # Data fetching
│   │   ├── __init__.py
│   │   └── fetcher.py
│   ├── analysis/               # Technical analysis
│   │   ├── __init__.py
│   │   ├── pivots.py
│   │   ├── slope.py
│   │   ├── kalman.py
│   │   ├── breakout.py
│   │   └── choppy.py
│   ├── trading/                # Order management
│   │   ├── __init__.py
│   │   ├── orders.py           # Order placement utilities
│   │   ├── order_monitor.py    # Limit order monitoring
│   │   ├── equity_monitor.py   # Market equity monitoring
│   │   └── position_monitor.py # Position tracking & stop loss
│   ├── messaging/              # Redis queue
│   │   ├── __init__.py
│   │   └── redis_queue.py
│   ├── config/                 # Configuration
│   │   ├── __init__.py
│   │   ├── loader.py
│   │   ├── database.py
│   │   └── defaults.py
│   └── utils/                  # Utilities
│       ├── __init__.py
│       ├── logger.py
│       ├── timing.py
│       └── helpers.py
├── config/                     # Configuration files
│   └── config.ini
├── logs/                       # Log files (gitignored)
├── tests/                      # Test files
├── main.py                     # Breakout detector entry point
├── order_monitor.py            # Limit order monitor entry point
├── equity_monitor.py           # Market equity monitor entry point
├── requirements.txt
├── pyproject.toml
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9
- MySQL database
- Redis server
- Alpaca trading account
- TA-Lib (system library)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/DeltaDyno.git
   cd DeltaDyno
   ```

2. **Install system dependencies**

   **macOS:**
   ```bash
   # Install TA-Lib
   brew install ta-lib
   
   # Install MySQL
   brew install mysql
   
   # Install Redis (optional, for local development)
   brew install redis
   ```

   **Ubuntu/Debian:**
   ```bash
   # Install TA-Lib
   sudo apt-get install ta-lib
   
   # Install MySQL
   sudo apt-get install mysql-server
   ```

3. **Create virtual environment (Python 3.9)**
   ```bash
   # macOS - install Python 3.9 if needed
   brew install python@3.9
   
   # Create virtual environment
   python3.9 -m venv venv
   source venv/bin/activate
   ```

4. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure the application**
   ```bash
   # Edit config/config.ini with your settings
   nano config/config.ini
   ```

6. **Set up database**
   - Create a MySQL database named `deltadyno`
   - Run the schema migrations (see `docs/schema.sql`)

### Running

**Breakout Detector:**
```bash
# Basic usage
python main.py

# With options
python main.py --symbol SPY --length 15 --timeframe_minutes 3

# With console logging
python main.py --log_to_console

# View all options
python main.py --help
```

**Limit Order Monitor:**
```bash
# Start the order monitor for a profile
python order_monitor.py <profile_id>

# With console logging
python order_monitor.py <profile_id> --log_to_console

# View all options
python order_monitor.py --help
```

**Market Equity Monitor:**
```bash
# Start the equity monitor for a profile
python equity_monitor.py <profile_id>

# With console logging
python equity_monitor.py <profile_id> --log_to_console

# View all options
python equity_monitor.py --help
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ALPACA_API_KEY` | Alpaca API key | - |
| `ALPACA_API_SECRET` | Alpaca API secret | - |
| `REDIS_HOST` | Redis server host | `localhost` |
| `REDIS_PORT` | Redis server port | `6379` |
| `DB_HOST` | MySQL database host | `localhost` |

### Configuration File (`config/config.ini`)

```ini
[Common]
redis_host = localhost
redis_port = 6379
redis_password = your_password
db_host = localhost
db_user = root
db_password = your_password
db_name = deltadyno
```

## 📊 How It Works

### Breakout Detection Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Fetch Market   │────▶│  Calculate      │────▶│  Detect         │
│  Data (Alpaca)  │     │  Indicators     │     │  Breakouts      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Execute Trade  │◀────│  Validate with  │◀────│  Apply Kalman   │
│  (Redis Queue)  │     │  Constraints    │     │  Filter         │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Key Components

1. **Pivot Detection**: Identifies significant high/low points
2. **Slope Calculation**: Uses ATR for dynamic support/resistance
3. **Kalman Filter**: Smooths signals and estimates trend velocity
4. **Choppy Day Detection**: Identifies range-bound market conditions

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=deltadyno

# Run specific test file
pytest tests/test_breakout.py
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

**This software is for educational purposes only. Use at your own risk.**

Trading involves substantial risk of loss and is not suitable for all investors. Past performance is not indicative of future results. Always do your own research and consult with a licensed financial advisor before making any trading decisions.

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) first.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

<div align="center">
Made with ❤️ by the DeltaDyno Team
</div>

