"""
Pytest configuration and shared fixtures for DeltaDyno test suite.

Provides:
- Mock trading clients (Alpaca, Redis)
- Frozen time utilities
- Common test data factories
- Database configuration mocks
"""

import asyncio
from datetime import datetime, timezone, timedelta, time as datetime_time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, AsyncMock, patch
from uuid import uuid4
import pytest


# =============================================================================
# Async Event Loop
# =============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =============================================================================
# Time Freezing
# =============================================================================

class FrozenClock:
    """Mock clock for deterministic time-based tests."""
    
    def __init__(self, frozen_time: datetime):
        self._time = frozen_time
    
    @property
    def is_open(self) -> bool:
        """Market is open 9:30-16:00 ET (14:30-21:00 UTC)."""
        market_open = datetime_time(14, 30)
        market_close = datetime_time(21, 0)
        current = self._time.time()
        return market_open <= current <= market_close
    
    def advance(self, seconds: float = 0, minutes: float = 0, hours: float = 0):
        """Advance the frozen time."""
        self._time += timedelta(seconds=seconds, minutes=minutes, hours=hours)
        return self._time
    
    @property
    def now(self) -> datetime:
        return self._time


@pytest.fixture
def frozen_time():
    """Fixture providing a frozen time during market hours."""
    # 10:30 AM ET = 15:30 UTC (market is open)
    base_time = datetime(2025, 1, 23, 15, 30, 0, tzinfo=timezone.utc)
    return FrozenClock(base_time)


@pytest.fixture
def frozen_time_premarket():
    """Fixture providing frozen time before market open."""
    # 8:00 AM ET = 13:00 UTC (pre-market)
    base_time = datetime(2025, 1, 23, 13, 0, 0, tzinfo=timezone.utc)
    return FrozenClock(base_time)


@pytest.fixture
def frozen_time_afterhours():
    """Fixture providing frozen time after market close."""
    # 5:00 PM ET = 22:00 UTC (after hours)
    base_time = datetime(2025, 1, 23, 22, 0, 0, tzinfo=timezone.utc)
    return FrozenClock(base_time)


# =============================================================================
# Mock Trading Client
# =============================================================================

@pytest.fixture
def mock_trading_client(frozen_time):
    """Create a mock Alpaca TradingClient."""
    client = MagicMock()
    
    # Clock simulation
    clock_mock = MagicMock()
    clock_mock.is_open = frozen_time.is_open
    client.get_clock.return_value = clock_mock
    
    # Calendar simulation
    calendar_entry = MagicMock()
    calendar_entry.open = datetime(2025, 1, 23, 9, 30)
    calendar_entry.close = datetime(2025, 1, 23, 16, 0)
    calendar_entry.date = "2025-01-23"
    client.get_calendar.return_value = [calendar_entry]
    
    # Account simulation
    account = MagicMock()
    account.equity = "100000.00"
    account.buying_power = "50000.00"
    account.cash = "25000.00"
    account.portfolio_value = "100000.00"
    account.daytrade_count = 0
    account.pattern_day_trader = False
    client.get_account.return_value = account
    
    # Empty positions by default
    client.get_all_positions.return_value = []
    client.get_orders.return_value = []
    
    # Order operations succeed by default
    client.submit_order.return_value = MagicMock(id=str(uuid4()))
    client.cancel_order_by_id.return_value = None
    client.replace_order_by_id.return_value = MagicMock(id=str(uuid4()))
    client.close_position.return_value = MagicMock()
    
    return client


# =============================================================================
# Mock Historical Data Client
# =============================================================================

@pytest.fixture
def mock_historical_client():
    """Create a mock Alpaca HistoricalDataClient."""
    client = MagicMock()
    
    # Default quote response
    quote = MagicMock()
    quote.ask_price = 5.25
    quote.bid_price = 5.20
    client.get_option_latest_quote.return_value = {"SPY250124C00595000": quote}
    
    return client


@pytest.fixture
def mock_option_historical_client():
    """Create a mock Alpaca OptionHistoricalDataClient."""
    client = MagicMock()
    
    quote = MagicMock()
    quote.ask_price = 5.25
    quote.bid_price = 5.20
    client.get_option_latest_quote.return_value = {"SPY250124C00595000": quote}
    
    return client


# =============================================================================
# Mock Redis Client
# =============================================================================

@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client."""
    client = MagicMock()
    
    # Track published messages for assertions
    client._published_messages = []
    
    def mock_xadd(stream_name: str, message: dict):
        msg_id = f"{int(datetime.now().timestamp() * 1000)}-0"
        client._published_messages.append({
            "stream": stream_name,
            "id": msg_id,
            "data": message
        })
        return msg_id
    
    client.xadd = MagicMock(side_effect=mock_xadd)
    client.xread = MagicMock(return_value=[])
    client.ping.return_value = True
    
    return client


@pytest.fixture
def mock_async_redis_client():
    """Create a mock async Redis client."""
    client = AsyncMock()
    
    client._published_messages = []
    
    async def mock_xadd(stream_name: str, message: dict):
        msg_id = f"{int(datetime.now().timestamp() * 1000)}-0"
        client._published_messages.append({
            "stream": stream_name,
            "id": msg_id,
            "data": message
        })
        return msg_id
    
    client.xadd = AsyncMock(side_effect=mock_xadd)
    client.xread = AsyncMock(return_value=[])
    client.ping = AsyncMock(return_value=True)
    
    return client


# =============================================================================
# Mock Database Config Loader
# =============================================================================

@pytest.fixture
def mock_db_config():
    """Create a mock DatabaseConfigLoader."""
    config = MagicMock()
    
    # Common attributes
    config.client_name = "TestClient"
    config.profile_id = "1"
    
    # Trading flags
    config.is_paper_trading = True
    config.create_order = True
    config.close_order = True
    config.read_real_data = True
    
    # Risk parameters
    config.max_daily_positions = 10
    config.max_volume_threshold = 500000
    config.skip_candle_with_size = 2.0
    config.min_gap_bars_cnt_for_breakout = 3
    config.hard_stop = 15
    config.min_profit_percent_to_enable_stoploss = 5
    
    # Trailing stop ranges
    config.trailing_stop_loss_percent_range = "0:10,10:20,20:30,30:50,50:100"
    config.trailing_stop_loss_percent_range_values = "5,10,15,20,25"
    config.trailing_stop_loss_percent_sell_quantity_once = "25,25,25,25,100"
    
    # Choppy day configuration
    config.choppy_trading_days = ""
    config.choppy_trailing_stop_loss_percent_range = "0:10,10:20,20:50"
    config.choppy_trailing_stop_loss_percent_range_values = "3,5,10"
    config.choppy_trailing_stop_loss_percent_sell_quantity_once = "50,50,100"
    
    # Order monitoring
    config.seconds_to_monitor_open_positions = "60,120,180,240,300"
    config.close_open_order_prcntage_of_open_qty = "25,50,75,100,100"
    config.regular_minus_limit_order_price_diff = "5,10,15,20,25"
    config.create_order_prcntage_of_open_qty = "25,50,75,100,100"
    config.close_open_if_price_diff_more_than = "10,20,30,40,50"
    
    # Timing
    config.close_pending_position_sleep_seconds = 1
    config.close_position_sleep_seconds = 2
    config.expire_sale_seconds = 300
    config.close_all_open_orders_at_local_time = "15:55"
    config.close_all_at_min_profit = 3.5
    config.cnt_of_times_to_skip_hard_stop = 2
    config.default_trailing_stop_loss = 5
    
    # Skip days
    config.skip_trading_days = ""
    
    # Methods
    config.get = MagicMock(side_effect=lambda key, default, dtype: getattr(config, key, default))
    config.get_active_profile_id = MagicMock(return_value=True)
    config.update_attr = MagicMock()
    
    return config


@pytest.fixture
def mock_file_config():
    """Create a mock file ConfigLoader."""
    config = MagicMock()
    
    config.db_host = "localhost"
    config.db_user = "test"
    config.db_password = "test"
    config.db_name = "test_db"
    
    config.redis_host = "localhost"
    config.redis_port = 6379
    config.redis_password = ""
    config.redis_stream_name_breakout_message = "breakout_messages:v1"
    config.redis_stream_name_option_flow = "option_flow:v1"
    
    config.data_feed = "IEX"
    config.max_retries = 3
    config.base_delay = 1
    
    return config


# =============================================================================
# Mock Logger
# =============================================================================

@pytest.fixture
def mock_logger():
    """Create a mock logger that captures all log calls."""
    logger = MagicMock()
    logger._logs = {"debug": [], "info": [], "warning": [], "error": []}
    
    def capture_log(level):
        def _log(msg, *args):
            logger._logs[level].append(msg % args if args else msg)
        return _log
    
    logger.debug = MagicMock(side_effect=capture_log("debug"))
    logger.info = MagicMock(side_effect=capture_log("info"))
    logger.warning = MagicMock(side_effect=capture_log("warning"))
    logger.error = MagicMock(side_effect=capture_log("error"))
    
    return logger


# =============================================================================
# Order Factory
# =============================================================================

class OrderFactory:
    """Factory for creating test order objects."""
    
    @staticmethod
    def create_limit_order(
        symbol: str = "SPY250124C00595000",
        qty: int = 5,
        limit_price: float = 5.25,
        status: str = "new",
        created_at: Optional[datetime] = None,
        filled_qty: int = 0,
        side: str = "buy"
    ) -> dict:
        """Create a mock limit order dict."""
        if created_at is None:
            created_at = datetime.now(timezone.utc)
        
        return {
            "id": str(uuid4()),
            "client_order_id": str(uuid4()),
            "symbol": symbol,
            "qty": str(qty),
            "filled_qty": str(filled_qty),
            "limit_price": str(limit_price),
            "status": status,
            "side": side,
            "order_type": "limit",
            "asset_class": "us_option",
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "submitted_at": created_at.isoformat().replace("+00:00", "Z"),
            "updated_at": created_at.isoformat().replace("+00:00", "Z"),
            "filled_at": None,
            "time_in_force": "day",
        }
    
    @staticmethod
    def create_market_order(
        symbol: str = "SPY250124C00595000",
        qty: int = 5,
        status: str = "filled",
        filled_avg_price: float = 5.25,
        side: str = "buy"
    ) -> dict:
        """Create a mock market order dict."""
        now = datetime.now(timezone.utc)
        return {
            "id": str(uuid4()),
            "client_order_id": str(uuid4()),
            "symbol": symbol,
            "qty": str(qty),
            "filled_qty": str(qty) if status == "filled" else "0",
            "filled_avg_price": str(filled_avg_price) if status == "filled" else None,
            "status": status,
            "side": side,
            "order_type": "market",
            "asset_class": "us_option",
            "created_at": now.isoformat().replace("+00:00", "Z"),
            "submitted_at": now.isoformat().replace("+00:00", "Z"),
            "updated_at": now.isoformat().replace("+00:00", "Z"),
            "filled_at": now.isoformat().replace("+00:00", "Z") if status == "filled" else None,
            "time_in_force": "day",
        }


@pytest.fixture
def order_factory():
    """Provide the OrderFactory for tests."""
    return OrderFactory()


# =============================================================================
# Position Factory
# =============================================================================

class PositionFactory:
    """Factory for creating test position objects."""
    
    @staticmethod
    def create_position(
        symbol: str = "SPY250124C00595000",
        qty: int = 5,
        avg_entry_price: float = 5.00,
        current_price: float = 5.50,
        unrealized_plpc: Optional[float] = None,
        asset_class: str = "us_option"
    ) -> MagicMock:
        """Create a mock position object."""
        position = MagicMock()
        position.symbol = symbol
        position.qty = str(qty)
        position.qty_available = str(qty)
        position.avg_entry_price = str(avg_entry_price)
        position.current_price = str(current_price)
        position.market_value = str(qty * current_price * 100)
        position.cost_basis = str(qty * avg_entry_price * 100)
        position.asset_class = asset_class
        
        # Calculate P/L if not provided
        if unrealized_plpc is None:
            unrealized_plpc = (current_price - avg_entry_price) / avg_entry_price
        position.unrealized_plpc = str(unrealized_plpc)
        position.unrealized_pl = str((current_price - avg_entry_price) * qty * 100)
        position.unrealized_intraday_plpc = position.unrealized_plpc
        position.unrealized_intraday_pl = position.unrealized_pl
        position.side = "long"
        
        return position


@pytest.fixture
def position_factory():
    """Provide the PositionFactory for tests."""
    return PositionFactory()


# =============================================================================
# Candle Data Factory
# =============================================================================

class CandleFactory:
    """Factory for creating test candle data."""
    
    @staticmethod
    def create_candle(
        open_price: float = 590.0,
        high: float = 592.0,
        low: float = 589.0,
        close: float = 591.5,
        volume: int = 50000,
        timestamp: Optional[datetime] = None
    ) -> dict:
        """Create a single candle dict."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        return {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "time": timestamp
        }
    
    @staticmethod
    def create_bullish_candle(base_price: float = 590.0, **kwargs) -> dict:
        """Create a bullish (green) candle."""
        return CandleFactory.create_candle(
            open_price=base_price,
            high=base_price + 2.0,
            low=base_price - 0.5,
            close=base_price + 1.5,
            **kwargs
        )
    
    @staticmethod
    def create_bearish_candle(base_price: float = 590.0, **kwargs) -> dict:
        """Create a bearish (red) candle."""
        return CandleFactory.create_candle(
            open_price=base_price,
            high=base_price + 0.5,
            low=base_price - 2.0,
            close=base_price - 1.5,
            **kwargs
        )
    
    @staticmethod
    def create_doji_candle(base_price: float = 590.0, **kwargs) -> dict:
        """Create a doji (indecisive) candle."""
        return CandleFactory.create_candle(
            open_price=base_price,
            high=base_price + 1.0,
            low=base_price - 1.0,
            close=base_price + 0.05,
            **kwargs
        )


@pytest.fixture
def candle_factory():
    """Provide the CandleFactory for tests."""
    return CandleFactory()


# =============================================================================
# Breakout Message Factory
# =============================================================================

class BreakoutMessageFactory:
    """Factory for creating test breakout messages."""
    
    @staticmethod
    def create_message(
        symbol: str = "SPY",
        direction: str = "upward",
        close_price: float = 591.5,
        candle_size: float = 1.5,
        bar_strength: float = 0.75,
        volume: int = 50000,
        choppy_day_count: int = 0,
        timestamp: Optional[datetime] = None
    ) -> dict:
        """Create a breakout message dict."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        return {
            "symbol": symbol,
            "direction": direction,
            "close_price": str(close_price),
            "close_time": timestamp.isoformat(),
            "candle_size": str(candle_size),
            "bar_strength": str(bar_strength),
            "volume": str(volume),
            "choppy_day_count": str(choppy_day_count),
            "timestamp": timestamp.isoformat()
        }
    
    @staticmethod
    def create_upward_breakout(**kwargs) -> dict:
        """Create an upward breakout message."""
        return BreakoutMessageFactory.create_message(direction="upward", **kwargs)
    
    @staticmethod
    def create_downward_breakout(**kwargs) -> dict:
        """Create a downward breakout message."""
        return BreakoutMessageFactory.create_message(direction="downward", **kwargs)


@pytest.fixture
def breakout_message_factory():
    """Provide the BreakoutMessageFactory for tests."""
    return BreakoutMessageFactory()


# =============================================================================
# Option Trade Factory
# =============================================================================

class OptionTradeFactory:
    """Factory for creating test option trade data."""
    
    @staticmethod
    def create_trade(
        symbol: str = "SPY250124C00595000",
        price: float = 5.25,
        size: int = 100,
        timestamp: Optional[datetime] = None
    ) -> MagicMock:
        """Create a mock option trade."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        trade = MagicMock()
        trade.symbol = symbol
        trade.price = price
        trade.size = size
        trade.timestamp = timestamp
        
        return trade
    
    @staticmethod
    def create_high_premium_trade(premium_target: float = 10000, **kwargs) -> MagicMock:
        """Create a trade that exceeds typical premium threshold."""
        # premium = price * size * 100
        # 10000 = price * size * 100
        return OptionTradeFactory.create_trade(price=10.0, size=10, **kwargs)
    
    @staticmethod
    def create_low_premium_trade(**kwargs) -> MagicMock:
        """Create a trade with low premium (below typical threshold)."""
        return OptionTradeFactory.create_trade(price=0.05, size=1, **kwargs)


@pytest.fixture
def option_trade_factory():
    """Provide the OptionTradeFactory for tests."""
    return OptionTradeFactory()

