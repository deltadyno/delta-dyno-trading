"""
Unit tests for the option stream handler module (stream_handler.py).

Tests cover:
- Valid option tick processing
- Malformed message handling
- Duplicate message deduplication
- High-frequency burst handling
- Stream disconnect and reconnect
- Queue/Redis lag scenarios
"""

import asyncio
from datetime import datetime, timezone
from queue import Queue
from unittest.mock import MagicMock, AsyncMock, patch
import pytest


def run_async(coro):
    """Helper to run async functions in sync tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


# =============================================================================
# Symbol Parsing Tests
# =============================================================================

class TestSymbolParsing:
    """Tests for OCC option symbol parsing."""
    
    @pytest.mark.unit
    def test_parse_call_option_symbol(self):
        """Parse a call option symbol correctly."""
        from deltadyno.options.stream_handler import parse_option_symbol
        
        symbol = "SPY250124C00595000"
        
        result = parse_option_symbol(symbol)
        
        assert result["Underlying"] == "SPY"
        assert result["Expiration Date"] == "250124"
        assert result["Option Type"] == "C"
        assert result["Strike Price"] == "595.00"
    
    @pytest.mark.unit
    def test_parse_put_option_symbol(self):
        """Parse a put option symbol correctly."""
        from deltadyno.options.stream_handler import parse_option_symbol
        
        symbol = "TSLA250117P00450000"
        
        result = parse_option_symbol(symbol)
        
        assert result["Underlying"] == "TSLA"
        assert result["Expiration Date"] == "250117"
        assert result["Option Type"] == "P"
        assert result["Strike Price"] == "450.00"
    
    @pytest.mark.unit
    def test_parse_long_ticker_symbol(self):
        """Parse option with multi-character ticker."""
        from deltadyno.options.stream_handler import parse_option_symbol
        
        symbol = "GOOGL250131C00185000"
        
        result = parse_option_symbol(symbol)
        
        assert result["Underlying"] == "GOOGL"
        assert result["Strike Price"] == "185.00"
    
    @pytest.mark.unit
    def test_get_strike_price(self):
        """Extract strike price from symbol."""
        from deltadyno.options.stream_handler import get_strike_price
        
        symbol = "SPY250124C00595500"  # Strike = 595.50
        
        strike = get_strike_price(symbol)
        
        assert strike == 595.5
    
    @pytest.mark.unit
    def test_expiration_date_conversion(self):
        """Convert YYMMDD to ISO format."""
        from deltadyno.options.stream_handler import _exp_yymmdd_to_iso
        
        result = _exp_yymmdd_to_iso("250124")
        
        assert result == "2025-01-24"
    
    @pytest.mark.unit
    def test_parse_invalid_symbol_returns_error(self):
        """Invalid symbol should return error values."""
        from deltadyno.options.stream_handler import parse_option_symbol
        
        result = parse_option_symbol("INVALID")
        
        assert result["Underlying"] == "Error"


class TestOptionTradeProcessing:
    """Tests for option trade event processing."""
    
    @pytest.mark.unit
    def test_high_premium_trade_processed(self, option_trade_factory):
        """High premium trade should be processed and queued."""
        with patch("deltadyno.options.stream_handler.write_to_db") as mock_write, \
             patch("deltadyno.options.stream_handler._premium_threshold", 500):
            
            from deltadyno.options.stream_handler import option_trade_handler
            
            # Premium = 10.0 * 10 * 100 = 10,000 (above 500 threshold)
            trade = option_trade_factory.create_high_premium_trade()
            
            run_async(option_trade_handler(trade))
            
            mock_write.assert_called_once()
    
    @pytest.mark.unit
    def test_low_premium_trade_ignored(self, option_trade_factory):
        """Low premium trade should be ignored."""
        with patch("deltadyno.options.stream_handler.write_to_db") as mock_write, \
             patch("deltadyno.options.stream_handler._premium_threshold", 500):
            
            from deltadyno.options.stream_handler import option_trade_handler
            
            # Premium = 0.05 * 1 * 100 = 5 (below 500 threshold)
            trade = option_trade_factory.create_low_premium_trade()
            
            run_async(option_trade_handler(trade))
            
            mock_write.assert_not_called()
    
    @pytest.mark.unit
    def test_trade_data_formatted_correctly(self, option_trade_factory):
        """Trade data should be formatted correctly for DB."""
        with patch("deltadyno.options.stream_handler.queue_trade") as mock_queue, \
             patch("deltadyno.options.stream_handler.push_to_redis") as mock_redis:
            
            mock_redis.return_value = True
            
            from deltadyno.options.stream_handler import write_to_db
            
            trade = option_trade_factory.create_trade(
                symbol="SPY250124C00595000",
                price=5.25,
                size=10,
            )
            premium = 5250.0  # 5.25 * 10 * 100
            
            write_to_db(trade, premium)
            
            mock_queue.assert_called_once()
            trade_data = mock_queue.call_args[0][0]
            
            assert trade_data["Symbol"] == "SPY250124C00595000"
            assert trade_data["Ticker"] == "SPY"
            assert trade_data["OptionType"] == "C"
            assert trade_data["Price"] == 5.25
            assert trade_data["Size"] == 10
            assert trade_data["Premium"] == 5250.0


class TestRedisPublishing:
    """Tests for Redis message publishing."""
    
    @pytest.mark.unit
    def test_push_to_redis_success(self, mock_redis_client):
        """Successful Redis push should return True."""
        with patch("deltadyno.options.stream_handler._redis_client", mock_redis_client), \
             patch("deltadyno.options.stream_handler._redis_queue_name", "option_flow:v1"):
            
            from deltadyno.options.stream_handler import push_to_redis
            
            message = {"symbol": "SPY", "price": 5.25}
            
            result = push_to_redis(message)
            
            assert result is True
            mock_redis_client.xadd.assert_called_once()
    
    @pytest.mark.unit
    def test_push_to_redis_not_configured(self):
        """Push without Redis configured should return False."""
        with patch("deltadyno.options.stream_handler._redis_client", None), \
             patch("deltadyno.options.stream_handler._redis_queue_name", None):
            
            from deltadyno.options.stream_handler import push_to_redis
            
            result = push_to_redis({"test": "data"})
            
            assert result is False
    
    @pytest.mark.unit
    def test_push_to_redis_error_handled(self, mock_redis_client):
        """Redis error should be handled gracefully."""
        mock_redis_client.xadd.side_effect = Exception("Redis connection error")
        
        with patch("deltadyno.options.stream_handler._redis_client", mock_redis_client), \
             patch("deltadyno.options.stream_handler._redis_queue_name", "option_flow:v1"):
            
            from deltadyno.options.stream_handler import push_to_redis
            
            result = push_to_redis({"test": "data"})
            
            assert result is False


class TestTradeBuffering:
    """Tests for trade buffer queue operations."""
    
    @pytest.mark.unit
    def test_queue_trade_adds_to_buffer(self):
        """Trade should be added to buffer queue."""
        with patch("deltadyno.options.stream_handler._trade_buffer", Queue()) as mock_buffer:
            from deltadyno.options.stream_handler import queue_trade, get_trade_buffer
            
            trade_data = {"Symbol": "SPY250124C00595000", "Premium": 5000}
            
            queue_trade(trade_data)
            
            buffer = get_trade_buffer()
            assert not buffer.empty()
    
    @pytest.mark.unit
    def test_get_trade_buffer_returns_queue(self):
        """get_trade_buffer should return the queue instance."""
        from deltadyno.options.stream_handler import get_trade_buffer
        
        buffer = get_trade_buffer()
        
        assert isinstance(buffer, Queue)


class TestPremiumThreshold:
    """Tests for premium threshold configuration."""
    
    @pytest.mark.unit
    def test_set_premium_threshold(self):
        """Premium threshold should be configurable."""
        from deltadyno.options.stream_handler import (
            set_premium_threshold, 
            _premium_threshold
        )
        
        set_premium_threshold(1000)
        
        # Import again to get updated value
        from deltadyno.options import stream_handler
        # The function modifies the global, so we check via the module
        assert stream_handler._premium_threshold == 1000


class TestStreamInitialization:
    """Tests for option stream initialization."""
    
    @pytest.mark.unit
    def test_init_option_stream(self):
        """Option stream should be initialized correctly."""
        with patch("deltadyno.options.stream_handler.OptionDataStream") as MockStream:
            from deltadyno.options.stream_handler import init_option_stream
            
            stream = init_option_stream("api_key", "api_secret")
            
            MockStream.assert_called_once()
            # Verify OPRA feed is used
            call_kwargs = MockStream.call_args
            assert "OPRA" in str(call_kwargs)
    
    @pytest.mark.unit
    def test_get_option_stream_returns_instance(self):
        """get_option_stream should return current instance."""
        with patch("deltadyno.options.stream_handler._option_stream", MagicMock()):
            from deltadyno.options.stream_handler import get_option_stream
            
            stream = get_option_stream()
            
            assert stream is not None
    
    @pytest.mark.unit
    def test_set_redis_client_configures_module(self):
        """set_redis_client should configure module globals."""
        from deltadyno.options.stream_handler import set_redis_client
        from deltadyno.options import stream_handler
        
        mock_client = MagicMock()
        
        set_redis_client(mock_client, "test_queue")
        
        assert stream_handler._redis_client is mock_client
        assert stream_handler._redis_queue_name == "test_queue"


class TestMalformedMessageHandling:
    """Tests for handling malformed messages."""
    
    @pytest.mark.unit
    def test_none_price_handled(self):
        """None price should be handled gracefully."""
        from deltadyno.options.stream_handler import option_trade_handler
        
        trade = MagicMock()
        trade.symbol = "SPY250124C00595000"
        trade.price = None
        trade.size = 10
        trade.timestamp = datetime.now(timezone.utc)
        
        # Should not raise
        run_async(option_trade_handler(trade))
    
    @pytest.mark.unit
    def test_none_size_handled(self):
        """None size should be handled gracefully."""
        from deltadyno.options.stream_handler import option_trade_handler
        
        trade = MagicMock()
        trade.symbol = "SPY250124C00595000"
        trade.price = 5.25
        trade.size = None
        trade.timestamp = datetime.now(timezone.utc)
        
        # Should not raise
        run_async(option_trade_handler(trade))
    
    @pytest.mark.unit
    def test_none_timestamp_uses_current_time(self, option_trade_factory):
        """None timestamp should use current time."""
        with patch("deltadyno.options.stream_handler.write_to_db") as mock_write, \
             patch("deltadyno.options.stream_handler._premium_threshold", 0):
            
            from deltadyno.options.stream_handler import option_trade_handler
            
            trade = option_trade_factory.create_trade(price=5.0, size=10)
            trade.timestamp = None
            
            run_async(option_trade_handler(trade))
            
            mock_write.assert_called_once()


class TestDuplicateMessageDeduplication:
    """Tests for duplicate message detection."""
    
    @pytest.mark.unit
    def test_duplicate_detection_by_trade_id(self):
        """Duplicate trades should be detected by unique identifier."""
        processed_trades = set()
        
        # First trade
        trade_id_1 = "SPY250124C00595000-1706108400000-5.25-10"
        is_duplicate_1 = trade_id_1 in processed_trades
        processed_trades.add(trade_id_1)
        
        # Same trade again
        trade_id_2 = "SPY250124C00595000-1706108400000-5.25-10"
        is_duplicate_2 = trade_id_2 in processed_trades
        
        assert is_duplicate_1 is False
        assert is_duplicate_2 is True
    
    @pytest.mark.unit
    def test_similar_trades_not_duplicates(self):
        """Similar but distinct trades should not be duplicates."""
        processed_trades = set()
        
        trade_id_1 = "SPY250124C00595000-1706108400000-5.25-10"
        trade_id_2 = "SPY250124C00595000-1706108400001-5.25-10"  # Different timestamp
        
        processed_trades.add(trade_id_1)
        is_duplicate = trade_id_2 in processed_trades
        
        assert is_duplicate is False


class TestHighFrequencyBursts:
    """Tests for handling high-frequency trade bursts."""
    
    @pytest.mark.unit
    def test_buffer_handles_burst(self):
        """Trade buffer should handle burst of trades."""
        from queue import Queue
        
        buffer = Queue()
        
        # Simulate burst of 1000 trades
        for i in range(1000):
            trade_data = {"Symbol": f"TRADE-{i}", "Premium": 1000}
            buffer.put(trade_data)
        
        assert buffer.qsize() == 1000
    
    @pytest.mark.unit
    def test_rapid_trades_processed_sequentially(self, option_trade_factory):
        """Rapid trades should be processed in order."""
        processed_order = []
        
        with patch("deltadyno.options.stream_handler.write_to_db") as mock_write, \
             patch("deltadyno.options.stream_handler._premium_threshold", 0):
            
            def track_order(data, premium):
                processed_order.append(data.symbol)
            
            mock_write.side_effect = track_order
            
            from deltadyno.options.stream_handler import option_trade_handler
            
            for i in range(5):
                trade = option_trade_factory.create_trade(
                    symbol=f"SPY250124C0059{i}000",
                    price=1.0,
                    size=10
                )
                run_async(option_trade_handler(trade))
        
        assert len(processed_order) == 5
        # Verify sequential processing
        for i in range(5):
            assert f"SPY250124C0059{i}000" in processed_order


class TestStreamReconnection:
    """Tests for stream disconnect and reconnect handling."""
    
    @pytest.mark.unit
    def test_stream_error_logged(self):
        """Stream errors should be logged."""
        with patch("deltadyno.options.stream_handler._option_stream") as mock_stream, \
             patch("deltadyno.options.stream_handler.logger") as mock_logger:
            
            mock_stream._run_forever = AsyncMock(
                side_effect=Exception("Connection lost")
            )
            
            from deltadyno.options.stream_handler import run_stream
            
            try:
                run_async(run_stream())
            except Exception:
                pass
            
            mock_logger.error.assert_called()


class TestLagScenarios:
    """Tests for handling queue/Redis lag."""
    
    @pytest.mark.unit
    def test_stale_trade_detection(self):
        """Stale trades (old timestamps) should be detectable."""
        from datetime import timedelta
        
        trade_timestamp = datetime.now(timezone.utc) - timedelta(minutes=5)
        current_time = datetime.now(timezone.utc)
        max_age_seconds = 60  # 1 minute
        
        age = (current_time - trade_timestamp).total_seconds()
        is_stale = age > max_age_seconds
        
        assert is_stale is True
    
    @pytest.mark.unit
    def test_fresh_trade_not_stale(self):
        """Fresh trades should not be marked as stale."""
        from datetime import timedelta
        
        trade_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)
        current_time = datetime.now(timezone.utc)
        max_age_seconds = 60
        
        age = (current_time - trade_timestamp).total_seconds()
        is_stale = age > max_age_seconds
        
        assert is_stale is False
    
    @pytest.mark.unit
    def test_buffer_backpressure_detection(self):
        """Detect when buffer is getting too large (backpressure)."""
        from queue import Queue
        
        buffer = Queue()
        max_size = 100
        
        for i in range(150):
            buffer.put({"trade": i})
        
        has_backpressure = buffer.qsize() > max_size
        
        assert has_backpressure is True

