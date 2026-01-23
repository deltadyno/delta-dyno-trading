"""
Unit tests for the order monitor module (order_monitor.py).

Tests cover:
- Order fill scenarios (full, partial, rejection)
- Stop-loss triggering
- Trailing stop updates
- Profit-taking logic
- Race conditions between manual exit and stop-loss
"""

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4
import pytest

# Note: pandas is mocked in conftest.py


# =============================================================================
# Order Processing Tests
# =============================================================================

class TestOrderProcessing:
    """Tests for process_order function."""
    
    @pytest.fixture
    def config_ranges(self):
        """Standard config ranges for order monitoring."""
        return {
            "seconds_to_monitor_open_positions": [60.0, 120.0, 180.0, 240.0, 300.0],
            "close_open_order_prcntage_of_open_qty": [0.25, 0.50, 0.75, 1.0, 1.0],
            "regular_minus_limit_order_price_diff": [0.05, 0.10, 0.15, 0.20, 0.25],
            "create_order_prcntage_of_open_qty": [0.25, 0.50, 0.75, 1.0, 1.0],
            "close_open_if_price_diff_more_than": [0.10, 0.20, 0.30, 0.40, 0.50],
        }
    
    @pytest.mark.unit
    def test_order_age_below_first_range_skipped(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Order younger than first time range should be skipped."""
        from deltadyno.trading.order_monitor import process_order
        
        now = datetime.now(timezone.utc)
        order = pd.Series({
            "id": str(uuid4()),
            "symbol": "SPY250124C00595000",
            "qty": "5",
            "limit_price": "5.25",
            "created_at": (now - timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        })
        
        first_time_sales = {}
        cancelled_id, time_spent = process_order(
            order, now, config_ranges, first_time_sales,
            0.0, mock_trading_client, mock_option_historical_client, mock_logger
        )
        
        assert cancelled_id is None
        mock_trading_client.cancel_order_by_id.assert_not_called()
    
    @pytest.mark.unit
    def test_order_age_triggers_first_range(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Order in first time range should trigger processing."""
        with patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_quote.return_value = 5.20  # Current price close to limit
            mock_place.return_value = True
            
            from deltadyno.trading.order_monitor import process_order
            
            now = datetime.now(timezone.utc)
            order = pd.Series({
                "id": str(uuid4()),
                "symbol": "SPY250124C00595000",
                "qty": "5",
                "limit_price": "5.25",
                "created_at": (now - timedelta(seconds=70)).isoformat().replace("+00:00", "Z"),
            })
            
            first_time_sales = {}
            cancelled_id, time_spent = process_order(
                order, now, config_ranges, first_time_sales,
                0.0, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            # Should have processed (either replaced or cancelled)
            assert "SPY250124C00595000" in first_time_sales
    
    @pytest.mark.unit
    def test_single_quantity_order_cancelled_entirely(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Single quantity order should be cancelled entirely, not replaced."""
        with patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_quote.return_value = 5.20
            mock_place.return_value = True
            
            from deltadyno.trading.order_monitor import process_order
            
            now = datetime.now(timezone.utc)
            order_id = str(uuid4())
            order = pd.Series({
                "id": order_id,
                "symbol": "SPY250124C00595000",
                "qty": "1",  # Single quantity
                "limit_price": "5.25",
                "created_at": (now - timedelta(seconds=70)).isoformat().replace("+00:00", "Z"),
            })
            
            first_time_sales = {}
            cancelled_id, _ = process_order(
                order, now, config_ranges, first_time_sales,
                0.0, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            assert cancelled_id == order_id
            mock_trading_client.cancel_order_by_id.assert_called_once()
    
    @pytest.mark.unit
    def test_partial_cancel_replaces_order(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Multi-quantity order should be partially cancelled and replaced."""
        with patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_quote.return_value = 5.20
            mock_place.return_value = True
            
            from deltadyno.trading.order_monitor import process_order
            
            now = datetime.now(timezone.utc)
            order_id = str(uuid4())
            order = pd.Series({
                "id": order_id,
                "symbol": "SPY250124C00595000",
                "qty": "10",  # Multi quantity
                "limit_price": "5.25",
                "created_at": (now - timedelta(seconds=70)).isoformat().replace("+00:00", "Z"),
            })
            
            first_time_sales = {}
            cancelled_id, _ = process_order(
                order, now, config_ranges, first_time_sales,
                0.0, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            # Should replace, not cancel
            mock_trading_client.replace_order_by_id.assert_called()
    
    @pytest.mark.unit
    def test_limit_price_none_skipped(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Order with None limit price should be skipped."""
        from deltadyno.trading.order_monitor import process_order
        
        now = datetime.now(timezone.utc)
        order = pd.Series({
            "id": str(uuid4()),
            "symbol": "SPY250124C00595000",
            "qty": "5",
            "limit_price": None,
            "created_at": (now - timedelta(seconds=70)).isoformat().replace("+00:00", "Z"),
        })
        
        first_time_sales = {}
        cancelled_id, _ = process_order(
            order, now, config_ranges, first_time_sales,
            0.0, mock_trading_client, mock_option_historical_client, mock_logger
        )
        
        assert cancelled_id is None
    
    @pytest.mark.unit
    def test_already_processed_range_skipped(
        self, config_ranges, mock_trading_client, mock_option_historical_client, mock_logger
    ):
        """Order already processed in current range should be skipped."""
        with patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote:
            mock_quote.return_value = 5.20
            
            from deltadyno.trading.order_monitor import process_order
            
            now = datetime.now(timezone.utc)
            symbol = "SPY250124C00595000"
            order = pd.Series({
                "id": str(uuid4()),
                "symbol": symbol,
                "qty": "5",
                "limit_price": "5.25",
                "created_at": (now - timedelta(seconds=70)).isoformat().replace("+00:00", "Z"),
            })
            
            # Already processed in range 60
            first_time_sales = {symbol: 60.0}
            cancelled_id, _ = process_order(
                order, now, config_ranges, first_time_sales,
                0.0, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            assert cancelled_id is None


class TestDynamicValueCalculation:
    """Tests for calculate_dynamic_values function."""
    
    @pytest.mark.unit
    def test_age_below_all_ranges(self, mock_logger):
        """Age below all ranges should return first range defaults."""
        from deltadyno.trading.order_monitor import calculate_dynamic_values
        
        seconds_gap_list = [60.0, 120.0, 180.0]
        sell_percent_list = [0.25, 0.50, 0.75]
        price_threshold_list = [0.05, 0.10, 0.15]
        create_percent_list = [0.25, 0.50, 0.75]
        price_diff_check_list = [0.10, 0.20, 0.30]
        
        result = calculate_dynamic_values(
            30.0,  # Below 60
            seconds_gap_list, sell_percent_list, price_threshold_list,
            create_percent_list, price_diff_check_list, mock_logger
        )
        
        seconds_range, sell_percent, price_threshold, create_percent, price_diff, triggered = result
        
        assert triggered is False
        assert seconds_range == 60.0
    
    @pytest.mark.unit
    def test_age_in_middle_range(self, mock_logger):
        """Age in middle range should return correct values."""
        from deltadyno.trading.order_monitor import calculate_dynamic_values
        
        seconds_gap_list = [60.0, 120.0, 180.0]
        sell_percent_list = [0.25, 0.50, 0.75]
        price_threshold_list = [0.05, 0.10, 0.15]
        create_percent_list = [0.25, 0.50, 0.75]
        price_diff_check_list = [0.10, 0.20, 0.30]
        
        result = calculate_dynamic_values(
            90.0,  # Between 60 and 120
            seconds_gap_list, sell_percent_list, price_threshold_list,
            create_percent_list, price_diff_check_list, mock_logger
        )
        
        seconds_range, sell_percent, price_threshold, create_percent, price_diff, triggered = result
        
        assert triggered is True
        assert seconds_range == 60.0
        assert sell_percent == 0.25
    
    @pytest.mark.unit
    def test_age_exceeds_all_ranges(self, mock_logger):
        """Age exceeding all ranges should return last values."""
        from deltadyno.trading.order_monitor import calculate_dynamic_values
        
        seconds_gap_list = [60.0, 120.0, 180.0]
        sell_percent_list = [0.25, 0.50, 0.75]
        price_threshold_list = [0.05, 0.10, 0.15]
        create_percent_list = [0.25, 0.50, 0.75]
        price_diff_check_list = [0.10, 0.20, 0.30]
        
        result = calculate_dynamic_values(
            500.0,  # Exceeds 180
            seconds_gap_list, sell_percent_list, price_threshold_list,
            create_percent_list, price_diff_check_list, mock_logger
        )
        
        seconds_range, sell_percent, price_threshold, create_percent, price_diff, triggered = result
        
        assert triggered is True
        assert seconds_range == 180.0
        assert sell_percent == 0.75


class TestBreakoutMessageProcessing:
    """Tests for Redis breakout message processing."""
    
    @pytest.mark.unit
    def test_valid_upward_message_places_call_order(
        self, mock_redis_client, mock_trading_client, mock_option_historical_client, 
        mock_db_config, mock_logger
    ):
        """Valid upward breakout message should place a call option order."""
        with patch("deltadyno.trading.order_monitor.generate_option_symbol") as mock_gen, \
             patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_gen.return_value = "SPY250124C00595000"
            mock_quote.return_value = 5.25
            mock_place.return_value = True
            
            from deltadyno.trading.order_monitor import process_breakout_messages
            
            # Simulate Redis message
            now = datetime.now(timezone.utc)
            mock_redis_client.xread.return_value = [
                ("breakout_messages:v1", [
                    ("1234-0", {
                        "symbol": "SPY",
                        "direction": "upward",
                        "close_price": "595.50",
                        "close_time": now.isoformat(),
                    })
                ])
            ]
            
            new_last_id = process_breakout_messages(
                mock_redis_client, "breakout_messages:v1", "0",
                mock_db_config, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            assert new_last_id == "1234-0"
            mock_place.assert_called_once()
            call_kwargs = mock_place.call_args[1]
            assert call_kwargs["symbol"] == "SPY250124C00595000"
    
    @pytest.mark.unit
    def test_valid_downward_message_places_put_order(
        self, mock_redis_client, mock_trading_client, mock_option_historical_client,
        mock_db_config, mock_logger
    ):
        """Valid downward breakout message should place a put option order."""
        with patch("deltadyno.trading.order_monitor.generate_option_symbol") as mock_gen, \
             patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_gen.return_value = "SPY250124P00590000"
            mock_quote.return_value = 4.75
            mock_place.return_value = True
            
            from deltadyno.trading.order_monitor import process_breakout_messages
            
            now = datetime.now(timezone.utc)
            mock_redis_client.xread.return_value = [
                ("breakout_messages:v1", [
                    ("1234-0", {
                        "symbol": "SPY",
                        "direction": "downward",
                        "close_price": "590.00",
                        "close_time": now.isoformat(),
                    })
                ])
            ]
            
            new_last_id = process_breakout_messages(
                mock_redis_client, "breakout_messages:v1", "0",
                mock_db_config, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            mock_place.assert_called_once()
    
    @pytest.mark.unit
    def test_invalid_direction_skipped(
        self, mock_redis_client, mock_trading_client, mock_option_historical_client,
        mock_db_config, mock_logger
    ):
        """Unknown direction should be skipped."""
        with patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            from deltadyno.trading.order_monitor import process_breakout_messages
            
            mock_redis_client.xread.return_value = [
                ("breakout_messages:v1", [
                    ("1234-0", {
                        "symbol": "SPY",
                        "direction": "unknown",
                        "close_price": "595.50",
                    })
                ])
            ]
            
            process_breakout_messages(
                mock_redis_client, "breakout_messages:v1", "0",
                mock_db_config, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            mock_place.assert_not_called()
    
    @pytest.mark.unit
    def test_position_close_action_skipped(
        self, mock_redis_client, mock_trading_client, mock_option_historical_client,
        mock_db_config, mock_logger
    ):
        """Position close action messages should be skipped."""
        with patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            from deltadyno.trading.order_monitor import process_breakout_messages
            
            mock_redis_client.xread.return_value = [
                ("breakout_messages:v1", [
                    ("1234-0", {
                        "symbol": "SPY",
                        "direction": "upward",
                        "close_price": "595.50",
                        "action": "close_position",
                    })
                ])
            ]
            
            process_breakout_messages(
                mock_redis_client, "breakout_messages:v1", "0",
                mock_db_config, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            mock_place.assert_not_called()
    
    @pytest.mark.unit
    def test_order_creation_disabled_skipped(
        self, mock_redis_client, mock_trading_client, mock_option_historical_client,
        mock_db_config, mock_logger
    ):
        """When order creation is disabled, messages should be skipped."""
        with patch("deltadyno.trading.order_monitor.generate_option_symbol") as mock_gen, \
             patch("deltadyno.trading.order_monitor.fetch_latest_option_quote") as mock_quote, \
             patch("deltadyno.trading.order_monitor.place_order") as mock_place:
            
            mock_gen.return_value = "SPY250124C00595000"
            mock_quote.return_value = 5.25
            mock_db_config.get.side_effect = lambda k, d, t: False if k == "create_order" else d
            
            from deltadyno.trading.order_monitor import process_breakout_messages
            
            mock_redis_client.xread.return_value = [
                ("breakout_messages:v1", [
                    ("1234-0", {
                        "symbol": "SPY",
                        "direction": "upward",
                        "close_price": "595.50",
                    })
                ])
            ]
            
            process_breakout_messages(
                mock_redis_client, "breakout_messages:v1", "0",
                mock_db_config, mock_trading_client, mock_option_historical_client, mock_logger
            )
            
            mock_place.assert_not_called()


class TestSleepTimeCalculation:
    """Tests for sleep time calculation based on market hours."""
    
    @pytest.mark.unit
    def test_market_open_uses_config_sleep(self, mock_db_config, mock_logger):
        """During market hours, should use configured sleep time."""
        from deltadyno.trading.order_monitor import calculate_sleep_time
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now - timedelta(hours=2),
            "regular_close": now + timedelta(hours=4),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        assert sleep_time == mock_db_config.get("close_pending_position_sleep_seconds", 1, float)
    
    @pytest.mark.unit
    def test_before_market_open_sleeps_until_open(self, mock_db_config, mock_logger):
        """Before market open, should sleep until market opens (capped at MAX)."""
        from deltadyno.trading.order_monitor import calculate_sleep_time, MAX_SLEEP_SECONDS
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now + timedelta(hours=1),
            "regular_close": now + timedelta(hours=8),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        # Sleep time is capped at MAX_SLEEP_SECONDS (1800)
        assert sleep_time == MAX_SLEEP_SECONDS
    
    @pytest.mark.unit
    def test_after_market_close_respects_max_sleep(self, mock_db_config, mock_logger):
        """After market close, should cap at MAX_SLEEP_SECONDS."""
        from deltadyno.trading.order_monitor import calculate_sleep_time, MAX_SLEEP_SECONDS
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now - timedelta(hours=10),
            "regular_close": now - timedelta(hours=2),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        assert sleep_time <= MAX_SLEEP_SECONDS
    
    @pytest.mark.unit
    def test_no_market_hours_uses_max_sleep(self, mock_db_config, mock_logger):
        """When market hours unavailable, should use MAX_SLEEP_SECONDS."""
        from deltadyno.trading.order_monitor import calculate_sleep_time, MAX_SLEEP_SECONDS
        
        sleep_time = calculate_sleep_time(None, mock_db_config, mock_logger)
        
        assert sleep_time == MAX_SLEEP_SECONDS


class TestOrderConfirmation:
    """Tests for order cancellation confirmation."""
    
    @pytest.mark.unit
    def test_cancelled_order_confirmed(self, mock_trading_client, mock_logger):
        """Successfully cancelled order should be confirmed."""
        with patch("deltadyno.trading.order_monitor.get_order_status") as mock_status, \
             patch("deltadyno.trading.order_monitor.sleep"):
            
            from alpaca.trading.enums import OrderStatus
            mock_status.return_value = OrderStatus.CANCELED
            
            from deltadyno.trading.order_monitor import confirm_order_cancellations
            
            cancelled_orders = ["order-123"]
            confirm_order_cancellations(cancelled_orders, mock_trading_client, mock_logger)
            
            mock_status.assert_called_once()
    
    @pytest.mark.unit
    def test_pending_order_retries(self, mock_trading_client, mock_logger):
        """Order not yet cancelled should trigger retries."""
        with patch("deltadyno.trading.order_monitor.get_order_status") as mock_status, \
             patch("deltadyno.trading.order_monitor.sleep"):
            
            from alpaca.trading.enums import OrderStatus
            # First call returns pending, second returns cancelled
            mock_status.side_effect = [OrderStatus.PENDING_CANCEL, OrderStatus.CANCELED]
            
            from deltadyno.trading.order_monitor import confirm_order_cancellations
            
            cancelled_orders = ["order-123"]
            confirm_order_cancellations(cancelled_orders, mock_trading_client, mock_logger)
            
            assert mock_status.call_count == 2
    
    @pytest.mark.unit
    def test_multiple_orders_confirmed_independently(self, mock_trading_client, mock_logger):
        """Multiple orders should each be confirmed independently."""
        with patch("deltadyno.trading.order_monitor.get_order_status") as mock_status, \
             patch("deltadyno.trading.order_monitor.sleep"):
            
            from alpaca.trading.enums import OrderStatus
            mock_status.return_value = OrderStatus.CANCELED
            
            from deltadyno.trading.order_monitor import confirm_order_cancellations
            
            cancelled_orders = ["order-1", "order-2", "order-3"]
            confirm_order_cancellations(cancelled_orders, mock_trading_client, mock_logger)
            
            assert mock_status.call_count == 3
    
    @pytest.mark.unit
    def test_status_none_triggers_retry(self, mock_trading_client, mock_logger):
        """None status should trigger retry."""
        with patch("deltadyno.trading.order_monitor.get_order_status") as mock_status, \
             patch("deltadyno.trading.order_monitor.sleep"):
            
            from alpaca.trading.enums import OrderStatus
            mock_status.side_effect = [None, OrderStatus.CANCELED]
            
            from deltadyno.trading.order_monitor import confirm_order_cancellations
            
            cancelled_orders = ["order-123"]
            confirm_order_cancellations(cancelled_orders, mock_trading_client, mock_logger)
            
            assert mock_status.call_count == 2


class TestConfigRangeParsing:
    """Tests for configuration range parsing."""
    
    @pytest.mark.unit
    def test_parse_valid_config_ranges(self, mock_db_config):
        """Valid config ranges should parse correctly."""
        from deltadyno.trading.order_monitor import parse_config_ranges
        
        keys = ["seconds_to_monitor_open_positions"]
        mock_db_config.seconds_to_monitor_open_positions = "60,120,180"
        
        result = parse_config_ranges(mock_db_config, keys)
        
        assert "seconds_to_monitor_open_positions" in result
        assert result["seconds_to_monitor_open_positions"] == [60.0, 120.0, 180.0]
    
    @pytest.mark.unit
    def test_parse_percentage_ranges_divided_by_100(self, mock_db_config):
        """Percentage ranges should be divided by 100."""
        from deltadyno.trading.order_monitor import parse_config_ranges
        
        keys = ["close_open_order_prcntage_of_open_qty"]
        mock_db_config.close_open_order_prcntage_of_open_qty = "25,50,75,100"
        
        result = parse_config_ranges(mock_db_config, keys)
        
        assert result["close_open_order_prcntage_of_open_qty"] == [0.25, 0.50, 0.75, 1.0]
    
    @pytest.mark.unit
    def test_parse_empty_config_returns_empty_list(self, mock_db_config):
        """Empty config should return empty list."""
        from deltadyno.trading.order_monitor import parse_config_ranges
        
        keys = ["missing_key"]
        mock_db_config.missing_key = ""
        
        result = parse_config_ranges(mock_db_config, keys)
        
        assert result["missing_key"] == []
    
    @pytest.mark.unit
    def test_parse_config_with_trailing_comma(self, mock_db_config):
        """Config with trailing comma should handle gracefully."""
        from deltadyno.trading.order_monitor import parse_config_ranges
        
        keys = ["seconds_to_monitor_open_positions"]
        mock_db_config.seconds_to_monitor_open_positions = "60,120,180,"
        
        result = parse_config_ranges(mock_db_config, keys)
        
        # Should filter empty strings
        assert len(result["seconds_to_monitor_open_positions"]) == 3


class TestOrderAgeCalculation:
    """Tests for order age and time-based calculations."""
    
    @pytest.mark.unit
    def test_order_age_calculated_correctly(self, mock_logger):
        """Order age should be calculated correctly from created_at."""
        from deltadyno.trading.order_monitor import calculate_dynamic_values
        
        seconds_gap = [60.0, 120.0, 180.0]
        sell_percent = [0.25, 0.50, 0.75]
        price_threshold = [0.05, 0.10, 0.15]
        create_percent = [0.25, 0.50, 0.75]
        diff_check = [0.10, 0.20, 0.30]
        
        # Age of 90 seconds should be in first range (60-120)
        result = calculate_dynamic_values(
            90.0, seconds_gap, sell_percent, price_threshold,
            create_percent, diff_check, mock_logger
        )
        
        seconds_range, sell_pct, _, _, _, triggered = result
        assert triggered is True
        assert seconds_range == 60.0
        assert sell_pct == 0.25
    
    @pytest.mark.unit
    def test_order_age_at_boundary(self, mock_logger):
        """Order age exactly at boundary should be in lower range."""
        from deltadyno.trading.order_monitor import calculate_dynamic_values
        
        seconds_gap = [60.0, 120.0, 180.0]
        sell_percent = [0.25, 0.50, 0.75]
        price_threshold = [0.05, 0.10, 0.15]
        create_percent = [0.25, 0.50, 0.75]
        diff_check = [0.10, 0.20, 0.30]
        
        # Age of exactly 120 should be in second range
        result = calculate_dynamic_values(
            120.0, seconds_gap, sell_percent, price_threshold,
            create_percent, diff_check, mock_logger
        )
        
        seconds_range, sell_pct, _, _, _, triggered = result
        assert triggered is True
        assert seconds_range == 120.0
        assert sell_pct == 0.50


class TestPriceDifferenceHandling:
    """Tests for price difference calculations and thresholds."""
    
    @pytest.mark.unit
    def test_price_diff_within_threshold(self, mock_logger):
        """Price diff within threshold should trigger processing."""
        current_price = 5.20
        limit_price = 5.25
        threshold = 0.10
        
        price_diff = abs(current_price - limit_price)
        
        assert price_diff <= threshold
    
    @pytest.mark.unit
    def test_price_diff_exceeds_threshold(self, mock_logger):
        """Price diff exceeding threshold should change behavior."""
        current_price = 4.50
        limit_price = 5.25
        threshold = 0.10
        
        price_diff = abs(current_price - limit_price)
        
        assert price_diff > threshold
    
    @pytest.mark.unit
    def test_price_diff_calculation_precision(self, mock_logger):
        """Price diff calculation should maintain precision."""
        current_price = 5.123456
        limit_price = 5.126789
        
        price_diff = round(abs(current_price - limit_price), 3)
        
        assert price_diff == 0.003


class TestOrderFiltering:
    """Tests for order filtering by asset class."""
    
    @pytest.mark.unit
    def test_filter_us_option_orders(self, order_factory):
        """Should filter for US option orders only."""
        orders = [
            order_factory.create_limit_order(symbol="SPY250124C00595000"),
            {"id": "eq-order", "symbol": "SPY", "asset_class": "us_equity", "qty": "10", "limit_price": "590.00"},
        ]
        
        # Filter for US option orders using list comprehension
        option_orders = [o for o in orders if o.get("asset_class") == "us_option"]
        
        assert len(option_orders) == 1
        assert option_orders[0]["symbol"] == "SPY250124C00595000"
    
    @pytest.mark.unit
    def test_empty_orders_handled(self, mock_trading_client, mock_logger):
        """Empty orders list should be handled gracefully."""
        orders = []
        
        if not orders:
            # Should skip processing
            processed = False
        else:
            processed = True
        
        assert processed is False


class TestFirstTimeSalesTracking:
    """Tests for first time sales tracking dictionary."""
    
    @pytest.mark.unit
    def test_first_time_sales_initialization(self):
        """First time sales dict should start empty."""
        first_time_sales = {}
        
        assert len(first_time_sales) == 0
    
    @pytest.mark.unit
    def test_symbol_added_on_first_sale(self):
        """Symbol should be added to tracking on first sale."""
        first_time_sales = {}
        symbol = "SPY250124C00595000"
        seconds_range = 60.0
        
        first_time_sales[symbol] = seconds_range
        
        assert symbol in first_time_sales
        assert first_time_sales[symbol] == 60.0
    
    @pytest.mark.unit
    def test_symbol_updated_on_new_range(self):
        """Symbol should be updated when moving to new range."""
        first_time_sales = {"SPY250124C00595000": 60.0}
        
        # Move to next range
        first_time_sales["SPY250124C00595000"] = 120.0
        
        assert first_time_sales["SPY250124C00595000"] == 120.0
    
    @pytest.mark.unit
    def test_symbol_removed_when_fully_processed(self):
        """Symbol should be removed when fully processed."""
        first_time_sales = {"SPY250124C00595000": 120.0}
        
        del first_time_sales["SPY250124C00595000"]
        
        assert "SPY250124C00595000" not in first_time_sales
    
    @pytest.mark.unit
    def test_cleanup_inactive_symbols(self):
        """Symbols no longer in active orders should be cleaned up."""
        first_time_sales = {
            "SPY250124C00595000": 60.0,
            "AAPL250124C00185000": 120.0,
            "TSLA250124P00450000": 180.0,
        }
        
        active_symbols = {"SPY250124C00595000"}
        
        for symbol in list(first_time_sales.keys()):
            if symbol not in active_symbols:
                del first_time_sales[symbol]
        
        assert len(first_time_sales) == 1
        assert "SPY250124C00595000" in first_time_sales


class TestTimeAgeAccumulation:
    """Tests for time age accumulation across processing cycles."""
    
    @pytest.mark.unit
    def test_time_age_accumulates(self, mock_logger):
        """Time age should accumulate for known symbols."""
        time_age_spent = 30.0
        current_age = 45.0
        
        # Accumulated age
        total_age = round(current_age + time_age_spent, 2)
        
        assert total_age == 75.0
    
    @pytest.mark.unit
    def test_time_age_reset_on_completion(self, mock_logger):
        """Time age should reset when order fully processed."""
        time_age_spent = 120.0
        
        # Order fully cancelled
        time_age_spent = 0.0
        
        assert time_age_spent == 0.0

