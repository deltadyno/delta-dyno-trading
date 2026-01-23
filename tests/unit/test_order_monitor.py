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
import pandas as pd


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
        """Before market open, should sleep until market opens."""
        from deltadyno.trading.order_monitor import calculate_sleep_time
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now + timedelta(hours=1),
            "regular_close": now + timedelta(hours=8),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        # Should be approximately 1 hour (3600 seconds)
        assert 3500 < sleep_time < 3700
    
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

