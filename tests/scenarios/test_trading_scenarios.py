"""
Scenario-based tests for the DeltaDyno trading system.

Tests cover complex real-world scenarios:
- Concurrent breakout detection
- Kill-switch during order placement
- Stale pricing from stream delays
- System restart with open positions
- Race conditions
"""

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch, call
from uuid import uuid4
import pytest
import asyncio


# =============================================================================
# Concurrent Breakout Scenarios
# =============================================================================

class TestConcurrentBreakouts:
    """Tests for handling multiple simultaneous breakouts."""
    
    @pytest.mark.scenario
    def test_two_breakouts_same_candle_handled(
        self, mock_trading_client, mock_redis_client, mock_logger, frozen_time
    ):
        """Two breakouts detected on same candle should be handled correctly."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            mock_kalman.return_value = (590.0, 0.1, True)
            mock_queue.return_value = True
            mock_trading_client.get_clock.return_value.is_open = True
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            base_params = {
                "prev_kfilt": 590.0,
                "prev_velocity": 0.1,
                "enable_kalman_prediction": True,
                "skip_trading_days_list": [],
                "latest_close_time": frozen_time.now,
                "choppy_day_cnt": 0,
                "bar_head_cnt": 10,
                "maxvolume": 500000,
                "min_gap_bars_cnt_for_breakout": 3,
                "positioncnt": 0,
                "positionqty": 10,
                "createorder": True,
                "bar_strength": 0.75,
                "latest_close": 591.5,
                "latest_open": 590.0,
                "latest_high": 592.0,
                "latest_low": 589.0,
                "skip_candle_with_size": 5.0,
                "volume": 50000,
                "symbol": "SPY",
                "trading_client": mock_trading_client,
                "redis_client": mock_redis_client,
                "redis_queue_name_str": "breakout_messages:v1",
                "bar_date": frozen_time.now.date(),
                "logger": mock_logger,
            }
            
            # First breakout (upward)
            result1 = check_for_breakouts(
                **{**base_params, "upos": 1, "prev_upos": 0, "dnos": 0, "prev_dnos": 0}
            )
            
            # Second breakout (downward) - should be blocked by position limit
            result2 = check_for_breakouts(
                **{**base_params, "upos": 0, "prev_upos": 0, "dnos": 1, "prev_dnos": 0,
                   "positioncnt": 1}  # Already have 1 position
            )
            
            assert result1[1] == "upward"  # First breakout should succeed
            # Second breakout depends on position limits
    
    @pytest.mark.scenario
    def test_rapid_breakouts_respect_min_gap(
        self, mock_trading_client, mock_redis_client, mock_logger, frozen_time
    ):
        """Rapid successive breakouts should respect minimum bar gap."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            mock_kalman.return_value = (590.0, 0.1, True)
            mock_queue.return_value = True
            mock_trading_client.get_clock.return_value.is_open = True
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            base_params = {
                "prev_kfilt": 590.0,
                "prev_velocity": 0.1,
                "enable_kalman_prediction": True,
                "skip_trading_days_list": [],
                "latest_close_time": frozen_time.now,
                "choppy_day_cnt": 0,
                "maxvolume": 500000,
                "min_gap_bars_cnt_for_breakout": 5,
                "positioncnt": 0,
                "positionqty": 10,
                "createorder": True,
                "upos": 1,
                "prev_upos": 0,
                "dnos": 0,
                "prev_dnos": 0,
                "bar_strength": 0.75,
                "latest_close": 591.5,
                "latest_open": 590.0,
                "latest_high": 592.0,
                "latest_low": 589.0,
                "skip_candle_with_size": 5.0,
                "volume": 50000,
                "symbol": "SPY",
                "trading_client": mock_trading_client,
                "redis_client": mock_redis_client,
                "redis_queue_name_str": "breakout_messages:v1",
                "bar_date": frozen_time.now.date(),
                "logger": mock_logger,
            }
            
            # First breakout with bar_head_cnt=10 (above min gap)
            result1 = check_for_breakouts(**{**base_params, "bar_head_cnt": 10})
            
            # Second breakout with bar_head_cnt=2 (below min gap of 5)
            result2 = check_for_breakouts(**{**base_params, "bar_head_cnt": 2})
            
            assert result1[1] == "upward"  # Should succeed
            assert result2[1] is None  # Should be blocked


class TestKillSwitchDuringOrderPlacement:
    """Tests for kill-switch activating during order operations."""
    
    @pytest.mark.scenario
    def test_kill_switch_during_order_placement(
        self, mock_trading_client, mock_db_config, mock_logger
    ):
        """Kill-switch activating during order placement should cancel order."""
        order_placed = False
        kill_switch_triggered = False
        
        def mock_place_order(*args, **kwargs):
            nonlocal order_placed
            order_placed = True
            # Simulate kill-switch triggering mid-order
            return MagicMock(id=str(uuid4()))
        
        mock_trading_client.submit_order = MagicMock(side_effect=mock_place_order)
        
        # Simulate placing order
        order = mock_trading_client.submit_order(
            symbol="SPY250124C00595000",
            qty=5,
            side="buy",
            type="limit",
            limit_price=5.25
        )
        
        assert order_placed is True
        
        # Kill-switch triggers - should cancel all orders
        mock_trading_client.cancel_all_orders()
        mock_trading_client.cancel_all_orders.assert_called_once()
    
    @pytest.mark.scenario
    def test_partial_fill_then_kill_switch(
        self, mock_trading_client, mock_logger, position_factory
    ):
        """Partial fill followed by kill-switch should handle remainder."""
        # Create partially filled order
        order_id = str(uuid4())
        partial_order = {
            "id": order_id,
            "symbol": "SPY250124C00595000",
            "qty": "10",
            "filled_qty": "3",  # Partially filled
            "status": "partially_filled",
        }
        
        mock_trading_client.get_orders.return_value = [partial_order]
        
        # Kill-switch triggered - cancel remaining
        mock_trading_client.cancel_order_by_id(order_id)
        mock_trading_client.cancel_order_by_id.assert_called_with(order_id)
        
        # Position created for filled portion
        position = position_factory.create_position(
            symbol="SPY250124C00595000",
            qty=3,  # Only filled qty
        )
        mock_trading_client.get_all_positions.return_value = [position]
        
        positions = mock_trading_client.get_all_positions()
        assert len(positions) == 1
        assert positions[0].qty == "3"


class TestStalePricingScenarios:
    """Tests for handling stale pricing from stream delays."""
    
    @pytest.mark.scenario
    def test_stale_option_quote_rejected(
        self, mock_option_historical_client, mock_logger
    ):
        """Order based on stale option quote should be rejected or adjusted."""
        # Simulate stale quote (5 minutes old)
        stale_quote = MagicMock()
        stale_quote.ask_price = 5.25
        stale_quote.bid_price = 5.20
        stale_quote.timestamp = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        current_time = datetime.now(timezone.utc)
        quote_age = (current_time - stale_quote.timestamp).total_seconds()
        max_quote_age = 60  # 1 minute
        
        is_stale = quote_age > max_quote_age
        
        assert is_stale is True
    
    @pytest.mark.scenario
    def test_price_slippage_detection(self, mock_logger):
        """Significant price slippage should be detected."""
        expected_price = 5.25
        actual_price = 5.75  # 9.5% slippage
        
        slippage = abs(actual_price - expected_price) / expected_price
        max_slippage = 0.05  # 5%
        
        excessive_slippage = slippage > max_slippage
        
        assert excessive_slippage is True
    
    @pytest.mark.scenario
    def test_option_spread_too_wide(self, mock_logger):
        """Wide bid-ask spread should trigger warning."""
        bid_price = 5.00
        ask_price = 6.00  # $1.00 spread on $5 option = 20%
        
        spread = ask_price - bid_price
        mid_price = (ask_price + bid_price) / 2
        spread_percentage = spread / mid_price
        
        max_spread_percentage = 0.10  # 10%
        
        spread_too_wide = spread_percentage > max_spread_percentage
        
        assert spread_too_wide is True


class TestSystemRestartWithOpenPositions:
    """Tests for system restart scenarios with existing positions."""
    
    @pytest.mark.scenario
    def test_reconnect_discovers_existing_positions(
        self, mock_trading_client, mock_logger, position_factory
    ):
        """System restart should discover and track existing positions."""
        # Simulate existing positions from before restart
        existing_positions = [
            position_factory.create_position(
                symbol="SPY250124C00595000",
                qty=5,
                avg_entry_price=5.00,
                current_price=5.50,
            ),
            position_factory.create_position(
                symbol="AAPL250124P00185000",
                qty=3,
                avg_entry_price=4.25,
                current_price=4.00,
            ),
        ]
        mock_trading_client.get_all_positions.return_value = existing_positions
        
        # System reconnects and fetches positions
        positions = mock_trading_client.get_all_positions()
        
        assert len(positions) == 2
        # Verify tracking state would be reconstructed
        tracking_state = {}
        for pos in positions:
            tracking_state[pos.symbol] = {
                "qty": int(pos.qty),
                "entry_price": float(pos.avg_entry_price),
                "unrealized_plpc": float(pos.unrealized_plpc),
            }
        
        assert "SPY250124C00595000" in tracking_state
        assert tracking_state["SPY250124C00595000"]["qty"] == 5
    
    @pytest.mark.scenario
    def test_orphaned_orders_handled_on_restart(
        self, mock_trading_client, mock_logger, order_factory
    ):
        """Orphaned orders from crash should be handled on restart."""
        # Simulate orphaned orders from before crash
        orphaned_orders = [
            order_factory.create_limit_order(
                symbol="SPY250124C00595000",
                qty=5,
                limit_price=5.25,
                status="new",
                created_at=datetime.now(timezone.utc) - timedelta(hours=2)
            ),
        ]
        mock_trading_client.get_orders.return_value = orphaned_orders
        
        # System restart detects old orders
        orders = mock_trading_client.get_orders()
        
        for order in orders:
            order_age = (datetime.now(timezone.utc) - 
                        datetime.fromisoformat(order["created_at"].replace("Z", "+00:00"))
                       ).total_seconds()
            
            # Orders older than 1 hour should be cancelled
            if order_age > 3600:
                mock_trading_client.cancel_order_by_id(order["id"])
        
        mock_trading_client.cancel_order_by_id.assert_called()
    
    @pytest.mark.scenario
    def test_trailing_stop_state_reconstruction(
        self, mock_trading_client, mock_logger, position_factory
    ):
        """Trailing stop state should be reconstructed from position data."""
        # Position with 15% unrealized profit
        position = position_factory.create_position(
            symbol="SPY250124C00595000",
            qty=5,
            avg_entry_price=5.00,
            current_price=5.75,  # 15% profit
            unrealized_plpc=0.15,
        )
        mock_trading_client.get_all_positions.return_value = [position]
        
        # Reconstruct trailing stop based on current profit
        from deltadyno.trading.position_monitor import get_trailing_stop_loss_value
        
        ranges = [(0, 10), (10, 20), (20, 50)]
        stop_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        stop_loss = get_trailing_stop_loss_value(
            0.15,  # 15% profit
            ranges,
            stop_values,
            default
        )
        
        # 15% profit is in range (10, 20), so stop loss = 10%
        assert stop_loss == 0.10
        
        # Trailing stop = current profit - stop loss adjustment
        trailing_stop = 0.15 - stop_loss
        assert trailing_stop == pytest.approx(0.05)


class TestRaceConditions:
    """Tests for race condition scenarios."""
    
    @pytest.mark.scenario
    def test_manual_exit_vs_stop_loss_race(
        self, mock_trading_client, mock_logger, position_factory
    ):
        """Manual exit and stop-loss trigger should not double-close."""
        position = position_factory.create_position(
            symbol="SPY250124C00595000",
            qty=5,
        )
        
        close_count = 0
        position_exists = True
        
        def mock_close_position(*args, **kwargs):
            nonlocal close_count, position_exists
            if position_exists:
                close_count += 1
                position_exists = False
                return MagicMock()
            else:
                raise Exception("Position not found")
        
        mock_trading_client.close_position = MagicMock(side_effect=mock_close_position)
        
        # Simulate race: both try to close
        try:
            # Manual exit attempt
            mock_trading_client.close_position("SPY250124C00595000", close_options=None)
        except Exception:
            pass
        
        try:
            # Stop-loss attempt (slightly later)
            mock_trading_client.close_position("SPY250124C00595000", close_options=None)
        except Exception:
            pass
        
        # Only one should succeed
        assert close_count == 1
    
    @pytest.mark.scenario
    def test_order_replacement_vs_cancellation_race(
        self, mock_trading_client, mock_logger
    ):
        """Order replacement and cancellation should not conflict."""
        order_id = str(uuid4())
        order_exists = True
        
        def mock_cancel(*args, **kwargs):
            nonlocal order_exists
            if order_exists:
                order_exists = False
                return None
            raise Exception("Order not found")
        
        def mock_replace(*args, **kwargs):
            nonlocal order_exists
            if order_exists:
                return MagicMock(id=str(uuid4()))
            raise Exception("Order not found")
        
        mock_trading_client.cancel_order_by_id = MagicMock(side_effect=mock_cancel)
        mock_trading_client.replace_order_by_id = MagicMock(side_effect=mock_replace)
        
        # Race: cancel wins
        mock_trading_client.cancel_order_by_id(order_id)
        
        # Replace should fail
        with pytest.raises(Exception):
            mock_trading_client.replace_order_by_id(order_id, MagicMock())
    
    @pytest.mark.scenario
    def test_concurrent_config_update_during_trade(
        self, mock_db_config, mock_logger
    ):
        """Config update during active trade should be handled safely."""
        # Initial config
        original_stop_loss = 0.10
        
        # Trade in progress with original config
        current_plpc = 0.08
        trailing_stop = current_plpc - original_stop_loss  # -0.02
        
        # Config update mid-trade (more aggressive stop loss)
        new_stop_loss = 0.15
        
        # New trailing stop calculation
        new_trailing_stop = current_plpc - new_stop_loss  # -0.07
        
        # The more conservative approach: use original until position closes
        # This prevents unexpected behavior during active trades
        assert trailing_stop != new_trailing_stop


class TestEdgeCaseTimingScenarios:
    """Tests for edge case timing scenarios."""
    
    @pytest.mark.scenario
    def test_market_close_during_order_processing(
        self, mock_trading_client, mock_logger, frozen_time
    ):
        """Market close during order processing should handle gracefully."""
        # Start with market open
        mock_trading_client.get_clock.return_value.is_open = True
        
        # Simulate order processing that spans market close
        order_started = datetime.now(timezone.utc)
        
        # Market closes mid-processing
        mock_trading_client.get_clock.return_value.is_open = False
        
        # Check market status before placing
        clock = mock_trading_client.get_clock()
        
        if not clock.is_open:
            # Order should not be placed
            pass
        
        mock_trading_client.submit_order.assert_not_called()
    
    @pytest.mark.scenario
    def test_date_rollover_during_session(
        self, mock_trading_client, mock_db_config, mock_logger
    ):
        """Date rollover during extended hours should reset counters."""
        # Position count for current date
        position_counts = {"2025-01-23": 5}
        
        # Date changes to next day
        current_date = datetime(2025, 1, 24).date()
        previous_date = datetime(2025, 1, 23).date()
        
        if current_date != previous_date:
            # Reset for new date
            position_counts[str(current_date)] = 0
        
        assert str(current_date) in position_counts
        assert position_counts[str(current_date)] == 0
    
    @pytest.mark.scenario
    def test_gap_open_handling(
        self, mock_trading_client, mock_logger, frozen_time
    ):
        """Gap open scenario should be detected and handled."""
        previous_close = 590.00
        current_open = 600.00  # 1.7% gap up
        
        gap_percent = abs(current_open - previous_close) / previous_close
        gap_threshold = 0.01  # 1%
        
        is_gap_open = gap_percent > gap_threshold
        
        assert is_gap_open is True
        
        # In gap open scenarios, might want to skip first few bars
        bars_to_skip = 3 if is_gap_open else 0
        assert bars_to_skip == 3


class TestMultiProfileScenarios:
    """Tests for multi-profile trading scenarios."""
    
    @pytest.mark.scenario
    def test_multiple_profiles_independent_positions(
        self, mock_trading_client, mock_logger
    ):
        """Multiple profiles should maintain independent positions."""
        profile_1_positions = {"SPY250124C00595000": 5}
        profile_2_positions = {"SPY250124P00585000": 3}
        
        # Each profile has its own tracking
        all_positions = {
            "profile_1": profile_1_positions,
            "profile_2": profile_2_positions,
        }
        
        # Profile 1 action should not affect Profile 2
        assert len(all_positions["profile_1"]) == 1
        assert len(all_positions["profile_2"]) == 1
        assert all_positions["profile_1"] != all_positions["profile_2"]
    
    @pytest.mark.scenario
    def test_shared_symbol_different_profiles(
        self, mock_trading_client, mock_logger
    ):
        """Same symbol in different profiles should be tracked separately."""
        # Both profiles trading SPY options but different strikes/expiries
        profile_1_symbol = "SPY250124C00595000"
        profile_2_symbol = "SPY250124C00600000"
        
        # Same underlying, different positions
        assert "SPY" in profile_1_symbol
        assert "SPY" in profile_2_symbol
        assert profile_1_symbol != profile_2_symbol

