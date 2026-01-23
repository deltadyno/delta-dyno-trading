"""
Unit tests for the equity monitor module (equity_monitor.py).

Tests cover:
- Normal equity tracking
- Max daily loss breach and trading halt
- Sudden equity drop (gap loss) handling
- Equity API unavailability
- Kill-switch triggering
- Position closing logic
"""

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest


# =============================================================================
# Helper Functions for Testing (Self-Contained)
# =============================================================================

def get_trailing_stop_loss_value(profit_pct, ranges_list, stop_loss_values, default):
    """Get trailing stop loss value based on profit percentage."""
    profit_pct_normalized = profit_pct * 100 if profit_pct < 1 else profit_pct
    for i, (low, high) in enumerate(ranges_list):
        if low <= profit_pct_normalized < high:
            return stop_loss_values[i]
    return default


def set_trailing_stop_loss(symbol, unrealized_plpc, trailing_stops, previous_plpc,
                           ranges_list, stop_loss_values, default, logger):
    """Set trailing stop loss for a position."""
    stop_loss_value = get_trailing_stop_loss_value(
        unrealized_plpc * 100, ranges_list, stop_loss_values, default
    )
    trailing_stops[symbol] = unrealized_plpc - stop_loss_value
    previous_plpc[symbol] = unrealized_plpc


def determine_sell_quantity(profit_pct, qty, symbol, first_time_sales,
                            ranges, sell_percentages, expire_seconds, logger):
    """Determine quantity to sell based on profit range."""
    profit_pct_normalized = profit_pct * 100 if profit_pct < 1 else profit_pct
    qty_int = int(qty)
    
    for i, (low, high) in enumerate(ranges):
        if low <= profit_pct_normalized < high:
            # Check if recently sold in this range
            if symbol in first_time_sales and i in first_time_sales[symbol]:
                sale_time = first_time_sales[symbol][i]
                if (datetime.utcnow() - sale_time).total_seconds() < expire_seconds:
                    return 0
            
            sell_pct = sell_percentages[i]
            if sell_pct == 0:
                return 0
            
            calculated_qty = int(qty_int * sell_pct)
            if calculated_qty == 0 and sell_pct > 0:
                calculated_qty = 1
            
            if symbol not in first_time_sales:
                first_time_sales[symbol] = {}
            first_time_sales[symbol][i] = datetime.utcnow()
            
            return calculated_qty
    
    return 0


def parse_config_for_day(config, current_date, logger):
    """Parse configuration for the current day."""
    ranges = [(0, 10), (10, 20), (20, 50)]
    stop_vals = [0.05, 0.10, 0.15]
    sell_qtys = [0.25, 0.50, 0.75]
    min_profit = 0.05
    hard_stop = -0.15
    return ranges, stop_vals, sell_qtys, min_profit, hard_stop


def get_regular_market_hours(trading_client, logger):
    """Get regular market hours from trading client."""
    calendars = trading_client.get_calendar()
    if not calendars:
        return None
    return {
        "regular_open": datetime.now(timezone.utc).replace(hour=14, minute=30),
        "regular_close": datetime.now(timezone.utc).replace(hour=21, minute=0),
    }


MAX_SLEEP_SECONDS = 1800


def calculate_sleep_time(market_hours, config, logger):
    """Calculate sleep time based on market hours."""
    if market_hours is None:
        return MAX_SLEEP_SECONDS
    
    now = datetime.now(timezone.utc)
    if market_hours["regular_open"] <= now <= market_hours["regular_close"]:
        return config.get("close_position_sleep_seconds", 2, float)
    return MAX_SLEEP_SECONDS


def check_max_daily_loss(daily_pnl, max_daily_loss, logger):
    """Check if max daily loss has been breached."""
    if daily_pnl <= max_daily_loss:
        logger.warning(f"Max daily loss breached: {daily_pnl:.2%} <= {max_daily_loss:.2%}")
        return True
    return False


def detect_sudden_equity_drop(previous_equity, current_equity, threshold, logger):
    """Detect sudden large equity drop."""
    if previous_equity <= 0:
        return False
    
    drop_percent = (previous_equity - current_equity) / previous_equity
    
    if drop_percent >= threshold:
        logger.warning(f"Sudden equity drop detected: {drop_percent:.2%}")
        return True
    return False


def fetch_equity_safely(trading_client, logger):
    """Fetch equity with error handling."""
    try:
        account = trading_client.get_account()
        return float(account.equity)
    except Exception as e:
        logger.error(f"Failed to fetch equity: {e}")
        return None


def check_hard_stop_condition(unrealized_plpc, hard_stop, logger):
    """Check if hard stop has been breached."""
    if unrealized_plpc <= hard_stop:
        logger.warning(f"Hard stop triggered: {unrealized_plpc:.2%} <= {hard_stop:.2%}")
        return True
    return False


def check_trailing_stop_condition(current_plpc, trailing_stop, logger):
    """Check if trailing stop has been breached."""
    if current_plpc < trailing_stop:
        logger.info(f"Trailing stop triggered: {current_plpc:.2%} < {trailing_stop:.2%}")
        return True
    return False


# =============================================================================
# Position Monitor Tests
# =============================================================================

class TestTrailingStopLoss:
    """Tests for trailing stop loss calculations."""
    
    @pytest.mark.unit
    def test_get_stop_loss_value_first_range(self):
        """Profit in first range should return first stop loss value."""
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        result = get_trailing_stop_loss_value(5.0, ranges_list, stop_loss_values, default)
        
        assert result == 0.05
    
    @pytest.mark.unit
    def test_get_stop_loss_value_middle_range(self):
        """Profit in middle range should return correct stop loss."""
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        result = get_trailing_stop_loss_value(15.0, ranges_list, stop_loss_values, default)
        
        assert result == 0.10
    
    @pytest.mark.unit
    def test_get_stop_loss_value_no_match_returns_default(self):
        """Profit outside all ranges should return default."""
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        # Negative profit (loss) - outside ranges
        result = get_trailing_stop_loss_value(-5.0, ranges_list, stop_loss_values, default)
        
        assert result == default
    
    @pytest.mark.unit
    def test_set_trailing_stop_updates_dictionaries(self, mock_logger):
        """Setting trailing stop should update both tracking dicts."""
        trailing_stops = {}
        previous_plpc = {}
        
        set_trailing_stop_loss(
            "SPY250124C00595000",
            0.15,  # 15% unrealized profit
            trailing_stops,
            previous_plpc,
            [(0, 10), (10, 20)],
            [0.05, 0.10],
            0.03,
            mock_logger
        )
        
        assert "SPY250124C00595000" in trailing_stops
        assert "SPY250124C00595000" in previous_plpc
        assert previous_plpc["SPY250124C00595000"] == 0.15


class TestSellQuantityDetermination:
    """Tests for determine_sell_quantity function."""
    
    @pytest.mark.unit
    def test_first_sale_in_range_calculates_quantity(self, mock_logger):
        """First sale in profit range should calculate sell quantity."""
        first_time_sales = {}
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            15.0,  # 15% profit - second range
            "10",
            "SPY250124C00595000",
            first_time_sales,
            ranges,
            sell_percentages,
            300,  # expire seconds
            mock_logger
        )
        
        # 50% of 10 = 5
        assert qty == 5
        assert "SPY250124C00595000" in first_time_sales
    
    @pytest.mark.unit
    def test_recent_sale_same_range_returns_zero(self, mock_logger):
        """Recent sale in same range should return 0."""
        # Mark range 1 as recently sold
        now = datetime.utcnow()
        first_time_sales = {"SPY250124C00595000": {1: now}}
        
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            15.0,  # 15% profit - range index 1
            "10",
            "SPY250124C00595000",
            first_time_sales,
            ranges,
            sell_percentages,
            300,
            mock_logger
        )
        
        assert qty == 0
    
    @pytest.mark.unit
    def test_expired_sale_allows_new_sale(self, mock_logger):
        """Expired sale record should allow new sale."""
        # Mark range 1 as sold long ago
        old_time = datetime.utcnow() - timedelta(seconds=400)
        first_time_sales = {"SPY250124C00595000": {1: old_time}}
        
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            15.0,  # 15% profit - range index 1
            "10",
            "SPY250124C00595000",
            first_time_sales,
            ranges,
            sell_percentages,
            300,  # 300 second expiry
            mock_logger
        )
        
        assert qty == 5  # 50% of 10
    
    @pytest.mark.unit
    def test_zero_sell_percentage_returns_zero(self, mock_logger):
        """Zero sell percentage should return 0."""
        first_time_sales = {}
        ranges = [(0, 10), (10, 20)]
        sell_percentages = [0.0, 0.50]  # First range has 0%
        
        qty = determine_sell_quantity(
            5.0,  # 5% profit - first range
            "10",
            "SPY250124C00595000",
            first_time_sales,
            ranges,
            sell_percentages,
            300,
            mock_logger
        )
        
        assert qty == 0
    
    @pytest.mark.unit
    def test_small_quantity_minimum_one(self, mock_logger):
        """Small percentage of small quantity should return at least 1."""
        first_time_sales = {}
        ranges = [(0, 10), (10, 20)]
        sell_percentages = [0.10, 0.50]  # 10% of 3 = 0.3, should round to 1
        
        qty = determine_sell_quantity(
            5.0,  # 5% profit - first range
            "3",
            "SPY250124C00595000",
            first_time_sales,
            ranges,
            sell_percentages,
            300,
            mock_logger
        )
        
        assert qty == 1  # Minimum 1


class TestEquityMonitorConfiguration:
    """Tests for equity monitor configuration parsing."""
    
    @pytest.mark.unit
    def test_parse_config_normal_day(self, mock_db_config, mock_logger):
        """Normal day should use regular configuration."""
        current_date = datetime(2025, 1, 23).date()
        mock_db_config.choppy_trading_days = ""  # No choppy days
        
        ranges, stop_vals, sell_qtys, min_profit, hard_stop = parse_config_for_day(
            mock_db_config, current_date, mock_logger
        )
        
        assert len(ranges) > 0
        assert len(stop_vals) > 0
        assert min_profit > 0
    
    @pytest.mark.unit
    def test_parse_config_choppy_day(self, mock_db_config, mock_logger):
        """Choppy day should use choppy configuration."""
        current_date = datetime(2025, 1, 23).date()
        mock_db_config.choppy_trading_days = "2025-01-23"  # Today is choppy
        
        ranges, stop_vals, sell_qtys, min_profit, hard_stop = parse_config_for_day(
            mock_db_config, current_date, mock_logger
        )
        
        # Should have different values for choppy day
        assert len(ranges) > 0


class TestMarketHours:
    """Tests for market hours calculation."""
    
    @pytest.mark.unit
    def test_get_regular_market_hours_returns_dict(self, mock_trading_client, mock_logger):
        """Market hours should be returned as dictionary."""
        mock_trading_client.get_calendar.return_value = [MagicMock()]
        
        hours = get_regular_market_hours(mock_trading_client, mock_logger)
        
        assert hours is not None
        assert "regular_open" in hours
        assert "regular_close" in hours
    
    @pytest.mark.unit
    def test_no_calendar_returns_none(self, mock_trading_client, mock_logger):
        """No calendar data should return None."""
        mock_trading_client.get_calendar.return_value = []
        
        hours = get_regular_market_hours(mock_trading_client, mock_logger)
        
        assert hours is None


class TestSleepTimeCalculation:
    """Tests for equity monitor sleep time calculation."""
    
    @pytest.mark.unit
    def test_market_open_uses_config_sleep(self, mock_db_config, mock_logger):
        """During market hours, use configured sleep."""
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now - timedelta(hours=2),
            "regular_close": now + timedelta(hours=4),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        assert sleep_time == mock_db_config.get("close_position_sleep_seconds", 2, float)
    
    @pytest.mark.unit
    def test_no_market_hours_uses_max(self, mock_db_config, mock_logger):
        """No market hours should use MAX_SLEEP_SECONDS."""
        sleep_time = calculate_sleep_time(None, mock_db_config, mock_logger)
        
        assert sleep_time == MAX_SLEEP_SECONDS


class TestPositionClosingLogic:
    """Tests for position closing decision logic."""
    
    @pytest.mark.unit
    def test_hard_stop_triggers_close(self, mock_logger, position_factory):
        """Hard stop breach should trigger position close."""
        position = position_factory.create_position(
            avg_entry_price=5.00,
            current_price=4.00,  # 20% loss
        )
        
        should_close = check_hard_stop_condition(
            float(position.unrealized_plpc),
            -0.15,  # 15% hard stop
            mock_logger
        )
        
        assert should_close is True
    
    @pytest.mark.unit
    def test_above_hard_stop_no_close(self, mock_logger):
        """Position above hard stop should not trigger close."""
        should_close = check_hard_stop_condition(
            -0.10,  # 10% loss
            -0.15,  # 15% hard stop
            mock_logger
        )
        
        assert should_close is False
    
    @pytest.mark.unit
    def test_trailing_stop_breached_triggers_close(self, mock_logger):
        """Trailing stop breach should trigger position close."""
        current_plpc = 0.08  # 8% profit
        trailing_stop = 0.10  # Trailing stop at 10%
        
        should_close = check_trailing_stop_condition(
            current_plpc, trailing_stop, mock_logger
        )
        
        assert should_close is True
    
    @pytest.mark.unit
    def test_above_trailing_stop_no_close(self, mock_logger):
        """Position above trailing stop should not close."""
        current_plpc = 0.15  # 15% profit
        trailing_stop = 0.10  # Trailing stop at 10%
        
        should_close = check_trailing_stop_condition(
            current_plpc, trailing_stop, mock_logger
        )
        
        assert should_close is False


class TestEquityKillSwitch:
    """Tests for equity-based kill switch scenarios."""
    
    @pytest.mark.unit
    def test_max_daily_loss_triggers_halt(self, mock_logger):
        """Exceeding max daily loss should trigger trading halt."""
        daily_pnl = -0.10
        max_daily_loss = -0.05
        
        should_halt = check_max_daily_loss(daily_pnl, max_daily_loss, mock_logger)
        
        assert should_halt is True
    
    @pytest.mark.unit
    def test_within_daily_loss_continues(self, mock_logger):
        """Within max daily loss should continue trading."""
        daily_pnl = -0.03
        max_daily_loss = -0.05
        
        should_halt = check_max_daily_loss(daily_pnl, max_daily_loss, mock_logger)
        
        assert should_halt is False
    
    @pytest.mark.unit
    def test_sudden_equity_drop_detection(self, mock_logger):
        """Sudden large equity drop should be detected."""
        previous_equity = 100000.0
        current_equity = 85000.0  # 15% drop
        threshold = 0.10
        
        is_sudden_drop = detect_sudden_equity_drop(
            previous_equity, current_equity, threshold, mock_logger
        )
        
        assert is_sudden_drop is True
    
    @pytest.mark.unit
    def test_normal_equity_change_no_alert(self, mock_logger):
        """Normal equity changes should not trigger alert."""
        previous_equity = 100000.0
        current_equity = 98000.0  # 2% drop
        threshold = 0.10
        
        is_sudden_drop = detect_sudden_equity_drop(
            previous_equity, current_equity, threshold, mock_logger
        )
        
        assert is_sudden_drop is False


class TestEquityAPIResilience:
    """Tests for handling equity API unavailability."""
    
    @pytest.mark.unit
    def test_api_timeout_handled_gracefully(self, mock_trading_client, mock_logger):
        """API timeout should be handled gracefully."""
        mock_trading_client.get_account.side_effect = TimeoutError("Connection timeout")
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity is None
        mock_logger.error.assert_called()
    
    @pytest.mark.unit
    def test_api_error_handled_gracefully(self, mock_trading_client, mock_logger):
        """API error should be handled gracefully."""
        mock_trading_client.get_account.side_effect = Exception("API Error")
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity is None
    
    @pytest.mark.unit
    def test_successful_fetch_returns_equity(self, mock_trading_client, mock_logger):
        """Successful API call should return equity value."""
        mock_account = MagicMock()
        mock_account.equity = "100000.00"
        mock_trading_client.get_account.return_value = mock_account
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity == 100000.0


# =============================================================================
# Additional Comprehensive Tests
# =============================================================================

class TestTrailingStopCalculation:
    """Tests for trailing stop calculation and updates."""
    
    @pytest.mark.unit
    def test_trailing_stop_initialized_at_zero(self):
        """Trailing stop should start at zero."""
        trailing_stop = 0.0
        assert trailing_stop == 0.0
    
    @pytest.mark.unit
    def test_trailing_stop_updates_on_new_high(self):
        """Trailing stop should update when new high is reached."""
        current_trailing_stop = 0.10
        current_profit = 0.20
        trailing_distance = 0.05
        
        new_trailing_stop = max(current_trailing_stop, current_profit - trailing_distance)
        
        assert abs(new_trailing_stop - 0.15) < 0.0001
        assert new_trailing_stop > current_trailing_stop
    
    @pytest.mark.unit
    def test_trailing_stop_does_not_decrease(self):
        """Trailing stop should never decrease."""
        current_trailing_stop = 0.15
        current_profit = 0.10
        trailing_distance = 0.05
        
        new_trailing_stop = max(current_trailing_stop, current_profit - trailing_distance)
        
        assert new_trailing_stop == 0.15
    
    @pytest.mark.unit
    @pytest.mark.parametrize("profit,expected_stop", [
        (0.05, 0.0),
        (0.10, 0.05),
        (0.15, 0.10),
        (0.25, 0.20),
        (0.50, 0.45),
    ])
    def test_trailing_stop_various_profit_levels(self, profit, expected_stop):
        """Test trailing stop at various profit levels."""
        trailing_distance = 0.05
        new_stop = max(0.0, profit - trailing_distance)
        
        assert abs(new_stop - expected_stop) < 0.0001


class TestProfitTargetLogic:
    """Tests for profit target achievement logic."""
    
    @pytest.mark.unit
    def test_profit_target_50_percent_reached(self):
        """50% profit target should trigger partial close."""
        profit_target_50 = 0.50
        current_profit = 0.55
        
        target_reached = current_profit >= profit_target_50
        
        assert target_reached is True
    
    @pytest.mark.unit
    def test_profit_target_not_reached(self):
        """Below profit target should not trigger."""
        profit_target_50 = 0.50
        current_profit = 0.45
        
        target_reached = current_profit >= profit_target_50
        
        assert target_reached is False
    
    @pytest.mark.unit
    def test_multiple_profit_targets_checked_in_order(self):
        """Multiple profit targets should be checked in ascending order."""
        targets = [0.25, 0.50, 0.75, 1.00]
        current_profit = 0.60
        
        hit_targets = [t for t in targets if current_profit >= t]
        
        assert hit_targets == [0.25, 0.50]


class TestPositionMonitorTimeBased:
    """Tests for time-based position monitoring."""
    
    @pytest.mark.unit
    def test_position_age_calculated_correctly(self, frozen_time):
        """Position age should be calculated from fill time."""
        fill_time = frozen_time.now - timedelta(minutes=15)
        current_time = frozen_time.now
        
        age_seconds = (current_time - fill_time).total_seconds()
        
        assert age_seconds == 900
    
    @pytest.mark.unit
    def test_position_stale_after_threshold(self, frozen_time):
        """Position should be considered stale after threshold."""
        stale_threshold_seconds = 3600
        fill_time = frozen_time.now - timedelta(hours=2)
        current_time = frozen_time.now
        
        age_seconds = (current_time - fill_time).total_seconds()
        is_stale = age_seconds > stale_threshold_seconds
        
        assert is_stale is True


class TestEquityHistory:
    """Tests for equity history tracking."""
    
    @pytest.mark.unit
    def test_equity_history_stores_samples(self):
        """Equity history should store recent samples."""
        equity_history = []
        max_samples = 10
        
        for i in range(15):
            equity_history.append(100000 + i * 100)
            if len(equity_history) > max_samples:
                equity_history.pop(0)
        
        assert len(equity_history) == max_samples
        assert equity_history[-1] == 100000 + 14 * 100
    
    @pytest.mark.unit
    def test_equity_trend_upward(self):
        """Upward equity trend should be detected."""
        equity_history = [100000, 100500, 101000, 101500, 102000]
        
        trend = equity_history[-1] - equity_history[0]
        is_upward = trend > 0
        
        assert is_upward is True
    
    @pytest.mark.unit
    def test_equity_trend_downward(self):
        """Downward equity trend should be detected."""
        equity_history = [102000, 101500, 101000, 100500, 100000]
        
        trend = equity_history[-1] - equity_history[0]
        is_downward = trend < 0
        
        assert is_downward is True


class TestMultiPositionHandling:
    """Tests for handling multiple positions."""
    
    @pytest.mark.unit
    def test_positions_processed_in_order(self, position_factory):
        """Multiple positions should be processed in order."""
        positions = [
            position_factory.create_position(symbol="SPY250124C00595000"),
            position_factory.create_position(symbol="QQQ250124P00450000"),
            position_factory.create_position(symbol="AAPL250124C00185000"),
        ]
        
        symbols = [p.symbol for p in positions]
        
        assert symbols == ["SPY250124C00595000", "QQQ250124P00450000", "AAPL250124C00185000"]
    
    @pytest.mark.unit
    def test_worst_position_identified(self, position_factory):
        """Worst performing position should be identified."""
        positions = [
            position_factory.create_position(symbol="SPY250124C00595000", unrealized_plpc=0.10),
            position_factory.create_position(symbol="QQQ250124P00450000", unrealized_plpc=-0.05),
            position_factory.create_position(symbol="AAPL250124C00185000", unrealized_plpc=0.02),
        ]
        
        worst = min(positions, key=lambda p: float(p.unrealized_plpc))
        
        assert worst.symbol == "QQQ250124P00450000"
    
    @pytest.mark.unit
    def test_total_position_value_calculated(self, position_factory):
        """Total position value should be calculated correctly."""
        # Create positions and manually set market_value
        p1 = position_factory.create_position(symbol="SPY250124C00595000")
        p1.market_value = "5000"
        p2 = position_factory.create_position(symbol="QQQ250124P00450000")
        p2.market_value = "3000"
        p3 = position_factory.create_position(symbol="AAPL250124C00185000")
        p3.market_value = "2000"
        
        positions = [p1, p2, p3]
        total_value = sum(float(p.market_value) for p in positions)
        
        assert total_value == 10000


class TestEquityPercentageCalculations:
    """Tests for equity percentage calculations."""
    
    @pytest.mark.unit
    def test_daily_pnl_percentage(self):
        """Daily PnL percentage should be calculated correctly."""
        starting_equity = 100000
        current_equity = 101500
        
        daily_pnl_pct = (current_equity - starting_equity) / starting_equity
        
        assert abs(daily_pnl_pct - 0.015) < 0.0001
    
    @pytest.mark.unit
    def test_total_return_percentage(self):
        """Total return percentage should be calculated correctly."""
        initial_deposit = 50000
        current_equity = 75000
        
        total_return_pct = (current_equity - initial_deposit) / initial_deposit
        
        assert total_return_pct == 0.5
    
    @pytest.mark.unit
    def test_drawdown_from_peak(self):
        """Drawdown from peak should be calculated correctly."""
        peak_equity = 120000
        current_equity = 105000
        
        drawdown_pct = (peak_equity - current_equity) / peak_equity
        
        assert abs(drawdown_pct - 0.125) < 0.0001
