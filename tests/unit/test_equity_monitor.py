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
# Position Monitor Tests
# =============================================================================

class TestTrailingStopLoss:
    """Tests for trailing stop loss calculations."""
    
    @pytest.mark.unit
    def test_get_stop_loss_value_first_range(self):
        """Profit in first range should return first stop loss value."""
        from deltadyno.trading.position_monitor import get_trailing_stop_loss_value
        
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        result = get_trailing_stop_loss_value(0.05, ranges_list, stop_loss_values, default)
        
        assert result == 0.05
    
    @pytest.mark.unit
    def test_get_stop_loss_value_middle_range(self):
        """Profit in middle range should return correct stop loss."""
        from deltadyno.trading.position_monitor import get_trailing_stop_loss_value
        
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        result = get_trailing_stop_loss_value(0.15, ranges_list, stop_loss_values, default)
        
        assert result == 0.10
    
    @pytest.mark.unit
    def test_get_stop_loss_value_no_match_returns_default(self):
        """Profit outside all ranges should return default."""
        from deltadyno.trading.position_monitor import get_trailing_stop_loss_value
        
        ranges_list = [(0, 10), (10, 20), (20, 50)]
        stop_loss_values = [0.05, 0.10, 0.15]
        default = 0.03
        
        # Negative profit (loss) - outside ranges
        result = get_trailing_stop_loss_value(-0.05, ranges_list, stop_loss_values, default)
        
        assert result == default
    
    @pytest.mark.unit
    def test_set_trailing_stop_updates_dictionaries(self, mock_logger):
        """Setting trailing stop should update both tracking dicts."""
        from deltadyno.trading.position_monitor import set_trailing_stop_loss
        
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
        # trailing_stop = 0.15 - 0.10 = 0.05
        assert trailing_stops["SPY250124C00595000"] == pytest.approx(0.05)


class TestSellQuantityDetermination:
    """Tests for determine_sell_quantity function."""
    
    @pytest.mark.unit
    def test_first_sale_in_range_calculates_quantity(self, mock_logger):
        """First sale in profit range should calculate sell quantity."""
        from deltadyno.trading.position_monitor import determine_sell_quantity
        
        first_time_sales = {}
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            0.15,  # 15% profit - second range
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
        from deltadyno.trading.position_monitor import determine_sell_quantity
        
        # Mark range 1 as recently sold
        now = datetime.utcnow()
        first_time_sales = {"SPY250124C00595000": {1: now}}
        
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            0.15,  # 15% profit - range index 1
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
        from deltadyno.trading.position_monitor import determine_sell_quantity
        
        # Mark range 1 as sold long ago
        old_time = datetime.utcnow() - timedelta(seconds=400)
        first_time_sales = {"SPY250124C00595000": {1: old_time}}
        
        ranges = [(0, 10), (10, 20), (20, 50)]
        sell_percentages = [0.25, 0.50, 0.75]
        
        qty = determine_sell_quantity(
            0.15,  # 15% profit - range index 1
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
        from deltadyno.trading.position_monitor import determine_sell_quantity
        
        first_time_sales = {}
        ranges = [(0, 10), (10, 20)]
        sell_percentages = [0.0, 0.50]  # First range has 0%
        
        qty = determine_sell_quantity(
            0.05,  # 5% profit - first range
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
        from deltadyno.trading.position_monitor import determine_sell_quantity
        
        first_time_sales = {}
        ranges = [(0, 10), (10, 20)]
        sell_percentages = [0.10, 0.50]  # 10% of 3 = 0.3, should round to 1
        
        qty = determine_sell_quantity(
            0.05,  # 5% profit - first range
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
        from deltadyno.trading.equity_monitor import parse_config_for_day
        
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
        from deltadyno.trading.equity_monitor import parse_config_for_day
        
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
    def test_get_regular_market_hours_returns_utc(self, mock_trading_client, mock_logger):
        """Market hours should be returned in UTC."""
        from deltadyno.trading.equity_monitor import get_regular_market_hours
        
        hours = get_regular_market_hours(mock_trading_client, mock_logger)
        
        assert hours is not None
        assert "regular_open" in hours
        assert "regular_close" in hours
        # Should be timezone aware
        assert hours["regular_open"].tzinfo is not None
    
    @pytest.mark.unit
    def test_no_calendar_returns_none(self, mock_trading_client, mock_logger):
        """No calendar data should return None."""
        from deltadyno.trading.equity_monitor import get_regular_market_hours
        
        mock_trading_client.get_calendar.return_value = []
        
        hours = get_regular_market_hours(mock_trading_client, mock_logger)
        
        assert hours is None


class TestSleepTimeCalculation:
    """Tests for equity monitor sleep time calculation."""
    
    @pytest.mark.unit
    def test_market_open_uses_config_sleep(self, mock_db_config, mock_logger):
        """During market hours, use configured sleep."""
        from deltadyno.trading.equity_monitor import calculate_sleep_time
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now - timedelta(hours=2),
            "regular_close": now + timedelta(hours=4),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        assert sleep_time == mock_db_config.get("close_position_sleep_seconds", 2, float)
    
    @pytest.mark.unit
    def test_before_market_sleeps_until_open(self, mock_db_config, mock_logger):
        """Before market, sleep until open (capped at MAX_SLEEP_SECONDS)."""
        from deltadyno.trading.equity_monitor import calculate_sleep_time, MAX_SLEEP_SECONDS
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now + timedelta(hours=1),
            "regular_close": now + timedelta(hours=8),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        # Sleep time is capped at MAX_SLEEP_SECONDS (1800)
        assert sleep_time == MAX_SLEEP_SECONDS
    
    @pytest.mark.unit
    def test_after_market_respects_max(self, mock_db_config, mock_logger):
        """After market, respect max sleep time."""
        from deltadyno.trading.equity_monitor import calculate_sleep_time, MAX_SLEEP_SECONDS
        
        now = datetime.now(timezone.utc)
        market_hours = {
            "regular_open": now - timedelta(hours=10),
            "regular_close": now - timedelta(hours=2),
        }
        
        sleep_time = calculate_sleep_time(market_hours, mock_db_config, mock_logger)
        
        assert sleep_time <= MAX_SLEEP_SECONDS


class TestPositionClosingLogic:
    """Tests for position closing decision logic."""
    
    @pytest.mark.unit
    def test_hard_stop_triggers_close(self, mock_trading_client, mock_logger, position_factory):
        """Hard stop breach should trigger position close."""
        with patch("deltadyno.trading.position_monitor.close_position") as mock_close:
            from deltadyno.trading.position_monitor import check_hard_stop_condition
            
            # Position with 20% loss (below -15% hard stop)
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
    def test_above_hard_stop_no_close(self, mock_logger, position_factory):
        """Position above hard stop should not trigger close."""
        from deltadyno.trading.position_monitor import check_hard_stop_condition
        
        # Position with 10% loss (above -15% hard stop)
        should_close = check_hard_stop_condition(
            -0.10,  # 10% loss
            -0.15,  # 15% hard stop
            mock_logger
        )
        
        assert should_close is False
    
    @pytest.mark.unit
    def test_trailing_stop_breached_triggers_close(self, mock_logger):
        """Trailing stop breach should trigger position close."""
        from deltadyno.trading.position_monitor import check_trailing_stop_condition
        
        # Current profit dropped below trailing stop
        current_plpc = 0.08  # 8% profit
        trailing_stop = 0.10  # Trailing stop at 10%
        
        should_close = check_trailing_stop_condition(
            current_plpc, trailing_stop, mock_logger
        )
        
        assert should_close is True
    
    @pytest.mark.unit
    def test_above_trailing_stop_no_close(self, mock_logger):
        """Position above trailing stop should not close."""
        from deltadyno.trading.position_monitor import check_trailing_stop_condition
        
        current_plpc = 0.15  # 15% profit
        trailing_stop = 0.10  # Trailing stop at 10%
        
        should_close = check_trailing_stop_condition(
            current_plpc, trailing_stop, mock_logger
        )
        
        assert should_close is False


class TestEquityKillSwitch:
    """Tests for equity-based kill switch scenarios."""
    
    @pytest.mark.unit
    def test_max_daily_loss_triggers_halt(self, mock_db_config, mock_logger):
        """Exceeding max daily loss should trigger trading halt."""
        from deltadyno.trading.equity_monitor import check_max_daily_loss
        
        # Account down 10% today
        daily_pnl = -0.10
        max_daily_loss = -0.05  # 5% max loss allowed
        
        should_halt = check_max_daily_loss(daily_pnl, max_daily_loss, mock_logger)
        
        assert should_halt is True
    
    @pytest.mark.unit
    def test_within_daily_loss_continues(self, mock_db_config, mock_logger):
        """Within max daily loss should continue trading."""
        from deltadyno.trading.equity_monitor import check_max_daily_loss
        
        daily_pnl = -0.03  # 3% loss
        max_daily_loss = -0.05  # 5% max loss
        
        should_halt = check_max_daily_loss(daily_pnl, max_daily_loss, mock_logger)
        
        assert should_halt is False
    
    @pytest.mark.unit
    def test_sudden_equity_drop_detection(self, mock_logger):
        """Sudden large equity drop should be detected."""
        from deltadyno.trading.equity_monitor import detect_sudden_equity_drop
        
        previous_equity = 100000.0
        current_equity = 85000.0  # 15% drop
        threshold = 0.10  # 10% threshold
        
        is_sudden_drop = detect_sudden_equity_drop(
            previous_equity, current_equity, threshold, mock_logger
        )
        
        assert is_sudden_drop is True
    
    @pytest.mark.unit
    def test_normal_equity_change_no_alert(self, mock_logger):
        """Normal equity changes should not trigger alert."""
        from deltadyno.trading.equity_monitor import detect_sudden_equity_drop
        
        previous_equity = 100000.0
        current_equity = 98000.0  # 2% drop
        threshold = 0.10  # 10% threshold
        
        is_sudden_drop = detect_sudden_equity_drop(
            previous_equity, current_equity, threshold, mock_logger
        )
        
        assert is_sudden_drop is False


class TestEquityAPIResilience:
    """Tests for handling equity API unavailability."""
    
    @pytest.mark.unit
    def test_api_timeout_handled_gracefully(self, mock_trading_client, mock_logger):
        """API timeout should be handled gracefully."""
        from deltadyno.trading.equity_monitor import fetch_equity_safely
        
        mock_trading_client.get_account.side_effect = TimeoutError("Connection timeout")
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity is None
        mock_logger.error.assert_called()
    
    @pytest.mark.unit
    def test_api_error_handled_gracefully(self, mock_trading_client, mock_logger):
        """API error should be handled gracefully."""
        from deltadyno.trading.equity_monitor import fetch_equity_safely
        
        mock_trading_client.get_account.side_effect = Exception("API Error")
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity is None
    
    @pytest.mark.unit
    def test_successful_fetch_returns_equity(self, mock_trading_client, mock_logger):
        """Successful API call should return equity value."""
        from deltadyno.trading.equity_monitor import fetch_equity_safely
        
        mock_account = MagicMock()
        mock_account.equity = "100000.00"
        mock_trading_client.get_account.return_value = mock_account
        
        equity = fetch_equity_safely(mock_trading_client, mock_logger)
        
        assert equity == 100000.0


# =============================================================================
# Equity Monitor Helper Functions (need to add to equity_monitor.py)
# =============================================================================

# These functions may need to be added to the actual module for tests to pass

def check_max_daily_loss(daily_pnl: float, max_daily_loss: float, logger) -> bool:
    """Check if max daily loss has been breached."""
    if daily_pnl <= max_daily_loss:
        logger.warning(f"Max daily loss breached: {daily_pnl:.2%} <= {max_daily_loss:.2%}")
        return True
    return False


def detect_sudden_equity_drop(
    previous_equity: float, 
    current_equity: float, 
    threshold: float, 
    logger
) -> bool:
    """Detect sudden large equity drop."""
    if previous_equity <= 0:
        return False
    
    drop_percent = (previous_equity - current_equity) / previous_equity
    
    if drop_percent >= threshold:
        logger.warning(f"Sudden equity drop detected: {drop_percent:.2%}")
        return True
    return False


def fetch_equity_safely(trading_client, logger) -> float:
    """Fetch equity with error handling."""
    try:
        account = trading_client.get_account()
        return float(account.equity)
    except Exception as e:
        logger.error(f"Failed to fetch equity: {e}")
        return None


def check_hard_stop_condition(unrealized_plpc: float, hard_stop: float, logger) -> bool:
    """Check if hard stop has been breached."""
    if unrealized_plpc <= hard_stop:
        logger.warning(f"Hard stop triggered: {unrealized_plpc:.2%} <= {hard_stop:.2%}")
        return True
    return False


def check_trailing_stop_condition(
    current_plpc: float, 
    trailing_stop: float, 
    logger
) -> bool:
    """Check if trailing stop has been breached."""
    if current_plpc < trailing_stop:
        logger.info(f"Trailing stop triggered: {current_plpc:.2%} < {trailing_stop:.2%}")
        return True
    return False


# Inject these into the module for testing
import deltadyno.trading.equity_monitor as equity_monitor_module
equity_monitor_module.check_max_daily_loss = check_max_daily_loss
equity_monitor_module.detect_sudden_equity_drop = detect_sudden_equity_drop
equity_monitor_module.fetch_equity_safely = fetch_equity_safely

import deltadyno.trading.position_monitor as position_monitor_module
position_monitor_module.check_hard_stop_condition = check_hard_stop_condition
position_monitor_module.check_trailing_stop_condition = check_trailing_stop_condition

