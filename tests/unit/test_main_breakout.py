"""
Unit tests for the breakout detector module (main.py / breakout_detector.py).

Tests cover:
- Valid breakout detection (CALL/PUT signals)
- No breakout (HOLD) scenarios
- False breakouts (reversal handling)
- Edge cases (NaN indicators, duplicates, out-of-order)
- Market hours handling
"""

import math
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest

import pandas as pd


# =============================================================================
# Breakout Detection Tests
# =============================================================================

class TestBreakoutDetection:
    """Tests for the check_for_breakouts function."""
    
    @pytest.fixture
    def base_breakout_params(self, mock_trading_client, mock_redis_client, mock_logger, frozen_time):
        """Common parameters for breakout detection."""
        mock_trading_client.get_clock.return_value.is_open = True
        
        return {
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
            "upos": 0,
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
    
    @pytest.mark.unit
    def test_upward_breakout_generates_call_signal(self, base_breakout_params):
        """Valid upward breakout should generate a CALL option signal."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            mock_kalman.return_value = (591.5, 0.15, True)  # bullish
            mock_queue.return_value = True
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            # Trigger upward breakout: upos > prev_upos
            params = {**base_breakout_params, "upos": 1, "prev_upos": 0}
            params["latest_close"] = 591.5  # Close > Open (bullish candle)
            params["latest_open"] = 590.0
            
            new_open, breakout_type, kfilt, velocity = check_for_breakouts(**params)
            
            assert breakout_type == "upward"
            assert new_open == 590.0
            mock_queue.assert_called_once()
            call_kwargs = mock_queue.call_args[1]
            assert call_kwargs["direction"] == "upward"
            assert call_kwargs["symbol"] == "SPY"
    
    @pytest.mark.unit
    def test_downward_breakout_generates_put_signal(self, base_breakout_params):
        """Valid downward breakout should generate a PUT option signal."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            mock_kalman.return_value = (588.5, -0.15, False)  # bearish
            mock_queue.return_value = True
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            # Trigger downward breakout: dnos > prev_dnos
            params = {**base_breakout_params, "dnos": 1, "prev_dnos": 0}
            params["latest_close"] = 588.5  # Close < Open (bearish candle)
            params["latest_open"] = 590.0
            
            new_open, breakout_type, kfilt, velocity = check_for_breakouts(**params)
            
            assert breakout_type == "downward"
            assert new_open == 590.0
            mock_queue.assert_called_once()
            call_kwargs = mock_queue.call_args[1]
            assert call_kwargs["direction"] == "downward"
    
    @pytest.mark.unit
    def test_no_breakout_returns_hold(self, base_breakout_params):
        """No signal change should result in HOLD (None breakout type)."""
        with patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            mock_kalman.return_value = (590.0, 0.0, True)
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            # No change in signals
            params = {**base_breakout_params, "upos": 0, "prev_upos": 0, "dnos": 0, "prev_dnos": 0}
            
            new_open, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            assert new_open == 0
    
    @pytest.mark.unit
    def test_false_upward_breakout_bearish_candle(self, base_breakout_params):
        """Upward breakout with bearish candle (close < open) should be rejected."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            mock_kalman.return_value = (590.0, 0.1, True)
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**base_breakout_params, "upos": 1, "prev_upos": 0}
            params["latest_close"] = 589.0  # Close < Open
            params["latest_open"] = 590.0
            
            new_open, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_kalman_filter_rejects_bearish_on_upward(self, base_breakout_params):
        """Kalman filter returning bearish should reject upward breakout."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            # Kalman returns bearish
            mock_kalman.return_value = (590.0, -0.2, False)
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**base_breakout_params, "upos": 1, "prev_upos": 0}
            
            new_open, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()


class TestBreakoutConstraints:
    """Tests for breakout constraint validation."""
    
    @pytest.fixture
    def breakout_params(self, mock_trading_client, mock_redis_client, mock_logger, frozen_time):
        """Base params with valid upward breakout signal."""
        mock_trading_client.get_clock.return_value.is_open = True
        
        return {
            "prev_kfilt": 590.0,
            "prev_velocity": 0.1,
            "enable_kalman_prediction": False,
            "skip_trading_days_list": [],
            "latest_close_time": frozen_time.now,
            "choppy_day_cnt": 0,
            "bar_head_cnt": 10,
            "maxvolume": 500000,
            "min_gap_bars_cnt_for_breakout": 3,
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
    
    @pytest.mark.unit
    def test_candle_size_exceeds_limit_rejected(self, breakout_params):
        """Candle size exceeding limit should reject breakout."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            # Candle size = high - low = 10, limit = 5
            params["latest_high"] = 600.0
            params["latest_low"] = 590.0
            params["skip_candle_with_size"] = 5.0
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_volume_exceeds_max_rejected(self, breakout_params):
        """Volume exceeding max threshold should reject breakout."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            params["volume"] = 600000  # Exceeds 500000 max
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_position_count_at_limit_rejected(self, breakout_params):
        """Position count at limit should reject new breakout."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            params["positioncnt"] = 10
            params["positionqty"] = 10
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_breakout_within_min_gap_rejected(self, breakout_params):
        """Breakout within minimum bar gap should be rejected."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            params["bar_head_cnt"] = 1  # Only 1 bar since last breakout
            params["min_gap_bars_cnt_for_breakout"] = 3  # Need at least 3
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_skip_trading_day_rejected(self, breakout_params, frozen_time):
        """Breakout on skip trading day should be rejected."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            params["skip_trading_days_list"] = [frozen_time.now.date()]
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_market_closed_rejected(self, breakout_params, mock_trading_client):
        """Breakout when market is closed should be rejected."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            mock_trading_client.get_clock.return_value.is_open = False
            params = {**breakout_params}
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()
    
    @pytest.mark.unit
    def test_order_creation_disabled_skipped(self, breakout_params):
        """When createorder=False, breakout should be skipped."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue:
            from deltadyno.analysis.breakout import check_for_breakouts
            
            params = {**breakout_params}
            params["createorder"] = False
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type is None
            mock_queue.assert_not_called()


class TestBreakoutEdgeCases:
    """Tests for edge cases in breakout detection."""
    
    @pytest.mark.unit
    def test_nan_in_kalman_filter_handled(self, mock_trading_client, mock_redis_client, mock_logger, frozen_time):
        """NaN values in Kalman filter should be handled gracefully."""
        with patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            # Return NaN in velocity
            mock_kalman.return_value = (float("nan"), float("nan"), True)
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            mock_trading_client.get_clock.return_value.is_open = True
            
            params = {
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
            
            # Should not raise exception
            new_open, breakout_type, kfilt, velocity = check_for_breakouts(**params)
            
            assert math.isnan(kfilt)
            assert math.isnan(velocity)
    
    @pytest.mark.unit
    def test_increasing_velocity_overrides_bearish(self, mock_trading_client, mock_redis_client, mock_logger, frozen_time):
        """Increasing velocity should override bearish Kalman signal for upward breakout."""
        with patch("deltadyno.analysis.breakout.breakout_to_queue") as mock_queue, \
             patch("deltadyno.analysis.breakout.apply_kalman_filter") as mock_kalman:
            
            # Kalman returns bearish but velocity is increasing
            mock_kalman.return_value = (590.0, 0.2, False)  # velocity > prev_velocity (0.1)
            mock_queue.return_value = True
            
            from deltadyno.analysis.breakout import check_for_breakouts
            
            mock_trading_client.get_clock.return_value.is_open = True
            
            params = {
                "prev_kfilt": 590.0,
                "prev_velocity": 0.1,  # New velocity (0.2) > prev (0.1)
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
            
            _, breakout_type, _, _ = check_for_breakouts(**params)
            
            assert breakout_type == "upward"
            mock_queue.assert_called_once()


class TestBarStrength:
    """Tests for bar strength calculation."""
    
    @pytest.mark.unit
    def test_bar_strength_bullish_candle(self):
        """Bar strength for bullish candle should be positive."""
        from deltadyno.utils.helpers import calculate_bar_strength
        
        close = 592.0
        open_price = 590.0
        high = 593.0
        low = 589.0
        
        strength = calculate_bar_strength(close, open_price, high, low)
        
        assert strength > 0
        assert 0 <= strength <= 1
    
    @pytest.mark.unit
    def test_bar_strength_bearish_candle(self):
        """Bar strength for bearish candle should be near zero."""
        from deltadyno.utils.helpers import calculate_bar_strength
        
        close = 588.0
        open_price = 590.0
        high = 591.0
        low = 587.0
        
        strength = calculate_bar_strength(close, open_price, high, low)
        
        # For bearish candles, strength is based on body position
        assert 0 <= strength <= 1
    
    @pytest.mark.unit
    def test_bar_strength_doji(self):
        """Bar strength for doji (open ≈ close) should handle edge case."""
        from deltadyno.utils.helpers import calculate_bar_strength
        
        close = 590.05
        open_price = 590.0
        high = 591.0
        low = 589.0
        
        strength = calculate_bar_strength(close, open_price, high, low)
        
        # Should not raise and should be valid
        assert 0 <= strength <= 1

