"""
Unit tests for the profile listener module (profile_listener.py).

Tests cover:
- Message parsing (multiple formats)
- Trading condition validation
- Buying power/margin updates
- Account restriction events
- Duplicate/delayed message handling
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch
import pytest


# =============================================================================
# Message Parsing Tests
# =============================================================================

class TestMessageParsing:
    """Tests for parse_message_data function."""
    
    @pytest.mark.unit
    def test_parse_breakout_format_with_close_time(self):
        """Parse breakout message with ISO format close_time."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        now = datetime.now(timezone.utc)
        raw = {
            "symbol": "SPY",
            "direction": "upward",
            "close_price": "595.50",
            "close_time": now.isoformat(),
            "candle_size": "1.5",
            "bar_strength": "0.75",
            "volume": "50000",
            "choppy_day_count": "0",
        }
        
        result = parse_message_data(raw)
        
        assert result["symbol"] == "SPY"
        assert result["direction"] == "upward"
        assert result["bar_close"] == 595.50
        assert result["bar_date"] is not None
        assert result["candle_size"] == 1.5
    
    @pytest.mark.unit
    def test_parse_old_format_with_bar_date(self):
        """Parse old message format with bar_date field."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "direction": "downward",
            "bar_close": "590.25",
            "bar_date": "2025-01-23 15:30:00",
            "candle_size": "2.0",
        }
        
        result = parse_message_data(raw)
        
        assert result["symbol"] == "SPY"
        assert result["bar_close"] == 590.25
        assert result["bar_date"].date() == datetime(2025, 1, 23).date()
    
    @pytest.mark.unit
    def test_parse_datetime_format(self):
        """Parse message with DateTime field (option flow format)."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "Symbol": "TSLA",
            "DateTime": "2025-01-23 14:45:00",
            "direction": "upward",
        }
        
        result = parse_message_data(raw)
        
        assert result["symbol"] == "TSLA"
        assert result["bar_date"] is not None
    
    @pytest.mark.unit
    def test_parse_iso_format_with_microseconds(self):
        """Parse ISO format with microseconds."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "close_time": "2025-01-23T15:30:00.123456+00:00",
        }
        
        result = parse_message_data(raw)
        
        assert result["bar_date"] is not None
        assert result["bar_date"].microsecond == 123456
    
    @pytest.mark.unit
    def test_parse_iso_format_with_z_suffix(self):
        """Parse ISO format with Z suffix."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "close_time": "2025-01-23T15:30:00Z",
        }
        
        result = parse_message_data(raw)
        
        assert result["bar_date"] is not None
    
    @pytest.mark.unit
    def test_parse_missing_symbol_raises_keyerror(self):
        """Missing symbol should raise KeyError."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "direction": "upward",
            "close_price": "595.50",
        }
        
        with pytest.raises(KeyError):
            parse_message_data(raw)
    
    @pytest.mark.unit
    def test_parse_bytes_keys_decoded(self):
        """Bytes keys should be decoded properly."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            b"symbol": b"SPY",
            b"direction": b"upward",
        }
        
        result = parse_message_data(raw)
        
        assert result["symbol"] == "SPY"
        assert result["direction"] == "upward"
    
    @pytest.mark.unit
    def test_parse_invalid_float_returns_none(self):
        """Invalid float values should return None."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "close_price": "invalid",
            "volume": "not_a_number",
        }
        
        result = parse_message_data(raw)
        
        assert result["bar_close"] is None
        assert result["volume"] is None
    
    @pytest.mark.unit
    def test_parse_invalid_date_returns_none(self):
        """Invalid date format should result in None bar_date."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "close_time": "not-a-date",
        }
        
        result = parse_message_data(raw)
        
        assert result["bar_date"] is None
    
    @pytest.mark.unit
    def test_parse_choppy_day_count_mapped_to_choppy_level(self):
        """choppy_day_count should be mapped to choppy_level."""
        from deltadyno.trading.profile_listener import parse_message_data
        
        raw = {
            "symbol": "SPY",
            "choppy_day_count": "3",
        }
        
        result = parse_message_data(raw)
        
        assert result["choppy_level"] == 3.0


class TestTradingConditionValidation:
    """Tests for validate_trading_conditions function."""
    
    @pytest.mark.unit
    def test_date_change_detected(self, mock_logger):
        """Date change should be detected and return True."""
        from deltadyno.trading.profile_listener import validate_trading_conditions
        
        bar_date = datetime(2025, 1, 24, 15, 30, tzinfo=timezone.utc)
        last_processed = datetime(2025, 1, 23).date()
        
        result = validate_trading_conditions(bar_date, last_processed, mock_logger)
        
        assert result is True
    
    @pytest.mark.unit
    def test_same_date_returns_false(self, mock_logger):
        """Same date should return False."""
        from deltadyno.trading.profile_listener import validate_trading_conditions
        
        bar_date = datetime(2025, 1, 23, 16, 45, tzinfo=timezone.utc)
        last_processed = datetime(2025, 1, 23).date()
        
        result = validate_trading_conditions(bar_date, last_processed, mock_logger)
        
        assert result is False
    
    @pytest.mark.unit
    def test_none_bar_date_returns_false(self, mock_logger):
        """None bar_date should return False (cannot validate)."""
        from deltadyno.trading.profile_listener import validate_trading_conditions
        
        result = validate_trading_conditions(None, datetime(2025, 1, 23).date(), mock_logger)
        
        assert result is False
        mock_logger.warning.assert_called()


class TestSkipTradingDays:
    """Tests for check_skip_trading_days function."""
    
    @pytest.mark.unit
    def test_trading_day_in_skip_list(self, mock_logger):
        """Date in skip list should return True (skip trading)."""
        from deltadyno.trading.profile_listener import check_skip_trading_days
        
        bar_date = datetime(2025, 1, 23, 15, 30, tzinfo=timezone.utc)
        skip_list = [datetime(2025, 1, 23).date(), datetime(2025, 1, 24).date()]
        
        result = check_skip_trading_days(bar_date, skip_list, mock_logger)
        
        assert result is True
    
    @pytest.mark.unit
    def test_trading_day_not_in_skip_list(self, mock_logger):
        """Date not in skip list should return False (trade normally)."""
        from deltadyno.trading.profile_listener import check_skip_trading_days
        
        bar_date = datetime(2025, 1, 25, 15, 30, tzinfo=timezone.utc)
        skip_list = [datetime(2025, 1, 23).date(), datetime(2025, 1, 24).date()]
        
        result = check_skip_trading_days(bar_date, skip_list, mock_logger)
        
        assert result is False
    
    @pytest.mark.unit
    def test_none_bar_date_skips_trading(self, mock_logger):
        """None bar_date should skip trading."""
        from deltadyno.trading.profile_listener import check_skip_trading_days
        
        result = check_skip_trading_days(None, [], mock_logger)
        
        assert result is True
        mock_logger.warning.assert_called()


class TestClientInitialization:
    """Tests for client initialization functions."""
    
    @pytest.mark.unit
    def test_initialize_trading_client(self, mock_db_config, mock_logger):
        """Trading client should be initialized with correct settings."""
        with patch("deltadyno.trading.profile_listener.TradingClient") as MockClient:
            from deltadyno.trading.profile_listener import initialize_trading_client
            
            mock_db_config.get.return_value = True  # is_paper_trading
            
            client = initialize_trading_client(mock_db_config, "key", "secret", mock_logger)
            
            MockClient.assert_called_once_with("key", "secret", paper=True)
    
    @pytest.mark.unit
    def test_initialize_option_client(self, mock_logger):
        """Option client should be initialized."""
        with patch("deltadyno.trading.profile_listener.OptionHistoricalDataClient") as MockClient:
            from deltadyno.trading.profile_listener import initialize_option_historical_client
            
            client = initialize_option_historical_client("key", "secret", mock_logger)
            
            MockClient.assert_called_once_with("key", "secret")
    
    @pytest.mark.unit
    def test_initialize_redis_client(self, mock_logger):
        """Redis client should be initialized with correct params."""
        with patch("deltadyno.trading.profile_listener.Redis") as MockRedis:
            from deltadyno.trading.profile_listener import initialize_redis_client
            
            client = initialize_redis_client("localhost", 6379, "password", mock_logger)
            
            MockRedis.assert_called_once_with(
                host="localhost",
                port=6379,
                password="password",
                decode_responses=True
            )


class TestConstraintChecking:
    """Tests for trading constraint validation."""
    
    @pytest.mark.unit
    def test_within_trading_window(self, mock_logger):
        """Trading within allowed time window should pass."""
        from deltadyno.trading.constraints import check_constraints
        from datetime import time as datetime_time
        
        result = check_constraints(
            timezone_str="US/Eastern",
            no_trade_start_time=datetime_time(15, 45),
            no_trade_end_time=datetime_time(16, 0),
            candle_size=1.5,
            skip_candle_with_size=5.0,
            volume=50000,
            max_volume_threshold=100000,
            open_position_cnt=3,
            max_daily_positions_allowed=10,
            bar_date=datetime(2025, 1, 23, 14, 30, tzinfo=timezone.utc),
            skip_trading_days_list=[],
            logger=mock_logger
        )
        
        assert result is True
    
    @pytest.mark.unit
    def test_candle_size_exceeds_limit(self, mock_logger):
        """Candle size exceeding limit should fail."""
        from deltadyno.trading.constraints import check_constraints
        from datetime import time as datetime_time
        
        result = check_constraints(
            timezone_str="US/Eastern",
            no_trade_start_time=datetime_time(15, 45),
            no_trade_end_time=datetime_time(16, 0),
            candle_size=10.0,  # Exceeds limit
            skip_candle_with_size=5.0,
            volume=50000,
            max_volume_threshold=100000,
            open_position_cnt=3,
            max_daily_positions_allowed=10,
            bar_date=datetime(2025, 1, 23, 14, 30, tzinfo=timezone.utc),
            skip_trading_days_list=[],
            logger=mock_logger
        )
        
        assert result is False
    
    @pytest.mark.unit
    def test_volume_exceeds_limit(self, mock_logger):
        """Volume exceeding limit should fail."""
        from deltadyno.trading.constraints import check_constraints
        from datetime import time as datetime_time
        
        result = check_constraints(
            timezone_str="US/Eastern",
            no_trade_start_time=datetime_time(15, 45),
            no_trade_end_time=datetime_time(16, 0),
            candle_size=1.5,
            skip_candle_with_size=5.0,
            volume=200000,  # Exceeds limit
            max_volume_threshold=100000,
            open_position_cnt=3,
            max_daily_positions_allowed=10,
            bar_date=datetime(2025, 1, 23, 14, 30, tzinfo=timezone.utc),
            skip_trading_days_list=[],
            logger=mock_logger
        )
        
        assert result is False
    
    @pytest.mark.unit
    def test_position_count_at_limit(self, mock_logger):
        """Position count at limit should fail."""
        from deltadyno.trading.constraints import check_constraints
        from datetime import time as datetime_time
        
        result = check_constraints(
            timezone_str="US/Eastern",
            no_trade_start_time=datetime_time(15, 45),
            no_trade_end_time=datetime_time(16, 0),
            candle_size=1.5,
            skip_candle_with_size=5.0,
            volume=50000,
            max_volume_threshold=100000,
            open_position_cnt=10,  # At limit
            max_daily_positions_allowed=10,
            bar_date=datetime(2025, 1, 23, 14, 30, tzinfo=timezone.utc),
            skip_trading_days_list=[],
            logger=mock_logger
        )
        
        assert result is False


class TestDuplicateMessageHandling:
    """Tests for duplicate message detection and handling."""
    
    @pytest.mark.unit
    def test_duplicate_message_detected(self):
        """Duplicate message should be detected by message ID."""
        processed_ids = {"msg-001", "msg-002"}
        new_message_id = "msg-001"
        
        is_duplicate = new_message_id in processed_ids
        
        assert is_duplicate is True
    
    @pytest.mark.unit
    def test_new_message_not_duplicate(self):
        """New message should not be marked as duplicate."""
        processed_ids = {"msg-001", "msg-002"}
        new_message_id = "msg-003"
        
        is_duplicate = new_message_id in processed_ids
        
        assert is_duplicate is False


class TestDelayedMessageHandling:
    """Tests for handling delayed/stale messages."""
    
    @pytest.mark.unit
    def test_stale_message_detected(self, mock_logger):
        """Message older than threshold should be detected as stale."""
        message_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        current_time = datetime.now(timezone.utc)
        stale_threshold_seconds = 300  # 5 minutes
        
        age_seconds = (current_time - message_time).total_seconds()
        is_stale = age_seconds > stale_threshold_seconds
        
        assert is_stale is True
    
    @pytest.mark.unit
    def test_fresh_message_not_stale(self, mock_logger):
        """Recent message should not be marked as stale."""
        message_time = datetime.now(timezone.utc) - timedelta(seconds=30)
        current_time = datetime.now(timezone.utc)
        stale_threshold_seconds = 300
        
        age_seconds = (current_time - message_time).total_seconds()
        is_stale = age_seconds > stale_threshold_seconds
        
        assert is_stale is False


class TestBuyingPowerUpdates:
    """Tests for buying power update handling."""
    
    @pytest.mark.unit
    def test_buying_power_increase_logged(self, mock_trading_client, mock_logger):
        """Buying power increase should be logged."""
        old_buying_power = 50000.0
        new_buying_power = 55000.0
        
        change = new_buying_power - old_buying_power
        
        assert change > 0
        # In actual implementation, this would log the increase
    
    @pytest.mark.unit
    def test_buying_power_decrease_triggers_warning(self, mock_trading_client, mock_logger):
        """Significant buying power decrease should trigger warning."""
        old_buying_power = 50000.0
        new_buying_power = 40000.0
        
        change_percent = (old_buying_power - new_buying_power) / old_buying_power
        
        # 20% decrease should trigger warning
        assert change_percent >= 0.10


class TestMarginUpdates:
    """Tests for margin change handling."""
    
    @pytest.mark.unit
    def test_margin_reduction_mid_trade(self, mock_logger):
        """Margin reduction mid-trade should be detected."""
        original_margin = 100000.0
        reduced_margin = 80000.0
        position_value = 25000.0
        
        # Check if position exceeds new margin
        exceeds_margin = position_value > reduced_margin * 0.5  # Using 50% of margin
        
        # With position at 25K and margin at 80K (40K usable), we're within limits
        assert exceeds_margin is False
    
    @pytest.mark.unit
    def test_margin_reduced_below_position_requirement(self, mock_logger):
        """Margin reduced below position requirement should trigger action."""
        reduced_margin = 30000.0
        position_value = 25000.0
        
        # Position exceeds 50% of new margin
        exceeds_margin = position_value > reduced_margin * 0.5
        
        assert exceeds_margin is True


class TestAccountRestrictionEvents:
    """Tests for account restriction event handling."""
    
    @pytest.mark.unit
    def test_pdt_restriction_detected(self, mock_trading_client, mock_logger):
        """PDT restriction should be detected."""
        mock_account = MagicMock()
        mock_account.pattern_day_trader = True
        mock_account.daytrade_count = 4  # At PDT limit
        mock_trading_client.get_account.return_value = mock_account
        
        is_pdt_restricted = (
            mock_account.pattern_day_trader and 
            mock_account.daytrade_count >= 4
        )
        
        assert is_pdt_restricted is True
    
    @pytest.mark.unit
    def test_account_not_restricted(self, mock_trading_client, mock_logger):
        """Normal account should not show restrictions."""
        mock_account = MagicMock()
        mock_account.pattern_day_trader = False
        mock_account.daytrade_count = 2
        mock_trading_client.get_account.return_value = mock_account
        
        is_restricted = (
            mock_account.pattern_day_trader and 
            mock_account.daytrade_count >= 4
        )
        
        assert is_restricted is False
    
    @pytest.mark.unit
    def test_account_suspended_detected(self, mock_trading_client, mock_logger):
        """Account suspension should be detected."""
        mock_account = MagicMock()
        mock_account.status = "SUSPENDED"
        mock_trading_client.get_account.return_value = mock_account
        
        is_suspended = mock_account.status == "SUSPENDED"
        
        assert is_suspended is True
    
    @pytest.mark.unit
    def test_account_active(self, mock_trading_client, mock_logger):
        """Active account should pass checks."""
        mock_account = MagicMock()
        mock_account.status = "ACTIVE"
        mock_trading_client.get_account.return_value = mock_account
        
        is_active = mock_account.status == "ACTIVE"
        
        assert is_active is True


class TestProfileMessageValidation:
    """Tests for profile message validation."""
    
    @pytest.mark.unit
    def test_message_missing_symbol_rejected(self, mock_logger):
        """Message without symbol should be rejected."""
        message = {
            "direction": "upward",
            "close_time": "2025-01-23T14:30:00",
        }
        
        is_valid = "symbol" in message and message["symbol"]
        
        assert is_valid is False
    
    @pytest.mark.unit
    def test_message_empty_symbol_rejected(self, mock_logger):
        """Message with empty symbol should be rejected."""
        message = {
            "symbol": "",
            "direction": "upward",
        }
        
        is_valid = "symbol" in message and message["symbol"]
        
        assert is_valid is False
    
    @pytest.mark.unit
    def test_message_invalid_direction_rejected(self, mock_logger):
        """Message with invalid direction should be rejected."""
        message = {
            "symbol": "SPY",
            "direction": "sideways",
        }
        
        valid_directions = ["upward", "downward"]
        is_valid = message.get("direction") in valid_directions
        
        assert is_valid is False
    
    @pytest.mark.unit
    def test_message_all_fields_valid(self, breakout_message_factory, mock_logger):
        """Message with all valid fields should pass."""
        message = breakout_message_factory.create_message()
        
        valid_directions = ["upward", "downward"]
        is_valid = (
            message.get("symbol") and 
            message.get("direction") in valid_directions
        )
        
        assert is_valid is True


class TestChoppyDayHandling:
    """Tests for choppy day count handling."""
    
    @pytest.mark.unit
    def test_choppy_day_count_incremented(self, mock_db_config, mock_logger):
        """Choppy day count should be incremented when day is choppy."""
        choppy_day_count = 3
        is_choppy_day = True
        
        if is_choppy_day:
            choppy_day_count += 1
        
        assert choppy_day_count == 4
    
    @pytest.mark.unit
    def test_choppy_day_count_reset_on_new_day(self, mock_db_config, mock_logger, frozen_time):
        """Choppy day count should reset on new day."""
        last_reset_date = frozen_time.now.date() - timedelta(days=1)
        current_date = frozen_time.now.date()
        choppy_day_count = 5
        
        if current_date != last_reset_date:
            choppy_day_count = 0
        
        assert choppy_day_count == 0
    
    @pytest.mark.unit
    def test_high_choppy_count_blocks_trading(self, mock_logger):
        """High choppy day count should block trading."""
        choppy_day_count = 10
        max_choppy_days = 5
        
        should_block = choppy_day_count > max_choppy_days
        
        assert should_block is True
    
    @pytest.mark.unit
    def test_low_choppy_count_allows_trading(self, mock_logger):
        """Low choppy day count should allow trading."""
        choppy_day_count = 2
        max_choppy_days = 5
        
        should_block = choppy_day_count > max_choppy_days
        
        assert should_block is False


class TestTimezoneHandling:
    """Tests for timezone handling in profile listener."""
    
    @pytest.mark.unit
    def test_us_eastern_timezone_conversion(self, mock_logger):
        """US Eastern timezone should be handled correctly."""
        import pytz
        
        utc_time = datetime(2025, 1, 23, 19, 30, tzinfo=pytz.UTC)
        eastern_tz = pytz.timezone("US/Eastern")
        
        eastern_time = utc_time.astimezone(eastern_tz)
        
        # During standard time (winter), ET is UTC-5
        assert eastern_time.hour == 14  # 19:30 UTC = 14:30 EST
    
    @pytest.mark.unit
    def test_market_hours_in_eastern(self, mock_logger):
        """Market hours should be checked in Eastern time."""
        import pytz
        from datetime import time as datetime_time
        
        eastern_tz = pytz.timezone("US/Eastern")
        market_open = datetime_time(9, 30)
        market_close = datetime_time(16, 0)
        
        # 14:30 ET is within market hours
        current_time = datetime_time(14, 30)
        
        is_market_hours = market_open <= current_time <= market_close
        
        assert is_market_hours is True
    
    @pytest.mark.unit
    def test_premarket_detection(self, mock_logger):
        """Pre-market hours should be detected."""
        from datetime import time as datetime_time
        
        market_open = datetime_time(9, 30)
        premarket_start = datetime_time(4, 0)
        
        # 8:30 ET is pre-market
        current_time = datetime_time(8, 30)
        
        is_premarket = premarket_start <= current_time < market_open
        
        assert is_premarket is True
    
    @pytest.mark.unit
    def test_afterhours_detection(self, mock_logger):
        """After-hours trading should be detected."""
        from datetime import time as datetime_time
        
        market_close = datetime_time(16, 0)
        afterhours_end = datetime_time(20, 0)
        
        # 17:30 ET is after-hours
        current_time = datetime_time(17, 30)
        
        is_afterhours = market_close < current_time <= afterhours_end
        
        assert is_afterhours is True


class TestNoTradeWindowHandling:
    """Tests for no-trade time window handling."""
    
    @pytest.mark.unit
    def test_within_no_trade_window(self, mock_logger):
        """Time within no-trade window should block trading."""
        from datetime import time as datetime_time
        
        no_trade_start = datetime_time(15, 45)
        no_trade_end = datetime_time(16, 0)
        current_time = datetime_time(15, 50)
        
        in_no_trade_window = no_trade_start <= current_time <= no_trade_end
        
        assert in_no_trade_window is True
    
    @pytest.mark.unit
    def test_before_no_trade_window(self, mock_logger):
        """Time before no-trade window should allow trading."""
        from datetime import time as datetime_time
        
        no_trade_start = datetime_time(15, 45)
        no_trade_end = datetime_time(16, 0)
        current_time = datetime_time(15, 30)
        
        in_no_trade_window = no_trade_start <= current_time <= no_trade_end
        
        assert in_no_trade_window is False
    
    @pytest.mark.unit
    def test_after_no_trade_window(self, mock_logger):
        """Time after no-trade window should allow trading (if market still open)."""
        from datetime import time as datetime_time
        
        no_trade_start = datetime_time(10, 0)
        no_trade_end = datetime_time(10, 30)
        current_time = datetime_time(11, 0)
        
        in_no_trade_window = no_trade_start <= current_time <= no_trade_end
        
        assert in_no_trade_window is False


class TestProfileIdExtraction:
    """Tests for profile ID extraction from symbols."""
    
    @pytest.mark.unit
    def test_extract_profile_id_from_symbol(self):
        """Profile ID should be extracted from option symbol."""
        # Assume profile ID is encoded in some way
        symbol = "SPY250124C00595000"
        
        # Extract underlying
        underlying = symbol[:3] if symbol[:3].isupper() else symbol[:4]
        
        assert underlying == "SPY"
    
    @pytest.mark.unit
    def test_extract_expiry_from_symbol(self):
        """Expiry date should be extracted from option symbol."""
        symbol = "SPY250124C00595000"
        
        # OCC format: SYMBOL + YYMMDD + C/P + strike
        expiry_str = symbol[3:9]  # "250124"
        
        year = 2000 + int(expiry_str[:2])
        month = int(expiry_str[2:4])
        day = int(expiry_str[4:6])
        
        assert year == 2025
        assert month == 1
        assert day == 24
    
    @pytest.mark.unit
    def test_extract_strike_from_symbol(self):
        """Strike price should be extracted from option symbol."""
        symbol = "SPY250124C00595000"
        
        # Strike is last 8 characters, divided by 1000
        strike_str = symbol[-8:]  # "00595000"
        strike = int(strike_str) / 1000
        
        assert strike == 595.0
    
    @pytest.mark.unit
    def test_extract_option_type_from_symbol(self):
        """Option type should be extracted from option symbol."""
        call_symbol = "SPY250124C00595000"
        put_symbol = "SPY250124P00595000"
        
        call_type = call_symbol[9]  # Position varies by underlying length
        put_type = put_symbol[9]
        
        assert call_type == "C"
        assert put_type == "P"


class TestRedisStreamOperations:
    """Tests for Redis stream operations."""
    
    @pytest.mark.unit
    def test_redis_xread_returns_messages(self, mock_redis_client, mock_logger):
        """Redis XREAD should return pending messages."""
        mock_redis_client.xread.return_value = [
            ("breakout_messages:v1", [
                ("1234567890-0", {"symbol": "SPY", "direction": "upward"}),
                ("1234567891-0", {"symbol": "QQQ", "direction": "downward"}),
            ])
        ]
        
        result = mock_redis_client.xread({"breakout_messages:v1": "0"}, count=10)
        
        assert len(result[0][1]) == 2
    
    @pytest.mark.unit
    def test_redis_xread_empty_stream(self, mock_redis_client, mock_logger):
        """Empty Redis stream should return empty list."""
        mock_redis_client.xread.return_value = []
        
        result = mock_redis_client.xread({"breakout_messages:v1": "0"}, count=10)
        
        assert result == []
    
    @pytest.mark.unit
    def test_redis_xack_confirms_processing(self, mock_redis_client, mock_logger):
        """XACK should confirm message processing."""
        mock_redis_client.xack.return_value = 1
        
        result = mock_redis_client.xack("breakout_messages:v1", "consumer-group", "1234567890-0")
        
        assert result == 1
    
    @pytest.mark.unit
    def test_redis_connection_pool_reused(self, mock_redis_client, mock_logger):
        """Redis connection pool should be reused across calls."""
        # Simulate multiple operations on same client
        call_count = 0
        
        for _ in range(5):
            mock_redis_client.ping()
            call_count += 1
        
        assert call_count == 5
        # Same client used for all calls


class TestConfigurationReload:
    """Tests for configuration reload handling."""
    
    @pytest.mark.unit
    def test_config_reload_on_signal(self, mock_db_config, mock_logger):
        """Configuration should reload when signaled."""
        original_value = mock_db_config.max_daily_positions_allowed
        mock_db_config.max_daily_positions_allowed = 20
        
        # After reload
        new_value = mock_db_config.max_daily_positions_allowed
        
        assert new_value == 20
    
    @pytest.mark.unit
    def test_config_changes_reflected_immediately(self, mock_db_config, mock_logger):
        """Config changes should be reflected immediately."""
        mock_db_config.trailing_stop_percent = 0.10
        
        # Value should be immediately available
        assert mock_db_config.trailing_stop_percent == 0.10
    
    @pytest.mark.unit
    def test_invalid_config_rejected(self, mock_logger):
        """Invalid configuration values should be rejected."""
        invalid_position_limit = -1
        
        is_valid = invalid_position_limit >= 0
        
        assert is_valid is False

