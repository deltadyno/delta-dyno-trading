"""
Default configuration values for the DeltaDyno trading system.

This module defines default values and their expected data types for all
configurable parameters. These defaults are used when values are not
specified in the database or configuration files.

Format: CONFIG_DEFAULTS[key] = (default_value, data_type)
"""

CONFIG_DEFAULTS = {
    # Logging configuration
    "log_level": ("INFO", str),
    
    # Trading behavior flags
    "read_real_data": (True, bool),
    "read_historical_data": (False, bool),
    "create_order": (True, bool),
    "close_order": (False, bool),
    "read_historical_data_create_order": (False, bool),
    "read_historical_data_close_order": (False, bool),
    "enable_kalman_prediction": (True, bool),
    "enable_chopping": (True, bool),
    
    # Sleep intervals (in seconds)
    "chart_sleep_seconds": (1.0, float),
    "error_sleep_seconds": (3.0, float),
    "historical_read_sleep_seconds": (2.0, float),
    "live_extra_sleep_seconds": (0.25, float),
    
    # Data fetch parameters
    "min_data_age_threshold": (0, int),
    "slope_bar_count": (100, int),
    "data_feed": ("IEX", str),  # Options: IEX (free tier) or SIP (premium)
    
    # Trading constraints
    "max_volume_threshold": (190000, int),
    "min_gap_bars_cnt_for_breakout": (100, int),
    "max_daily_positions": (50, int),
    "skip_candle_with_size": (50.0, float),
    
    # Date/time configuration
    "start_date": ("2025-01-22T23:00:00.000-00:00", str),
    "end_date": ("2025-01-22T23:00:00.000-00:00", str),
    "skip_trading_days": ("", str),
    
    # Market hours configuration
    "pre_market_hour": (4, int),
    "pre_market_minute": (30, int),
    "post_market_hour": (4, int),
    "post_market_minute": (0, int),
    
    # Option trading parameters
    "open_position_expiry_trading_day": (0, int),
    "option_expiry_day_flip_to_next_trading_day": ("15:00", str),
    "cents_to_rollover": (50, int),
    
    # Order quantity parameters
    "limit_order_qty": (1, int),
    "market_order_qty": (1, int),
    "max_order_amount": (1000.0, float),
    "buy_for_amount": (500.0, float),
    "buy_if_price_lt": (10.0, float),
    
    # Choppy day detection parameters
    "atr_threshold": (0.5, float),
    "price_range_threshold": (0.02, float),
    "reverse_candle_threshold": (0.3, float),
    "low_volume_threshold": (0.5, float),
    "cross_cnt_to_mark_choppy_day": (3, int),
}


def get_default(key: str):
    """
    Get the default value for a configuration key.
    
    Args:
        key: Configuration key name
        
    Returns:
        The default value, or None if key is not found
    """
    if key in CONFIG_DEFAULTS:
        return CONFIG_DEFAULTS[key][0]
    return None


def get_type(key: str):
    """
    Get the expected data type for a configuration key.
    
    Args:
        key: Configuration key name
        
    Returns:
        The expected data type, or str if key is not found
    """
    if key in CONFIG_DEFAULTS:
        return CONFIG_DEFAULTS[key][1]
    return str

