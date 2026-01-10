"""
Application-wide constants for the DeltaDyno trading system.

This module defines constant values used throughout the application for:
- Breakout direction types
- Option types
- Position status indicators
"""

# Breakout direction constants
UPWARD = "upward"
DOWNWARD = "downward"
REVERSE_UPWARD = "reverse_upward"
REVERSE_DOWNWARD = "reverse_downward"

# Option type constants
PUT = "P"
CALL = "C"

# Position status constants
NO_POSITION_FOUND = "no_position_found"
POSITION_CLOSED = "position_closed"
ERROR_OCCURRED = "error_occurred"
POSITION_CLOSE_SKIP = "position_close_skip"
MARKET_CLOSED = "market_closed"

# Time-related constants (in seconds)
DEFAULT_SLEEP_SECONDS = 180  # 3-minute candle interval
MAX_SLEEP_SECONDS = 1800  # 30 minutes maximum sleep
MIN_SLEEP_SECONDS = 1  # Minimum sleep time

# Data processing constants
SLOPE_BAR_COUNT_DEFAULT = 100
PIVOT_LENGTH_DEFAULT = 15

# Market hours constants (UTC)
PRE_MARKET_START_HOUR = 9  # 4:00 AM ET
PRE_MARKET_START_MINUTE = 0
REGULAR_MARKET_START_HOUR = 14  # 9:30 AM ET
REGULAR_MARKET_START_MINUTE = 30
REGULAR_MARKET_END_HOUR = 21  # 4:00 PM ET
REGULAR_MARKET_END_MINUTE = 0

