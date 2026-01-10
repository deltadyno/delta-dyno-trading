"""
Choppy day detection module for the DeltaDyno trading system.

This module provides functions to detect choppy (range-bound) market conditions
by analyzing price crossings and volatility patterns.

Choppy conditions indicate low-conviction market environments where breakout
signals may be less reliable.
"""

from datetime import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# Candle Tracking - High/Low Method
# =============================================================================

def monitor_candles_high_low(
    tracked_candles: Dict,
    latest_close_time: datetime,
    current_high: float,
    current_low: float,
    cross_cnt_to_mark_choppy_day: int,
    logger
) -> Tuple[Dict, int]:
    """
    Monitor price action by tracking high/low crossings across candles.

    This method tracks how many times price completely crosses above or below
    previous candle ranges, indicating potential choppy conditions when
    crossings are frequent.

    Args:
        tracked_candles: Dictionary of tracked candle data
        latest_close_time: Timestamp of the current candle
        current_high: High price of current candle
        current_low: Low price of current candle
        cross_cnt_to_mark_choppy_day: Number of crossings to mark as choppy
        logger: Logger instance

    Returns:
        Tuple of (updated_tracked_candles, choppy_day_count)
    """
    choppy_day_count = 0

    # Add new candle to tracking if not already tracked
    if latest_close_time not in tracked_candles:
        tracked_candles[latest_close_time] = {
            "high": current_high,
            "low": current_low,
            "crossings": 0,
            "last_direction": None,
        }

    # Get candles to compare (exclude current candle)
    candles_to_check = {
        timestamp: data
        for timestamp, data in tracked_candles.items()
        if timestamp != latest_close_time
    }

    # Check each tracked candle for crossings
    for tracked_timestamp, tracked_data in candles_to_check.items():
        tracked_high = tracked_data["high"]
        tracked_low = tracked_data["low"]

        # Skip if current candle overlaps with tracked candle
        if not (current_high < tracked_low or current_low > tracked_high):
            logger.debug(
                f"Skipping: Current candle {latest_close_time} overlaps "
                f"with tracked candle {tracked_timestamp}."
            )
            continue

        # Check for complete cross above
        if current_high > tracked_high and current_low > tracked_high:
            if tracked_data["last_direction"] != "above":
                tracked_data["crossings"] += 1
                tracked_data["last_direction"] = "above"
            logger.debug(
                f"Candle {latest_close_time} completely above {tracked_timestamp}. "
                f"Crossings: {tracked_data['crossings']}"
            )

        # Check for complete cross below
        elif current_low < tracked_low and current_high < tracked_low:
            if tracked_data["last_direction"] != "below":
                tracked_data["crossings"] += 1
                tracked_data["last_direction"] = "below"
            logger.debug(
                f"Candle {latest_close_time} completely below {tracked_timestamp}. "
                f"Crossings: {tracked_data['crossings']}"
            )

        # Check if choppy threshold reached
        if tracked_data["crossings"] >= cross_cnt_to_mark_choppy_day:
            print(f"CHOPPY DAY detected at {latest_close_time} with candle {tracked_timestamp}")
            logger.debug(
                f"CHOPPY DAY detected at {latest_close_time} "
                f"with historical candle {tracked_timestamp}"
            )
            tracked_candles.clear()
            choppy_day_count = tracked_data["crossings"]
            break

    return tracked_candles, choppy_day_count


# =============================================================================
# Candle Tracking - Close Price Method
# =============================================================================

def monitor_candles_close(
    tracked_candles: Dict,
    latest_close_time: datetime,
    current_close: float,
    latest_high: float,
    latest_low: float,
    logger
) -> Tuple[Dict, int]:
    """
    Monitor price action by tracking close price crossings.

    This method tracks how many times the closing price crosses above or below
    previous candle closes, returning the maximum crossing count as an
    indicator of choppy conditions.

    Args:
        tracked_candles: Dictionary of tracked candle data
        latest_close_time: Timestamp of the current candle
        current_close: Closing price of current candle
        latest_high: High price of current candle
        latest_low: Low price of current candle
        logger: Logger instance

    Returns:
        Tuple of (updated_tracked_candles, maximum_crossing_count)
    """
    logger.debug(f"latest_close_time: {latest_close_time}, current_close: {current_close}")

    choppy_day_count = 0

    # Add new candle to tracking if not already tracked
    if latest_close_time not in tracked_candles:
        tracked_candles[latest_close_time] = {
            "current_close": current_close,
            "crossings": 0,
            "close_time": latest_close_time,
            "last_direction": None,
        }

    logger.debug(f"Number of tracked candles: {len(tracked_candles)}")

    # Get candles to compare (exclude current candle)
    candles_to_check = {
        timestamp: data
        for timestamp, data in tracked_candles.items()
        if timestamp != latest_close_time
    }

    # Check each tracked candle for crossings
    for tracked_timestamp, tracked_data in candles_to_check.items():
        tracked_close = tracked_data["current_close"]

        # Skip if tracked close is within current candle's range
        if latest_low <= tracked_close <= latest_high:
            choppy_day_count = max(choppy_day_count, tracked_data["crossings"])
            logger.debug(
                f"Skipping: {tracked_close} is within range [{latest_low}, {latest_high}]."
            )
            continue

        logger.debug(
            f"Tracking: {tracked_close} vs {current_close} - "
            f"Crossings: {tracked_data['crossings']}. "
            f"Candle: {tracked_data['close_time']}"
        )

        # Check for cross above
        if current_close > tracked_close:
            if tracked_data["last_direction"] != "above":
                tracked_data["crossings"] += 1
                tracked_data["last_direction"] = "above"
            logger.debug(
                f"Close {current_close} > Tracked {tracked_close} - "
                f"Crossings: {tracked_data['crossings']}. Candle: {tracked_data['close_time']}"
            )

        # Check for cross below
        elif current_close < tracked_close:
            if tracked_data["last_direction"] != "below":
                tracked_data["crossings"] += 1
                tracked_data["last_direction"] = "below"
            logger.debug(
                f"Close {current_close} < Tracked {tracked_close} - "
                f"Crossings: {tracked_data['crossings']}. Candle: {tracked_data['close_time']}"
            )

        # Update maximum crossing count
        choppy_day_count = max(choppy_day_count, tracked_data["crossings"])

    return tracked_candles, choppy_day_count


# =============================================================================
# ATR-Based Choppy Day Detection
# =============================================================================

def calculate_atr(data: pd.DataFrame, logger, length: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).

    ATR measures market volatility by decomposing the entire range of an
    asset price for that period.

    Args:
        data: DataFrame with 'high', 'low', 'close' columns
        logger: Logger instance
        length: Lookback period for ATR calculation

    Returns:
        Series containing ATR values
    """
    logger.debug(f"Calculating ATR with length {length}")

    high_low = data["high"] - data["low"]
    high_close = np.abs(data["high"] - data["close"].shift(1))
    low_close = np.abs(data["low"] - data["close"].shift(1))

    true_range = np.maximum(high_low, np.maximum(high_close, low_close))
    atr = true_range.rolling(window=length).mean()

    logger.debug(f"ATR calculation complete. Sample values: {atr.head()}")
    return atr


def is_choppy_day(
    data: pd.DataFrame,
    atr_threshold_config: float,
    price_range_threshold_config: float,
    reverse_candle_threshold_config: float,
    low_volume_threshold_config: float,
    logger,
    length: int = 14
) -> bool:
    """
    Determine if current market conditions indicate a choppy day.

    Evaluates multiple criteria:
    1. Low ATR (low volatility)
    2. Narrow price range
    3. Low volume
    4. Frequent price reversals

    Args:
        data: DataFrame with OHLCV data
        atr_threshold_config: Multiplier for ATR threshold (e.g., 0.5)
        price_range_threshold_config: Multiplier for price range threshold
        reverse_candle_threshold_config: Threshold for reversal frequency
        low_volume_threshold_config: Multiplier for volume threshold
        logger: Logger instance
        length: Lookback period

    Returns:
        True if all choppy conditions are met, False otherwise
    """
    logger.info("Checking if the day is choppy")

    # Use latest bars for analysis
    sliced_data = data.iloc[-(length * 2 + 1):]
    logger.debug(f"Using the last {len(sliced_data)} entries from the data")

    atr = calculate_atr(sliced_data, logger, length)
    avg_price = (sliced_data["high"] + sliced_data["low"]) / 2
    price_range = sliced_data["high"] - sliced_data["low"]

    # Condition 1: Low ATR
    atr_threshold = atr.mean() * atr_threshold_config
    low_atr_condition = atr.iloc[-1] < atr_threshold
    logger.debug(
        f"Low ATR condition: {low_atr_condition} "
        f"(ATR: {atr.iloc[-1]:.5f}, Threshold: {atr_threshold:.5f})"
    )

    # Condition 2: Narrow Price Range
    price_range_threshold = avg_price.mean() * price_range_threshold_config
    narrow_range_condition = price_range.max() < price_range_threshold
    logger.debug(
        f"Narrow Price Range condition: {narrow_range_condition} "
        f"(Max Range: {price_range.max():.5f}, Threshold: {price_range_threshold:.5f})"
    )

    # Condition 3: Low Volume
    avg_volume = sliced_data["volume"].mean()
    volume_threshold = avg_volume * low_volume_threshold_config
    low_volume_condition = sliced_data["volume"].iloc[-1] < volume_threshold
    logger.debug(
        f"Low Volume condition: {low_volume_condition} "
        f"(Volume: {sliced_data['volume'].iloc[-1]:.0f}, Threshold: {volume_threshold:.0f})"
    )

    # Condition 4: Frequent Price Reversals
    reversal_data = sliced_data.iloc[-length:]
    logger.debug(f"Using {len(reversal_data)} entries for reversal calculation")

    reversals = 0
    for i in range(2, len(reversal_data)):
        current_bullish = reversal_data["close"].iloc[i] > reversal_data["open"].iloc[i]
        prev_bullish = reversal_data["close"].iloc[i - 1] > reversal_data["open"].iloc[i - 1]

        # Count direction changes
        if current_bullish != prev_bullish:
            reversals += 1

    reversal_threshold = len(reversal_data) * reverse_candle_threshold_config
    frequent_reversals_condition = reversals > reversal_threshold
    logger.debug(
        f"Frequent Price Reversals condition: {frequent_reversals_condition} "
        f"(Reversals: {reversals}, Threshold: {reversal_threshold:.0f})"
    )

    # Combine all conditions
    is_choppy = (
        low_atr_condition and
        narrow_range_condition and
        frequent_reversals_condition and
        low_volume_condition
    )

    logger.info(f"Choppy day detected: {is_choppy}")
    return is_choppy

