"""
Pivot point detection for the DeltaDyno trading system.

This module provides functions to detect pivot high and pivot low points
in price data, which are key levels for breakout detection.
"""

from typing import Tuple

import numpy as np
import pandas as pd

from deltadyno.utils.timing import time_it


@time_it
def calculate_pivots(
    df: pd.DataFrame,
    length: int,
    logger=None
) -> Tuple[float, float]:
    """
    Calculate pivot high and pivot low values from price data.

    A pivot high is a bar where the high is greater than or equal to 
    all highs in the lookback period and strictly greater than all highs
    in the lookforward period.

    A pivot low is a bar where the low is less than or equal to all lows
    in the lookback period and strictly less than all lows in the 
    lookforward period.

    Args:
        df: DataFrame with 'high' and 'low' columns
        length: Lookback/lookforward period for pivot detection
        logger: Logger instance

    Returns:
        Tuple of (pivot_high, pivot_low), each rounded to 10 decimal places
        Returns (0.0, 0.0) if insufficient data
    """
    logger.debug("Calculating pivots...")

    pivot_bar_count = length * 2 + 1

    # Validate data sufficiency
    if df is None or len(df) < pivot_bar_count:
        logger.info(
            f"Insufficient data for pivot calculation. "
            f"Required: {pivot_bar_count}, Found: {len(df) if df is not None else 0}."
        )
        return 0.0, 0.0

    # Work with the most recent bars needed for pivot calculation
    df = df.tail(pivot_bar_count).copy()

    def is_strict_pivot_high(window: np.ndarray) -> bool:
        """Check if center value is a valid pivot high."""
        center_value = window[length]
        is_highest_left = center_value >= max(window[:length])
        is_strictly_higher_right = all(center_value > x for x in window[length + 1:])
        return is_highest_left and is_strictly_higher_right

    def is_strict_pivot_low(window: np.ndarray) -> bool:
        """Check if center value is a valid pivot low."""
        center_value = window[length]
        is_lowest_left = center_value <= min(window[:length])
        is_strictly_lower_right = all(center_value < x for x in window[length + 1:])
        return is_lowest_left and is_strictly_lower_right

    # Calculate pivot highs
    df.loc[:, "pivot_high"] = df["high"].rolling(window=pivot_bar_count, center=True).apply(
        lambda x: x[length] if is_strict_pivot_high(x) else np.nan,
        raw=True
    )

    # Calculate pivot lows
    df.loc[:, "pivot_low"] = df["low"].rolling(window=pivot_bar_count, center=True).apply(
        lambda x: x[length] if is_strict_pivot_low(x) else np.nan,
        raw=True
    )

    # Extract the most recent pivot values
    pivot_high = None
    pivot_low = None

    if not df["pivot_high"].isnull().all():
        pivot_high = df["pivot_high"].dropna().iloc[-1]

    if not df["pivot_low"].isnull().all():
        pivot_low = df["pivot_low"].dropna().iloc[-1]

    logger.info(f"Pivot based fetch:\n{str(df.tail())}")
    logger.info(f"Pivot high: {pivot_high}, Pivot low: {pivot_low}")

    # Return rounded values or 0.0 if not found
    result_pivot_high = round(pivot_high, 10) if pivot_high is not None else 0.0
    result_pivot_low = round(pivot_low, 10) if pivot_low is not None else 0.0

    return result_pivot_high, result_pivot_low

