"""
Kalman filter implementation for the DeltaDyno trading system.

This module provides a simplified Kalman filter for signal smoothing
and trend estimation.
"""

from typing import Tuple

import numpy as np

from deltadyno.utils.timing import time_it


@time_it
def apply_kalman_filter(
    prev_kfilt: float,
    prev_velocity: float,
    latest_close: float,
    latest_open: float,
    latest_high: float,
    latest_low: float,
    logger
) -> Tuple[float, float, bool]:
    """
    Apply a simplified Kalman filter for signal smoothing.

    The Kalman filter helps smooth price movements and estimate
    the underlying trend velocity.

    Args:
        prev_kfilt: Previous filter value
        prev_velocity: Previous velocity estimate
        latest_close: Current closing price
        latest_open: Current opening price
        latest_high: Current high price
        latest_low: Current low price
        logger: Logger instance

    Returns:
        Tuple of (new_filter_value, new_velocity, is_bullish)
        is_bullish is True if velocity is positive
    """
    try:
        # Calculate source value (OHLC average)
        source = round((latest_close + latest_high + latest_low + latest_open) / 4, 10)

        # Get previous filter value (use source if first calculation)
        prev_filter = prev_kfilt if not np.isnan(prev_kfilt) else source

        # Calculate distance from previous estimate
        distance = round(source - prev_filter, 10)

        # Calculate error term with smoothing factor
        smoothing_factor = np.sqrt(1.0 / 100)
        error = round(prev_filter + distance * smoothing_factor, 10)

        # Calculate velocity with momentum factor
        momentum_factor = 1.0 / 100
        prev_vel = prev_velocity if not np.isnan(prev_velocity) else 0
        velocity = round(prev_vel + distance * momentum_factor, 10)

        # Calculate new filter value
        kfilt = error + velocity

        logger.debug(f"prev_kfilt: {prev_kfilt}, prev_velocity: {prev_velocity}, source: {source}, error: {error}")

        # Determine trend direction
        is_bullish = velocity > 0

        logger.info(f"Kalman filter - source: {source}, kfilt: {kfilt}, velocity: {velocity}, is_bullish: {is_bullish}")

        return kfilt, velocity, is_bullish

    except Exception as e:
        print(f"Error applying Kalman filter: {e}")
        logger.error(f"Error applying Kalman filter: {e}")
        # Return safe default values
        return prev_kfilt or 0.0, prev_velocity or 0.0, False

