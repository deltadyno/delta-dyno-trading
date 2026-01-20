"""
Trading constraints validation for the DeltaDyno trading system.

This module provides functions to validate various trading constraints
before allowing order creation or position opening.
"""

from datetime import datetime, time as datetime_time
from typing import List


def check_constraints(
    timezone_str: str,
    no_trade_start_time: datetime_time,
    no_trade_end_time: datetime_time,
    candle_size: float,
    skip_candle_with_size: float,
    volume: float,
    max_volume_threshold: int,
    open_position_cnt: int,
    max_daily_positions_allowed: int,
    bar_date: datetime,
    skip_trading_days_list: List[datetime.date],
    logger
) -> bool:
    """
    Validate all trading constraints before allowing order creation.

    Args:
        timezone_str: Timezone string (e.g., 'America/New_York')
        no_trade_start_time: Start time of no-trade window
        no_trade_end_time: End time of no-trade window
        candle_size: Size of the current candle (high - low)
        skip_candle_with_size: Maximum allowed candle size
        volume: Current bar volume
        max_volume_threshold: Maximum allowed volume
        open_position_cnt: Current number of open positions
        max_daily_positions_allowed: Maximum positions allowed per day
        bar_date: Current bar datetime
        skip_trading_days_list: List of dates to skip trading
        logger: Logger instance

    Returns:
        True if all constraints pass, False otherwise
    """
    # Check candle size constraint
    if candle_size > skip_candle_with_size:
        logger.debug(
            f"Constraint failed: Candle size {candle_size} > "
            f"skip_candle_with_size {skip_candle_with_size}"
        )
        print(
            f"Skipping: Candle size {candle_size} exceeds "
            f"threshold {skip_candle_with_size}"
        )
        return False

    # Check volume constraint
    if volume > max_volume_threshold:
        logger.debug(
            f"Constraint failed: Volume {volume} > "
            f"max_volume_threshold {max_volume_threshold}"
        )
        print(
            f"Skipping: Volume {volume} exceeds "
            f"threshold {max_volume_threshold}"
        )
        return False

    # Check position count constraint
    if open_position_cnt >= max_daily_positions_allowed:
        logger.debug(
            f"Constraint failed: Position count {open_position_cnt} >= "
            f"max_daily_positions_allowed {max_daily_positions_allowed}"
        )
        print(
            f"Skipping: Position count {open_position_cnt} exceeds "
            f"daily limit {max_daily_positions_allowed}"
        )
        return False

    # Check skip trading days
    if bar_date.date() in skip_trading_days_list:
        logger.debug(f"Constraint failed: {bar_date.date()} is in skip_trading_days_list")
        print(f"Skipping: Trading is disabled for {bar_date.date()}")
        return False

    # Check no-trade time window
    current_time = bar_date.time()
    if no_trade_start_time <= current_time <= no_trade_end_time:
        logger.debug(
            f"Constraint failed: Current time {current_time} is within "
            f"no-trade window {no_trade_start_time} - {no_trade_end_time}"
        )
        print(
            f"Skipping: Current time {current_time} is within "
            f"no-trade window"
        )
        return False

    logger.debug("All trading constraints passed")
    return True


def validate_order_parameters(
    option_price: float,
    buy_if_price_lt: float,
    limit_price: float,
    logger
) -> bool:
    """
    Validate order parameters before placing an order.

    Args:
        option_price: Current option price
        buy_if_price_lt: Maximum price threshold for buying
        limit_price: Calculated limit price
        logger: Logger instance

    Returns:
        True if parameters are valid, False otherwise
    """
    # Check option price against threshold
    if option_price * 100 > buy_if_price_lt:
        logger.error(
            f"Option price {option_price * 100} > configured max {buy_if_price_lt}. "
            f"Skipping order."
        )
        print(
            f"Option price {option_price * 100} > configured max {buy_if_price_lt}. "
            f"Skipping order."
        )
        return False

    # Check limit price
    if limit_price is None:
        logger.debug("Limit price is None. Skipping order.")
        print("Limit price is None. Skipping order.")
        return False

    return True


