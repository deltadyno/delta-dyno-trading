"""
Breakout detection for the DeltaDyno trading system.

This module provides the main breakout detection logic that evaluates
multiple conditions to determine valid breakout signals.
"""

from datetime import datetime
from typing import Optional, Tuple

from deltadyno.messaging.redis_queue import breakout_to_queue
from deltadyno.analysis.kalman import apply_kalman_filter
from deltadyno.constants import UPWARD, DOWNWARD
from deltadyno.utils.timing import time_it


@time_it
def check_for_breakouts(
    prev_kfilt: float,
    prev_velocity: float,
    enable_kalman_prediction: bool,
    skip_trading_days_list: list,
    latest_close_time: datetime,
    choppy_day_cnt: int,
    bar_head_cnt: int,
    maxvolume: int,
    min_gap_bars_cnt_for_breakout: int,
    positioncnt: int,
    positionqty: int,
    createorder: bool,
    upos: int,
    prev_upos: int,
    dnos: int,
    prev_dnos: int,
    bar_strength: float,
    latest_close: float,
    latest_open: float,
    latest_high: float,
    latest_low: float,
    skip_candle_with_size: float,
    volume: int,
    symbol: str,
    trading_client,
    redis_client,
    redis_queue_name_str: str,
    bar_date: datetime.date,
    logger
) -> Tuple[float, Optional[str], float, float]:
    """
    Check for upward or downward breakout conditions.

    Evaluates multiple constraints and signal validations before
    determining if a valid breakout has occurred.

    Args:
        prev_kfilt: Previous Kalman filter value
        prev_velocity: Previous velocity from Kalman filter
        enable_kalman_prediction: Whether to use Kalman filter validation
        skip_trading_days_list: List of dates to skip trading
        latest_close_time: Timestamp of latest close
        choppy_day_cnt: Choppy day indicator count
        bar_head_cnt: Bars since last breakout
        maxvolume: Maximum allowed volume
        min_gap_bars_cnt_for_breakout: Minimum bars between breakouts
        positioncnt: Current position count
        positionqty: Maximum allowed positions
        createorder: Whether order creation is enabled
        upos: Current upper position signal
        prev_upos: Previous upper position signal
        dnos: Current lower position signal
        prev_dnos: Previous lower position signal
        bar_strength: Calculated bar strength
        latest_close: Current closing price
        latest_open: Current opening price
        latest_high: Current high price
        latest_low: Current low price
        skip_candle_with_size: Maximum candle size to process
        volume: Current volume
        symbol: Trading symbol
        trading_client: Alpaca trading client
        redis_client: Redis client
        redis_queue_name_str: Redis queue name
        bar_date: Current bar date
        logger: Logger instance

    Returns:
        Tuple of (new_open, new_breakout_type, new_kfilt, new_velocity)
        - new_open: Open price if breakout detected, 0 otherwise
        - new_breakout_type: "upward", "downward", or None
        - new_kfilt: Updated Kalman filter value
        - new_velocity: Updated velocity value
    """
    logger.debug(
        f"Checking for breakouts: upos={upos}, prev_upos={prev_upos}, "
        f"dnos={dnos}, prev_dnos={prev_dnos}..."
    )

    # Initialize return values
    new_open = 0
    new_breakout_type = None
    kalman_velocity_determined = False

    # Calculate candle metrics
    candle_high_low_diff = round(latest_high - latest_low, 4)
    latest_close_date = latest_close_time.date()

    def check_constraints() -> bool:
        """Validate all trading constraints before allowing breakout."""
        # Check candle size
        if candle_high_low_diff > skip_candle_with_size:
            _log_skip(f"Candle size {candle_high_low_diff} exceeds limit {skip_candle_with_size}", logger)
            return False

        # Check volume
        if volume > maxvolume:
            _log_skip(f"Volume {volume} exceeds max {maxvolume}", logger)
            return False

        # Check position count
        if positioncnt >= positionqty:
            _log_skip(f"Position count {positioncnt} exceeds limit {positionqty}", logger)
            return False

        # Check minimum gap between breakouts
        if 0 < bar_head_cnt < min_gap_bars_cnt_for_breakout:
            _log_skip(
                f"Breakout within {bar_head_cnt} bars, less than required {min_gap_bars_cnt_for_breakout}",
                logger
            )
            return False

        # Check skip trading days
        if latest_close_date in skip_trading_days_list:
            _log_skip(f"Trading skipped for {latest_close_date} (in skip list)", logger)
            return False

        # Check market status
        if not trading_client.get_clock().is_open:
            _log_skip("Market is closed", logger)
            return False

        return True

    # Check for upward breakout
    if upos > prev_upos:
        logger.info("Upward breakout detected. Evaluating call option.")

        kfilt, velocity, is_bullish = prev_kfilt, prev_velocity, True

        if enable_kalman_prediction:
            kfilt, velocity, is_bullish = apply_kalman_filter(
                prev_kfilt, prev_velocity,
                latest_close, latest_open, latest_high, latest_low,
                logger=logger
            )
            kalman_velocity_determined = True

            # Allow if velocity is increasing even if not bullish
            if not is_bullish and velocity > prev_velocity:
                logger.debug("Setting bullish because velocity is increasing positively.")
                is_bullish = True

        if is_bullish:
            if check_constraints():
                if latest_close >= latest_open:
                    if createorder:
                        logger.debug("Sending to queue - Up")
                        result = breakout_to_queue(
                            symbol=symbol,
                            direction=UPWARD,
                            bar_strength=bar_strength,
                            close_time=latest_close_time,
                            close_price=latest_close,
                            candle_size=candle_high_low_diff,
                            volume=volume,
                            choppy_day_count=choppy_day_cnt,
                            logger=logger,
                            redis_client=redis_client,
                            queue_name=redis_queue_name_str
                        )
                        if result:
                            new_breakout_type = "upward"
                            new_open = latest_open
                    else:
                        _log_skip(f"Order creation disabled for {symbol}", logger)
                else:
                    _log_skip(f"Close {latest_close} < Open {latest_open} (bearish candle)", logger)
        else:
            _log_skip(f"Kalman filter returned bearish with velocity {velocity}", logger)

    # Check for downward breakout
    elif dnos > prev_dnos:
        logger.info("Downward breakout detected. Evaluating put option.")

        kfilt, velocity, is_bullish = prev_kfilt, prev_velocity, False

        if enable_kalman_prediction:
            kfilt, velocity, is_bullish = apply_kalman_filter(
                prev_kfilt, prev_velocity,
                latest_close, latest_open, latest_high, latest_low,
                logger=logger
            )
            kalman_velocity_determined = True

            # Confirm bearish if velocity is decreasing
            if is_bullish and velocity < prev_velocity:
                logger.debug("Setting bearish because velocity is decreasing.")
                is_bullish = False

        if not is_bullish:
            if check_constraints():
                if latest_close <= latest_open:
                    if createorder:
                        logger.debug("Sending to queue - Down")
                        result = breakout_to_queue(
                            symbol=symbol,
                            direction=DOWNWARD,
                            bar_strength=bar_strength,
                            close_time=latest_close_time,
                            close_price=latest_close,
                            candle_size=candle_high_low_diff,
                            volume=volume,
                            choppy_day_count=choppy_day_cnt,
                            logger=logger,
                            redis_client=redis_client,
                            queue_name=redis_queue_name_str
                        )
                        if result:
                            new_breakout_type = "downward"
                            new_open = latest_open
                    else:
                        _log_skip(f"Order creation disabled for {symbol}", logger)
                else:
                    _log_skip(f"Close {latest_close} > Open {latest_open} (bullish candle)", logger)
        else:
            _log_skip(f"Kalman filter returned bullish with velocity {velocity}", logger)

    # Ensure Kalman filter is always updated
    if not kalman_velocity_determined:
        kfilt, velocity, _ = apply_kalman_filter(
            prev_kfilt, prev_velocity,
            latest_close, latest_open, latest_high, latest_low,
            logger=logger
        )

    return new_open, new_breakout_type, kfilt, velocity


def _log_skip(reason: str, logger) -> None:
    """Log a skipped breakout with consistent formatting."""
    print(f"Skipping breakout: {reason}")
    logger.info(f"Skipping breakout: {reason}")

