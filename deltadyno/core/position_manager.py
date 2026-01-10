"""
Position management module for the DeltaDyno trading system.

This module handles:
- Processing and updating position bounds based on pivot points
- Updating position signals based on price movements
- Closing positions based on breakout reversals
"""

import math
import traceback
from datetime import datetime
from typing import Optional, Tuple

from alpaca.common.exceptions import APIError

from deltadyno.messaging.redis_queue import breakout_to_queue
from deltadyno.constants import (
    REVERSE_UPWARD,
    REVERSE_DOWNWARD,
    PUT,
    CALL,
    NO_POSITION_FOUND,
    POSITION_CLOSED,
    ERROR_OCCURRED,
    POSITION_CLOSE_SKIP,
)
from deltadyno.utils.helpers import log_exception, identify_option_type


# =============================================================================
# Position Processing
# =============================================================================

def process_positions(
    pivot_high: float,
    pivot_low: float,
    upper: float,
    lower: float,
    slope_ph: float,
    slope_pl: float,
    length: int,
    logger
) -> Tuple[float, float]:
    """
    Update upper and lower position bounds based on pivot points and slopes.

    When a new pivot is detected, the bound is set to that pivot value.
    Otherwise, the bound is adjusted by the corresponding slope value.

    Args:
        pivot_high: Detected pivot high value, or 0/None if not detected
        pivot_low: Detected pivot low value, or 0/None if not detected
        upper: Current upper bound value
        lower: Current lower bound value
        slope_ph: Slope value for pivot high adjustment
        slope_pl: Slope value for pivot low adjustment
        length: Data length parameter (unused but kept for API compatibility)
        logger: Logger instance for logging

    Returns:
        Tuple of (updated_upper, updated_lower) bounds
    """
    logger.debug(
        f"Processing positions with pivot high: {pivot_high}, pivot low: {pivot_low}, "
        f"upper: {upper}, lower: {lower}..."
    )

    # Update upper bound
    if pivot_high:
        upper = pivot_high
    elif slope_ph is not None and slope_ph != 0:
        upper -= float(slope_ph)

    # Update lower bound
    if pivot_low:
        lower = pivot_low
    elif slope_pl is not None and slope_pl != 0:
        lower += float(slope_pl)

    # Round to 10 decimal places for precision
    upper = round(upper, 10) if upper is not None else upper
    lower = round(lower, 10) if lower is not None else lower

    logger.info(f"Updated upper: {upper}, lower: {lower}")
    return upper, lower


def update_positions(
    latest_close: float,
    prev_upos: float,
    prev_dnos: float,
    upper: float,
    lower: float,
    pivot_high: float,
    pivot_low: float,
    slope_ph: float,
    slope_pl: float,
    length: int,
    logger
) -> Tuple[int, int]:
    """
    Update position signals based on current price relative to bounds.

    Position signals indicate whether the price has broken through the
    upper or lower bounds, accounting for slope adjustments.

    Args:
        latest_close: Current closing price
        prev_upos: Previous upper position signal
        prev_dnos: Previous lower (down) position signal
        upper: Current upper bound
        lower: Current lower bound
        pivot_high: Detected pivot high (if any)
        pivot_low: Detected pivot low (if any)
        slope_ph: Pivot high slope
        slope_pl: Pivot low slope
        length: Data length for slope projection
        logger: Logger instance

    Returns:
        Tuple of (upper_position_signal, lower_position_signal)
        Values are 1 if breakout detected, 0 otherwise
    """
    logger.debug(f"Close Price: {latest_close}, Upper: {upper}, Lower: {lower}")
    logger.debug(f"Slope PH: {slope_ph}, Slope PL: {slope_pl}")
    logger.debug(f"prev_upos: {prev_upos}, prev_dnos: {prev_dnos}, Length: {length}")

    # Calculate projected bounds
    upper_threshold = upper - float(slope_ph) * length
    lower_threshold = lower + float(slope_pl) * length
    logger.debug(f"Upper threshold: {upper_threshold}, Lower threshold: {lower_threshold}")

    # Update upper position signal
    if pivot_high:
        upper_position_signal = 0  # Reset on new pivot high
    elif latest_close > upper_threshold:
        upper_position_signal = 1  # Breakout above upper bound
    else:
        upper_position_signal = prev_upos

    # Update lower position signal
    if pivot_low:
        lower_position_signal = 0  # Reset on new pivot low
    elif latest_close < lower_threshold:
        lower_position_signal = 1  # Breakout below lower bound
    else:
        lower_position_signal = prev_dnos

    # Handle NaN values (convert to 0)
    upper_position_signal = 0 if math.isnan(upper_position_signal) else upper_position_signal
    lower_position_signal = 0 if math.isnan(lower_position_signal) else lower_position_signal

    logger.info(f"Updated positions: upos: {upper_position_signal}, dnos: {lower_position_signal}")
    return int(upper_position_signal), int(lower_position_signal)


# =============================================================================
# Position Closing
# =============================================================================

def close_positions(
    closeorder: bool,
    redis_queue_name: str,
    redis_client,
    bar_strength: float,
    latest_close_time: datetime,
    prev_open: float,
    prev_breakout_type: Optional[str],
    latest_close: float,
    symbol: str,
    volume: int,
    choppy_day_cnt: int,
    logger
) -> str:
    """
    Check if positions should be closed based on price reversal.

    Positions are closed when the price moves against the previous breakout
    direction (e.g., if a downward breakout was detected but price now 
    closes above the breakout open).

    Args:
        closeorder: Flag indicating if closing orders is enabled
        redis_queue_name: Name of the Redis queue for messages
        redis_client: Redis client connection
        bar_strength: Calculated bar strength
        latest_close_time: Timestamp of the latest close
        prev_open: Open price of the previous breakout candle
        prev_breakout_type: Type of previous breakout ('upward' or 'downward')
        latest_close: Current closing price
        symbol: Trading symbol
        volume: Trading volume
        choppy_day_cnt: Choppy day indicator count
        logger: Logger instance

    Returns:
        Status string indicating result:
        - POSITION_CLOSED: Position was closed
        - NO_POSITION_FOUND: No position to close
        - POSITION_CLOSE_SKIP: Closing was skipped (disabled or no reversal)
    """
    logger.debug(
        f"Closing positions - Latest Close: {latest_close}, "
        f"Previous Open: {prev_open}, Previous Breakout Type: {prev_breakout_type}"
    )

    # Check for downward breakout reversal (price moved up)
    if prev_breakout_type == "downward" and latest_close > prev_open:
        return _execute_position_close(
            closeorder=closeorder,
            symbol=symbol,
            direction=REVERSE_UPWARD,
            bar_strength=bar_strength,
            latest_close_time=latest_close_time,
            latest_close=latest_close,
            volume=volume,
            choppy_day_cnt=choppy_day_cnt,
            logger=logger,
            redis_client=redis_client,
            redis_queue_name=redis_queue_name
        )

    # Check for upward breakout reversal (price moved down)
    if prev_breakout_type == "upward" and latest_close < prev_open:
        return _execute_position_close(
            closeorder=closeorder,
            symbol=symbol,
            direction=REVERSE_DOWNWARD,
            bar_strength=bar_strength,
            latest_close_time=latest_close_time,
            latest_close=latest_close,
            volume=volume,
            choppy_day_cnt=choppy_day_cnt,
            logger=logger,
            redis_client=redis_client,
            redis_queue_name=redis_queue_name
        )

    return POSITION_CLOSE_SKIP


def _execute_position_close(
    closeorder: bool,
    symbol: str,
    direction: str,
    bar_strength: float,
    latest_close_time: datetime,
    latest_close: float,
    volume: int,
    choppy_day_cnt: int,
    logger,
    redis_client,
    redis_queue_name: str
) -> str:
    """
    Execute position close by publishing to Redis queue.

    Args:
        closeorder: Flag indicating if closing is enabled
        symbol: Trading symbol
        direction: Close direction (REVERSE_UPWARD or REVERSE_DOWNWARD)
        bar_strength: Bar strength value
        latest_close_time: Close timestamp
        latest_close: Close price
        volume: Trading volume
        choppy_day_cnt: Choppy day count
        logger: Logger instance
        redis_client: Redis client
        redis_queue_name: Queue name

    Returns:
        Status string indicating result
    """
    if not closeorder:
        print("close_positions: Skipping closing - config is disabled.")
        logger.info("close_positions: Skipping closing - config is disabled.")
        return POSITION_CLOSE_SKIP

    result = breakout_to_queue(
        symbol=symbol,
        direction=direction,
        bar_strength=bar_strength,
        close_time=latest_close_time,
        close_price=latest_close,
        candle_size=0.0,
        volume=volume,
        choppy_day_count=choppy_day_cnt,
        logger=logger,
        redis_client=redis_client,
        queue_name=redis_queue_name
    )

    return POSITION_CLOSED if result else NO_POSITION_FOUND


# =============================================================================
# Directional Position Closing
# =============================================================================

def close_positions_directional(
    trading_client,
    direction: str,
    profile_id: int,
    logger
) -> int:
    """
    Close all positions matching a specific direction.

    Used to close positions when a reversal signal is detected.
    Only closes US option positions matching the specified direction.

    Args:
        trading_client: Alpaca trading client
        direction: Direction to close (REVERSE_UPWARD or REVERSE_DOWNWARD)
        profile_id: Client profile identifier
        logger: Logger instance

    Returns:
        Number of positions closed, or -1 on error
    """
    try:
        positions = trading_client.get_all_positions()
        closed_count = 0

        for position in positions:
            try:
                logger.debug(f"Position Details: {str(position)}")
                symbol = position.symbol

                # Skip non-options positions
                if position.asset_class != "us_option":
                    logger.debug(
                        f"Skipping non-US option position: {symbol}, "
                        f"Asset Class: {position.asset_class}"
                    )
                    continue

                # Determine option type
                option_type = identify_option_type(symbol, logger)
                logger.debug(f"option_type: {option_type}")

                if option_type is None:
                    logger.warning(f"Unable to determine option type for: {symbol}")
                    continue

                # Check if position matches the close direction
                should_close = _should_close_position(direction, option_type)
                
                if not should_close:
                    logger.debug(
                        f"Skipping position: {symbol}, "
                        f"Direction {direction} - Option type {option_type} does not match"
                    )
                    continue

                logger.debug("Found a position to close.")

                # Attempt to close the position
                if handle_position_closing(trading_client, symbol, profile_id, logger):
                    closed_count += 1

            except Exception as position_error:
                logger.error(f"Error processing position: {position.symbol}. {position_error}")
                continue

        logger.info(f"Total positions closed: {closed_count}")
        return closed_count

    except Exception as e:
        log_exception("Unexpected error while closing positions", e, logger)
        return -1


def _should_close_position(direction: str, option_type: str) -> bool:
    """
    Determine if a position should be closed based on direction and option type.

    Args:
        direction: Close direction (REVERSE_UPWARD or REVERSE_DOWNWARD)
        option_type: Option type (PUT or CALL)

    Returns:
        True if position should be closed, False otherwise
    """
    return (
        (direction == REVERSE_UPWARD and option_type == PUT) or
        (direction == REVERSE_DOWNWARD and option_type == CALL)
    )


def handle_position_closing(
    trading_client,
    option_symbol: str,
    profile_idx: int,
    logger
) -> bool:
    """
    Handle the actual closing of a single position.

    Args:
        trading_client: Alpaca trading client
        option_symbol: Symbol of the option to close
        profile_idx: Client profile identifier
        logger: Logger instance

    Returns:
        True if position was closed successfully, False otherwise
    """
    try:
        # Check if position exists
        position = None
        try:
            position = trading_client.get_open_position(symbol_or_asset_id=option_symbol)
        except Exception as exception:
            if "position does not exist" in str(exception).lower():
                logger.info(
                    f"Client {profile_idx}: Position {option_symbol} "
                    "is already closed or does not exist."
                )
                print(
                    f"Client {profile_idx}: Position {option_symbol} "
                    "is already closed or does not exist."
                )
                return False
            else:
                logger.error(
                    f"Client {profile_idx}: Unexpected error fetching position "
                    f"{option_symbol}: {exception}"
                )
                return False

        logger.debug(f"Position: {position}")

        # Close the position
        logger.info(f"Client {profile_idx}: Closing position for {option_symbol}")
        trading_client.close_position(symbol_or_asset_id=option_symbol)
        
        logger.info(f"Client {profile_idx}: Position {option_symbol} closed successfully.")
        print(f"Client {profile_idx}: Position {option_symbol} closed successfully.")
        return True

    except APIError as api_err:
        logger.error(
            f"Client {profile_idx}: API Error closing position "
            f"{option_symbol}: {api_err}"
        )
        print(
            f"Client {profile_idx}: API Error closing position "
            f"{option_symbol}: {api_err}"
        )
        return False

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(
            f"Client {profile_idx}: Unexpected error closing {option_symbol} "
            f"at {error_time}: {e}\nTraceback:\n{error_traceback}"
        )
        print(
            f"Client {profile_idx}: Unexpected error closing {option_symbol} "
            f"at {error_time}: {e}"
        )
        return False

