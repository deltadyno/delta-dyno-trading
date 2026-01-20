"""
Position handling for the DeltaDyno trading system.

This module provides functions for processing positions, handling
position closing on reversal signals, and updating position state.
"""

import math
import traceback
from datetime import datetime
from typing import Optional, Tuple

from alpaca.common.exceptions import APIError

from deltadyno.constants import CALL, PUT, REVERSE_DOWNWARD, REVERSE_UPWARD
from deltadyno.messaging.redis_queue import breakout_to_queue
from deltadyno.utils.helpers import identify_option_type, log_exception


# =============================================================================
# Constants
# =============================================================================

NO_POSITION_FOUND = "no_position_found"
POSITION_CLOSED = "position_closed"
ERROR_OCCURRED = "error_occurred"
POSITION_CLOSE_SKIP = "position_close_skip"


# =============================================================================
# Position Processing Functions
# =============================================================================

def process_positions(
    pivot_high: Optional[float],
    pivot_low: Optional[float],
    upper: Optional[float],
    lower: Optional[float],
    slope_ph: Optional[float],
    slope_pl: Optional[float],
    length: int,
    logger
) -> Tuple[Optional[float], Optional[float]]:
    """
    Process positions based on pivot points and slopes.

    Args:
        pivot_high: Pivot high value
        pivot_low: Pivot low value
        upper: Current upper bound
        lower: Current lower bound
        slope_ph: Slope for pivot high
        slope_pl: Slope for pivot low
        length: Length parameter
        logger: Logger instance

    Returns:
        Tuple of (updated upper, updated lower)
    """
    logger.debug(
        f"Processing positions with pivot high: {pivot_high}, pivot low: {pivot_low}, "
        f"upper: {upper}, lower: {lower}..."
    )

    if pivot_high:
        upper = pivot_high
    else:
        if slope_ph is not None and slope_ph != 0:
            upper -= float(slope_ph)

    if pivot_low:
        lower = pivot_low
    else:
        if slope_pl is not None and slope_pl != 0:
            lower += float(slope_pl)

    # Round upper and lower to 10 decimal places
    if upper is not None:
        upper = round(upper, 10)
    if lower is not None:
        lower = round(lower, 10)

    logger.info(f"Updated upper: {upper}, lower: {lower}")
    return upper, lower


def update_positions(
    latest_close: float,
    prev_upos: int,
    prev_dnos: int,
    upper: float,
    lower: float,
    pivot_high: Optional[float],
    pivot_low: Optional[float],
    slope_ph: float,
    slope_pl: float,
    length: int,
    logger
) -> Tuple[int, int]:
    """
    Update position states based on price action.

    Args:
        latest_close: Latest closing price
        prev_upos: Previous upward position state
        prev_dnos: Previous downward position state
        upper: Upper bound
        lower: Lower bound
        pivot_high: Pivot high value
        pivot_low: Pivot low value
        slope_ph: Slope for pivot high
        slope_pl: Slope for pivot low
        length: Length parameter
        logger: Logger instance

    Returns:
        Tuple of (updated upos, updated dnos)
    """
    logger.debug(f"Close Price: {latest_close}, Upper: {upper}, Lower: {lower}, Slope PH: {slope_ph}, Slope PL: {slope_pl}")
    logger.debug(f"prev_upos: {prev_upos}, prev_dnos: {prev_dnos}, Length: {length}")
    logger.debug(f"Upper - Slope PH * Length: {upper - float(slope_ph) * length}, Lower + Slope PL * Length: {lower + float(slope_pl) * length}")

    # Update upos based on pivot_high and slope_ph
    if pivot_high:
        upos = 0
    else:
        if latest_close > upper - float(slope_ph) * length:
            upos = 1
        else:
            upos = prev_upos

    # Update dnos based on pivot_low and slope_pl
    if pivot_low:
        dnos = 0
    else:
        if latest_close < lower + float(slope_pl) * length:
            dnos = 1
        else:
            dnos = prev_dnos

    # Handle NaN values
    upos = 0 if math.isnan(upos) else upos
    dnos = 0 if math.isnan(dnos) else dnos

    logger.info(f"Updated positions: upos: {upos}, dnos: {dnos}")
    return upos, dnos


# =============================================================================
# Position Closing Functions
# =============================================================================

def close_positions_directional(
    trading_client,
    direction: str,
    profile_id: str,
    logger
) -> int:
    """
    Close all positions matching a specific direction (PUT or CALL based on reversal).

    Args:
        trading_client: Alpaca TradingClient instance
        direction: Direction (REVERSE_UPWARD or REVERSE_DOWNWARD)
        profile_id: Profile ID for logging
        logger: Logger instance

    Returns:
        Number of positions closed, or -1 on error
    """
    try:
        # Fetch all positions
        positions = trading_client.get_all_positions()
        closed_count = 0

        for position in positions:
            try:
                logger.debug(f"Position Details JSON -> {str(position)}")
                symbol = position.symbol

                # Skip non-options positions
                if position.asset_class != 'us_option':
                    logger.debug(f"Skipping non-US option position: {symbol}, Asset Class: {position.asset_class}")
                    continue

                option_type = identify_option_type(symbol, logger)

                logger.debug(f"option_type: {option_type}")
                if option_type is None:
                    logger.warning(f"Unable to determine option type for position: {symbol}")
                    continue

                # Determine if position should be closed based on direction
                if (direction == REVERSE_UPWARD and option_type == PUT) or \
                   (direction == REVERSE_DOWNWARD and option_type == CALL):
                    logger.debug("Found a position to close.")
                else:
                    logger.debug(f"Skipping position: {symbol}, Direction {direction} - OptionType {option_type} does not match put or call.")
                    continue

                # Attempt to close the position
                position_closed = handle_position_closing(trading_client, symbol, profile_id, logger)
                if position_closed:
                    closed_count += 1

            except Exception as position_error:
                logger.error(f"Error processing position: {position.symbol}. {position_error}")
                continue

        logger.info(f"Total positions closed: {closed_count}")
        return closed_count

    except Exception as e:
        log_exception("Unexpected error while closing positions", e, logger)
        return -1


def handle_position_closing(
    trading_client,
    option_symbol: str,
    profile_idx: str,
    logger
) -> bool:
    """
    Handle closing a single position.

    Args:
        trading_client: Alpaca TradingClient instance
        option_symbol: Option symbol to close
        profile_idx: Profile ID for logging
        logger: Logger instance

    Returns:
        True if position was closed successfully, False otherwise
    """
    try:
        position = None
        try:
            position = trading_client.get_open_position(symbol_or_asset_id=option_symbol)
        except Exception as exception:
            if "position does not exist" in str(exception).lower():
                logger.info(f"Client {profile_idx}: Position {option_symbol} is already closed or does not exist.")
                print(f"Client {profile_idx}: Position {option_symbol} is already closed or does not exist.")
                return False
            else:
                logger.error(f"Client {profile_idx}: Unexpected error while fetching position {option_symbol}: {exception}")
                return False

        logger.debug(f"Position: {position}")

        # Close the position
        logger.info(f"Client {profile_idx}: Closing position for {option_symbol}")
        trading_client.close_position(symbol_or_asset_id=option_symbol)
        logger.info(f"Client {profile_idx}: Position {option_symbol} closed successfully.")
        print(f"Client {profile_idx}: Position {option_symbol} closed successfully.")
        return True

    except APIError as api_err:
        logger.error(f"Client {profile_idx}: API Error in getting/closing market positions - {option_symbol}: {api_err}")
        print(f"Client {profile_idx}: API Error in getting/closing market positions - {option_symbol}: {api_err}")
        return False

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(f"Client {profile_idx}: Unexpected error while closing {option_symbol} at {error_time}: {e}\nTraceback:\n{error_traceback}")
        print(f"Client {profile_idx}: Unexpected error while closing {option_symbol} at {error_time}: {e}\nTraceback:\n{error_traceback}")
        return False


def close_positions(
    closeorder: bool,
    redis_queue_name: str,
    redis_client,
    bar_strength: float,
    latest_close_time: datetime,
    prev_open: float,
    prev_breakout_type: str,
    latest_close: float,
    symbol: str,
    volume: float,
    choppy_day_cnt: int,
    logger
) -> str:
    """
    Close positions based on price reversal conditions.

    Args:
        closeorder: Whether closing orders is enabled
        redis_queue_name: Redis queue name for breakout messages
        redis_client: Redis client instance
        bar_strength: Bar strength value
        latest_close_time: Latest close timestamp
        prev_open: Previous open price
        prev_breakout_type: Previous breakout type ('upward' or 'downward')
        latest_close: Latest close price
        symbol: Trading symbol
        volume: Bar volume
        choppy_day_cnt: Choppy day count
        logger: Logger instance

    Returns:
        Status string (POSITION_CLOSED, NO_POSITION_FOUND, POSITION_CLOSE_SKIP)
    """
    logger.debug(
        f"Closing positions - Latest Close Price: {latest_close} at breakout "
        f"previous open at {prev_open} and prev_breakout_type {prev_breakout_type}"
    )

    # Check for downward reversal (price crossing up)
    if prev_breakout_type == "downward" and latest_close > prev_open:
        if closeorder:
            result = breakout_to_queue(
                symbol, REVERSE_UPWARD, bar_strength, latest_close_time,
                latest_close, 0.0, volume, choppy_day_cnt, logger,
                redis_client, redis_queue_name
            )
            if result:
                return POSITION_CLOSED
            else:
                return NO_POSITION_FOUND
        else:
            print("close_positions: Skipping closing the positions as config is disabled.")
            logger.info("close_positions: Skipping closing the positions as config is disabled.")
            return POSITION_CLOSE_SKIP

    # Check for upward reversal (price crossing down)
    if prev_breakout_type == "upward" and latest_close < prev_open:
        if closeorder:
            result = breakout_to_queue(
                symbol, REVERSE_DOWNWARD, bar_strength, latest_close_time,
                latest_close, 0.0, volume, choppy_day_cnt, logger,
                redis_client, redis_queue_name
            )
            if result:
                return POSITION_CLOSED
            else:
                return NO_POSITION_FOUND
        else:
            print("close_positions: Skipping closing the positions as config is disabled.")
            logger.info("close_positions: Skipping closing the positions as config is disabled.")
            return POSITION_CLOSE_SKIP

    return POSITION_CLOSE_SKIP


