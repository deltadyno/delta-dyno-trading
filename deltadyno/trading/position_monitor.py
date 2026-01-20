"""
Position monitoring and trailing stop loss management.

This module monitors active positions and closes them based on:
- Trailing stop loss thresholds
- Profit/loss targets
- Hard stop limits
- Time-based closures
"""

import math
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from time import sleep
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID

from alpaca.trading.enums import OrderSide
from alpaca.trading.requests import ClosePositionRequest

from deltadyno.utils.helpers import log_exception


# =============================================================================
# Data Classes and Enums
# =============================================================================

class AssetClassType(Enum):
    """Asset class types."""
    US_OPTION = 'us_option'
    US_EQUITY = 'us_equity'


class AssetExchangeType(Enum):
    """Asset exchange types."""
    EMPTY = ''


class PositionSideType(Enum):
    """Position side types."""
    LONG = 'long'
    SHORT = 'short'


@dataclass
class Position:
    """Representation of a trading position."""
    asset_class: str
    asset_id: UUID
    asset_marginable: bool
    avg_entry_price: str
    change_today: str
    cost_basis: str
    current_price: str
    exchange: AssetExchangeType
    lastday_price: str
    market_value: str
    qty: str
    qty_available: str
    side: PositionSideType
    symbol: str
    unrealized_intraday_pl: str
    unrealized_intraday_plpc: str
    unrealized_pl: str
    unrealized_plpc: str
    avg_entry_swap_rate: Optional[str] = None
    swap_rate: Optional[str] = None
    usd: Optional[str] = None


# =============================================================================
# Trailing Stop Loss Functions
# =============================================================================

def get_trailing_stop_loss_value(
    unrealized_plpc: float,
    ranges_list: List[Tuple[int, int]],
    stop_loss_values_list: List[float],
    default_stop_loss: float
) -> float:
    """
    Get trailing stop loss value based on unrealized profit/loss percentage.

    Args:
        unrealized_plpc: Unrealized profit/loss percentage (as decimal)
        ranges_list: List of (low, high) percentage ranges
        stop_loss_values_list: Stop loss values for each range
        default_stop_loss: Default stop loss if no range matches

    Returns:
        Applicable stop loss value
    """
    for i, (lower, upper) in enumerate(ranges_list):
        if lower <= unrealized_plpc * 100 < upper:
            return stop_loss_values_list[i]
    return default_stop_loss


def set_trailing_stop_loss(
    symbol: str,
    unrealized_plpc: float,
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict[str, float],
    ranges_list: List[Tuple[int, int]],
    stop_loss_values_list: List[float],
    default_stop_loss: float,
    logger
) -> None:
    """
    Set or update the trailing stop loss for a symbol.

    Args:
        symbol: Trading symbol
        unrealized_plpc: Current unrealized profit/loss percentage
        trailing_stop_loss_percentages: Dict tracking stop losses per symbol
        previous_unrealized_plpc: Dict tracking previous P/L per symbol
        ranges_list: Profit percentage ranges
        stop_loss_values_list: Stop loss values per range
        default_stop_loss: Default stop loss value
        logger: Logger instance
    """
    stop_loss_adjustment = get_trailing_stop_loss_value(
        unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss
    )
    trailing_stop = unrealized_plpc - stop_loss_adjustment

    # Update tracking dictionaries
    trailing_stop_loss_percentages[symbol] = trailing_stop
    previous_unrealized_plpc[symbol] = unrealized_plpc

    print(f"Setting trailing stop loss for {symbol} at {trailing_stop:.2%}")
    logger.info(f"Setting trailing stop loss for {symbol} at {trailing_stop:.2%}")


# =============================================================================
# Sell Quantity Determination
# =============================================================================

def determine_sell_quantity(
    unrealized_plpc: float,
    qty: str,
    symbol: str,
    first_time_sales: Dict,
    profit_percent_ranges: List[Tuple[int, int]],
    sell_quantity_percentages: List[float],
    expire_sale_seconds: int,
    logger
) -> int:
    """
    Determine the quantity to sell based on profit ranges.

    Args:
        unrealized_plpc: Unrealized profit/loss percentage
        qty: Available quantity
        symbol: Trading symbol
        first_time_sales: Dict tracking first sales per symbol/range
        profit_percent_ranges: List of (low, high) percentage ranges
        sell_quantity_percentages: Sell percentage for each range
        expire_sale_seconds: Seconds before a sale record expires
        logger: Logger instance

    Returns:
        Quantity to sell (0 if no action needed)
    """
    logger.debug(f"Determining sell quantity for {symbol} with unrealized profit: {unrealized_plpc:.2%}")
    logger.debug(f"Profit ranges: {profit_percent_ranges}: to_sell_qty : {qty}")
    print(f"[DEBUG] determine_sell_quantity called for symbol={symbol}, unrealized_plpc={unrealized_plpc}, qty={qty}")

    now_utc = datetime.utcnow()

    for i, (low, high) in enumerate(profit_percent_ranges):
        if low <= unrealized_plpc * 100 < high:
            # Initialize symbol tracking if needed
            if symbol not in first_time_sales:
                first_time_sales[symbol] = {}
                print(f"[DEBUG] {symbol} not in first_time_sales, initializing.")

            # Check if this range was already handled
            if i in first_time_sales[symbol]:
                recorded_utc = first_time_sales[symbol][i]
                if now_utc - recorded_utc > timedelta(seconds=expire_sale_seconds):
                    print(f"{symbol} in range {low}-{high}% was last recorded over {expire_sale_seconds}s ago. Resetting.")
                    logger.debug(f"{symbol} in range {low}-{high}% was last recorded over {expire_sale_seconds}s ago. Resetting.")
                    del first_time_sales[symbol][i]
                else:
                    print(f"Range {low}-{high}% already sold for {symbol} recently. No further action.")
                    logger.debug(f"Range {low}-{high}% already sold for {symbol} recently. No further action.")
                    return 0

            # Record this sale and calculate quantity
            # Now safe to set new timestamp and sell
            first_time_sales[symbol][i] = now_utc

            sell_percent = sell_quantity_percentages[i]
            sell_qty = max(math.floor(int(qty) * sell_percent), 1) if sell_percent > 0 else 0

            logger.debug(f"Range {low}-{high}% triggered for {symbol}. Selling {sell_qty} units.")
            return sell_qty

    print(f"[DEBUG] No matching profit range found for {symbol}. Returning 0.")
    return 0


# =============================================================================
# Position Operations
# =============================================================================

def close_position(
    symbol: str,
    qty: str,
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict[str, float],
    first_time_sales: Dict,
    full_close: bool,
    trading_client,
    current_price: float,
    logger
) -> None:
    """
    Close a position and update tracking dictionaries.

    Args:
        symbol: Trading symbol
        qty: Quantity to close
        trailing_stop_loss_percentages: Stop loss tracking dict
        previous_unrealized_plpc: Previous P/L tracking dict
        first_time_sales: First sales tracking dict
        full_close: Whether this is a full position close
        trading_client: Alpaca trading client
        current_price: Current price
        logger: Logger instance
    """
    logger.debug(f"Closing position: {symbol} for qty: {qty}")

    close_request = ClosePositionRequest(side=OrderSide.SELL, qty=qty)
    response = trading_client.close_position(symbol, close_options=close_request)

    print(f"Closed position for {symbol}")
    logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: market, side: sell, status: filled, price: {current_price}")
    logger.debug(f"Response: {response}")

    # Clean up tracking dictionaries
    if symbol in trailing_stop_loss_percentages:
        del trailing_stop_loss_percentages[symbol]
        del previous_unrealized_plpc[symbol]
        if full_close and symbol in first_time_sales:
            del first_time_sales[symbol]


def print_position_status(
    symbol: str,
    qty: str,
    close_all_at_min_profit: float,
    unrealized_plpc: float,
    hardstop: float,
    trailing_stop_loss_percentages: Dict[str, float],
    minimum_plpc: float,
    logger
) -> None:
    """
    Log the status of a position.

    Args:
        symbol: Trading symbol
        qty: Position quantity
        close_all_at_min_profit: Profit threshold for closing all
        unrealized_plpc: Unrealized profit/loss percentage
        hardstop: Hard stop loss percentage
        trailing_stop_loss_percentages: Stop loss tracking dict
        minimum_plpc: Minimum profit for stop loss activation
        logger: Logger instance
    """
    trailing_stop = trailing_stop_loss_percentages.get(symbol)

    # Match original message format with trailing space
    if trailing_stop is None:
        print(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: {minimum_plpc:.2%}, trailing_stop: NA, close_all_at_min_profit: {close_all_at_min_profit:.2%} ")
        logger.info(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: {minimum_plpc:.2%}, trailing_stop: NA, close_all_at_min_profit: {close_all_at_min_profit:.2%}")
    else:
        print(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: NA, trailing_stop: {trailing_stop:.2%}, close_all_at_min_profit: {close_all_at_min_profit:.2%} ")
        logger.info(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: NA, trailing_stop: {trailing_stop:.2%}, close_all_at_min_profit: {close_all_at_min_profit:.2%}")


def cleanup_inactive_symbols(
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict[str, float],
    first_time_sales: Dict,
    active_symbols: Set[str],
    logger
) -> None:
    """
    Remove symbols from tracking if they are no longer active.

    Args:
        trailing_stop_loss_percentages: Stop loss tracking dict
        previous_unrealized_plpc: Previous P/L tracking dict
        first_time_sales: First sales tracking dict
        active_symbols: Set of currently active symbols
        logger: Logger instance
    """
    # Clean trailing stop loss percentages
    for symbol in list(trailing_stop_loss_percentages.keys()):
        if symbol not in active_symbols:
            del trailing_stop_loss_percentages[symbol]

    # Clean previous unrealized P/L
    for symbol in list(previous_unrealized_plpc.keys()):
        if symbol not in active_symbols:
            del previous_unrealized_plpc[symbol]

    # Clean first time sales
    for symbol in list(first_time_sales.keys()):
        if symbol not in active_symbols:
            del first_time_sales[symbol]


# =============================================================================
# Main Position Monitoring
# =============================================================================

def monitor_positions_and_close(
    testcnt: int,
    previous_unrealized_plpc: Dict,
    trailing_stop_loss_percentages: Dict[str, float],
    first_time_sales: Dict,
    expire_sale_seconds: int,
    closeorder: bool,
    logger,
    close_all_at_min_profit: float,
    trading_client,
    minimum_plpc: float,
    ranges_list: List[Tuple[int, int]],
    stop_loss_values_list: List[float],
    stop_loss_quantity_sell_list: List[float],
    default_stop_loss: float,
    close_all_trade_time_str: str,
    tap_cnt_to_skip_hard_stop: int,
    cnt_to_skip_hard_stop: int,
    hardstop: float = 0.25
) -> Tuple[Dict[str, float], int]:
    """
    Monitor active positions and close them based on trailing stop loss and profit thresholds.

    Args:
        testcnt: Test counter (for debugging)
        previous_unrealized_plpc: Dict tracking previous P/L per symbol
        trailing_stop_loss_percentages: Dict tracking stop losses per symbol
        first_time_sales: Dict tracking first sales per symbol
        expire_sale_seconds: Seconds before sale record expires
        closeorder: Whether to actually close orders
        logger: Logger instance
        close_all_at_min_profit: Profit threshold for closing all
        trading_client: Alpaca trading client
        minimum_plpc: Minimum profit for stop loss activation
        ranges_list: Profit percentage ranges
        stop_loss_values_list: Stop loss values per range
        stop_loss_quantity_sell_list: Sell quantities per range
        default_stop_loss: Default stop loss value
        close_all_trade_time_str: Time string for closing all trades
        tap_cnt_to_skip_hard_stop: Counter for skipping hard stop
        cnt_to_skip_hard_stop: Max count to skip hard stop
        hardstop: Hard stop percentage

    Returns:
        Tuple of (updated trailing_stop_loss_percentages, updated tap_cnt_to_skip_hard_stop)
    """
    try:
        # Fetch all positions
        positions = trading_client.get_all_positions()
        close_all_trade_time = datetime.strptime(str(close_all_trade_time_str), "%H:%M").time()

        active_symbols: Set[str] = set()
        print("*************")
        logger.info("*************")

        for position in positions:
            try:
                logger.debug(f"Position Details json -> {str(position)}")

                # Skip non-option positions
                # Check if position asset class is 'us_equity' (original message says "non-US equity")
                if position.asset_class != 'us_option':
                    logger.debug(f"Skipping non-US equity position: {position.symbol}, position.asset_class: {position.asset_class}")
                    print(f"Skipping non-US equity position: {position.symbol}, position.asset_class: {position.asset_class}")
                    continue

                # Extract position data
                symbol = position.symbol
                unrealized_plpc = float(position.unrealized_plpc)
                current_price = float(position.current_price)
                qty = str(int(position.qty_available)) if hasattr(position, 'qty_available') else None

                # Skip if quantity is unavailable
                if qty is None or qty == "0":
                    print(f"Qty available is None/0. Skipping position -> {symbol}, qty {qty}")
                    logger.warning(f"Qty available is None/0. Skipping position -> {symbol}, qty {qty}")
                    continue

                active_symbols.add(symbol)
                print_position_status(
                    symbol, qty, close_all_at_min_profit, unrealized_plpc,
                    hardstop, trailing_stop_loss_percentages, minimum_plpc, logger
                )
                logger.debug(f"close_all_trade_time: {close_all_trade_time}, current time: {datetime.now().time()}")

                # Check if hard stop skip counter should apply
                if unrealized_plpc <= -hardstop and tap_cnt_to_skip_hard_stop < cnt_to_skip_hard_stop:
                    print(f"Skipping closing order. Hard stop skip {tap_cnt_to_skip_hard_stop} < Count configured ({cnt_to_skip_hard_stop})")
                    logger.info(f"Skipping closing order. Hard stop skip {tap_cnt_to_skip_hard_stop} < Count configured ({cnt_to_skip_hard_stop})")
                    tap_cnt_to_skip_hard_stop += 1
                    continue

                # Check for immediate close conditions
                should_close_all = (
                    unrealized_plpc <= -hardstop or
                    unrealized_plpc >= close_all_at_min_profit or
                    close_all_trade_time <= datetime.now().time()
                )

                if should_close_all:
                    # Match original message format exactly
                    print(f"Closing all - unrealizedPL {unrealized_plpc:.2%} either Hard stop {hardstop:.2%} has met, or max profit {close_all_at_min_profit:.2%} has met or close time {close_all_trade_time} is less than curren time {datetime.now().time()}.")
                    logger.info(f"Closing all - unrealizedPL {unrealized_plpc:.2%} either Hard stop {hardstop:.2%} has met, or max profit {close_all_at_min_profit:.2%} has met or close time {close_all_trade_time} is less than curren time {datetime.now().time()}.")

                    if closeorder:
                        close_position(
                            symbol, qty, trailing_stop_loss_percentages,
                            previous_unrealized_plpc, first_time_sales,
                            True, trading_client, current_price, logger
                        )
                        tap_cnt_to_skip_hard_stop = 0
                    else:
                        # Match original message exactly
                        print("Skipping closing order. Close Order flag is disabled")
                        logger.info("Skipping closing order. Close flag Order is disabled")
                    continue

                # Handle profitable positions - initial trailing stop setup
                if unrealized_plpc >= minimum_plpc and symbol not in trailing_stop_loss_percentages:
                    tap_cnt_to_skip_hard_stop = _handle_profitable_position(
                        symbol, qty, unrealized_plpc, current_price,
                        trailing_stop_loss_percentages, previous_unrealized_plpc,
                        first_time_sales, expire_sale_seconds, closeorder,
                        ranges_list, stop_loss_values_list, stop_loss_quantity_sell_list,
                        default_stop_loss, trading_client, logger
                    )

                # Handle trailing stop adjustment as profit rises
                elif symbol in trailing_stop_loss_percentages and unrealized_plpc > trailing_stop_loss_percentages[symbol]:
                    # Match original: direct comparison with previous_unrealized_plpc[symbol]
                    if unrealized_plpc > previous_unrealized_plpc[symbol]:
                        _handle_trailing_stop_adjustment(
                            symbol, qty, unrealized_plpc, current_price,
                            trailing_stop_loss_percentages, previous_unrealized_plpc,
                            first_time_sales, expire_sale_seconds, closeorder,
                            ranges_list, stop_loss_values_list, stop_loss_quantity_sell_list,
                            default_stop_loss, trading_client, logger
                        )

                # Trigger trailing stop loss if profit falls below stop
                elif symbol in trailing_stop_loss_percentages and unrealized_plpc <= trailing_stop_loss_percentages[symbol]:
                    if closeorder:
                        close_position(
                            symbol, qty, trailing_stop_loss_percentages,
                            previous_unrealized_plpc, first_time_sales,
                            True, trading_client, current_price, logger
                        )
                    else:
                        print("Skipping closing order. Close Order flag is disabled")
                        logger.info("Skipping closing order. Close Order flag is disabled")

                # Handle positions below minimum profit threshold
                elif unrealized_plpc < minimum_plpc:
                    _handle_loss_position(
                        symbol, qty, unrealized_plpc, current_price,
                        trailing_stop_loss_percentages, previous_unrealized_plpc,
                        first_time_sales, expire_sale_seconds, closeorder,
                        ranges_list, stop_loss_quantity_sell_list,
                        trading_client, logger
                    )
                else:
                    print(f"No action taken for {symbol}.")
                    logger.debug(f"No action taken for {symbol}.")

            except Exception as e:
                log_exception("Unexpected error. Moving to next position.", e, logger)
                continue

        # Cleanup inactive symbols
        cleanup_inactive_symbols(
            trailing_stop_loss_percentages, previous_unrealized_plpc,
            first_time_sales, active_symbols, logger
        )

    except Exception as e:
        log_exception("Unexpected error", e, logger)

    finally:
        print("*************")
        logger.info("*************")
        return trailing_stop_loss_percentages, tap_cnt_to_skip_hard_stop


def _handle_profitable_position(
    symbol: str,
    qty: str,
    unrealized_plpc: float,
    current_price: float,
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict,
    first_time_sales: Dict,
    expire_sale_seconds: int,
    closeorder: bool,
    ranges_list: List[Tuple[int, int]],
    stop_loss_values_list: List[float],
    stop_loss_quantity_sell_list: List[float],
    default_stop_loss: float,
    trading_client,
    logger
) -> int:
    """Handle a profitable position - sell portion and set trailing stop."""
    to_sell_qty = determine_sell_quantity(
        unrealized_plpc, qty, symbol, first_time_sales,
        ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger
    )

    if to_sell_qty > 0:
        print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")
        logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")

        full_close = int(qty) - int(to_sell_qty) < 1

        if closeorder:
            logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty}.")
            close_position(
                symbol, str(to_sell_qty), trailing_stop_loss_percentages,
                previous_unrealized_plpc, first_time_sales,
                full_close, trading_client, current_price, logger
            )
        else:
            print("Skipping close order. Close Order flag is disabled.")
            logger.info("Skipping closing order. Close flag is disabled.")

        if full_close:
            print("All quantity sold; trailing stop will not be set.")
            logger.debug("All quantity sold; trailing stop will not be set.")
            return 0
    else:
        print(f"No quantity closed for symbol: {symbol}.")
        logger.debug(f"No quantity closed for symbol: {symbol}")

    set_trailing_stop_loss(
        symbol, unrealized_plpc, trailing_stop_loss_percentages,
        previous_unrealized_plpc, ranges_list, stop_loss_values_list,
        default_stop_loss, logger
    )
    return 0


def _handle_trailing_stop_adjustment(
    symbol: str,
    qty: str,
    unrealized_plpc: float,
    current_price: float,
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict,
    first_time_sales: Dict,
    expire_sale_seconds: int,
    closeorder: bool,
    ranges_list: List[Tuple[int, int]],
    stop_loss_values_list: List[float],
    stop_loss_quantity_sell_list: List[float],
    default_stop_loss: float,
    trading_client,
    logger
) -> None:
    """Handle trailing stop adjustment as profit rises."""
    logger.debug(
        f"unrealized_plpc: {unrealized_plpc}, symbol: {symbol}, "
        f"first_time_sales: {first_time_sales}, previous_unrealized_plpc: {previous_unrealized_plpc.get(symbol)}"
    )

    to_sell_qty = determine_sell_quantity(
        unrealized_plpc, qty, symbol, first_time_sales,
        ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger
    )

    if to_sell_qty > 0:
        print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")
        logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")

        full_close = int(qty) - int(to_sell_qty) < 1

        if closeorder:
            logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty}.")
            close_position(
                symbol, str(to_sell_qty), trailing_stop_loss_percentages,
                previous_unrealized_plpc, first_time_sales,
                full_close, trading_client, current_price, logger
            )
        else:
            print("Skipping close order. Close Order flag is disabled.")
            logger.info("Skipping closing order. Close flag is disabled.")

        if full_close:
            print("All quantity sold; trailing stop will not be set.")
            logger.debug("All quantity sold; trailing stop will not be set.")
            return
    else:
        print(f"No quantity closed for symbol: {symbol}.")
        logger.debug(f"No quantity closed for symbol: {symbol}")

    set_trailing_stop_loss(
        symbol, unrealized_plpc, trailing_stop_loss_percentages,
        previous_unrealized_plpc, ranges_list, stop_loss_values_list,
        default_stop_loss, logger
    )


def _handle_loss_position(
    symbol: str,
    qty: str,
    unrealized_plpc: float,
    current_price: float,
    trailing_stop_loss_percentages: Dict[str, float],
    previous_unrealized_plpc: Dict,
    first_time_sales: Dict,
    expire_sale_seconds: int,
    closeorder: bool,
    ranges_list: List[Tuple[int, int]],
    stop_loss_quantity_sell_list: List[float],
    trading_client,
    logger
) -> None:
    """Handle a position below minimum profit threshold."""
    to_sell_qty = determine_sell_quantity(
        unrealized_plpc, qty, symbol, first_time_sales,
        ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger
    )

    if to_sell_qty > 0:
        print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} loss")
        logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} loss")

        full_close = int(qty) - int(to_sell_qty) < 1

        if closeorder:
            logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty} for loss.")
            close_position(
                symbol, str(to_sell_qty), trailing_stop_loss_percentages,
                previous_unrealized_plpc, first_time_sales,
                full_close, trading_client, current_price, logger
            )
        else:
            print("Skipping close order. Close Order flag is disabled.")
            logger.info("Skipping closing order. Close flag is disabled.")

        if full_close:
            print("All quantity sold; trailing stop will not be set.")
            logger.debug("All quantity sold; trailing stop will not be set.")
    else:
        print(f"No quantity closed for symbol: {symbol}.")
        logger.debug(f"No quantity closed for symbol: {symbol}")

