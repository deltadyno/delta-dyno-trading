"""
Order creation and management for the DeltaDyno trading system.

This module provides functions for creating, canceling, and managing
option orders based on breakout signals and configuration parameters.
"""

import traceback
from datetime import datetime
from typing import List, Optional, Tuple

import pytz
from alpaca.common.exceptions import APIError
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import (
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
)

from deltadyno.constants import CALL, PUT, REVERSE_DOWNWARD, REVERSE_UPWARD
from deltadyno.utils.helpers import (
    adjust_order_quantities,
    adjust_order_quantities_per_fixed_amount,
    fetch_latest_option_quote,
    generate_option_symbol,
    identify_option_type,
)
from deltadyno.utils.timing import time_it


# =============================================================================
# Order Closing Functions
# =============================================================================

def close_order_for_symbol(
    trading_client,
    option_symbol: str,
    profile_idx: str,
    logger
) -> int:
    """
    Close all open limit orders for a specific symbol.

    Args:
        trading_client: Alpaca TradingClient instance
        option_symbol: The option symbol to close orders for
        profile_idx: Profile ID for logging
        logger: Logger instance

    Returns:
        Number of orders closed
    """
    closed_count = 0

    try:
        logger.debug("Fetching open orders...")
        orders = trading_client.get_orders(GetOrdersRequest(status="open"))

        if not orders:
            print(f"Client {profile_idx}: No limit orders found.")
            logger.info(f"Client {profile_idx}: No limit orders found.")
            return False

        for order in orders:
            logger.debug(f"Order: {order}")
            logger.debug(f"Symbol is: {order.symbol} and option symbol is {option_symbol}")

            if order.symbol == option_symbol:
                trading_client.cancel_order_by_id(order_id=order.id)
                print(f"Client {profile_idx}: Cancelled limit order for {order.symbol} with ID: {order.id}")
                logger.info(f"Client {profile_idx}: Cancelled limit order for {order.symbol} with ID: {order.id}")
                closed_count += 1

        return closed_count

    except APIError as api_err:
        logger.error(f"Client {profile_idx}: API Error in getting/closing positions: {api_err}")
        print(f"Client {profile_idx}: API Error in getting/closing positions: {api_err}")
        return closed_count

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(f"Client {profile_idx}: Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        print(f"Client {profile_idx}: Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        return closed_count


def close_all_orders_directional(
    trading_client,
    direction: str,
    profile_idx: str,
    logger
) -> int:
    """
    Close all open limit orders matching a specific direction (CALL or PUT).

    Args:
        trading_client: Alpaca TradingClient instance
        direction: Direction to match (REVERSE_UPWARD or REVERSE_DOWNWARD)
        profile_idx: Profile ID for logging
        logger: Logger instance

    Returns:
        Number of orders closed
    """
    closed_count = 0

    try:
        logger.debug("Fetching open orders...")
        orders = trading_client.get_orders(GetOrdersRequest(status="open"))

        if not orders:
            print(f"Client {profile_idx}: No limit orders found.")
            logger.info(f"Client {profile_idx}: No limit orders found.")
            return False

        for order in orders:
            symbol = order.symbol
            logger.debug(f"Symbol is: {symbol}")

            option_type = identify_option_type(symbol, logger)

            # Determine position side based on direction
            if (direction == REVERSE_UPWARD and option_type == PUT) or \
               (direction == REVERSE_DOWNWARD and option_type == CALL):

                trading_client.cancel_order_by_id(order_id=order.id)
                print(f"Client {profile_idx}: Cancelled limit order for {symbol} with ID: {order.id}")
                logger.info(f"Client {profile_idx}: Cancelled limit order for {symbol} with ID: {order.id}")
                closed_count += 1

        return closed_count

    except APIError as api_err:
        logger.error(f"Client {profile_idx}: API Error in getting/closing positions: {api_err}")
        print(f"Client {profile_idx}: API Error in getting/closing positions: {api_err}")
        return closed_count

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(f"Client {profile_idx}: Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        print(f"Client {profile_idx}: Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        return closed_count


# =============================================================================
# Order Creation Functions
# =============================================================================

def create_order(
    profile_id: str,
    trading_client,
    option_historicaldata_client,
    current_date: datetime,
    symbol: str,
    candle_size: float,
    bar_close: float,
    choppy_level: float,
    bar_strength: float,
    option_type: str,
    config,
    logger
) -> Optional[str]:
    """
    Create an order based on breakout signal and configuration.

    Args:
        profile_id: Client profile ID
        trading_client: Alpaca TradingClient instance
        option_historicaldata_client: Alpaca OptionHistoricalDataClient instance
        current_date: Current bar datetime
        symbol: Underlying symbol (e.g., 'SPY')
        candle_size: Size of the breakout candle
        bar_close: Closing price of the bar
        choppy_level: Current choppy level
        bar_strength: Bar strength value
        option_type: Option type ('C' for Call, 'P' for Put)
        config: Configuration object
        logger: Logger instance

    Returns:
        Option symbol if order was created, None otherwise
    """
    try:
        # Parse choppy trading days
        choppy_days_list = [
            datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
            for date_str in config.choppy_trading_days.split(",") if date_str.strip()
        ]

        # Parse buy range configurations
        try:
            choppy_buy_range = _parse_range(config.choppy_buy_range, "choppy_buy_range", logger)
            regular_buy_range = _parse_range(config.regular_buy_range, "regular_buy_range", logger)
        except ValueError as e:
            logger.error(f"Error parsing level ranges: {e}")
            choppy_buy_range = (0, 0)
            regular_buy_range = (0, 0)

        logger.debug(f"current_date.date(): {current_date.date()}, choppy_days_list: {choppy_days_list}")

        # Determine which properties to use
        use_choppy_properties = _should_use_choppy_properties(
            current_date, choppy_days_list, choppy_level, choppy_buy_range, logger
        )

        logger.debug(f"bar_strength: {bar_strength}")

        # Load appropriate configuration ranges
        order_params = _load_order_parameters(config, bar_strength, use_choppy_properties, logger)

        logger.debug(f"option_type: {option_type}")

        # Generate option symbol
        option_symbol = generate_option_symbol(
            symbol,
            config.get("option_expiry_day", False, int),
            config.option_expiry_day_flip_to_next_trading_day,
            config.get("cents_to_rollover_option_expiry", False, int) * 100,
            bar_close,
            option_type,
            datetime.now(),
            trading_client,
            logger=logger
        )

        if option_symbol is None:
            print(f"Failed to create order. Contact the culprit Gurpreet")
            logger.info(f"Failed to create order. Contact the culprit Gurpreet")
            return None

        # Place the order
        print(f"Client {profile_id} is creating order for {option_symbol}")
        _log_order_parameters(order_params, logger)

        result = place_option_order(
            trading_client,
            candle_size,
            order_params["candle_size_range"],
            order_params["limit_price_cutoff"],
            order_params["limit_order_qty_to_buy"],
            order_params["market_order_qty_to_buy"],
            order_params["max_order_amount"],
            order_params["buy_if_price_lt"],
            order_params["buy_for_amount"],
            option_historicaldata_client,
            option_symbol,
            logger=logger
        )

        if not result:
            print(f"Failed to create order. Contact the culprit Gurpreet")
            logger.info(f"Failed to create order. Contact the culprit Gurpreet")
            return None
        else:
            return option_symbol

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        print(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
        return None


def _parse_range(range_str: str, name: str, logger) -> Tuple[int, int]:
    """Parse a range string like '0-100' into a tuple."""
    if range_str.strip():
        return tuple(map(int, range_str.split('-')))
    else:
        logger.warning(f"{name} is empty. Defaulting to (0, 0).")
        return (0, 0)


def _should_use_choppy_properties(
    current_date: datetime,
    choppy_days_list: List[datetime.date],
    choppy_level: float,
    choppy_buy_range: Tuple[int, int],
    logger
) -> bool:
    """Determine whether to use choppy or regular properties."""
    # Priority 1: Check if current_date is in choppy_days_list
    if current_date.date() in choppy_days_list:
        logger.info(f"Priority 1: {current_date} is in choppy_days_list. Using choppy properties.")
        return True

    # Priority 2: Check if choppy_level falls in choppy_buy_range
    if choppy_buy_range[0] <= choppy_level <= choppy_buy_range[1]:
        logger.info(f"Priority 2: choppy_level {choppy_level} is in choppy_buy_range. Using choppy properties.")
        return True

    # Else, use regular properties
    logger.info(f"Priority 3: choppy_level {choppy_level} is in regular_buy_range. Using regular properties.")
    return False


def _load_order_parameters(config, bar_strength: float, use_choppy: bool, logger) -> dict:
    """Load order parameters from configuration."""
    if use_choppy:
        ranges = config.get_bar_order_ranges(bar_strength, order_type="choppy")
        logger.info("Loaded choppy properties.")
    else:
        ranges = config.get_bar_order_ranges(bar_strength)
        logger.info("Loaded regular properties.")

    logger.debug(f"ranges: {ranges}")

    return {
        "candle_size_range": [
            tuple(map(float, r.split('-'))) for r in ranges["candle_size_range"].split(',')
        ],
        "limit_price_cutoff": list(map(float, ranges["limit_order_cutoff_price"].split(','))),
        "limit_order_qty_to_buy": list(map(float, ranges["limit_order_qty_to_buy"].split(','))),
        "market_order_qty_to_buy": list(map(float, ranges["market_order_qty_to_buy"].split(','))),
        "max_order_amount": list(map(float, ranges["max_order_amount"].split(','))),
        "buy_if_price_lt": list(map(float, ranges["buy_if_price_lt"].split(','))),
        "buy_for_amount": list(map(float, ranges["buy_for_amount"].split(','))),
    }


def _log_order_parameters(params: dict, logger) -> None:
    """Log order parameters for debugging."""
    logger.debug(f"candle_size_range: {params['candle_size_range']}")
    logger.debug(f"limit_price_cutoff: {params['limit_price_cutoff']}")
    logger.debug(f"limit_order_qty_to_buy: {params['limit_order_qty_to_buy']}")
    logger.debug(f"market_order_qty_to_buy: {params['market_order_qty_to_buy']}")
    logger.debug(f"max_order_amount: {params['max_order_amount']}")
    logger.debug(f"buy_if_price_lt: {params['buy_if_price_lt']}")
    logger.debug(f"buy_for_amount: {params['buy_for_amount']}")


# =============================================================================
# Order Placement Functions
# =============================================================================

def place_option_order(
    trading_client,
    candle_high_low_diff: float,
    candle_size_range: List[Tuple[float, float]],
    limit_price_cutoff: List[float],
    limit_order_qty_to_buy: List[float],
    market_order_qty_to_buy: List[float],
    max_order_amountlist: List[float],
    buy_if_price_ltlist: List[float],
    buy_for_amountlist: List[float],
    option_historicaldata_client,
    symbol: str,
    logger
) -> bool:
    """
    Place market and limit orders for an option.

    Args:
        trading_client: Alpaca TradingClient instance
        candle_high_low_diff: Candle size (high - low)
        candle_size_range: List of candle size ranges
        limit_price_cutoff: Limit price cutoff values per range
        limit_order_qty_to_buy: Limit order quantities per range
        market_order_qty_to_buy: Market order quantities per range
        max_order_amountlist: Maximum order amounts per range
        buy_if_price_ltlist: Buy price thresholds per range
        buy_for_amountlist: Fixed buy amounts per range
        option_historicaldata_client: Alpaca option data client
        symbol: Option symbol
        logger: Logger instance

    Returns:
        True if at least one order was placed successfully
    """
    all_success = False
    limit_price = None

    logger.debug(f"symbol: {symbol}, candle_high_low_diff: {candle_high_low_diff}")

    # Fetch current option price
    option_price = fetch_latest_option_quote(option_historicaldata_client, symbol, logger)

    if option_price is None or option_price == 0.0:
        logger.error("No option price returned. Skipping order creation.")
        print("No option price returned. Skipping create order.")
        return all_success

    # Find matching range and get parameters
    fetch_limit_orderqty = None
    fetch_market_orderqty = None
    max_order_amount = None
    buy_if_price_lt = None
    buy_for_amount = None

    for i, (lower, upper) in enumerate(candle_size_range):
        if lower <= candle_high_low_diff < upper:
            limit_price = limit_price_cutoff[i]
            fetch_limit_orderqty = limit_order_qty_to_buy[i]
            fetch_market_orderqty = market_order_qty_to_buy[i]
            max_order_amount = max_order_amountlist[i]
            buy_if_price_lt = buy_if_price_ltlist[i]
            buy_for_amount = buy_for_amountlist[i]
            break

    # Validate price threshold
    if buy_if_price_lt is not None and option_price * 100 > buy_if_price_lt:
        logger.error(f"Option price: {option_price * 100} greater than configured price {buy_if_price_lt}. Skipping order creation.")
        print(f"Option price: {option_price * 100} greater than configured price {buy_if_price_lt}. Skipping order creation.")
        return all_success

    if limit_price is None:
        logger.debug(f"limit price is none. Skipping placing an order.")
        print(f"limit price is none. Skipping placing an order.")
        return all_success
    else:
        limit_price = option_price - limit_price
        if limit_price <= 0.0:
            logger.debug("Setting limit_price to 0.0 as it can't be negative.")
            limit_price = 0.0

    limit_price = round(limit_price, 2)

    # Adjust order quantities
    logger.debug(f"buy_for_amount: {buy_for_amount}")
    if buy_for_amount > 0:
        limit_orderqty, market_orderqty = adjust_order_quantities_per_fixed_amount(
            fetch_limit_orderqty, fetch_market_orderqty, option_price * 100, buy_for_amount
        )
    else:
        limit_orderqty, market_orderqty = adjust_order_quantities(
            fetch_limit_orderqty, fetch_market_orderqty, option_price, max_order_amount
        )

    print(f"Adjusted Limit Order Qty: {limit_orderqty}, Adjusted Market Order Qty: {market_orderqty}")
    logger.debug(f"Adjusted Limit Order Qty: {limit_orderqty}, Adjusted Market Order Qty: {market_orderqty}")

    if trading_client:
        try:
            # Default market order quantity
            if market_orderqty is None:
                print("Market Order quantity not found in the string. Defaulting to 1")
                logger.warning("Market Order quantity not found in the string. Defaulting to 1")
                market_orderqty = 1

            # Place market order
            market_order = place_single_order(
                trading_client, symbol, market_orderqty, limit_price, option_price, False, logger=logger
            )

            if market_order:
                print(f"Market Order submitted successfully for {symbol} for orderqty {market_orderqty} at option_price: {option_price} with candle size: {candle_high_low_diff}")
                logger.info(f"Market Order submitted successfully for {symbol} for orderqty {market_orderqty} at option_price: {option_price} with candle size: {candle_high_low_diff}")
                all_success = True

            logger.debug(f"Option Price: {option_price}, Limit Price: {limit_price}, Candle High-Low Diff: {candle_high_low_diff}")

            # Default limit order quantity
            if limit_orderqty is None:
                print("Limit Order quantity not found in the string. Defaulting to 1")
                logger.warning("Limit Order quantity not found in the string. Defaulting to 1")
                limit_orderqty = 1

            # Place limit order
            limit_order = place_single_order(
                trading_client, symbol, limit_orderqty, limit_price, option_price, True, logger=logger
            )

            if limit_order:
                print(f"Limit Order submitted successfully for {symbol} for orderqty {limit_orderqty} at option_price: {limit_price} with candle size: {candle_high_low_diff}")
                logger.info(f"Limit Order submitted successfully for {symbol} for orderqty {limit_orderqty} at option_price: {limit_price} with candle size: {candle_high_low_diff}")
                all_success = True

        except Exception as e:
            current_time = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
            error_msg = f"{current_time} - Error placing order for {symbol}: {e}"
            logger.error(error_msg)
            print(error_msg)
    else:
        logger.warning("Trading client is not initialized. Skipping...")
        print("Trading client is not initialized. Skipping...")

    return all_success


@time_it
def place_single_order(
    trading_client,
    symbol: str,
    orderqty: int,
    limit_price: float,
    option_price: float,
    create_limit_order: bool,
    logger
) -> Optional[dict]:
    """
    Place a single limit or market order.

    Args:
        trading_client: Alpaca TradingClient instance
        symbol: Option symbol to trade
        orderqty: Order quantity
        limit_price: Limit price (for limit orders)
        option_price: Current option price
        create_limit_order: True for limit order, False for market order
        logger: Logger instance

    Returns:
        Order response if successful, None otherwise
    """
    try:
        if orderqty < 1:
            order_type = "Limit" if create_limit_order else "Market"
            print(f"{order_type} Order creation skipped as order qty is configured {orderqty}.")
            logger.info(f"{order_type} Order creation skipped as order qty is configured {orderqty}.")
            return None

        # Prepare order request
        if create_limit_order:
            if limit_price <= 0.0:
                print(f"Limit Order creation skipped as limit price {limit_price} is less than 0, ask_price is {option_price}")
                logger.info(f"Limit Order creation skipped as limit price {limit_price} is less than 0, ask_price is {option_price}")
                return None

            logger.info(f"Order will be placed with limit_price: {limit_price}, whereas current ask_price is {option_price}")
            logger.info(f"Update position - Symbol: {symbol}, qty: {orderqty}, order_type: limit, side: buy, status: created, price: {option_price}")

            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=orderqty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price
            )
        else:
            logger.info(f"Update position - Symbol: {symbol}, qty: {orderqty}, order_type: market, side: buy, status: created, price: {option_price}")

            order_data = MarketOrderRequest(
                symbol=symbol,
                qty=orderqty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY
            )

        # Submit order
        market_order = trading_client.submit_order(order_data=order_data)
        logger.debug(f"Market Order --> {market_order}")
        return market_order

    except Exception as e:
        logger.error(f"Error placing order for {symbol}: {e}")
        print(f"Error placing order for {symbol}: {e}")
        return None


