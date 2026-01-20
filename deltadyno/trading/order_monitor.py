"""
Limit Order Monitor for the DeltaDyno trading system.

This module monitors open limit orders and converts them to market orders
or cancels them based on age and price conditions.
"""

import os
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from time import sleep
from typing import Dict, List, Optional, Tuple
from uuid import UUID

import pandas as pd
import pytz
import redis
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderSide
from alpaca.trading.requests import (
    GetCalendarRequest,
    GetOrdersRequest,
    ReplaceOrderRequest,
)

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader
from deltadyno.trading.orders import place_order
from deltadyno.utils.helpers import (
    fetch_latest_option_quote,
    generate_option_symbol,
    get_credentials,
    get_order_status,
    get_ssm_parameter,
)
from deltadyno.utils.logger import setup_logger


# =============================================================================
# Constants
# =============================================================================

MAX_SLEEP_SECONDS = 1800  # 30 minutes maximum sleep time
ORDER_CONFIRMATION_RETRIES = 3
ORDER_CONFIRMATION_DELAY = 0.5


# =============================================================================
# Data Classes
# =============================================================================

class OrderClassType(Enum):
    """Order class types."""
    SIMPLE = 'simple'
    BRACKET = 'bracket'


class OrderTypeValue(Enum):
    """Order types."""
    LIMIT = 'limit'
    STOP = 'stop'


class OrderSideValue(Enum):
    """Order sides."""
    BUY = 'buy'
    SELL = 'sell'


@dataclass
class LimitOrder:
    """Representation of a limit order for monitoring."""
    id: UUID
    client_order_id: UUID
    created_at: datetime
    updated_at: datetime
    submitted_at: datetime
    filled_at: Optional[datetime]
    expired_at: Optional[datetime]
    canceled_at: Optional[datetime]
    failed_at: Optional[datetime]
    replaced_at: Optional[datetime]
    replaced_by: Optional[UUID]
    replaces: Optional[UUID]
    asset_id: UUID
    symbol: str
    asset_class: str
    notional: Optional[float]
    qty: int
    filled_qty: int
    filled_avg_price: Optional[float]
    order_class: OrderClassType
    order_type: OrderTypeValue
    side: OrderSideValue
    position_intent: str
    time_in_force: str
    limit_price: Optional[float]
    stop_price: Optional[float]
    status: str
    extended_hours: bool
    legs: Optional[str]
    trail_percent: Optional[float]
    trail_price: Optional[float]
    hwm: Optional[float]
    subtag: Optional[str]
    source: Optional[str]
    expires_at: Optional[datetime]


# =============================================================================
# Client Initialization
# =============================================================================

def initialize_trading_client(
    config,
    api_key: str,
    api_secret: str,
    logger
) -> TradingClient:
    """
    Initialize Alpaca TradingClient.

    Args:
        config: Configuration with is_paper_trading setting
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        logger: Logger instance

    Returns:
        Configured TradingClient instance
    """
    is_paper = config.get("is_paper_trading", True, bool)
    print(f"Initializing Alpaca TradingClient (paper={is_paper}).")
    logger.info(f"Initializing Alpaca TradingClient (paper={is_paper}).")
    return TradingClient(api_key, api_secret, paper=is_paper, raw_data=True)


def initialize_option_historical_client(
    api_key: str,
    api_secret: str,
    logger
) -> OptionHistoricalDataClient:
    """
    Initialize Alpaca Option Historical Data Client.

    Args:
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        logger: Logger instance

    Returns:
        Configured OptionHistoricalDataClient instance
    """
    print("Initializing Alpaca Option Historical Data Client.")
    logger.info("Initializing Alpaca Option Historical Data Client.")
    return OptionHistoricalDataClient(api_key, api_secret)


# =============================================================================
# Market Hours
# =============================================================================

def get_regular_market_hours(
    trading_client: TradingClient,
    logger,
    target_date: Optional[datetime.date] = None
) -> Optional[Dict[str, datetime]]:
    """
    Get regular market hours for a specific date.

    Args:
        trading_client: Alpaca TradingClient instance
        logger: Logger instance
        target_date: Date to query (defaults to today UTC)

    Returns:
        Dictionary with regular_open and regular_close in UTC,
        or None if market is closed
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    logger.debug(f"target_date: {target_date}")

    calendar_request = GetCalendarRequest(start=target_date, end=target_date)
    market_calendar = trading_client.get_calendar(calendar_request)

    if not market_calendar:
        logger.info(f"No market calendar data available for {target_date}.")
        return None

    # Access dictionary values
    market_date = market_calendar[0]['date']
    market_open = market_calendar[0]['open']
    market_close = market_calendar[0]['close']

    # Parse strings into datetime objects
    market_open_time = datetime.strptime(f"{market_date} {market_open}", '%Y-%m-%d %H:%M')
    market_close_time = datetime.strptime(f"{market_date} {market_close}", '%Y-%m-%d %H:%M')

    # Localize to US/Eastern and convert to UTC
    eastern_tz = pytz.timezone("US/Eastern")
    market_open_localized = eastern_tz.localize(market_open_time)
    market_close_localized = eastern_tz.localize(market_close_time)

    market_open_utc = market_open_localized.astimezone(timezone.utc)
    market_close_utc = market_close_localized.astimezone(timezone.utc)

    return {
        "regular_open": market_open_utc,
        "regular_close": market_close_utc,
    }


# =============================================================================
# Configuration Parsing
# =============================================================================

def parse_config_ranges(config, keys: List[str]) -> Dict[str, List[float]]:
    """
    Parse comma-separated config values into float lists.

    Values are divided by 100 except for 'seconds_to_monitor_open_positions'.

    Args:
        config: Configuration object with attributes for each key
        keys: List of config keys to parse

    Returns:
        Dictionary mapping keys to lists of float values
    """
    try:
        result = {}
        for key in keys:
            raw_value = getattr(config, key, "")
            if not raw_value:
                print(f"Warning: Config key '{key}' is empty or not set")
                result[key] = []
                continue
            
            # Filter out empty strings from split (handles trailing commas)
            str_values = [v.strip() for v in raw_value.split(',') if v.strip()]
            if not str_values:
                print(f"Warning: Config key '{key}' has no valid values")
                result[key] = []
                continue
                
            values = list(map(float, str_values))
            if key != "seconds_to_monitor_open_positions":
                values = [v / 100 for v in values]
            result[key] = values
        return result
    except ValueError as e:
        print(f"Error while converting config values for key '{key}': {e}")
        raise


def calculate_dynamic_values(
    age_seconds: float,
    seconds_gap_list: List[float],
    sell_percent_list: List[float],
    price_threshold_list: List[float],
    create_percent_list: List[float],
    price_diff_check_list: List[float],
    logger
) -> Tuple[float, float, float, float, float, bool]:
    """
    Determine applicable values based on order age.

    Args:
        age_seconds: Order age in seconds
        seconds_gap_list: Time thresholds for each tier
        sell_percent_list: Sell percentage for each tier
        price_threshold_list: Price threshold for each tier
        create_percent_list: Create percentage for each tier
        price_diff_check_list: Price difference check for each tier
        logger: Logger instance

    Returns:
        Tuple of (seconds_range, sell_percent, price_threshold,
                  create_percent, price_diff_check, triggered_range)
    """
    logger.debug(f"age_seconds: {age_seconds}")
    logger.debug(f"seconds_gap_list before conversion: {seconds_gap_list}")

    # Ensure all gap list values are floats
    try:
        seconds_gap_list = [float(item) for item in seconds_gap_list]
    except ValueError as e:
        print(f"Error converting seconds_gap_list to float: {e}")
        logger.debug(f"Error converting seconds_gap_list to float: {e}")
        raise

    logger.debug(f"seconds_gap_list after conversion: {seconds_gap_list}")

    # Case 1: Age less than smallest gap - return defaults
    if age_seconds < seconds_gap_list[0]:
        logger.debug(f"age_seconds ({age_seconds}) < smallest gap. Returning defaults.")
        return (
            seconds_gap_list[0],
            sell_percent_list[0],
            price_threshold_list[0],
            create_percent_list[0],
            price_diff_check_list[0],
            False
        )

    # Case 2: Find matching range
    for i in range(len(seconds_gap_list) - 1):
        lower_bound = seconds_gap_list[i]
        upper_bound = seconds_gap_list[i + 1]

        if lower_bound <= age_seconds < upper_bound:
            logger.debug(f"age_seconds ({age_seconds}) in range [{lower_bound}, {upper_bound}).")
            return (
                seconds_gap_list[i],
                sell_percent_list[i],
                price_threshold_list[i],
                create_percent_list[i],
                price_diff_check_list[i],
                True
            )

    # Case 3: Age exceeds all gaps - return last values
    logger.debug(f"age_seconds ({age_seconds}) exceeds all gaps. Returning last values.")
    return (
        seconds_gap_list[-1],
        sell_percent_list[-1],
        price_threshold_list[-1],
        create_percent_list[-1],
        price_diff_check_list[-1],
        True
    )


# =============================================================================
# Order Processing
# =============================================================================

def truncate_isoformat(iso_str: str) -> str:
    """
    Truncate fractional seconds in an ISO string to 6 digits.

    Args:
        iso_str: ISO format datetime string

    Returns:
        Truncated ISO string with timezone offset
    """
    if '.' in iso_str:
        date_part, frac_part = iso_str.split('.')
        frac_part = frac_part[:6]
        if 'Z' in frac_part:
            frac_part = frac_part.split('Z')[0]
        iso_str = f"{date_part}.{frac_part}Z"
    return iso_str.replace('Z', '+00:00')


def process_order(
    order: pd.Series,
    now: datetime,
    config_ranges: Dict[str, List[float]],
    first_time_sales: Dict[str, float],
    time_age_spent: float,
    trading_client: TradingClient,
    option_client: OptionHistoricalDataClient,
    logger
) -> Tuple[Optional[str], float]:
    """
    Process a single order for potential cancellation or replacement.

    Args:
        order: Order data as pandas Series
        now: Current timestamp
        config_ranges: Parsed configuration ranges
        first_time_sales: Tracking dict for first-time sales per symbol
        time_age_spent: Accumulated age time
        trading_client: Alpaca TradingClient
        option_client: Alpaca Option Historical Data Client
        logger: Logger instance

    Returns:
        Tuple of (cancelled_order_id or None, updated time_age_spent)
    """
    logger.debug(f"Order --> {order}")
    order_id = order["id"]

    # Parse created_at timestamp
    created_at = order["created_at"].replace("Z", "")
    created_at_truncated = created_at[:26]
    order_created_at = datetime.strptime(
        created_at_truncated, "%Y-%m-%dT%H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)

    symbol = order["symbol"]
    qty = float(order["qty"])
    age_seconds = (now - order_created_at).total_seconds()

    # Skip if limit price is None
    if order["limit_price"] is None:
        print(f"Symbol: {symbol} - Qty {qty}, Age: {age_seconds}s. Skipping (limit price is None)")
        logger.info(f"Symbol: {symbol} - Qty {qty}, Age: {age_seconds}s. Skipping (limit price is None)")
        return None, time_age_spent

    canceled_price = float(order["limit_price"])

    logger.debug(f"time_age_spent: {time_age_spent}")
    logger.debug(f"age_seconds: {age_seconds}")

    # Add accumulated age for known symbols
    if symbol in first_time_sales:
        logger.debug(f"Adding seconds: {time_age_spent}")
        age_seconds = round(age_seconds + time_age_spent, 2)

    # Calculate dynamic values based on age
    (
        seconds_range, sell_percent, price_threshold,
        create_percent, sell_diff_check, triggered_range
    ) = calculate_dynamic_values(
        age_seconds,
        config_ranges["seconds_to_monitor_open_positions"],
        config_ranges["close_open_order_prcntage_of_open_qty"],
        config_ranges["regular_minus_limit_order_price_diff"],
        config_ranges["create_order_prcntage_of_open_qty"],
        config_ranges["close_open_if_price_diff_more_than"],
        logger
    )

    logger.debug(f"seconds_range: {seconds_range}")

    if not triggered_range:
        print(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has not met the first range {seconds_range}. Skipping")
        logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has not met the first range {seconds_range}. Skipping")
        print("\n")
        return None, time_age_spent

    # Already processed in this range
    # Match old implementation: check if symbol exists before accessing (line 539)
    if symbol in first_time_sales and seconds_range == first_time_sales[symbol]:
        print(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Already sold in this range {seconds_range}. Skipping")
        logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Already sold in this range {seconds_range}. Skipping")
        print("\n")
        return None, time_age_spent

    # Track previous range for potential rollback
    # Match old implementation: initialize to 0, then update if symbol exists
    prev_seconds_range = 0
    if symbol in first_time_sales:
        prev_seconds_range = first_time_sales[symbol]
        del first_time_sales[symbol]

    logger.debug(f"prev_seconds_range : {prev_seconds_range}")
    first_time_sales[symbol] = seconds_range  # Track canceled quantity

    # Fetch latest option quote
    # Match old implementation: parameter order is (client, symbol, logger)
    current_price = fetch_latest_option_quote(option_client, symbol, logger)

    if current_price is None:
        print(f"Symbol: {symbol}: qty {qty} skipping because current price fetched is None.")
        logger.info(f"Symbol: {symbol}: qty {qty} skipping because current price fetched is None.")
        return None, time_age_spent

    price_diff = round(abs(current_price - canceled_price), 3)

    logger.info(
        f"Symbol: {symbol}, qty: {qty}, Current_price: {current_price}, "
        f"canceled_price: {canceled_price}, price_diff: {price_diff}, "
        f"Price_Threshold: {price_threshold}, Sell_Percent: {sell_percent}, "
        f"Create_Percent: {create_percent}, sell_diff_check: {sell_diff_check}, age: {age_seconds}"
    )

    cancelled_order_id = None

    if price_diff <= price_threshold:
        # Price within threshold - process cancellation/replacement
        cancelled_order_id, time_age_spent = _handle_within_threshold(
            order_id, symbol, qty, age_seconds, current_price,
            price_diff, price_threshold, sell_diff_check,
            sell_percent, create_percent, time_age_spent,
            first_time_sales, trading_client, logger
        )
    else:
        # Price exceeds threshold
        cancelled_order_id, time_age_spent = _handle_exceeds_threshold(
            order_id, symbol, qty, age_seconds, current_price,
            price_diff, price_threshold, sell_diff_check,
            sell_percent, prev_seconds_range,
            time_age_spent, first_time_sales, trading_client, logger
        )

    print("\n")
    return cancelled_order_id, time_age_spent


def _handle_within_threshold(
    order_id: str,
    symbol: str,
    qty: float,
    age_seconds: float,
    current_price: float,
    price_diff: float,
    price_threshold: float,
    sell_diff_check: float,
    sell_percent: float,
    create_percent: float,
    time_age_spent: float,
    first_time_sales: Dict[str, float],
    trading_client: TradingClient,
    logger
) -> Tuple[Optional[str], float]:
    """Handle order when price difference is within threshold."""
    logger.debug("Price diff is less than threshold configured")
    cancelled_order_id = None
    create_sell_qty = 0

    if int(qty) == 1:
        # Single quantity - cancel entire order
        create_sell_qty = 1
        time_age_spent = 0.0
        trading_client.cancel_order_by_id(order_id=order_id)
        cancelled_order_id = order_id

        logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")

        print(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty {qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Pending qty : 0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
        logger.info(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty {qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Pending qty : 0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
    else:
        # Multiple quantities - partial cancel/replace
        create_sell_qty = max(1, int(qty * create_percent))
        pending_qty = max(1, int(qty - create_sell_qty))
        time_age_spent = age_seconds

        if int(create_sell_qty) == int(qty):
            trading_client.cancel_order_by_id(order_id=order_id)
            cancelled_order_id = order_id
            logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")
            logger.debug("Cancelled the order")
        else:
            trading_client.replace_order_by_id(order_id, ReplaceOrderRequest(qty=pending_qty))
            logger.info(f"Update position - Symbol: {symbol}, qty: {pending_qty}, order_type: limit, side: buy, status: replaced, price: {current_price}")
            logger.debug(f"time_age_spent is set to {age_seconds}")

        print(f"Symbol: {symbol} - Qty {qty - int(create_sell_qty)}, Successfully canceled for qty {create_sell_qty} at {create_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
        logger.info(f"Symbol: {symbol} - Qty {qty - int(create_sell_qty)}, Successfully canceled for qty {create_sell_qty} at {create_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")

    # Place new market order for cancelled quantity
    logger.info(f"Symbol: {symbol} - Placing new order for {create_sell_qty} qty at {create_percent:.2%} of original qty {qty} at price: {current_price}. PriceDiff : {price_diff}, PriceThreshold :{price_threshold}, sell_diff_check :{sell_diff_check}")
    print(f"Symbol: {symbol} - Placing new order for {create_sell_qty} qty at {create_percent:.2%} of original qty {qty} at price: {current_price}. PriceDiff : {price_diff}, PriceThreshold :{price_threshold}, sell_diff_check :{sell_diff_check}")

    place_order(trading_client, symbol, create_sell_qty, 0.0, current_price, False, logger)

    print(f"Symbol {symbol} : Market Order submitted successfully for order qty {create_sell_qty} at price {current_price}")
    logger.info(f"Symbol {symbol} : Market Order submitted successfully for order qty {create_sell_qty} at price {current_price}")

    # Clean up tracking if all quantities processed
    if int(qty) - int(create_sell_qty) < 1:
        time_age_spent = 0.0
        if symbol in first_time_sales:
            del first_time_sales[symbol]

    sleep(2)
    return cancelled_order_id, time_age_spent


def _handle_exceeds_threshold(
    order_id: str,
    symbol: str,
    qty: float,
    age_seconds: float,
    current_price: float,
    price_diff: float,
    price_threshold: float,
    sell_diff_check: float,
    sell_percent: float,
    prev_seconds_range: float,
    time_age_spent: float,
    first_time_sales: Dict[str, float],
    trading_client: TradingClient,
    logger
) -> Tuple[Optional[str], float]:
    """Handle order when price difference exceeds threshold."""
    cancelled_order_id = None

    if price_diff <= sell_diff_check:
        print(f"Symbol: {symbol} Qty {qty}, Age {age_seconds} seconds. Skip sell as Price_diff: {price_diff} less than equal to Sell Diff check: {sell_diff_check}")
        logger.info(f"Symbol: {symbol} - Qty {qty}, Age {age_seconds} seconds. Skip sell as Price_diff: {price_diff} less than equal to Sell Diff check: {sell_diff_check}")
        first_time_sales[symbol] = prev_seconds_range
        return None, time_age_spent

    if qty == 1 and sell_percent > 0:
        # Single quantity - cancel
        trading_client.cancel_order_by_id(order_id=order_id)
        logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")
        cancelled_order_id = order_id

        if symbol in first_time_sales:
            del first_time_sales[symbol]
        time_age_spent = 0.0

        print(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty 1 due to order age {age_seconds} seconds. Pending qty :0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
        logger.info(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty 1 due to order age {age_seconds} seconds. Pending qty :0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
        sleep(2)

    elif sell_percent > 0:
        # Multiple quantities - partial cancel
        # Note: Converting to string to match original implementation (line 639 of old code)
        sell_qty = str(max(1, int(qty * sell_percent)))
        pending_qty = max(1, int(qty - int(sell_qty)))
        time_age_spent = age_seconds

        if int(sell_qty) == int(qty):
            trading_client.cancel_order_by_id(order_id=order_id)
            logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")
            cancelled_order_id = order_id
        else:
            trading_client.replace_order_by_id(order_id, ReplaceOrderRequest(qty=pending_qty))
            logger.debug(f"time_age_spent set to {age_seconds}")
            logger.info(f"Update position - Symbol: {symbol}, qty: {pending_qty}, order_type: limit, side: buy, status: replaced, price: {current_price}")

        print(f"Symbol: {symbol} - Qty {qty - int(sell_qty)}, Successfully canceled for qty {sell_qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
        logger.info(f"Symbol: {symbol} - Qty {qty - int(sell_qty)}, Successfully canceled for qty {sell_qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")

        # Clean up if all quantities processed
        if int(qty) - int(sell_qty) < 1:  # sell_qty is string, convert to int
            time_age_spent = 0.0
            if symbol in first_time_sales:
                del first_time_sales[symbol]

        sleep(2)
    else:
        first_time_sales[symbol] = prev_seconds_range
        print(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has sell_percent {sell_percent} with prev sell was at {prev_seconds_range}. Skipping")
        logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has sell_percent {sell_percent} with prev sell was at {prev_seconds_range}. Skipping")

    return cancelled_order_id, time_age_spent


def confirm_order_cancellations(
    cancelled_orders: List[str],
    trading_client: TradingClient,
    logger
) -> None:
    """
    Confirm that cancelled orders have been processed.

    Args:
        cancelled_orders: List of order IDs that were cancelled
        trading_client: Alpaca TradingClient
        logger: Logger instance
    """
    logger.debug(f"Cancelled Orders --> {cancelled_orders}")

    for order_id in cancelled_orders:
        retries = 0

        while retries < ORDER_CONFIRMATION_RETRIES:
            # Match old implementation: get_order_status called without logger
            status = get_order_status(trading_client, order_id)

            if status == OrderStatus.CANCELED:
                logger.debug(f"Order {order_id} successfully cancelled.")
                break
            elif status is None:
                logger.debug(f"Retrying to fetch order {order_id} status...")
            else:
                logger.debug(f"Order {order_id} still in status: {status}. Retrying...")

            sleep(ORDER_CONFIRMATION_DELAY)
            retries += 1
        else:
            logger.debug(f"Warning: Order {order_id} cancellation confirmation failed after {ORDER_CONFIRMATION_RETRIES} retries.")


def calculate_sleep_time(
    market_hours: Optional[Dict[str, datetime]],
    config,
    logger
) -> float:
    """
    Calculate appropriate sleep time based on market hours.

    Args:
        market_hours: Dictionary with regular_open and regular_close
        config: Configuration object
        logger: Logger instance

    Returns:
        Sleep time in seconds
    """
    current_time = datetime.now(timezone.utc)
    sleeptime = config.get("close_pending_position_sleep_seconds", 1, float)

    if not market_hours:
        sleeptime = MAX_SLEEP_SECONDS
        logger.info(f"Market is closed. Sleeping for {sleeptime} seconds.")
    elif current_time < market_hours["regular_open"]:
        wake_up_time = market_hours["regular_open"]
        sleeptime = (wake_up_time - current_time).total_seconds()
        logger.info(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
        print(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
    elif current_time > market_hours["regular_close"]:
        wake_up_time = market_hours["regular_open"] + timedelta(days=1)
        sleeptime = (wake_up_time - current_time).total_seconds()
        logger.info(f"Market is closed. Sleeping until tomorrow's market opens at {wake_up_time} (UTC).")
        print(f"Market is closed. Sleeping until tomorrow's market opens at {wake_up_time} (UTC).")

    if sleeptime > MAX_SLEEP_SECONDS:
        sleeptime = MAX_SLEEP_SECONDS
        logger.debug(f"Defaulting sleep time to max {MAX_SLEEP_SECONDS} seconds.")

    return sleeptime


# =============================================================================
# Redis Breakout Message Processing
# =============================================================================

def process_breakout_messages(
    redis_client: redis.Redis,
    stream_name: str,
    last_id: str,
    config,
    trading_client: TradingClient,
    option_client: OptionHistoricalDataClient,
    logger
) -> str:
    """
    Read and process breakout messages from Redis stream.

    Args:
        redis_client: Redis client instance
        stream_name: Name of the Redis stream
        last_id: Last processed message ID (use '0' to start from beginning)
        config: Database configuration loader
        trading_client: Alpaca TradingClient
        option_client: Alpaca Option Historical Data Client
        logger: Logger instance

    Returns:
        New last processed message ID
    """
    try:
        # Read messages from Redis stream (non-blocking, read up to 10 messages)
        messages = redis_client.xread({stream_name: last_id}, count=10, block=0)
        
        if not messages:
            return last_id
        
        current_last_id = last_id
        
        for stream, stream_messages in messages:
            for msg_id, msg_data in stream_messages:
                try:
                    # Parse message data
                    symbol = msg_data.get("symbol", "")
                    direction = msg_data.get("direction", "")
                    close_price_str = msg_data.get("close_price", "")
                    
                    if not symbol or not direction or not close_price_str:
                        logger.warning(f"Invalid breakout message format: {msg_data}")
                        continue
                    
                    # Skip position close messages
                    if msg_data.get("action") == "close_position":
                        logger.debug(f"Skipping position close message: {msg_id}")
                        current_last_id = msg_id
                        continue
                    
                    # Parse close price
                    try:
                        close_price = float(close_price_str)
                    except ValueError:
                        logger.warning(f"Invalid close_price in message: {close_price_str}")
                        current_last_id = msg_id
                        continue
                    
                    # Determine option type based on direction
                    if direction == "upward":
                        option_type = "C"  # Call option
                    elif direction == "downward":
                        option_type = "P"  # Put option
                    else:
                        logger.warning(f"Unknown direction: {direction}, skipping")
                        current_last_id = msg_id
                        continue
                    
                    # Get configuration for option generation
                    open_position_expiry_trading_day = config.get("open_position_expiry_trading_day", 0, int)
                    option_expiry_day_flip_to_next_trading_day = config.get(
                        "option_expiry_day_flip_to_next_trading_day", "15:00", str
                    )
                    cents_to_rollover = config.get("cents_to_rollover", 50, int)
                    limit_order_qty = config.get("limit_order_qty", 1, int)
                    
                    # Generate option symbol
                    now = datetime.now(timezone.utc)
                    option_symbol = generate_option_symbol(
                        symbol=symbol,
                        open_position_expiry_trading_day=open_position_expiry_trading_day,
                        option_expiry_day_flip_to_next_trading_day=option_expiry_day_flip_to_next_trading_day,
                        cents_to_rollover=cents_to_rollover,
                        price=close_price,
                        option_type=option_type,
                        now=now,
                        trading_client=trading_client,
                        logger=logger
                    )
                    
                    if not option_symbol:
                        logger.warning(f"Failed to generate option symbol for {symbol} {direction}")
                        current_last_id = msg_id
                        continue
                    
                    # Get current option quote for limit price
                    current_price = fetch_latest_option_quote(
                        option_client, option_symbol, logger
                    )
                    
                    if current_price is None:
                        logger.warning(f"Could not fetch current price for {option_symbol}, using breakout price")
                        # Fallback to using the underlying stock price as a rough estimate
                        current_price = close_price
                    
                    # Check if order creation is enabled
                    create_order_enabled = config.get("create_order", True, bool)
                    if not create_order_enabled:
                        logger.info(f"Order creation disabled. Skipping breakout: {symbol} {direction}")
                        current_last_id = msg_id
                        continue
                    
                    # Place limit order
                    logger.info(
                        f"Processing breakout: {symbol} {direction} at ${close_price:.2f}, "
                        f"placing order for {option_symbol} at ${current_price:.2f}"
                    )
                    print(
                        f"Breakout detected: {symbol} {direction} -> Placing order for "
                        f"{option_symbol} (qty: {limit_order_qty}, price: ${current_price:.2f})"
                    )
                    
                    order_result = place_order(
                        trading_client=trading_client,
                        symbol=option_symbol,
                        qty=limit_order_qty,
                        stop_price=0.0,
                        limit_price=current_price,
                        is_limit_order=True,
                        logger=logger,
                        side=OrderSide.BUY
                    )
                    
                    if order_result:
                        logger.info(f"Successfully placed order for {option_symbol} from breakout message")
                    else:
                        logger.error(f"Failed to place order for {option_symbol}")
                    
                    current_last_id = msg_id
                    
                except Exception as e:
                    logger.error(
                        f"Error processing breakout message {msg_id}: {e}\n"
                        f"Traceback:\n{traceback.format_exc()}"
                    )
                    # Still update last_id to avoid reprocessing the same message
                    current_last_id = msg_id
        
        return current_last_id
        
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        return last_id
    except Exception as e:
        logger.error(
            f"Error reading breakout messages: {e}\n"
            f"Traceback:\n{traceback.format_exc()}"
        )
        return last_id


# =============================================================================
# Main Monitor Loop
# =============================================================================

def monitor_limit_orders(
    profile_id: str,
    config,
    trading_client: TradingClient,
    option_client: OptionHistoricalDataClient,
    redis_client: Optional[redis.Redis],
    redis_stream_name: str,
    logger
) -> None:
    """
    Main monitoring loop for limit orders.

    Args:
        profile_id: Client profile ID
        config: Database configuration loader
        trading_client: Alpaca TradingClient
        option_client: Alpaca Option Historical Data Client
        redis_client: Redis client for reading breakout messages (optional)
        redis_stream_name: Name of Redis stream for breakout messages
        logger: Logger instance
    """
    print(f"Client {profile_id}: {config.client_name} started monitoring limit orders.")
    logger.info(f"Client {profile_id}: {config.client_name} started monitoring limit orders.")
    print("********************************")
    logger.info("********************************")

    # Initialize tracking state
    first_time_sales: Dict[str, float] = defaultdict(lambda: 0.0)
    active_symbols: set = set()
    time_age_spent = 0.0
    prev_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    market_hours = get_regular_market_hours(trading_client, logger)
    
    # Initialize Redis stream tracking
    redis_last_id = "0"  # Start from beginning of stream

    while True:
        try:
            # Check if profile is active
            if not config.get_active_profile_id():
                print(f"Profile: {profile_id} is not active. Skipping")
                logger.info(f"Profile: {profile_id} is not active. Skipping")
                continue  # Skip to finally block for sleep
            
            # Process breakout messages from Redis (if Redis client is available)
            if redis_client and redis_stream_name:
                try:
                    redis_last_id = process_breakout_messages(
                        redis_client=redis_client,
                        stream_name=redis_stream_name,
                        last_id=redis_last_id,
                        config=config,
                        trading_client=trading_client,
                        option_client=option_client,
                        logger=logger
                    )
                except Exception as e:
                    logger.error(f"Error processing breakout messages: {e}")
                    # Continue with order monitoring even if Redis fails

            # Parse configuration ranges
            config_keys = [
                "seconds_to_monitor_open_positions",
                "close_open_order_prcntage_of_open_qty",
                "regular_minus_limit_order_price_diff",
                "create_order_prcntage_of_open_qty",
                "close_open_if_price_diff_more_than"
            ]
            config_ranges = parse_config_ranges(config, config_keys)

            # Validate that all required config ranges have values
            missing_configs = [k for k, v in config_ranges.items() if not v]
            if missing_configs:
                print(f"Missing config values for: {missing_configs}. Skipping this cycle.")
                logger.warning(f"Missing config values for: {missing_configs}. Skipping this cycle.")
                continue

            # Fetch open orders
            logger.debug("Fetching open orders...")
            orders = trading_client.get_orders(GetOrdersRequest(status="open"))

            if not orders:
                logger.info("No open orders found.")
                continue

            # Filter for US option orders
            orders_df = pd.DataFrame(orders)
            option_orders = orders_df.query('asset_class == "us_option"', engine="python")

            if option_orders.empty:
                logger.info("No US option orders to process.")
                continue

            logger.debug(f"Processing {len(option_orders)} US option orders.")
            now = datetime.utcnow().replace(tzinfo=timezone.utc)

            # Track cancelled orders for confirmation
            cancelled_orders: List[str] = []
            active_symbols.clear()

            # Process each order
            for _, order in option_orders.iterrows():
                try:
                    active_symbols.add(order["symbol"])
                    cancelled_id, time_age_spent = process_order(
                        order, now, config_ranges, first_time_sales,
                        time_age_spent, trading_client, option_client, logger
                    )
                    if cancelled_id:
                        cancelled_orders.append(cancelled_id)
                except Exception as e:
                    print(f"Unexpected error: {e}\n")
                    logger.error(f"Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
                    continue

            # Clean up symbols no longer active
            for symbol in list(first_time_sales.keys()):
                if symbol not in active_symbols:
                    del first_time_sales[symbol]

            logger.debug(f"first_time_sales: {first_time_sales}")

            # Confirm order cancellations
            confirm_order_cancellations(cancelled_orders, trading_client, logger)

        except Exception as e:
            print(f"Main Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
            logger.error(f"Main Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
            time_age_spent = 0.0

        finally:
            # Update market hours if date changed
            current_date = datetime.now(timezone.utc).date()
            logger.debug(f"Current date: {current_date}, prev date: {prev_date}")

            if current_date != prev_date:
                market_hours = get_regular_market_hours(trading_client, logger)
                prev_date = current_date

            sleeptime = calculate_sleep_time(market_hours, config, logger)

            print("********************************")
            logger.info("********************************")
            print(f"[{datetime.now(timezone.utc)}] - Sleep for {sleeptime} seconds")
            sleep(sleeptime)


def run_order_monitor(
    profile_id: str,
    log_to_console: bool = False
) -> None:
    """
    Entry point for the order monitor.

    Args:
        profile_id: Client profile ID
        log_to_console: If True, log to console instead of file
    """
    # Fetch credentials from SSM
    #api_key = get_ssm_parameter(f'profile{profile_id}_apikey')
    #api_secret = get_ssm_parameter(f'profile{profile_id}_apisecret')

    # Get API credentials
    api_key, api_secret = get_credentials(profile_id)

    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Load file configuration
    file_config = ConfigLoader(config_file='config/config.ini')

    # Initialize database configuration loader
    db_config_loader = DatabaseConfigLoader(
        profile_id=profile_id,
        db_host=file_config.db_host,
        db_user=file_config.db_user,
        db_password=file_config.db_password,
        db_name=file_config.db_name,
        tables=["dd_common_config", "dd_close_position_config"],
        refresh_interval=45
    )

    # Initialize logger
    log_file = os.path.join(logs_dir, f"trading_limit_order_monitor_{profile_id}.log")
    logger = setup_logger(
        db_config_loader,
        log_to_file=not log_to_console,
        file_name=log_file
    )

    # Initialize clients
    trading_client = initialize_trading_client(db_config_loader, api_key, api_secret, logger)
    option_client = initialize_option_historical_client(api_key, api_secret, logger)
    
    # Initialize Redis client for reading breakout messages
    redis_client = None
    redis_stream_name = file_config.redis_stream_name_breakout_message
    try:
        redis_client = initialize_redis_client(
            host=file_config.redis_host,
            port=file_config.redis_port,
            password=file_config.redis_password,
            logger=logger
        )
        logger.info(f"Redis client initialized. Will read from stream: {redis_stream_name}")
        print(f"Redis client initialized. Will read breakout messages from: {redis_stream_name}")
    except Exception as e:
        logger.warning(f"Failed to initialize Redis client: {e}. Breakout message processing disabled.")
        print(f"Warning: Redis client initialization failed. Breakout message processing disabled.")

    # Start monitoring
    monitor_limit_orders(
        profile_id=profile_id,
        config=db_config_loader,
        trading_client=trading_client,
        option_client=option_client,
        redis_client=redis_client,
        redis_stream_name=redis_stream_name,
        logger=logger
    )


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import argparse

    if len(sys.argv) < 2:
        print("Usage: python -m deltadyno.trading.order_monitor <profile_id>")
        sys.exit(1)

    profile_id = sys.argv[1]
    run_order_monitor(profile_id)

