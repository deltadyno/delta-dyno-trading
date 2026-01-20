import argparse
import sys
import os
from logger_config import setup_logger, update_logger_level
from alpaca.trading.client import TradingClient
import logging
import configparser
from config_loader import ConfigLoader
from time import sleep
from datetime import datetime, timedelta, timezone, time as datetime_time  # Alias to avoid conflict
import pytz
import traceback
from credentials import credentials
from alpaca.trading.requests import GetOrdersRequest, ClosePositionRequest, GetCalendarRequest, ReplaceOrderRequest
import pandas as pd
from alpaca.data.historical import OptionHistoricalDataClient
from orders import place_order
from misc import fetch_latest_option_quote, get_credentials, get_order_status, get_ssm_parameter
from alpaca.data.requests import OptionLatestQuoteRequest
from collections import defaultdict
from alpaca.trading.enums import OrderStatus
from database_config_loader import DatabaseConfigLoader

# Initialize Alpaca API clients
def initialize_trading_client(config, api_key, api_secret, logger) -> TradingClient:
    print("Initializing Alpaca TradingClient with provided API keys.")
    logger.info("Initializing Alpaca TradingClient with provided API keys.")
    return TradingClient(api_key, api_secret, paper=config.get("is_paper_trading", True, bool), raw_data=True)

def initialize_option_historical_client(api_key, api_secret, logger) -> OptionHistoricalDataClient:
    print("Initializing Alpaca Option historical API")
    logger.info("Initializing Alpaca Option historical API")
    return OptionHistoricalDataClient(api_key, api_secret)

def get_regular_market_hours(trading_client, logger, target_date=None):
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    logger.debug(f"target_date: {target_date}")

    calendar_request = GetCalendarRequest(start=target_date, end=target_date)
    market_calendar = trading_client.get_calendar(calendar_request)

    if not market_calendar:
        logger.info(f"No market calendar data available for {target_date}.")
        return None

    # Access dictionary values using keys
    market_date = market_calendar[0]['date']  # Ensure correct date
    market_open = market_calendar[0]['open']
    market_close = market_calendar[0]['close']

    # Parse strings into datetime objects
    market_open_time = datetime.strptime(f"{market_date} {market_open}", '%Y-%m-%d %H:%M')
    market_close_time = datetime.strptime(f"{market_date} {market_close}", '%Y-%m-%d %H:%M')

    # Localize to US/Eastern
    eastern_tz = pytz.timezone("US/Eastern")
    market_open_localized = eastern_tz.localize(market_open_time)
    market_close_localized = eastern_tz.localize(market_close_time)

    # Convert to UTC
    market_open_utc = market_open_localized.astimezone(timezone.utc)
    market_close_utc = market_close_localized.astimezone(timezone.utc)

    # Extract open and close times
    #market_open = market_calendar[0].open
    #market_close = market_calendar[0].close


    # Localize to US/Eastern and adjust for DST
    #eastern_tz = pytz.timezone("US/Eastern")
    #market_open_localized = eastern_tz.localize(market_open)
    #market_close_localized = eastern_tz.localize(market_close)

    # Convert to UTC
    #market_open_utc = market_open_localized.astimezone(timezone.utc)
    #market_close_utc = market_close_localized.astimezone(timezone.utc)

    return {
        "regular_open": market_open_utc,
        "regular_close": market_close_utc,
    }

from dataclasses import dataclass
from enum import Enum
from typing import Optional
from uuid import UUID
from uuid import uuid4

# Enum definitions for OrderClass, OrderType, and OrderSide
class OrderClass(Enum):
    SIMPLE = 'simple'
    BRACKET = 'bracket'


class OrderType(Enum):
    LIMIT = 'limit'
    STOP = 'stop'


class OrderSide(Enum):
    BUY = 'buy'
    SELL = 'sell'


@dataclass
class Order:
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
    order_class: OrderClass
    order_type: OrderType
    side: OrderSide
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


def truncate_isoformat(iso_str: str) -> str:
    """Truncate the fractional seconds in an ISO string to 6 digits."""
    if '.' in iso_str:
        date_part, frac_part = iso_str.split('.')
        frac_part = frac_part[:6]  # Keep only up to 6 fractional digits
        if 'Z' in frac_part:
            frac_part = frac_part.split('Z')[0]
        iso_str = f"{date_part}.{frac_part}Z"
    return iso_str.replace('Z', '+00:00')


def get_orders():
    order1 = Order(
        id=UUID("79d38093-f9cf-4795-baa8-f08158d00e47"),
        client_order_id=UUID("2eb020b1-8ddb-477c-96f5-89a76136b87a"),
        created_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T10:28:00.819981059Z")),
        updated_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T21:44:22.819981059Z")),
        submitted_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T21:44:22.818936569Z")),
        filled_at=None,
        expired_at=None,
        canceled_at=None,
        failed_at=None,
        replaced_at=None,
        replaced_by=None,
        replaces=None,
        asset_id=UUID("f419585c-dd29-4c0d-b2eb-d3e0477140c8"),
        symbol="SPY241218C00604000",
        asset_class="us_option",
        notional=None,
        qty=11,
        filled_qty=0,
        filled_avg_price=None,
        order_class=OrderClass.SIMPLE,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        position_intent="buy_to_open",
        time_in_force="day",
        limit_price=1.25,
        stop_price=None,
        status="accepted",
        extended_hours=False,
        legs=None,
        trail_percent=None,
        trail_price=None,
        hwm=None,
        subtag=None,
        source=None,
        expires_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T21:15:00Z")),
    )

    order2 = Order(
        id=UUID("d7d38093-f9cf-4795-baa8-f08158d00e49"),
        client_order_id=UUID("4ab020b1-8ddb-477c-96f5-89a76136b88b"),
        created_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T10:29:00.819981059Z")),
        updated_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T22:00:00.819981059Z")),
        submitted_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T22:00:00.818936569Z")),
        filled_at=None,
        expired_at=None,
        canceled_at=None,
        failed_at=None,
        replaced_at=None,
        replaced_by=None,
        replaces=None,
        asset_id=UUID("a019585c-dd29-4c0d-b2eb-d3e0477140d9"),
        symbol="SPY241218P00604000",
        asset_class="us_option",
        notional=None,
        qty=5,
        filled_qty=0,
        filled_avg_price=None,
        order_class=OrderClass.SIMPLE,
        order_type=OrderType.LIMIT,
        side=OrderSide.SELL,
        position_intent="sell_to_open",
        time_in_force="gtc",
        limit_price=1.50,
        stop_price=None,
        status="pending_new",
        extended_hours=False,
        legs=None,
        trail_percent=None,
        trail_price=None,
        hwm=None,
        subtag=None,
        source=None,
        expires_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T22:15:00Z")),
    )

    return [order1, order2]

def get_orders2():
    order1 = Order(
        id=UUID("79d38093-f9cf-4795-baa8-f08158d00e47"),
        client_order_id=UUID("2eb020b1-8ddb-477c-96f5-89a76136b87a"),
        created_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T10:26:00.819981059Z")),
        updated_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T21:44:22.819981059Z")),
        submitted_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T21:44:22.818936569Z")),
        filled_at=None,
        expired_at=None,
        canceled_at=None,
        failed_at=None,
        replaced_at=None,
        replaced_by=None,
        replaces=None,
        asset_id=UUID("f419585c-dd29-4c0d-b2eb-d3e0477140c8"),
        symbol="SPY241218C00604000",
        asset_class="us_option",
        notional=None,
        qty=7,
        filled_qty=0,
        filled_avg_price=None,
        order_class=OrderClass.SIMPLE,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        position_intent="buy_to_open",
        time_in_force="day",
        limit_price=1.24,
        stop_price=None,
        status="accepted",
        extended_hours=False,
        legs=None,
        trail_percent=None,
        trail_price=None,
        hwm=None,
        subtag=None,
        source=None,
        expires_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T21:15:00Z")),
    )

    order2 = Order(
        id=UUID("d7d38093-f9cf-4795-baa8-f08158d00e49"),
        client_order_id=UUID("4ab020b1-8ddb-477c-96f5-89a76136b88b"),
        created_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T10:28:00.819981059Z")),
        updated_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T22:00:00.819981059Z")),
        submitted_at=datetime.fromisoformat(truncate_isoformat("2024-12-09T22:00:00.818936569Z")),
        filled_at=None,
        expired_at=None,
        canceled_at=None,
        failed_at=None,
        replaced_at=None,
        replaced_by=None,
        replaces=None,
        asset_id=UUID("a019585c-dd29-4c0d-b2eb-d3e0477140d9"),
        symbol="SPY241218P00604000",
        asset_class="us_option",
        notional=None,
        qty=2,
        filled_qty=0,
        filled_avg_price=None,
        order_class=OrderClass.SIMPLE,
        order_type=OrderType.LIMIT,
        side=OrderSide.SELL,
        position_intent="sell_to_open",
        time_in_force="gtc",
        limit_price=1.51,
        stop_price=None,
        status="pending_new",
        extended_hours=False,
        legs=None,
        trail_percent=None,
        trail_price=None,
        hwm=None,
        subtag=None,
        source=None,
        expires_at=datetime.fromisoformat(truncate_isoformat("2024-12-10T22:15:00Z")),
    )

    return [order1, order2]

def calculate_dynamic_values(age_seconds, close_pending_order_seconds_gap_list, close_pending_order_sell_percent_list,
                              order_post_limit_cancel_price_threshold_list, order_create_percentage_of_limit_list, close_pending_if_price_diff_more_than,
                              logger):
    """Determine the applicable values for sell percentage and thresholds based on order age."""

    # Print the initial list and age_seconds to ensure values are correct
    logger.debug(f"age_seconds: {age_seconds}")
    logger.debug(f"close_pending_order_seconds_gap_list before conversion: {close_pending_order_seconds_gap_list}")
    
    # Ensure all gap list values are floats for comparison
    try:
        close_pending_order_seconds_gap_list = [float(item) for item in close_pending_order_seconds_gap_list]
    except ValueError as e:
        print(f"Error converting items in close_pending_order_seconds_gap_list to float: {e}")
        logger.debug(f"Error converting items in close_pending_order_seconds_gap_list to float: {e}")
        raise
    
    # Print the list after conversion
    logger.debug(f"close_pending_order_seconds_gap_list after conversion to float: {close_pending_order_seconds_gap_list}")
    
#   Case 1: Return default values if age_seconds is less than the smallest gap
    if age_seconds < close_pending_order_seconds_gap_list[0]:
        logger.debug(f"age_seconds ({age_seconds}) is less than the smallest gap. Returning default values.")
        return (
            close_pending_order_seconds_gap_list[0],
            close_pending_order_sell_percent_list[0],
            order_post_limit_cancel_price_threshold_list[0],
            order_create_percentage_of_limit_list[0],
            close_pending_if_price_diff_more_than[0],
            False
        )
    
    # Case 2: Iterate through ranges and find the matching range
    for i in range(len(close_pending_order_seconds_gap_list) - 1):
        lower_bound = close_pending_order_seconds_gap_list[i]
        upper_bound = close_pending_order_seconds_gap_list[i + 1]
        
        if lower_bound <= age_seconds < upper_bound:
            logger.debug(f"age_seconds ({age_seconds}) falls between {lower_bound} and {upper_bound}. Returning corresponding values.")
            return (
                close_pending_order_seconds_gap_list[i],
                close_pending_order_sell_percent_list[i],
                order_post_limit_cancel_price_threshold_list[i],
                order_create_percentage_of_limit_list[i],
                close_pending_if_price_diff_more_than[i],
                True
            )
    
    # Case 3: Return the last set of values if age_seconds exceeds all gaps
    logger.debug(f"age_seconds ({age_seconds}) exceeds all gap ranges. Returning last set of values.")
    return (
        close_pending_order_seconds_gap_list[-1],
        close_pending_order_sell_percent_list[-1],
        order_post_limit_cancel_price_threshold_list[-1],
        order_create_percentage_of_limit_list[-1],
        close_pending_if_price_diff_more_than[-1],
        True
    )


def parse_config_ranges_old(config, keys):
    """Helper function to parse comma-separated config values into float lists."""
    try:
        return {key: list(map(float, getattr(config, key).split(','))) for key in keys}
    except ValueError as e:
        print(f"Error while converting config values: {e}")
        raise

def parse_config_ranges(config, keys):
    """Helper function to parse comma-separated config values into float lists,
    dividing each value by 100 except for 'seconds_to_monitor_open_positions'."""
    try:
        result = {}
        for key in keys:
            values = list(map(float, getattr(config, key).split(',')))
            if key != "seconds_to_monitor_open_positions":
                values = [v / 100 for v in values]
            result[key] = values
        return result
    except ValueError as e:
        print(f"Error while converting config values: {e}")
        raise


def handle_market_hours(trading_client, logger, config):
    """Handle market hours logic and determine appropriate sleep time."""
    market_hours = get_regular_market_hours(trading_client, logger)
    current_time = datetime.now(timezone.utc)
    sleeptime = config.get("close_pending_position_sleep_seconds", 1, float)

    if not market_hours:
        sleeptime = 1800
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
    if sleeptime > 1800:
        sleeptime = 1800
        logger.debug("Defaulting sleep time to max 1800 seconds.")

    print("********************************")
    logger.info("********************************")

    print(f"[{current_time}] - Sleep for {sleeptime} seconds")
    #sleeptime = 2
    sleep(sleeptime)
    return

def main(profile_id, config, trading_client, option_historicaldata_client, logger):
    print(f"Client {profile_id} : {config.client_name} started monitoring limit orders.")
    logger.info(f"Client {profile_id} : {config.client_name} started monitoring limit orders.")

    print("********************************")
    logger.info("********************************")

    cnt = 1
    
    # Initialize the dictionary for first-time sales
    first_time_sales = defaultdict(lambda: 0.0)

    active_symbols = set()

    time_age_spent = 0.0

    prev_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    market_hours = get_regular_market_hours(trading_client, logger)

    while True:
        try:

            if not config.get_active_profile_id():
                print(f"Profile: {profile_id} is not active. Skipping")
                logger.info(f"Profile: {profile_id} is not active. Skipping")

            # Parse configuration
            config_keys = [
                "seconds_to_monitor_open_positions",
                "close_open_order_prcntage_of_open_qty",
                "regular_minus_limit_order_price_diff",
                "create_order_prcntage_of_open_qty",
                "close_open_if_price_diff_more_than"
            ]
            config_ranges = parse_config_ranges(config, config_keys)

            logger.debug("Fetching open orders...")
            orders = trading_client.get_orders(GetOrdersRequest(status="open"))
            #if cnt == 1:
            #    orders = get_orders()
            #else:
            #    orders = get_orders2()

            cnt=cnt+1
            if not orders:
                logger.info("No open orders found.")
                continue

            # Filter for SPY option orders
            orders_df = pd.DataFrame(orders)
            orders_to_cancel = orders_df.query('asset_class == "us_option"', engine="python")
            if orders_to_cancel.empty:
                logger.info("No SPY option orders to process.")
                continue

            logger.debug(f"Processing {len(orders_to_cancel)} SPY option orders.")
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            #now = datetime(2024, 12, 10, 10, 30, 0, tzinfo=timezone.utc)

            # List to store orders pending cancellation confirmation
            cancelled_orders = []


            for _, order in orders_to_cancel.iterrows():
                
                try:
                    logger.debug(f"Order --> {order}")
                    order_id = order["id"]
                    #order_created_at = datetime.fromisoformat(str(order["created_at"])).replace(tzinfo=timezone.utc)
                    created_at = order["created_at"].replace("Z", "")  # Remove the 'Z'
                    created_at_truncated = created_at[:26]  # Keep only up to microseconds
                    order_created_at = datetime.strptime(created_at_truncated, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)

                    symbol = order["symbol"]
                    qty = float(order["qty"])
                    age_seconds = (now - order_created_at).total_seconds()
      
                    if order["limit_price"] is None:
                        print(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Skipping as limit price is None")
                        logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Skipping as limit price is None")
                        continue

                    canceled_price = float(order["limit_price"])
                    
                    active_symbols.add(symbol)


                    logger.debug(f"time_age_spent {time_age_spent}")
                    logger.debug(f"age_seconds {age_seconds}")

                    #add back the age seconds
                    if symbol in first_time_sales:
                        #print(f"Adding age seconds : {first_time_sales[symbol]}")
                        logger.debug(f"Adding seconds {time_age_spent}")
                        age_seconds = round(age_seconds + time_age_spent, 2)
                        #print(f"New age seconds: {age_seconds}")

                    seconds_range, sell_percent, price_threshold, create_percent, sell_diff_check, triggeredRange = calculate_dynamic_values(
                        age_seconds,
                        config_ranges["seconds_to_monitor_open_positions"],
                        config_ranges["close_open_order_prcntage_of_open_qty"],
                        config_ranges["regular_minus_limit_order_price_diff"],
                        config_ranges["create_order_prcntage_of_open_qty"],
                        config_ranges["close_open_if_price_diff_more_than"],
                        logger
                    )

                    logger.debug(f"seconds_range : {seconds_range}")
                    if triggeredRange:
                        if seconds_range == first_time_sales[symbol]:
                            print(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Already sold in this range {seconds_range}. Skipping")
                            logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds : {age_seconds}. Already sold in this range {seconds_range}. Skipping")
                        else:
                            
                            prev_seconds_range = 0
                            # Delete any old signal if present and add the new one
                            if symbol in first_time_sales:
                                prev_seconds_range = first_time_sales[symbol]
                                del first_time_sales[symbol]

                            logger.debug(f"prev_seconds_range : {prev_seconds_range}")

                            first_time_sales[symbol] = seconds_range  # Track canceled quantity
                            
                            # Fetch the latest quote to compare
                            current_price = fetch_latest_option_quote(option_historicaldata_client, symbol, logger)

                            if current_price is None:
                                print(f"Symbol: {symbol}: qty {qty} skipping because current price fetched is None.")
                                logger.info(f"Symbol: {symbol}: qty {qty} skipping because current price fetched is None.")
                                continue

                            price_diff = round(abs(current_price - canceled_price), 3)
                            
                            #print(f"Symbol: {symbol}: qty {qty}, Current price {current_price}, canceled_price: {canceled_price}, price_diff: {price_diff},"
                                         #f" Price Threshold: {price_threshold}, Sell Percent: {sell_percent}, Create Percent: {create_percent}, sell_diff_check :{sell_diff_check}")
                            logger.info(f"Symbol: {symbol}, qty: {qty}, Current_price: {current_price}, canceled_price: {canceled_price}, price_diff: {price_diff},"
                                         f" Price_Threshold: {price_threshold}, Sell_Percent: {sell_percent}, Create_Percent: {create_percent}, sell_diff_check: {sell_diff_check}, age: {age_seconds}")


                            if price_diff <= price_threshold:
                                logger.debug("Price diff is less than threshold configured")
                                create_sell_qty = 0
                                if int(qty) == 1:
                                    create_sell_qty = 1
                                    time_age_spent = 0.0 # cancelling all the quantities
                                    trading_client.cancel_order_by_id(order_id=order_id)
                                    cancelled_orders.append(order_id)
                                    logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")

                                    print(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty {qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Pending qty : 0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                    logger.info(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty {qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Pending qty : 0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                
                                else:
                                    # if price is within the threshold then cancel the percent
                                    create_sell_qty = max(1, int(qty * create_percent))
                                    pending_qty = max(1,qty - int(create_sell_qty))
                                    time_age_spent = age_seconds
 
                                    # seems selling 100% here
                                    if int(create_sell_qty) == int(qty):
                                        trading_client.cancel_order_by_id(order_id=order_id)
                                        cancelled_orders.append(order_id)
                                        logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")

                                        logger.debug("Cancelled the order")
                                    else:
                                        trading_client.replace_order_by_id(order_id, ReplaceOrderRequest(qty = pending_qty))
                                        logger.info(f"Update position - Symbol: {symbol}, qty: {pending_qty}, order_type: limit, side: buy, status: replaced, price: {current_price}")
                                        logger.debug(f"time_age_spent is set to {age_seconds}")
                                    
                                    print(f"Symbol: {symbol} - Qty {qty - int(create_sell_qty)}, Successfully canceled for qty {create_sell_qty} at {create_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                    logger.info(f"Symbol: {symbol} - Qty {qty - int(create_sell_qty)}, Successfully canceled for qty {create_sell_qty} at {create_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")

                                # Step. to place the order for cancelled quantity
                                logger.info(f"Symbol: {symbol} - Placing new order for {create_sell_qty} qty at {create_percent:.2%} of original qty {qty} at price: {current_price}. PriceDiff : {price_diff}, PriceThreshold :{price_threshold}, sell_diff_check :{sell_diff_check}")
                                print(f"Symbol: {symbol} - Placing new order for {create_sell_qty} qty at {create_percent:.2%} of original qty {qty} at price: {current_price}. PriceDiff : {price_diff}, PriceThreshold :{price_threshold}, sell_diff_check :{sell_diff_check}")
                                
                                place_order(trading_client, symbol, create_sell_qty, 0.0, current_price, False, logger)
                                #logger.info(f"Update position - Symbol: {symbol}, qty: {create_sell_qty}, order_type: market, side: buy, status: conversion")

                                print(f"Symbol {symbol} : Market Order submitted successfully for order qty {create_sell_qty} at price {current_price}")
                                logger.info(f"Symbol {symbol} : Market Order submitted successfully for order qty {create_sell_qty} at price {current_price}")

                                # if qty - sell qty. < 1 then del the symbol
                                if int(qty) - int(create_sell_qty) < 1 :
                                    time_age_spent = 0.0 # cancelling all the quantities
                                    del first_time_sales[symbol]

                                sleep(2)
                            else:
                                if price_diff <= sell_diff_check:
                                        print(f"Symbol: {symbol} Qty {qty}, Age {age_seconds} seconds. Skip sell as Price_diff: {price_diff} less than equal to Sell Diff check: {sell_diff_check}")
                                        logger.info(f"Symbol: {symbol} - Qty {qty}, Age {age_seconds} seconds. Skip sell as Price_diff: {price_diff} less than equal to Sell Diff check: {sell_diff_check}")
                                        first_time_sales[symbol] = prev_seconds_range
                                else:
                                    if qty == 1 and sell_percent > 0:
                                        trading_client.cancel_order_by_id(order_id=order_id)
                                        logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")
                                        cancelled_orders.append(order_id)

                                        del first_time_sales[symbol]
                                        time_age_spent = 0.0 # cancelling all the quantities
                                        print(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty 1 due to order age {age_seconds} seconds. Pending qty :0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                        logger.info(f"Symbol: {symbol} - Qty 0, Successfully canceled for qty 1 due to order age {age_seconds} seconds. Pending qty :0, price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                        sleep(2)

                                    else:
                                        if sell_percent > 0:
                                            sell_qty = str(max(1, int(qty * sell_percent)))
                                            #print(f"sell_qty: {sell_qty}, qty:{qty}")
                                            pending_qty = max(1,qty - int(sell_qty))
                                            time_age_spent = age_seconds

                                            if int(sell_qty) == int(qty):
                                                trading_client.cancel_order_by_id(order_id=order_id)
                                                logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: limit, side: buy, status: cancelled, price: {current_price}")
                                                cancelled_orders.append(order_id)
                                            else:
                                                trading_client.replace_order_by_id(order_id, ReplaceOrderRequest(qty = pending_qty))
                                                logger.debug(f"time_age_spent set to {age_seconds}")
                                                logger.info(f"Update position - Symbol: {symbol}, qty: {pending_qty}, order_type: limit, side: buy, status: replaced, price: {current_price}")

                                            
                                            print(f"Symbol: {symbol} - Qty {qty - int(sell_qty)}, Successfully canceled for qty {sell_qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                            logger.info(f"Symbol: {symbol} - Qty {qty - int(sell_qty)}, Successfully canceled for qty {sell_qty} at {sell_percent:.2%} due to order age {age_seconds} seconds. Price_diff: {price_diff}, Price Threshold: {price_threshold}, sell_diff_check :{sell_diff_check}")
                                    
                                            # if qty - sell qty. < 1 then del the symbol
                                            if int(qty) - int(sell_qty) < 1 :
                                                time_age_spent = 0.0 # cancelling all the quantities
                                                del first_time_sales[symbol]

                                            sleep(2)

                                        else:
                                            first_time_sales[symbol] = prev_seconds_range
                                            print(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has sell_percent {sell_percent} with prev sell was at {prev_seconds_range}. Skipping")
                                            logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has sell_percent {sell_percent} with prev sell was at {prev_seconds_range}. Skipping")
                    else:
                        print(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has not met the first range {seconds_range}. Skipping")
                        logger.info(f"Symbol: {symbol} - Qty {qty}, Age seconds {age_seconds} has not met the first range {seconds_range}. Skipping")
                    #Next line before moving to second symbol
                    print("\n")
                except Exception as e:
                    print(f"Unexpected error: {e}\n")
                    logger.error(f"Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
                    continue

            #Cleanup unwanted symbols
            for symbol in list(first_time_sales.keys()):
                if symbol not in active_symbols:
                    del first_time_sales[symbol]

            logger.debug(f"first_time_sales : {first_time_sales}")


            #print(f"Canelled Orders --> {cancelled_orders}")
            logger.debug(f"Canelled Orders --> {cancelled_orders}")
            # Step 2: Confirm Order Cancellations
            for order_id in cancelled_orders:
                retries = 0
                max_retries = 3  # Limit retries to avoid infinite loops
                delay = 0.5  # Seconds between retries

                while retries < max_retries:
                    status = get_order_status(trading_client, order_id)
                    #print(f"Tried {retries} times.")
                    if status == OrderStatus.CANCELED:
                        #print(f"Order {order_id} successfully cancelled.")
                        logger.debug(f"Order {order_id} successfully cancelled.")
                        break  # Exit loop once cancelled
                    elif status is None:
                        #print(f"Retrying to fetch order {order_id} status...")
                        logger.debug(f"Retrying to fetch order {order_id} status...")
                    else:
                        #print(f"Order {order_id} is still in status: {status}. Retrying...")
                        logger.debug(f"Order {order_id} is still in status: {status}. Retrying...")

                    sleep(delay)
                    retries += 1
                else:
                    #print(f"Warning: Order {order_id} cancellation confirmation failed after {max_retries} retries.")
                    logger.debug(f"Warning: Order {order_id} cancellation confirmation failed after {max_retries} retries.")

        except Exception as e:
            print(f"Main Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
            logger.error(f" Main Unexpected error: {e}\nTraceback:\n{traceback.format_exc()}")
            time_age_spent = 0.0
        finally:
            """Handle market hours logic and determine appropriate sleep time."""
            current_date = datetime.now(timezone.utc).date()
            logger.debug(f"Current date  {current_date}, prev date :{prev_date}")
            if current_date != prev_date:
                market_hours = get_regular_market_hours(trading_client, logger)
                prev_date = current_date

            current_time = datetime.now(timezone.utc)
            sleeptime = config.get("close_pending_position_sleep_seconds", 1, float)

            if not market_hours:
                sleeptime = 1800
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
            if sleeptime > 1800:
                sleeptime = 1800
                logger.debug("Defaulting sleep time to max 1800 seconds.")

            print("********************************")
            logger.info("********************************")

            print(f"[{current_time}] - Sleep for {sleeptime} seconds")
            #sleeptime = 2
            sleep(sleeptime)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    if len(sys.argv) != 2:
        print("Usage: python trading_manager_close_pending.py <profile_id>")
        sys.exit(1)

    profile_id = sys.argv[1]

    api_key = get_ssm_parameter(f'profile{profile_id}_apikey')
    api_secret = get_ssm_parameter(f'profile{profile_id}_apisecret')
    
#    api_key, api_secret = get_credentials(profile_id)

    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    file_config = ConfigLoader(config_file=f'config.ini')

    # Initialize DatabaseConfigLoader with credentials from config.ini
    db_config_loader = DatabaseConfigLoader(
        profile_id=profile_id,
        db_host=file_config.db_host,
        db_user=file_config.db_user,
        db_password=file_config.db_password,
        db_name=file_config.db_name,
        tables=["dd_common_config", "dd_close_position_config"],
        refresh_interval=45
    )

   # Initialize logger with initial level from config
    #db_config_loader = DatabaseConfigLoader( profile_id, tables=["common_config", "close_position_config"], refresh_interval=45)
    logger = setup_logger(db_config_loader, log_to_file=True, file_name=os.path.join(logs_dir,f"trading_limit_order_monitor_{profile_id}.log"))

    trading_client = initialize_trading_client(db_config_loader, api_key, api_secret, logger)
    option_historicaldata_client = initialize_option_historical_client(api_key, api_secret, logger)

    main(profile_id, db_config_loader, trading_client, option_historicaldata_client, logger)  # Pass True for logging to file
