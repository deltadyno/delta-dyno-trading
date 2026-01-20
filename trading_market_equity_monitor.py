import argparse
from monitor_positions import monitor_positions_and_close
from logger_config import setup_logger, update_logger_level
from alpaca.trading.client import TradingClient
import time
import sys
import os
import threading
from datetime import datetime, timedelta, timezone, time as datetime_time  # Alias to avoid conflict
from alpaca.trading.requests import GetCalendarRequest
import pytz
import logging
import configparser
from config_loader import ConfigLoader
from time import sleep
from collections import defaultdict
import traceback
from credentials import credentials
from misc import get_credentials, get_ssm_parameter
from database_config_loader import DatabaseConfigLoader

# Initialize Alpaca API clients
def initialize_trading_client(config, api_key, api_secret, logger) -> TradingClient:
    print("Initializing Alpaca TradingClient with provided API keys.")
    logger.info("Initializing Alpaca TradingClient with provided API keys.")
    return TradingClient(api_key, api_secret, paper=config.get("is_paper_trading", True, bool))

def get_regular_market_hours(trading_client, logger, target_date=None):
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    logger.debug(f"target_date: {target_date}")

    calendar_request = GetCalendarRequest(start=target_date, end=target_date)
    market_calendar = trading_client.get_calendar(calendar_request)

    if not market_calendar:
        logger.info(f"No market calendar data available for {target_date}.")
        return None

    logger.debug(f"market_calendar : {market_calendar[0]}")

    # Extract open and close times
    market_open = market_calendar[0].open
    market_close = market_calendar[0].close


    # Localize to US/Eastern and adjust for DST
    eastern_tz = pytz.timezone("US/Eastern")
    market_open_localized = eastern_tz.localize(market_open)
    market_close_localized = eastern_tz.localize(market_close)

    # Convert to UTC
    market_open_utc = market_open_localized.astimezone(timezone.utc)
    market_close_utc = market_close_localized.astimezone(timezone.utc)

    return {
        "regular_open": market_open_utc,
        "regular_close": market_close_utc,
    }

def main(profile_id, config, trading_client, logger):

    print(f"Client {profile_id} : {config.client_name} started monitoring market orders.")
    logger.info(f"Client {profile_id} : {config.client_name} started monitoring market orders.")

    # Use defaultdict for automatic default values
    trailing_stop_loss_percentages = defaultdict(lambda: 0.0)
    # Set default trailing stop loss percentages and previous plpc tracking
    #previous_unrealized_plpc = defaultdict(lambda: 0.0)  # Track the last unrealized_plpc per symbol
    previous_unrealized_plpc = defaultdict(lambda: (0.0, datetime.now(timezone.utc)))

    # dictioinary to save first time save
    first_time_sales = defaultdict(lambda: {"value": 0.0, "timestamp": None})

    tap_cnt_to_skip_hard_stop = 0

    testcnt = 1

    prev_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    market_hours = get_regular_market_hours(trading_client, logger)

    while True:
        try:

            if not config.get_active_profile_id():
                print(f"Profile: {profile_id} is not active. Skipping")
                logger.info(f"Profile: {profile_id} is not active. Skipping")

            # Periodically check and update the log level
            update_logger_level(logger, config)

            # Convert to list of datetime.date objects
            choppy_days_list = [datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
                          for date_str in config.choppy_trading_days.split(",") if date_str.strip()]

            current_date = datetime.now(timezone.utc).date()

            if current_date in choppy_days_list:
                # Parsing the ranges
                ranges_list = [tuple(map(int, r.split(':'))) for r in config.choppy_trailing_stop_loss_percent_range.split(',')]
                # Parsing the values
                #stop_loss_values_list = list(map(float, config.choppy_trailing_stop_loss_percent_range_values.split(',')))
                # Parsing the values for quantity
                #stop_loss_quantity_sell_list = list(map(float, config.choppy_trailing_stop_loss_percent_sell_quantity_once.split(',')))
                
                # Parsing and dividing each value by 100
                stop_loss_values_list = list(map(lambda x: float(x) / 100, config.choppy_trailing_stop_loss_percent_range_values.split(',')))

                # Parsing and dividing each quantity value by 100
                stop_loss_quantity_sell_list = list(map(lambda x: float(x) / 100, config.choppy_trailing_stop_loss_percent_sell_quantity_once.split(',')))


                min_profit_percent = config.get("choppy_min_profit_percent_to_enable_stoploss", 1, float) / 100
                hard_stop = config.get("choppy_hard_stop", 15, float) / 100
            else:
                # Parsing the ranges
                ranges_list = [tuple(map(int, r.split(':'))) for r in config.trailing_stop_loss_percent_range.split(',')]
                # Parsing the values
                #stop_loss_values_list = list(map(float, config.trailing_stop_loss_percent_range_values.split(',')))
                # Parsing the values for quantity
                #stop_loss_quantity_sell_list = list(map(float, config.trailing_stop_loss_percent_sell_quantity_once.split(',')))
                # Parsing and dividing each value by 100
                stop_loss_values_list = list(map(lambda x: float(x) / 100, config.trailing_stop_loss_percent_range_values.split(',')))

                # Parsing and dividing each quantity value by 100
                stop_loss_quantity_sell_list = list(map(lambda x: float(x) / 100, config.trailing_stop_loss_percent_sell_quantity_once.split(',')))

                min_profit_percent = config.get("min_profit_percent_to_enable_stoploss", 1, float) / 100
                hard_stop = config.get("hard_stop", 15, float) / 100

            trailing_stop_loss_percentages, tap_cnt_to_skip_hard_stop = monitor_positions_and_close( 
                            testcnt,
                            previous_unrealized_plpc, trailing_stop_loss_percentages, first_time_sales, config.get("expire_sale_seconds", 0, int),
                            config.get("close_order", False, bool), logger, config.get("close_all_at_min_profit", 3.5, float)/100,
                            trading_client, min_profit_percent,
                            ranges_list, stop_loss_values_list, stop_loss_quantity_sell_list,
                            config.get("default_trailing_stop_loss", 0.0, float) / 100, config.close_all_open_orders_at_local_time,
                            tap_cnt_to_skip_hard_stop, config.get("cnt_of_times_to_skip_hard_stop", 0, int),
                            hard_stop)


            #testcnt = testcnt + 1

            # Print the current value of trailing_stop_loss_percentages for debugging
            #print(f"Current trailing_stop_loss_percentages Dict: {dict(trailing_stop_loss_percentages)}")  # Convert to regular dict for clearer printing
            #print(f"Current previous_unrealized_plpc Dict: {dict(previous_unrealized_plpc)}")  # Convert to regular dict for clearer printing
            #print(f"Current first_time_sales Dict: {dict(first_time_sales)}")  # Convert to regular dict for clearer printing
            logger.debug(f"Current trailing_stop_loss_percentages Dict: {dict(trailing_stop_loss_percentages)}")  # Convert to regular dict for clearer printing
            logger.debug(f"Current previous_unrealized_plpc Dict: {dict(previous_unrealized_plpc)}")  # Convert to regular dict for clearer printing
            logger.debug(f"Current first_time_sales Dict: {dict(first_time_sales)}")  # Convert to regular dict for clearer printing

        except Exception as e:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_traceback = traceback.format_exc()
            
            logger.error(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
            print(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
            
        finally:
            logger.debug(f"Current date  {current_date}, prev date :{prev_date}")
            if current_date != prev_date:
                market_hours = get_regular_market_hours(trading_client, logger)
                prev_date = current_date

            current_time = datetime.now(timezone.utc)
            sleeptime = config.get('close_position_sleep_seconds', 2, float)

            if not market_hours:
                sleeptime = 1800
                print(f"Sleep for {sleeptime} seconds")
                logger.info(f"Sleep for {sleeptime} seconds")
                sleep(sleeptime)  # Check every 10 seconds or adjust as needed
                continue

            if current_time < market_hours["regular_open"]:
                # Before pre-market, sleep until pre-market opens
                wake_up_time = market_hours["regular_open"]
                sleeptime = (wake_up_time - current_time).total_seconds()
                print(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
                logger.info(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
            elif current_time > market_hours["regular_close"]:
                # After after-hours, sleep until next day pre-market
                wake_up_time = market_hours["regular_open"] + timedelta(days=1)
                sleeptime = (wake_up_time - current_time).total_seconds()
                print(f"Market is closed. Sleeping until tomorrow's market opens at {wake_up_time} (UTC).")

            if sleeptime > 1800:
                logger.debug("Defaulting sleep time max to 1800 seconds")
                sleeptime = 1800

            print(f"[{current_time}] - Sleep for {sleeptime} seconds")
            logger.info(f"[{current_time}] - Sleep for {sleeptime} seconds")
            sleep(sleeptime)  # Check every 10 seconds or adjust as needed

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    if len(sys.argv) != 2:
        print("Usage: python trading_manager_close.py <profile_id>")
        sys.exit(1)

    profile_id = sys.argv[1]
    api_key = get_ssm_parameter(f'profile{profile_id}_apikey')
    api_secret = get_ssm_parameter(f'profile{profile_id}_apisecret')

    #api_key, api_secret = get_credentials(profile_id)

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
        refresh_interval=25
    )

   # Initialize logger with initial level from config
    #config = DatabaseConfigLoader( profile_id, tables=["common_config", "close_position_config"], refresh_interval=25)
    logger = setup_logger(db_config_loader, log_to_file=True, file_name=os.path.join(logs_dir,f"trading_market_equity_monitor_{profile_id}.log"))

    trading_client = initialize_trading_client(db_config_loader, api_key, api_secret, logger)

    main(profile_id, db_config_loader, trading_client, logger)  # Pass True for logging to file
