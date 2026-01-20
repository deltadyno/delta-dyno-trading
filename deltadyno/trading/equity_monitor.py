"""
Market Equity Monitor for the DeltaDyno trading system.

This module monitors open market orders and closes them based on:
- Trailing stop loss
- Profit targets
- Hard stop limits
- Time-based closures
- Special handling for choppy trading days
"""

import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Dict, Optional

import pytz
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetCalendarRequest

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader
from deltadyno.trading.position_monitor import monitor_positions_and_close
from deltadyno.utils.helpers import get_credentials, get_ssm_parameter
from deltadyno.utils.logger import setup_logger, update_logger_level


# =============================================================================
# Constants
# =============================================================================

MAX_SLEEP_SECONDS = 1800  # 30 minutes maximum sleep


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
    return TradingClient(api_key, api_secret, paper=is_paper)


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

    logger.debug(f"market_calendar: {market_calendar[0]}")

    # Extract open and close times
    market_open = market_calendar[0].open
    market_close = market_calendar[0].close

    # Localize to US/Eastern and convert to UTC
    eastern_tz = pytz.timezone("US/Eastern")
    market_open_localized = eastern_tz.localize(market_open)
    market_close_localized = eastern_tz.localize(market_close)

    market_open_utc = market_open_localized.astimezone(timezone.utc)
    market_close_utc = market_close_localized.astimezone(timezone.utc)

    return {
        "regular_open": market_open_utc,
        "regular_close": market_close_utc,
    }


# =============================================================================
# Configuration Parsing
# =============================================================================

def parse_config_for_day(config, current_date: datetime.date, logger):
    """
    Parse configuration values, adjusting for choppy vs normal days.

    Args:
        config: Database configuration loader
        current_date: Current date
        logger: Logger instance

    Returns:
        Tuple of (ranges_list, stop_loss_values_list, stop_loss_quantity_sell_list,
                  min_profit_percent, hard_stop)
    """
    # Parse choppy trading days
    choppy_days_str = getattr(config, 'choppy_trading_days', '')
    choppy_days_list = []
    if choppy_days_str:
        choppy_days_list = [
            datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
            for date_str in choppy_days_str.split(",") if date_str.strip()
        ]

    if current_date in choppy_days_list:
        # Choppy day configuration
        logger.debug("Using choppy day configuration")
        ranges_list = [
            tuple(map(int, r.split(':')))
            for r in config.choppy_trailing_stop_loss_percent_range.split(',')
        ]
        stop_loss_values_list = [
            float(x) / 100
            for x in config.choppy_trailing_stop_loss_percent_range_values.split(',')
        ]
        stop_loss_quantity_sell_list = [
            float(x) / 100
            for x in config.choppy_trailing_stop_loss_percent_sell_quantity_once.split(',')
        ]
        min_profit_percent = config.get("choppy_min_profit_percent_to_enable_stoploss", 1, float) / 100
        hard_stop = config.get("choppy_hard_stop", 15, float) / 100
    else:
        # Normal day configuration
        logger.debug("Using normal day configuration")
        ranges_list = [
            tuple(map(int, r.split(':')))
            for r in config.trailing_stop_loss_percent_range.split(',')
        ]
        stop_loss_values_list = [
            float(x) / 100
            for x in config.trailing_stop_loss_percent_range_values.split(',')
        ]
        stop_loss_quantity_sell_list = [
            float(x) / 100
            for x in config.trailing_stop_loss_percent_sell_quantity_once.split(',')
        ]
        min_profit_percent = config.get("min_profit_percent_to_enable_stoploss", 1, float) / 100
        hard_stop = config.get("hard_stop", 15, float) / 100

    return (
        ranges_list,
        stop_loss_values_list,
        stop_loss_quantity_sell_list,
        min_profit_percent,
        hard_stop
    )


# =============================================================================
# Sleep Time Calculation
# =============================================================================

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
    sleeptime = config.get('close_position_sleep_seconds', 2, float)

    if not market_hours:
        sleeptime = MAX_SLEEP_SECONDS
        print(f"Sleep for {sleeptime} seconds")
        logger.info(f"Sleep for {sleeptime} seconds")
        return sleeptime

    if current_time < market_hours["regular_open"]:
        wake_up_time = market_hours["regular_open"]
        sleeptime = (wake_up_time - current_time).total_seconds()
        print(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
        logger.info(f"Market is closed. Sleeping until market opens at {wake_up_time} (UTC).")
    elif current_time > market_hours["regular_close"]:
        wake_up_time = market_hours["regular_open"] + timedelta(days=1)
        sleeptime = (wake_up_time - current_time).total_seconds()
        print(f"Market is closed. Sleeping until tomorrow's market opens at {wake_up_time} (UTC).")

    if sleeptime > MAX_SLEEP_SECONDS:
        logger.debug(f"Defaulting sleep time max to {MAX_SLEEP_SECONDS} seconds")
        sleeptime = MAX_SLEEP_SECONDS

    return sleeptime


# =============================================================================
# Main Monitor Loop
# =============================================================================

def monitor_market_equity(
    profile_id: str,
    config,
    trading_client: TradingClient,
    logger
) -> None:
    """
    Main monitoring loop for market equity positions.

    Args:
        profile_id: Client profile ID
        config: Database configuration loader
        trading_client: Alpaca TradingClient
        logger: Logger instance
    """
    print(f"Client {profile_id}: {config.client_name} started monitoring market orders.")
    logger.info(f"Client {profile_id}: {config.client_name} started monitoring market orders.")

    # Initialize tracking state
    trailing_stop_loss_percentages: Dict[str, float] = defaultdict(lambda: 0.0)
    previous_unrealized_plpc = defaultdict(lambda: (0.0, datetime.now(timezone.utc)))
    first_time_sales = defaultdict(lambda: {"value": 0.0, "timestamp": None})

    tap_cnt_to_skip_hard_stop = 0
    testcnt = 1

    prev_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    market_hours = get_regular_market_hours(trading_client, logger)
    current_date = datetime.now(timezone.utc).date()

    while True:
        try:
            # Check if profile is active
            # Note: Original code does NOT have continue here - it logs but continues processing
            if not config.get_active_profile_id():
                print(f"Profile: {profile_id} is not active. Skipping")
                logger.info(f"Profile: {profile_id} is not active. Skipping")

            # Update logger level if needed
            update_logger_level(logger, config)

            current_date = datetime.now(timezone.utc).date()

            # Parse configuration for current day type
            (
                ranges_list,
                stop_loss_values_list,
                stop_loss_quantity_sell_list,
                min_profit_percent,
                hard_stop
            ) = parse_config_for_day(config, current_date, logger)

            # Monitor and close positions
            trailing_stop_loss_percentages, tap_cnt_to_skip_hard_stop = monitor_positions_and_close(
                testcnt,
                previous_unrealized_plpc,
                trailing_stop_loss_percentages,
                first_time_sales,
                config.get("expire_sale_seconds", 0, int),
                config.get("close_order", False, bool),
                logger,
                config.get("close_all_at_min_profit", 3.5, float) / 100,
                trading_client,
                min_profit_percent,
                ranges_list,
                stop_loss_values_list,
                stop_loss_quantity_sell_list,
                config.get("default_trailing_stop_loss", 0.0, float) / 100,
                config.close_all_open_orders_at_local_time,
                tap_cnt_to_skip_hard_stop,
                config.get("cnt_of_times_to_skip_hard_stop", 0, int),
                hard_stop
            )

            # Debug logging
            logger.debug(f"Current trailing_stop_loss_percentages Dict: {dict(trailing_stop_loss_percentages)}")
            logger.debug(f"Current previous_unrealized_plpc Dict: {dict(previous_unrealized_plpc)}")
            logger.debug(f"Current first_time_sales Dict: {dict(first_time_sales)}")

        except Exception as e:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_traceback = traceback.format_exc()
            logger.error(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
            print(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")

        finally:
            # Update market hours if date changed
            logger.debug(f"Current date: {current_date}, prev date: {prev_date}")
            if current_date != prev_date:
                market_hours = get_regular_market_hours(trading_client, logger)
                prev_date = current_date

            sleeptime = calculate_sleep_time(market_hours, config, logger)

            print(f"[{datetime.now(timezone.utc)}] - Sleep for {sleeptime} seconds")
            logger.info(f"[{datetime.now(timezone.utc)}] - Sleep for {sleeptime} seconds")
            sleep(sleeptime)


def run_equity_monitor(
    profile_id: str,
    log_to_console: bool = False
) -> None:
    """
    Entry point for the equity monitor.

    Args:
        profile_id: Client profile ID
        log_to_console: If True, log to console instead of file
    """
    # Fetch credentials from SSM
    #api_key = get_ssm_parameter(f'profile{profile_id}_apikey')
    #api_secret = get_ssm_parameter(f'profile{profile_id}_apisecret')

    # Alternative: Use local credentials
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
        refresh_interval=25
    )

    # Initialize logger
    log_file = os.path.join(logs_dir, f"trading_market_equity_monitor_{profile_id}.log")
    logger = setup_logger(
        db_config_loader,
        log_to_file=not log_to_console,
        file_name=log_file
    )

    # Initialize trading client
    trading_client = initialize_trading_client(db_config_loader, api_key, api_secret, logger)

    # Start monitoring
    monitor_market_equity(profile_id, db_config_loader, trading_client, logger)


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m deltadyno.trading.equity_monitor <profile_id>")
        sys.exit(1)

    profile_id = sys.argv[1]
    run_equity_monitor(profile_id)

