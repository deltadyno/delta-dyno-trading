"""
Profile Listener for the DeltaDyno trading system.

This module listens to Redis streams for breakout messages and
creates orders based on configuration for a specific profile.
"""

import asyncio
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone, time as datetime_time
from typing import Dict, List, Optional, Tuple

from redis.asyncio import Redis

from alpaca.trading.client import TradingClient
from alpaca.data.historical import OptionHistoricalDataClient

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader
from deltadyno.constants import (
    CALL, DOWNWARD, PUT, REVERSE_DOWNWARD, REVERSE_UPWARD, UPWARD
)
from deltadyno.trading.constraints import check_constraints
from deltadyno.trading.order_creator import (
    close_all_orders_directional,
    close_order_for_symbol,
    create_order,
)
from deltadyno.trading.position_handler import (
    close_positions_directional,
    handle_position_closing,
)
from deltadyno.utils.helpers import (
    generate_option_symbol,
    get_credentials,
    identify_option_type,
)
from deltadyno.utils.logger import setup_logger, update_logger_level


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
        config: Configuration object
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        logger: Logger instance

    Returns:
        Configured TradingClient instance
    """
    print("Initializing Alpaca TradingClient with provided API keys.")
    logger.info("Initializing Alpaca TradingClient with provided API keys.")
    return TradingClient(api_key, api_secret, paper=config.get("is_paper_trading", True, bool))


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
    print("Initializing Alpaca Option historical API")
    logger.info("Initializing Alpaca Option historical API")
    return OptionHistoricalDataClient(api_key, api_secret)


def initialize_redis_client(
    host: str,
    port: int,
    password: str,
    logger
) -> Redis:
    """
    Initialize async Redis client.

    Args:
        host: Redis host
        port: Redis port
        password: Redis password
        logger: Logger instance

    Returns:
        Configured async Redis client
    """
    print("Initializing Redis Client...")
    logger.info("Initializing Redis Client....")
    client = Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=True
    )
    return client


# =============================================================================
# Message Parsing
# =============================================================================

def parse_message_data(raw: Dict) -> Dict:
    """
    Parse raw message data from Redis stream.

    Args:
        raw: Raw message dictionary from Redis

    Returns:
        Parsed message dictionary with typed values
    """
    def get(name: str, default=None):
        """Try string key, then bytes key. Return default if missing."""
        v = raw.get(name)
        if v is None:
            v = raw.get(name.encode())
        if v is None:
            return default

        if isinstance(v, bytes):
            return v.decode()
        return v

    # Required field: symbol
    symbol = get("symbol") or get("Symbol")
    if symbol is None:
        raise KeyError("symbol")

    # Breakout format fields (support both old and new field names)
    candle_size = get("candle_size")
    direction = get("direction")
    bar_strength = get("bar_strength")
    # choppy_level OR choppy_day_count (new format uses choppy_day_count)
    choppy_level = get("choppy_level") or get("choppy_day_count")
    # bar_close OR close_price (new format uses close_price)
    bar_close = get("bar_close") or get("close_price")
    volume = get("volume")

    # Bar date handling - support multiple field names and formats
    # Old format: bar_date, DateTime
    # New format: close_time (ISO format)
    bar_date_str = get("bar_date") or get("DateTime") or get("close_time")
    bar_date = None
    if bar_date_str:
        # Try multiple datetime formats
        formats_to_try = [
            "%Y-%m-%d %H:%M:%S%z",           # With timezone
            "%Y-%m-%d %H:%M:%S",             # Without timezone
            "%Y-%m-%dT%H:%M:%S.%f%z",        # ISO format with microseconds and timezone
            "%Y-%m-%dT%H:%M:%S%z",           # ISO format with timezone
            "%Y-%m-%dT%H:%M:%S.%f",          # ISO format with microseconds
            "%Y-%m-%dT%H:%M:%S",             # ISO format basic
        ]
        for fmt in formats_to_try:
            try:
                bar_date = datetime.strptime(bar_date_str, fmt)
                break
            except ValueError:
                continue
        
        # If standard parsing fails, try fromisoformat (handles more variations)
        if bar_date is None:
            try:
                bar_date = datetime.fromisoformat(bar_date_str.replace('Z', '+00:00'))
            except:
                pass

    # Profile ID (optional)
    profile_raw = get("profile_id")
    profile_id = int(profile_raw) if profile_raw is not None else None

    # Safe float conversion
    def to_float(x):
        try:
            return float(x) if x is not None else None
        except:
            return None

    return {
        "symbol": symbol,
        "candle_size": to_float(candle_size),
        "bar_date": bar_date,
        "direction": direction,
        "volume": to_float(volume),
        "bar_strength": to_float(bar_strength),
        "choppy_level": to_float(choppy_level),
        "profile_id": profile_id,
        "bar_close": to_float(bar_close),
    }


# =============================================================================
# Trading Condition Validation
# =============================================================================

def validate_trading_conditions(
    bar_date: Optional[datetime],
    last_processed_bar_date: datetime.date,
    logger
) -> bool:
    """
    Validate if trading conditions require state reset.

    Args:
        bar_date: Current bar datetime (can be None)
        last_processed_bar_date: Last processed bar date
        logger: Logger instance

    Returns:
        True if date has changed and state should be reset
    """
    if bar_date is None:
        logger.warning("bar_date is None, cannot validate trading conditions")
        return False
    
    logger.debug(f"bar_date.date(): {bar_date.date()}, last_processed_bar_date: {last_processed_bar_date}")
    if bar_date.date() != last_processed_bar_date:
        logger.debug(f"Date has changed, resetting position count. bar_date: {bar_date.date()}, last_processed_bar_date: {last_processed_bar_date}")
        return True
    return False


def check_skip_trading_days(
    bar_date: Optional[datetime],
    skip_trading_days_list: List[datetime.date],
    logger
) -> bool:
    """
    Check if trading should be skipped for the current date.

    Args:
        bar_date: Current bar datetime (can be None)
        skip_trading_days_list: List of dates to skip
        logger: Logger instance

    Returns:
        True if trading should be skipped
    """
    if bar_date is None:
        logger.warning("bar_date is None, skipping trading day check")
        return True  # Skip if we can't determine the date
    
    if bar_date.date() in skip_trading_days_list:
        print(f"Skipping trading for date: {bar_date.date()}")
        logger.info(f"Skipping trading for date: {bar_date.date()}")
        return True
    return False


def parse_skip_trading_days(config, logger) -> List[datetime.date]:
    """
    Parse skip trading days from configuration.

    Args:
        config: Configuration object
        logger: Logger instance

    Returns:
        List of dates to skip trading
    """
    try:
        return [
            datetime.strptime(date.strip(), "%Y-%m-%d").date()
            for date in config.skip_trading_days.split(",") if date.strip()
        ]
    except Exception as e:
        logger.error(f"Error parsing skip trading days: {e}")
        return []


def determine_option_type(direction: str) -> Optional[str]:
    """
    Determine option type based on direction.

    Args:
        direction: Direction string (UPWARD or DOWNWARD)

    Returns:
        'C' for Call, 'P' for Put, None if invalid
    """
    if direction == UPWARD:
        return "C"
    elif direction == DOWNWARD:
        return "P"
    return None


# =============================================================================
# Choppy Day Handling
# =============================================================================

async def handle_choppy_day(
    profile_id: str,
    bar_date: datetime,
    data: Dict,
    config,
    trading_client: TradingClient,
    logger
) -> None:
    """
    Handle choppy day detection and database update.

    Args:
        profile_id: Profile ID
        bar_date: Current bar datetime
        data: Parsed message data
        config: Configuration object
        trading_client: Alpaca TradingClient
        logger: Logger instance
    """
    try:
        if trading_client.get_clock().is_open:
            logger.debug("Market is open. About to update choppy setting.")

            try:
                if config.mark_as_choppy_day_range.strip():
                    mark_as_choppy_day_range = tuple(map(int, config.mark_as_choppy_day_range.split('-')))
                else:
                    logger.warning("mark_as_choppy_day_range is empty. Defaulting to (0, 0).")
                    mark_as_choppy_day_range = (0, 0)
            except ValueError as e:
                logger.error(f"Error parsing mark_as_choppy_day_range: {e}")
                mark_as_choppy_day_range = (0, 0)

            # Check if choppy level is within range
            if mark_as_choppy_day_range[0] <= data["choppy_level"] <= mark_as_choppy_day_range[1]:
                logger.info(
                    f"choppy_level {data['choppy_level']} is in choppy_level_range "
                    f"{mark_as_choppy_day_range}. Setting the day {bar_date} as a choppy day."
                )

                # Fetch existing choppy days
                existing_value = config.get("choppy_trading_days", default="")

                if existing_value:
                    choppy_trading_days_list = existing_value.split(',')
                else:
                    choppy_trading_days_list = []

                # Format bar_date
                bar_date_str = bar_date.replace(tzinfo=None).strftime("%Y-%m-%d")

                # Add if not already present
                if bar_date_str not in choppy_trading_days_list:
                    choppy_trading_days_list.append(bar_date_str)
                    updated_value = ",".join(choppy_trading_days_list)

                    # Update database
                    query = """
                        INSERT INTO dd_common_config (profile_id, config_key, value) 
                        VALUES (%s, 'choppy_trading_days', %s)
                        ON DUPLICATE KEY UPDATE value = %s
                    """
                    params = (profile_id, updated_value, updated_value)

                    try:
                        config.update_config_in_db(query, params)
                        logger.info(
                            f"{bar_date_str} added to choppy_trading_days in common_config "
                            f"for profile_id {profile_id}."
                        )
                    except Exception as db_error:
                        logger.error(f"Database error while updating choppy_trading_days: {db_error}")
                        traceback.print_exc()
                else:
                    logger.info(f"{bar_date_str} already exists in choppy_trading_days.")
            else:
                logger.info(f"choppy_level {data['choppy_level']} not within range {mark_as_choppy_day_range}, skipping.")

        else:
            logger.debug("Market is closed. Choppy settings won't be updated.")

    except Exception as e:
        logger.error(f"Error handling choppy day for {bar_date}: {e}")
        traceback.print_exc()


# =============================================================================
# Reversal Handling
# =============================================================================

async def handle_reversal(
    profile_id: str,
    direction: str,
    config,
    trading_client: TradingClient,
    recent_opened_symbol: Optional[str],
    logger
) -> None:
    """
    Handle reversal signals and close positions if configured.

    Args:
        profile_id: Profile ID
        direction: Reversal direction (REVERSE_UPWARD or REVERSE_DOWNWARD)
        config: Configuration object
        trading_client: Alpaca TradingClient
        recent_opened_symbol: Most recently opened symbol
        logger: Logger instance
    """
    logger.info(f"Reverse direction detected: {direction}. Closing positions. Recent opened symbol is {recent_opened_symbol}")

    if not config.get("close_on_reverse", False, bool):
        print(f"Client {profile_id}: No positions closed. 'close_on_reverse' is disabled.")
        logger.info(f"Client {profile_id}: No positions closed. 'close_on_reverse' is disabled.")
        return

    if config.get("close_recent_option", False, bool):
        logger.debug("Closing Recent option.")

        if not recent_opened_symbol:
            print(f"Client {profile_id}: No recent position to close.")
            logger.info(f"Client {profile_id}: No recent position to close.")
        else:
            option_type = identify_option_type(recent_opened_symbol, logger)

            # Check if direction matches option type
            if (direction == REVERSE_UPWARD and option_type == PUT) or \
               (direction == REVERSE_DOWNWARD and option_type == CALL):

                closed_cnt = close_order_for_symbol(
                    trading_client, recent_opened_symbol, profile_id, logger=logger
                )
                if closed_cnt > 0:
                    logger.info(
                        f"Client {profile_id}: Successfully closed limit order position "
                        f"for symbol: {recent_opened_symbol}"
                    )

                result = handle_position_closing(
                    trading_client, recent_opened_symbol, profile_id, logger=logger
                )
                if result:
                    logger.info(
                        f"Client {profile_id}: Successfully closed market order position "
                        f"for symbol: {recent_opened_symbol}"
                    )
            else:
                print(f"Skipping symbol: {recent_opened_symbol} as direction: {direction} does not matches option type: {option_type} ")
                logger.info(f"Skipping symbol: {recent_opened_symbol} as direction: {direction} does not matches option type: {option_type} ")

    elif config.get("close_all_reversal_options", False, bool):
        logger.debug("Closing all reversal options.")

        # Close limit orders
        closed_cnt = close_all_orders_directional(
            trading_client, direction, profile_id, logger=logger
        )
        if closed_cnt == 0:
            print(f"Client {profile_id}: No limit order positions found to close.")
            logger.info(f"Client {profile_id}: No limit order positions found to close.")
        else:
            print(f"Client {profile_id}: Successfully closed limit order positions with count: {closed_cnt}")
            logger.info(f"Client {profile_id}: Successfully closed limit order positions with count: {closed_cnt}")

        # Close market order positions
        closed_cnt = close_positions_directional(
            trading_client, direction, profile_id, logger=logger
        )
        if closed_cnt == 0:
            print(f"Client {profile_id}: No market order positions found to close.")
            logger.info(f"Client {profile_id}: No market order positions found to close.")
        else:
            print(f"Client {profile_id}: Successfully closed market order positions with count: {closed_cnt}")
            logger.info(f"Client {profile_id}: Successfully closed  market order positions with count: {closed_cnt}")

    else:
        print(
            f"Client {profile_id}: No limit order positions closed as both "
            f"'close_recent_option' and 'close_all_reversal_options' are disabled."
        )
        logger.info(
            f"Client {profile_id}: No limit order positions closed as both "
            f"'close_recent_option' and 'close_all_reversal_options' are disabled."
        )


# =============================================================================
# Breakout Handling
# =============================================================================

async def handle_breakout(
    profile_id: str,
    data: Dict,
    config,
    trading_client: TradingClient,
    option_historicaldata_client: OptionHistoricalDataClient,
    bar_date: datetime,
    open_position_cnt: int,
    skip_trading_days_list: List[datetime.date],
    logger
) -> Optional[str]:
    """
    Handle breakout signals and create orders.

    Args:
        profile_id: Profile ID
        data: Parsed message data
        config: Configuration object
        trading_client: Alpaca TradingClient
        option_historicaldata_client: Alpaca option data client
        bar_date: Current bar datetime
        open_position_cnt: Current open position count
        skip_trading_days_list: List of dates to skip
        logger: Logger instance

    Returns:
        Option symbol if order was created, None otherwise
    """
    option_type = determine_option_type(data["direction"])
    if not option_type:
        logger.debug("No valid option type determined. Skipping breakout handling.")
        return None

    if check_constraints(
        config.timezone,
        datetime_time(config.get("no_trade_start_hour", 1, int), config.get("no_trade_start_minute", 0, int)),
        datetime_time(config.get("no_trade_end_hour", 23, int), config.get("no_trade_end_minute", 59, int)),
        data["candle_size"],
        config.get("skip_candle_with_size", 5, float),
        data["volume"],
        config.get("max_volume_threshold", 1000000, int),
        open_position_cnt,
        config.get("max_daily_positions_allowed", 50, int),
        bar_date,
        skip_trading_days_list,
        logger
    ):
        return create_order(
            profile_id, trading_client, option_historicaldata_client,
            bar_date, data["symbol"], data["candle_size"], data["bar_close"],
            data["choppy_level"], data["bar_strength"],
            option_type, config, logger
        )
    else:
        return None


# =============================================================================
# Message Processing
# =============================================================================

async def process_message(
    profile_id: str,
    message: Tuple,
    config,
    trading_client: TradingClient,
    option_historicaldata_client: OptionHistoricalDataClient,
    redis_client: Redis,
    logger,
    last_processed_bar_date: datetime.date,
    open_position_cnt: int,
    recent_opened_symbol: Optional[str]
) -> Tuple[datetime.date, int, Optional[str]]:
    """
    Process a single message from the Redis stream.

    Args:
        profile_id: Profile ID
        message: Message tuple (entry_id, data)
        config: Configuration object
        trading_client: Alpaca TradingClient
        option_historicaldata_client: Alpaca option data client
        redis_client: Redis client
        logger: Logger instance
        last_processed_bar_date: Last processed date
        open_position_cnt: Current position count
        recent_opened_symbol: Most recent opened symbol

    Returns:
        Tuple of (updated last_processed_bar_date, open_position_cnt, recent_opened_symbol)
    """
    try:
        logger.debug(f"Message is: {message[1]}")
        data = parse_message_data(message[1])
        logger.debug(f"Parsed message: {data}")

        print()
        logger.info(" ")

        logger.debug(f"Comparing profile_id: {profile_id} with data['profile_id']: {data['profile_id']}.")
        logger.debug(f"Open Position cnt: {open_position_cnt}, recent_opened_symbol: {recent_opened_symbol}, last_processed_bar_date: {last_processed_bar_date}")

        # Check profile_id match
        if data["profile_id"] is not None and data["profile_id"] != int(profile_id):
            print(f"Message skipped due to profile_id mismatch: {data['profile_id']} != {profile_id}")
            logger.info(f"Message skipped due to profile_id mismatch: {data['profile_id']} != {profile_id}")
            return last_processed_bar_date, open_position_cnt, recent_opened_symbol

        bar_date = data["bar_date"]
        
        # Early exit if bar_date could not be parsed
        if bar_date is None:
            logger.warning(f"Could not parse bar_date from message. Skipping message.")
            print(f"Warning: Could not parse bar_date from message. Skipping.")
            return last_processed_bar_date, open_position_cnt, recent_opened_symbol
        
        skip_trading_days_list = parse_skip_trading_days(config, logger)

        # Reset state if date changed
        if validate_trading_conditions(bar_date, last_processed_bar_date, logger):
            open_position_cnt = 0
            recent_opened_symbol = None

        # Skip non-trading days
        if check_skip_trading_days(bar_date, skip_trading_days_list, logger):
            return last_processed_bar_date, open_position_cnt, recent_opened_symbol

        direction = data["direction"]

        # Handle reversal direction
        if direction in (REVERSE_UPWARD, REVERSE_DOWNWARD):
            await handle_reversal(
                profile_id, direction, config, trading_client,
                recent_opened_symbol, logger
            )
            return last_processed_bar_date, open_position_cnt, recent_opened_symbol

        # Handle upward/downward breakout
        if direction in (UPWARD, DOWNWARD):
            breakout_success_symbol = await handle_breakout(
                profile_id, data, config, trading_client,
                option_historicaldata_client, bar_date,
                open_position_cnt, skip_trading_days_list, logger
            )
            if breakout_success_symbol is not None:
                last_processed_bar_date = bar_date.date()
                open_position_cnt += 1
                logger.debug(f"Position count updated: {open_position_cnt}")
                recent_opened_symbol = breakout_success_symbol

        # Handle choppy day logic
        await handle_choppy_day(profile_id, bar_date, data, config, trading_client, logger)

        return last_processed_bar_date, open_position_cnt, recent_opened_symbol

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        traceback.print_exc()
        await asyncio.sleep(1)
        return last_processed_bar_date, open_position_cnt, recent_opened_symbol


# =============================================================================
# Main Consumer Loop
# =============================================================================

async def consume_orders(
    profile_id: str,
    breakout_queue: str,
    config,
    trading_client: TradingClient,
    option_historicaldata_client: OptionHistoricalDataClient,
    redis_client: Redis,
    logger
) -> None:
    """
    Main consumer loop for processing breakout messages.

    Args:
        profile_id: Profile ID
        breakout_queue: Redis stream name for breakout messages
        config: Configuration object
        trading_client: Alpaca TradingClient
        option_historicaldata_client: Alpaca option data client
        redis_client: Redis client
        logger: Logger instance
    """
    print(f"Profile {profile_id}: {config.client_name} started listening for orders.")
    logger.info(f"Profile {profile_id}: {config.client_name} started listening for orders.")

    open_position_cnt = 0
    last_processed_bar_date = (datetime.now() - timedelta(days=1)).date()
    recent_opened_symbol = None

    while True:
        try:
            messages = await redis_client.xread({breakout_queue: "$"}, block=0, count=1)

            # Update logger level periodically
            update_logger_level(logger, config)

            if config.get_active_profile_id():
                for stream, entries in messages:
                    for entry_id, data in entries:
                        print("***********************************************************")
                        logger.info("***********************************************************")

                        last_processed_bar_date, open_position_cnt, recent_opened_symbol = await process_message(
                            profile_id, (entry_id, data), config, trading_client,
                            option_historicaldata_client, redis_client, logger,
                            last_processed_bar_date, open_position_cnt, recent_opened_symbol
                        )

                        print("***********************************************************")
                        logger.info("***********************************************************")
            else:
                print(f"Profile: {profile_id} is not active. Skipping")
                logger.info(f"Profile: {profile_id} is not active. Skipping")

        except Exception as e:
            logger.error(f"Error in consume_orders: {e}")
            traceback.print_exc()
            await asyncio.sleep(1)


# =============================================================================
# Entry Point
# =============================================================================

async def run_profile_listener(profile_id: str) -> None:
    """
    Entry point for the profile listener.

    Args:
        profile_id: Profile ID to listen for
    """
    # Get API credentials (uses environment setting from config.ini)
    # - development: loads from config/credentials.py
    # - production: loads from AWS SSM Parameter Store
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
        tables=["dd_common_config", "dd_open_position_config", "dd_bar_order_range", "dd_choppy_bar_order_range"],
        refresh_interval=40
    )

    # Initialize logger
    logger = setup_logger(
        db_config_loader,
        log_to_file=True,
        file_name=os.path.join(logs_dir, f"profile_{profile_id}.log")
    )

    # Initialize clients
    trading_client = initialize_trading_client(db_config_loader, api_key, api_secret, logger)
    redis_client = initialize_redis_client(
        file_config.redis_host,
        file_config.redis_port,
        file_config.redis_password,
        logger
    )
    option_historicaldata_client = initialize_option_historical_client(api_key, api_secret, logger)

    # Start consuming orders
    await consume_orders(
        profile_id,
        file_config.redis_stream_name_breakout_message,
        db_config_loader,
        trading_client,
        option_historicaldata_client,
        redis_client,
        logger
    )


async def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python profile_listener.py <profile_id>")
        sys.exit(1)

    profile_id = sys.argv[1]
    await run_profile_listener(profile_id)


if __name__ == "__main__":
    asyncio.run(main())


