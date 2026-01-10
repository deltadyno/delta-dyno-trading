"""
Breakout Detector - Main entry point for the DeltaDyno trading system.

This module orchestrates the breakout detection workflow:
1. Reads configuration from database and config files
2. Initializes trading and data clients
3. Continuously monitors market data for breakout signals
4. Publishes detected breakouts to Redis for downstream processing
"""

import argparse
import os
import time
import traceback
from datetime import datetime, timedelta, timezone, time as datetime_time
from typing import Optional, Tuple

import pandas as pd
import pytz
import redis
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient

from deltadyno.data.fetcher import fetch_latest_data, fetch_daily_historicaldata
from deltadyno.analysis.pivots import calculate_pivots
from deltadyno.analysis.slope import calculate_slope
from deltadyno.analysis.breakout import check_for_breakouts
from deltadyno.analysis.choppy import monitor_candles_close
from deltadyno.core.position_manager import process_positions, update_positions, close_positions
from deltadyno.utils.helpers import (
    get_market_hours,
    get_credentials,
    sleep_determination_extended,
    calculate_bar_strength,
)
from deltadyno.utils.logger import setup_logger, update_logger_level
from deltadyno.config.loader import ConfigLoader
from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.constants import (
    NO_POSITION_FOUND,
    POSITION_CLOSED,
    ERROR_OCCURRED,
    POSITION_CLOSE_SKIP,
    MARKET_CLOSED,
)


# =============================================================================
# Client Initialization
# =============================================================================

def initialize_redis_client(host: str, port: int, password: str, logger) -> redis.Redis:
    """
    Initialize and return a Redis client connection.

    Args:
        host: Redis server hostname
        port: Redis server port
        password: Redis authentication password
        logger: Logger instance for logging

    Returns:
        Configured Redis client instance
    """
    logger.info("Initializing Redis client...")
    print("Initializing Redis Client...")
    
    return redis.Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=True,
        socket_timeout=5
    )


def initialize_trading_client(api_key: str, api_secret: str, logger) -> TradingClient:
    """
    Initialize and return an Alpaca Trading client.

    Args:
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        logger: Logger instance for logging

    Returns:
        Configured TradingClient instance
    """
    logger.info("Initializing Alpaca TradingClient...")
    print("Initializing Alpaca API...")
    
    return TradingClient(api_key, api_secret, paper=False)


def initialize_historical_data_client(
    api_key: str, 
    api_secret: str, 
    logger
) -> StockHistoricalDataClient:
    """
    Initialize and return an Alpaca Historical Data client.

    Args:
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        logger: Logger instance for logging

    Returns:
        Configured StockHistoricalDataClient instance
    """
    logger.info("Initializing Alpaca HistoricalDataClient...")
    print("Initializing Alpaca Historical Data Client API...")
    
    return StockHistoricalDataClient(api_key=api_key, secret_key=api_secret)


# =============================================================================
# Data Fetching
# =============================================================================

def fetch_data(
    end_of_data: bool,
    symbol: str,
    timeframe_minutes: int,
    trading_client: TradingClient,
    historicaldata_client: StockHistoricalDataClient,
    start_index: int,
    max_retries: int,
    base_delay: int,
    config,
    file_config,
    logger
) -> Tuple[pd.DataFrame, datetime, bool, float, bool, bool, bool, bool]:
    """
    Fetch market data based on configuration mode (real-time or historical).

    Args:
        end_of_data: Flag indicating if historical data has been exhausted
        symbol: Trading symbol to fetch data for
        timeframe_minutes: Candle timeframe in minutes
        trading_client: Alpaca trading client
        historicaldata_client: Alpaca historical data client
        start_index: Starting index for historical data
        max_retries: Maximum retry attempts for failed requests
        base_delay: Base delay for exponential backoff
        config: Database configuration loader instance
        file_config: File-based configuration loader instance
        logger: Logger instance

    Returns:
        Tuple containing:
            - DataFrame with fetched data
            - End timestamp of fetched data
            - Flag indicating if all historical data has been processed
            - Sleep time before next fetch
            - Flag indicating history mode
            - Flag for order creation permission
            - Flag for order closing permission
            - Flag indicating real-time mode has started
    """
    logger.debug(f"Fetching data for symbol: {symbol}, start_index: {start_index}, end_of_data: {end_of_data}")
    logger.debug(f"max_retries: {max_retries}, base_delay: {base_delay}")

    # Real-time data mode
    if config.get("read_real_data", True, bool) and end_of_data:
        logger.info("Fetching real-time data.")
        print("Fetching real-time data.")

        df = fetch_latest_data(
            symbol=symbol,
            trading_client=trading_client,
            historicaldata_client=historicaldata_client,
            end_time=datetime.now(pytz.UTC),
            length=1,
            timeframe_minutes=timeframe_minutes,
            max_retries=max_retries,
            base_delay=base_delay,
            data_feed=file_config.data_feed or "IEX",
            logger=logger
        )
        
        logger.debug(f"Fetched {len(df)} real-time data points.")
        
        return (
            df,
            datetime.now(pytz.UTC),
            True,  # end_of_data
            config.get("chart_sleep_seconds", 1, float),
            False,  # history_mode
            config.get("create_order", True, bool),
            config.get("close_order", False, bool),
            True  # is_real_time_started
        )

    # Historical data mode
    elif config.get("read_historical_data", True, bool) and not end_of_data:
        logger.info("Using historical data fetch mode.")
        print("Using historical data fetch mode.")

        # Parse end_date from configuration
        try:
            end_date_str = config.get("end_date", "2025-01-22T23:00:00.000-00:00", str)
            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"Failed to parse end_date: {end_date_str}. Error: {e}")
            return _create_error_response(config)

        # Adjust end_date if it's too recent
        # For SIP feed, require at least 15 minutes delay; for IEX, use timeframe_minutes
        current_utc_time = datetime.now(pytz.UTC)
        data_feed = file_config.data_feed or "IEX"
        
        if data_feed.upper() == "SIP":
            min_data_age = timedelta(minutes=15)  # SIP requires 15 min delay
        else:
            min_data_age = timedelta(minutes=timeframe_minutes)  # IEX can use shorter delay
        
        if end_date > current_utc_time - min_data_age:
            end_date = current_utc_time - min_data_age
            logger.debug(
                f"Adjusted end_date to {end_date} (minimum {min_data_age.total_seconds()/60} minutes "
                f"in past required for {data_feed} feed)"
            )

        # Fetch historical data
        df, end_of_history_data = fetch_daily_historicaldata(
            symbol=symbol,
            start_date_str=config.get("start_date", "2050-01-22T23:00:00.000-00:00", str),
            end_date_str=end_date,
            historicaldata_client=historicaldata_client,
            timeframe_minutes=timeframe_minutes,
            length=1,
            start_index=start_index,
            data_feed=file_config.data_feed or "IEX",
            logger=logger
        )

        # Determine end time from fetched data
        end_time = df["time"].iloc[-1] if len(df) > 0 else current_utc_time

        # Check if we've reached the end of historical data
        if end_of_history_data:
            return (
                df,
                end_time,
                end_of_history_data,
                config.get("historical_read_sleep_seconds", 1, float),
                True,  # history_mode
                config.get("read_historical_data_create_order", False, bool),
                config.get("read_historical_data_close_order", False, bool),
                False  # is_real_time_started
            )

        # Check if end_time is close to current time
        if (current_utc_time - end_time) <= timedelta(minutes=timeframe_minutes):
            end_of_history_data = True
            logger.info(
                f"End time is within {timeframe_minutes} minutes of current UTC timestamp. "
                "Setting end_of_history_data to True."
            )

        return (
            df,
            end_time,
            end_of_history_data,
            config.get("historical_read_sleep_seconds", 2, float),
            True,  # history_mode
            config.get("read_historical_data_create_order", False, bool),
            config.get("read_historical_data_close_order", False, bool),
            False  # is_real_time_started
        )

    # No valid data mode selected
    logger.warning("No valid data mode selected.")
    return _create_error_response(config)


def _create_error_response(config) -> Tuple[pd.DataFrame, datetime, bool, float, bool, bool, bool, bool]:
    """Create a standard error response tuple for fetch_data."""
    return (
        pd.DataFrame(),
        datetime.now(pytz.UTC),
        True,
        config.get("error_sleep_seconds", 3, float),
        False,
        config.get("create_order", True, bool),
        config.get("close_order", False, bool),
        True
    )


# =============================================================================
# Position Handling
# =============================================================================

def handle_positions(
    symbol: str,
    length: int,
    timeframe_minutes: int,
    slope_method: str,
    trading_client: TradingClient,
    historicaldata_client: StockHistoricalDataClient,
    redis_client: redis.Redis,
    logger,
    config,
    file_config: ConfigLoader
) -> None:
    """
    Main trading loop that handles position monitoring and breakout detection.

    This function continuously:
    1. Fetches market data (real-time or historical)
    2. Calculates technical indicators (slope, pivots)
    3. Detects breakout signals
    4. Publishes signals to Redis queue
    5. Monitors and closes positions when appropriate

    Args:
        symbol: Trading symbol to monitor
        length: Data length for analysis calculations
        timeframe_minutes: Candle timeframe in minutes
        slope_method: Method for slope calculation ('Atr', 'Atr2', 'Atr3')
        trading_client: Alpaca trading client
        historicaldata_client: Alpaca historical data client
        redis_client: Redis client for message publishing
        logger: Logger instance
        config: Database configuration loader
        file_config: File-based configuration loader
    """
    print(f"Trading Manager started for symbol {symbol}.")
    
    # Initialize tracking variables
    state = _initialize_tracking_state(config)
    
    while True:
        try:
            # Update logger level from configuration
            update_logger_level(logger, config)

            # Fetch market data
            (
                df, end_time, state["end_of_data"], sleep_time,
                history_mode, create_order_enabled, close_order_enabled, is_real_time_started
            ) = fetch_data(
                end_of_data=state["end_of_data"],
                symbol=symbol,
                timeframe_minutes=timeframe_minutes,
                trading_client=trading_client,
                historicaldata_client=historicaldata_client,
                start_index=state["start_index"],
                max_retries=file_config.max_retries,
                base_delay=file_config.base_delay,
                config=config,
                file_config=file_config,
                logger=logger
            )

            # Check for end of data condition
            if state["end_of_data"] and not config.get("read_real_data", False, bool):
                print("End of data reached. Real-time data mode is off. EXIT!")
                logger.info("End of data reached. Real-time data mode is off. EXIT!")
                break

            # Determine current date from data
            current_date = _get_current_date(df)
            logger.debug(f"current_date: {current_date}")

            # Handle date change logic
            if state["previous_date"] != current_date:
                state = _handle_date_change(
                    state, current_date, config, trading_client, logger
                )

            # Handle empty data
            if df.empty:
                _handle_empty_data(
                    config, end_time, state["latest_close_time"],
                    timeframe_minutes, trading_client, state["market_hours"], logger
                )
                continue

            logger.info("Data fetched:\n" + str(df.tail()))

            # Update slope calculation dataframe
            state["slope_cal_df"] = _update_slope_dataframe(
                state["slope_cal_df"], df, config, logger
            )

            # Calculate slope
            slope_df, slope = calculate_slope(
                slope_cal_df=state["slope_cal_df"],
                history_mode=history_mode,
                real_data_mode=not history_mode,
                timestamp=end_time,
                slope_bar_count=config.get("slope_bar_count", 0, int),
                trading_client=trading_client,
                historicaldata_client=historicaldata_client,
                symbol=symbol,
                length=length,
                timeframe_minutes=timeframe_minutes,
                max_retries=file_config.max_retries,
                base_delay=file_config.base_delay,
                method=slope_method,
                start_index=state["start_index"],
                data_feed=file_config.data_feed or "IEX",
                logger=logger
            )

            # Calculate pivots
            pivot_high, pivot_low = calculate_pivots(slope_df, length, logger=logger)

            # Update trendlines
            state["slope_ph"] = slope if pivot_high else state["slope_ph"]
            state["slope_pl"] = slope if pivot_low else state["slope_pl"]
            logger.debug(f"Assigned slope - slope_ph: {state['slope_ph']}, slope_pl: {state['slope_pl']}")

            # Process upper and lower bounds
            state["upper"], state["lower"] = process_positions(
                pivot_high=pivot_high,
                pivot_low=pivot_low,
                upper=state["upper"],
                lower=state["lower"],
                slope_ph=state["slope_ph"],
                slope_pl=state["slope_pl"],
                length=length,
                logger=logger
            )

            # Extract latest candle data
            latest_close = df["close"].iloc[-1]
            latest_open = df["open"].iloc[-1]
            latest_high = df["high"].iloc[-1]
            latest_low = df["low"].iloc[-1]
            volume = df["volume"].iloc[-1]
            state["latest_close_time"] = df["time"].iloc[-1]

            logger.debug(f"Latest data - Close: {latest_close}, Open: {latest_open}, Volume: {volume}")
            logger.debug(f"Latest data - High: {latest_high}, Low: {latest_low}")

            # Update position signals
            state["upper_position_signal"], state["lower_position_signal"] = update_positions(
                latest_close=latest_close,
                prev_upos=state["prev_upper_signal"],
                prev_dnos=state["prev_lower_signal"],
                upper=state["upper"],
                lower=state["lower"],
                pivot_high=pivot_high,
                pivot_low=pivot_low,
                slope_ph=state["slope_ph"],
                slope_pl=state["slope_pl"],
                length=length,
                logger=logger
            )

            # Handle daily position count reset
            logger.debug(f"current_date: {current_date}, last_processed_date: {state['last_processed_date']}")
            if current_date != state["last_processed_date"]:
                state["open_position_count"] = 0
                logger.debug("Date has changed, resetting position count to 0.")

            # Parse skip trading days
            skip_trading_days = _parse_skip_trading_days(config)
            redis_queue_name = file_config.redis_stream_name_breakout_message
            logger.debug(f"Redis Queue name: {redis_queue_name}, skip_trading_days: {skip_trading_days}")

            # Calculate bar strength
            bar_strength = calculate_bar_strength(latest_close, latest_open, latest_high, latest_low)
            logger.debug(f"bar_strength: {bar_strength}")

            # Check for breakouts
            (
                new_open, new_breakout_type, state["prev_kalman_filter"], state["prev_velocity"]
            ) = check_for_breakouts(
                prev_kfilt=state["prev_kalman_filter"],
                prev_velocity=state["prev_velocity"],
                enable_kalman_prediction=config.get("enable_kalman_prediction", True, bool),
                skip_trading_days_list=skip_trading_days,
                latest_close_time=state["latest_close_time"],
                choppy_day_cnt=state["choppy_day_count"],
                bar_head_cnt=state["bar_head_count"],
                maxvolume=config.get("max_volume_threshold", 190000, int),
                min_gap_bars_cnt_for_breakout=config.get("min_gap_bars_cnt_for_breakout", 100, int),
                positioncnt=state["open_position_count"],
                positionqty=config.get("max_daily_positions", 50, int),
                createorder=create_order_enabled,
                upos=state["upper_position_signal"],
                prev_upos=state["prev_upper_signal"],
                dnos=state["lower_position_signal"],
                prev_dnos=state["prev_lower_signal"],
                bar_strength=bar_strength,
                latest_close=latest_close,
                latest_open=latest_open,
                latest_high=latest_high,
                latest_low=latest_low,
                skip_candle_with_size=config.get("skip_candle_with_size", 50, float),
                volume=volume,
                symbol=symbol,
                trading_client=trading_client,
                redis_client=redis_client,
                redis_queue_name_str=redis_queue_name,
                bar_date=current_date,
                logger=logger
            )

            # Handle new breakout detection
            if new_breakout_type is not None:
                state["prev_open"] = new_open
                state["prev_breakout_type"] = new_breakout_type
                state["bar_head_count"] = 0
                state["monitor_bar_count"] = True
                state["last_processed_date"] = current_date
                state["open_position_count"] += 1
                logger.debug(
                    f"Position count incremented. New count: {state['open_position_count']} "
                    f"for date {state['last_processed_date']}"
                )

            # Increment bar head count if monitoring
            if state["monitor_bar_count"]:
                state["bar_head_count"] += 1
                logger.debug(f"bar_head_count: {state['bar_head_count']}")

            # Handle position closing
            close_result = _handle_position_closing(
                trading_client=trading_client,
                close_order_enabled=close_order_enabled,
                redis_queue_name=redis_queue_name,
                redis_client=redis_client,
                bar_strength=bar_strength,
                latest_close_time=state["latest_close_time"],
                prev_open=state["prev_open"],
                prev_breakout_type=state["prev_breakout_type"],
                latest_close=latest_close,
                symbol=symbol,
                volume=volume,
                choppy_day_count=state["choppy_day_count"],
                logger=logger
            )

            if close_result in (POSITION_CLOSED, NO_POSITION_FOUND, ERROR_OCCURRED):
                state["prev_open"] = 0
                state["prev_breakout_type"] = None

            # Store current values for next iteration
            state["prev_upper_signal"] = state["upper_position_signal"]
            state["prev_lower_signal"] = state["lower_position_signal"]

            # Monitor for choppy day conditions
            state = _monitor_choppy_conditions(
                state=state,
                config=config,
                is_real_time_started=is_real_time_started,
                trading_client=trading_client,
                latest_close=latest_close,
                latest_high=latest_high,
                latest_low=latest_low,
                logger=logger
            )

            # Increment start index for next iteration
            state["start_index"] += 1
            logger.debug(f"End time returned is: {end_time}")

            # Determine sleep time
            sleep_time = _calculate_sleep_time(
                is_real_time_started=is_real_time_started,
                config=config,
                end_time=end_time,
                latest_close_time=state["latest_close_time"],
                timeframe_minutes=timeframe_minutes,
                trading_client=trading_client,
                market_hours=state["market_hours"],
                logger=logger,
                default_sleep=sleep_time
            )

            time.sleep(sleep_time)

        except ValueError as ve:
            _handle_exception(ve, "ValueError", config, logger)
        except Exception as e:
            _handle_exception(e, "Unexpected error", config, logger)
        finally:
            logger.info("------------------------------------")


def _initialize_tracking_state(config) -> dict:
    """Initialize the state dictionary for position tracking."""
    return {
        # Position bounds
        "upper": 0,
        "lower": 0,
        "slope_ph": 0,
        "slope_pl": 0,
        
        # Position signals
        "upper_position_signal": 0,
        "lower_position_signal": 0,
        "prev_upper_signal": float("nan"),
        "prev_lower_signal": float("nan"),
        
        # Kalman filter state
        "prev_kalman_filter": 0.0,
        "prev_velocity": 0.0,
        
        # Breakout tracking
        "prev_open": 0,
        "prev_breakout_type": None,
        "bar_head_count": 0,
        "monitor_bar_count": False,
        
        # Position counts
        "open_position_count": 0,
        
        # Date tracking
        "last_processed_date": (datetime.now() - timedelta(days=1)).date(),
        "previous_date": None,
        
        # Data state
        "start_index": 14,
        "end_of_data": not config.read_historical_data,
        "slope_cal_df": pd.DataFrame(),
        "latest_close_time": None,
        
        # Choppy day tracking
        "choppy_day_count": 0,
        "tracked_candles": {},
        
        # Market hours
        "market_hours": None,
    }


def _get_current_date(df: pd.DataFrame) -> datetime.date:
    """Extract current date from dataframe or use current UTC date."""
    if df.empty:
        return datetime.now(timezone.utc).date()
    return df["time"].iloc[-1].date()


def _handle_date_change(state: dict, current_date, config, trading_client, logger) -> dict:
    """Handle logic when the trading date changes."""
    logger.debug("Previous date is not equal to current date.")
    
    # Update market hours
    state["market_hours"] = get_market_hours(config, trading_client, logger)
    logger.info(f"Market hours: {state['market_hours']}")
    
    # Reset choppy day tracking
    state["choppy_day_count"] = 0
    state["tracked_candles"].clear()
    
    # Update date tracking
    state["previous_date"] = current_date
    
    # Reset breakout tracking for new day
    state["prev_open"] = 0
    state["prev_breakout_type"] = None
    
    return state


def _handle_empty_data(config, end_time, latest_close_time, timeframe_minutes, 
                       trading_client, market_hours, logger) -> None:
    """Handle case when no data is fetched."""
    min_data_age = timedelta(minutes=config.get("min_data_age_threshold", 0, int))
    sleep_time = sleep_determination_extended(
        config=config,
        current_time=end_time - min_data_age,
        latest_close_time=latest_close_time,
        timeframe_minutes=timeframe_minutes,
        trading_client=trading_client,
        market_hours=market_hours,
        live_extra_sleep_seconds=config.get("live_extra_sleep_seconds", 0.25, float),
        logger=logger
    )
    
    print(f"No data fetched. Retrying in {sleep_time} seconds.")
    logger.warning(f"No data fetched. Retrying in {sleep_time} seconds.")
    time.sleep(sleep_time)


def _update_slope_dataframe(slope_cal_df: pd.DataFrame, df: pd.DataFrame, 
                            config, logger) -> pd.DataFrame:
    """Update the slope calculation dataframe with new data."""
    slope_cal_df = pd.concat([slope_cal_df, df], ignore_index=True)
    
    max_bars = config.get("slope_bar_count", 0, int)
    if len(slope_cal_df) > max_bars:
        slope_cal_df = slope_cal_df.iloc[-100:].reset_index(drop=True)
    
    logger.info("Slope Data fetched:\n" + str(slope_cal_df.tail()))
    return slope_cal_df


def _parse_skip_trading_days(config) -> list:
    """Parse skip trading days from configuration."""
    skip_days_str = config.get("skip_trading_days", "", str)
    return [
        datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        for date_str in skip_days_str.split(",")
        if date_str.strip()
    ]


def _handle_position_closing(
    trading_client, close_order_enabled, redis_queue_name, redis_client,
    bar_strength, latest_close_time, prev_open, prev_breakout_type,
    latest_close, symbol, volume, choppy_day_count, logger
) -> str:
    """Handle position closing logic."""
    if trading_client.get_clock().is_open:
        return close_positions(
            closeorder=close_order_enabled,
            redis_queue_name=redis_queue_name,
            redis_client=redis_client,
            bar_strength=bar_strength,
            latest_close_time=latest_close_time,
            prev_open=prev_open,
            prev_breakout_type=prev_breakout_type,
            latest_close=latest_close,
            symbol=symbol,
            volume=volume,
            choppy_day_cnt=choppy_day_count,
            logger=logger
        )
    return MARKET_CLOSED


def _monitor_choppy_conditions(
    state: dict, config, is_real_time_started, trading_client,
    latest_close, latest_high, latest_low, logger
) -> dict:
    """Monitor and update choppy day conditions."""
    if not config.enable_chopping:
        return state
    
    if is_real_time_started:
        if trading_client.get_clock().is_open:
            state["tracked_candles"], state["choppy_day_count"] = monitor_candles_close(
                tracked_candles=state["tracked_candles"],
                latest_close_time=state["latest_close_time"],
                current_close=latest_close,
                latest_high=latest_high,
                latest_low=latest_low,
                logger=logger
            )
    else:
        # Check if within regular trading hours (14:30 - 21:00 UTC)
        start_utc_time = datetime_time(14, 30)
        end_utc_time = datetime_time(21, 0)
        current_time = state["latest_close_time"].time()
        
        if start_utc_time <= current_time <= end_utc_time:
            state["tracked_candles"], state["choppy_day_count"] = monitor_candles_close(
                tracked_candles=state["tracked_candles"],
                latest_close_time=state["latest_close_time"],
                current_close=latest_close,
                latest_high=latest_high,
                latest_low=latest_low,
                logger=logger
            )
    
    logger.debug(f"Return choppy_day_count is: {state['choppy_day_count']}")
    return state


def _calculate_sleep_time(
    is_real_time_started, config, end_time, latest_close_time,
    timeframe_minutes, trading_client, market_hours, logger, default_sleep
) -> float:
    """Calculate appropriate sleep time before next iteration."""
    if is_real_time_started:
        min_data_age = timedelta(minutes=config.get("min_data_age_threshold", 0, int))
        extra_sleep = config.get("live_extra_sleep_seconds", 0.25, float)
        
        sleep_time = sleep_determination_extended(
            config=config,
            current_time=end_time - min_data_age,
            latest_close_time=latest_close_time,
            timeframe_minutes=timeframe_minutes,
            trading_client=trading_client,
            market_hours=market_hours,
            live_extra_sleep_seconds=extra_sleep,
            logger=logger
        ) + extra_sleep
        
        print(f"Retrying in {sleep_time} seconds. Latest fetched bar time is {latest_close_time}")
        logger.warning(f"Retrying in {sleep_time} seconds. Latest fetched bar time is {latest_close_time}")
    else:
        sleep_time = default_sleep
        print(f"Sleep configured is {sleep_time} seconds. Latest fetched bar time is {latest_close_time}")
        logger.info(f"Sleep configured is {sleep_time} seconds. Latest fetched bar time is {latest_close_time}")
    
    return sleep_time


def _handle_exception(exception, error_type: str, config, logger) -> None:
    """Handle exceptions during position handling."""
    error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    error_traceback = traceback.format_exc()
    
    logger.error(f"{error_type} encountered at {error_time}: {exception}\nTraceback:\n{error_traceback}")
    print(f"{error_type} encountered: {exception}")
    
    sleep_seconds = config.get("error_sleep_seconds", 1, float)
    logger.info(f"Sleep configured is {sleep_seconds} seconds")
    time.sleep(sleep_seconds)


# =============================================================================
# Main Entry Point
# =============================================================================

def main(
    symbol: str,
    length: int,
    timeframe_minutes: int,
    slope_method: str,
    log_to_file: bool
) -> None:
    """
    Main entry point for the breakout detection system.

    Args:
        symbol: Trading symbol to monitor (e.g., 'SPY')
        length: Data length for analysis calculations
        timeframe_minutes: Candle timeframe in minutes
        slope_method: Method for slope calculation
        log_to_file: If True, log to file; otherwise log to console
    """
    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Load file-based configuration
    file_config = ConfigLoader(config_file="config/config.ini")

    # Initialize database configuration loader
    db_config_loader = DatabaseConfigLoader(
        profile_id=1,
        db_host=file_config.db_host,
        db_user=file_config.db_user,
        db_password=file_config.db_password,
        db_name=file_config.db_name,
        tables=["dd_common_config", "dd_open_position_config"],
        refresh_interval=300
    )

    # Fetch credentials from SSM
    #api_key = get_ssm_parameter(f'profile{profile_id}_apikey')
    #api_secret = get_ssm_parameter(f'profile{profile_id}_apisecret')
    
    # Get API credentials
    api_key, api_secret = get_credentials("1")

    # Initialize logger
    log_file_path = os.path.join(logs_dir, "breakout_detector.log")
    logger = setup_logger(db_config_loader, log_to_file=log_to_file, file_name=log_file_path)

    # Initialize clients
    trading_client = initialize_trading_client(api_key, api_secret, logger)
    historicaldata_client = initialize_historical_data_client(api_key, api_secret, logger)
    redis_client = initialize_redis_client(
        host=file_config.redis_host,
        port=file_config.redis_port,
        password=file_config.redis_password,
        logger=logger
    )

    # Start position handling loop
    handle_positions(
        symbol=symbol,
        length=length,
        timeframe_minutes=timeframe_minutes,
        slope_method=slope_method,
        trading_client=trading_client,
        historicaldata_client=historicaldata_client,
        redis_client=redis_client,
        logger=logger,
        config=db_config_loader,
        file_config=file_config
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the breakout detection trading script."
    )
    parser.add_argument(
        "--symbol",
        default="SPY",
        help="Stock symbol to trade (default: SPY)"
    )
    parser.add_argument(
        "--length",
        type=int,
        default=15,
        help="Data length for analysis (default: 15)"
    )
    parser.add_argument(
        "--timeframe_minutes",
        type=int,
        default=3,
        help="Timeframe in minutes (default: 3)"
    )
    parser.add_argument(
        "--multiplier",
        type=float,
        default=1.0,
        help="Multiplier for slope calculation (default: 1.0)"
    )
    parser.add_argument(
        "--slope_method",
        default="Atr",
        choices=["Atr", "Atr2", "Atr3"],
        help="Method for slope calculation (default: Atr)"
    )
    parser.add_argument(
        "--log_to_console",
        default=False,
        action="store_true",
        help="Log to console (default is to file)"
    )
    parser.add_argument(
        "--log_to_file",
        dest="log_to_console",
        action="store_false",
        help="Log to file instead of console"
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        length=args.length,
        timeframe_minutes=args.timeframe_minutes,
        slope_method=args.slope_method,
        log_to_file=not args.log_to_console
    )

