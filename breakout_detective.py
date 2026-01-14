import argparse
import threading
import time
import pandas as pd
import pytz
import os
import logging
import traceback
from datetime import datetime, timedelta, timezone, time as datetime_time  # Alias to avoid conflict
from fetch_data import fetch_latest_data, fetch_datafromfile, fetch_daily_historicaldata
from pivot_slope_calculations import calculate_pivots, calculate_slope, check_for_breakouts
from positions import process_positions, update_positions, close_positions
from misc import get_market_hours, get_credentials, sleep_determination_extended, calculate_bar_strength, get_ssm_parameter
from logger_config import setup_logger, update_logger_level
from alpaca.trading.client import TradingClient
from choppingday import monitor_candles_close
from config_loader import ConfigLoader
from alpaca.data.historical import StockHistoricalDataClient
from customtimeit import time_it
from database_config_loader import DatabaseConfigLoader
import redis

# Initialize Redis API clients
def initialize_redis_client(host, port, password, logger):
    print("Initializing Redis Client...")
    logger.info("Initializing Redis Client....")
    return redis.Redis(
        host=host,
        port=port,
        password=password,     # <--- Added
        decode_responses=True, # optional, returns strings instead of bytes
        socket_timeout=5
    )

# Initialize Alpaca API clients
def initialize_trading_client(api_key, api_secret, logger) -> TradingClient:
    print("Initializing Alpaca API...")
    logger.info("Initializing Alpaca TradingClient with provided API keys.")
    return TradingClient(api_key, api_secret, paper=False)

# Initialize Alpaca API clients
def initialize_historical_data_client(api_key, api_secret,logger) -> StockHistoricalDataClient:
    print("Initializing Alpaca Historical Data Client API...")
    logger.info("Initializing Alpaca HistoricalDataClient with provided API keys.")
    return StockHistoricalDataClient(api_key=api_key, secret_key=api_secret)


# Function to fetch appropriate data
def fetch_data(end_of_data, symbol, timeframe_minutes, trading_client, historicaldata_client, start_index, max_retries, base_delay, config, logger) -> pd.DataFrame:
    logger.debug(f"Fetching data for symbol: {symbol}, start_index: {start_index}, end_of_data: {end_of_data}")
    logger.debug(f"max_retries: {max_retries}, base_delay: {base_delay}")


    if config.get("read_real_data", True, bool) and end_of_data:
        logger.info("Fetching real-time data.")
        print("Fetching real-time data.")

        df = fetch_latest_data(
            symbol, trading_client, historicaldata_client,
            datetime.now(pytz.UTC), 1, timeframe_minutes, 
            max_retries, base_delay,
            logger=logger
        )
        logger.debug(f"Fetched {len(df)} real-time data points.")
        return df, datetime.now(pytz.UTC), True, config.get("chart_sleep_seconds", 1, float), False, config.get("create_order", True, bool), config.get("close_order", False, bool), True
 
    elif config.get("read_historical_data", True, bool) and not end_of_data:
        logger.info("Using historical data fetch mode.")
        print("Using historical data fetch mode.")

        # Convert config.end_date to datetime if it's a string
        try:
            end_date = datetime.fromisoformat(config.get("end_date", '2025-01-22T23:00:00.000-00:00', str).replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"Failed to parse end_date: {config.get('end_date', '2025-01-22T23:00:00.000-00:00', str)}. Error: {e}")
            return pd.DataFrame(), datetime.now(pytz.UTC), True, config.get("error_sleep_seconds", 3, float), False, config.get("create_order", True, bool), config.get("close_order", False, bool), False

        # Check if end_date needs adjustment
        current_utc_time = datetime.now(pytz.UTC)
        
        if end_date > current_utc_time - timedelta(minutes=timeframe_minutes):
            # Adjust end_date to meet the minimum data age requirement
            end_date = current_utc_time - timedelta(minutes=timeframe_minutes)

        df, endofHistorydata = fetch_daily_historicaldata(
            symbol, config.get('start_date', '2050-01-22T23:00:00.000-00:00', str), end_date, historicaldata_client, 
            timeframe_minutes, 1, start_index=start_index, logger=logger
        )

        if len(df) > 0:
            end_time = df['time'].iloc[-1]
        else:
            end_time = current_utc_time

        if endofHistorydata:            
            return df, end_time, endofHistorydata, config.get("historical_read_sleep_seconds", 1, float), True, config.get("read_historical_data_create_order", False, bool), config.get("read_historical_data_close_order", False, bool), False

        # Check if end_time is within 18 minutes of the current UTC time
        if (current_utc_time - end_time) <=  timedelta(minutes=timeframe_minutes):
            endofHistorydata = True
            logger.info(f"End time is within {timeframe_minutes} minutes of the current UTC timestamp. Setting endofHistorydata to True.")

        return df, end_time, endofHistorydata, config.get("historical_read_sleep_seconds", 2, float), True, config.get("read_historical_data_create_order", False, bool), config.get("read_historical_data_close_order", False, bool), False
    
   
    logger.warning("No valid data mode selected.")
    return pd.DataFrame(), datetime.now(pytz.UTC), True, config.get("error_sleep_seconds", 3, float), False, config.get("create_order", True, bool), config.get("close_order", False, bool), True



def handle_positions(symbol, length, timeframe_minutes, slope_method, trading_client, historicaldata_client, redis_client, logger, config, file_config):
    print(f"Trading Manager started for symbol {symbol}.")
    upper, lower, slope_ph, slope_pl, prev_open, prev_breakout_type = 0, 0, 0, 0, 0, None
    upos, dnos, start_index = 0, 0, 14
    prev_upos, prev_dnos = float('nan'), float('nan')
    prev_kfilt, prev_velocity = 0.0, 0.0

    open_position_cnt = 0  # Reset the position count

    
    # Initialize last processed datetime when the script starts
    #last_processed_datetime = datetime.now() - timedelta(hours=180)
    # repeatable breakouts to be controlled variable
    bar_head_cnt = 0
    monitor_bar_cnt = False

    # Initialize last_processed_date with  the current date
    #last_processed_date = datetime.now().date()
    last_processed_date = (datetime.now() - timedelta(days=1)).date()

    # Initialize previous_date as None to track the last processed date
    previous_date = None

    # Define constants for return indicators
    NO_POSITION_FOUND = "no_position_found"
    POSITION_CLOSED = "position_closed"
    ERROR_OCCURRED = "error_occurred"
    POSITION_CLOSE_SKIP = "position_close_skip"


    slope_cal_df = pd.DataFrame()
    latest_close_time = None
    
    choppy_day_cnt = 0
    tracked_candles = {}

    if not config.read_historical_data:
        end_of_data = True
    else:
        end_of_data = False

    while True:

        try:

            # Periodically check and update the log level
            update_logger_level(logger, config)

            df, endtime, end_of_data, sleeptime, history_mode, create_order_arg, close_order_arg, is_real_time_started = fetch_data(
                end_of_data, symbol, timeframe_minutes, trading_client, historicaldata_client, start_index, file_config.max_retries, file_config.base_delay, config, logger=logger
            )
            if end_of_data and not config.get("read_real_data", False, bool):
                print("End of data reached. Real Time data mode is off. EXIT !")
                logger.info("End of data reached. Real Time data mode is off. EXIT !")
                break

            if df.empty:
                current_date = datetime.now(timezone.utc).date()
            else: 
                latest_close_time = df['time'].iloc[-1]
                current_date = latest_close_time.date()


            logger.debug(f"current_date: {current_date}")

            if previous_date != current_date:
                logger.debug("Previous date is not equal to current date.")
                # Run the code since the date has changed
                market_hours = get_market_hours(config, trading_client, logger)
                logger.info(f"Market hours: {market_hours}")

                # rest to 0 as the day changes...
                choppy_day_cnt = 0

                # new date switching copping date
                tracked_candles.clear()

                # Update previous_date to the current date
                previous_date = current_date

                #reset the prev open and breakouts
                prev_open, prev_breakout_type = 0, None


            if df.empty:
                #print(f"config.live_extra_sleep_seconds : {config.live_extra_sleep_seconds}")
                # Determine sleep time
                no_data_fetch_sleep_time = sleep_determination_extended(config, endtime - timedelta(minutes=config.get("min_data_age_threshold", 0, int)), 
                                                        latest_close_time, timeframe_minutes, trading_client,
                                                        market_hours, config.get("live_extra_sleep_seconds", 0.25, float), logger )

                print(f"No data fetched. Retrying in {no_data_fetch_sleep_time} seconds.")
                logger.warning(f"No data fetched. Retrying in {no_data_fetch_sleep_time} seconds.")
                time.sleep(no_data_fetch_sleep_time)
                continue

            logger.info("Data fetched :\n" + str(df.tail()))

            slope_cal_df = pd.concat([slope_cal_df, df], ignore_index=True)
            # Keep only the last 100 rows
            if len(slope_cal_df) > config.get("slope_bar_count", 0, int):
                slope_cal_df = slope_cal_df.iloc[-100:].reset_index(drop=True)
            
            logger.info("Slope Data fetched :\n" + str(slope_cal_df.tail()))

            slope_df, slope = calculate_slope(
                slope_cal_df, history_mode, not history_mode, endtime, 
                config.get("slope_bar_count", 0, int), trading_client, historicaldata_client, 
                symbol, length, timeframe_minutes, 
                file_config.max_retries, file_config.base_delay,
                slope_method, start_index=start_index, logger=logger)

            pivot_high, pivot_low = calculate_pivots(slope_df, length, logger=logger)

            # Update trendlines
            slope_ph, slope_pl = (slope if pivot_high else slope_ph), (slope if pivot_low else slope_pl)
            logger.debug(f"Assigned slope to slope_ph: {slope_ph}, slope_pl: {slope_pl}")
            upper, lower = process_positions(pivot_high, pivot_low, upper, lower, slope_ph, slope_pl, length, logger=logger)

            latest_close, latest_open, volume = df['close'].iloc[-1], df['open'].iloc[-1], df['volume'].iloc[-1]
            logger.debug(f"Latest data - Close: {latest_close}, Open: {latest_open}, Volume: {volume}")
            latest_high, latest_low = df['high'].iloc[-1], df['low'].iloc[-1]
            logger.debug(f"Latest data - High: {latest_high}, Low: {latest_low}")

            upos, dnos = update_positions(latest_close, prev_upos, prev_dnos, upper, lower, pivot_high, pivot_low, slope_ph, slope_pl, length, logger=logger)

            logger.debug(f"current_date: {current_date}, last_processed_date: {last_processed_date}")
            # Check if the date has changed
            if current_date != last_processed_date:
                open_position_cnt = 0  # Reset the position count
                logger.debug("Date has changed, resetting position count to 0.")

            # Convert to list of datetime.date objects
            skip_trading_days_list = [datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
                          for date_str in config.get("skip_trading_days", "", str).split(",") if date_str.strip()]

            redis_queue_name_str = file_config.redis_stream_name_breakout_message
            logger.debug(f"Redis Queue name : {redis_queue_name_str}, skip_trading_days_list : {skip_trading_days_list}")


            bar_strength = calculate_bar_strength(latest_close, latest_open, latest_high, latest_low)
            logger.debug(f"bar_strength : {bar_strength}")

            new_open, new_breakout_type, prev_kfilt, prev_velocity = check_for_breakouts(
                prev_kfilt, prev_velocity, config.get("enable_kalman_prediction", True, bool), skip_trading_days_list, latest_close_time, choppy_day_cnt,
                bar_head_cnt,config.get("max_volume_threshold", 190000, int), config.get("min_gap_bars_cnt_for_breakout", 100, int), open_position_cnt, config.get("max_daily_positions", 50, int), 
                create_order_arg , upos, prev_upos, dnos, prev_dnos, bar_strength,
                latest_close, latest_open, latest_high, latest_low, config.get("skip_candle_with_size", 50, float), volume, symbol, trading_client, redis_client, redis_queue_name_str,
                current_date, logger=logger
            )


            # Check if the previous open position count is not equal to the new open position count
            if new_breakout_type is not None:
                
                # Saving previous values
                prev_open = new_open
                prev_breakout_type = new_breakout_type

                # setting current timestamp as latest processed datetime.
                bar_head_cnt = 0
                monitor_bar_cnt = True

                last_processed_date = current_date  # Update the last processed date
                open_position_cnt += 1  # Increment the position count
                logger.debug(f"Position count incremented. New position count: {open_position_cnt} for last_processed_date {last_processed_date}")

            if monitor_bar_cnt:
                bar_head_cnt = bar_head_cnt + 1
                logger.debug(f"bar_head_cnt : {bar_head_cnt}")

            if trading_client.get_clock().is_open:
                result = close_positions(close_order_arg, redis_queue_name_str, redis_client, bar_strength, latest_close_time, prev_open, prev_breakout_type, latest_close, symbol, volume, choppy_day_cnt, logger=logger)
            else:
                result = "MARKET_CLSOED"
                
            logger.debug(f"Closing positions resulted in {result}")

            if result in (POSITION_CLOSED, NO_POSITION_FOUND,ERROR_OCCURRED):
                prev_open, prev_breakout_type = 0, None

            # Store the current values as previous for the next iteration
            prev_upos, prev_dnos = upos, dnos

            if config.enable_chopping:
                if is_real_time_started:
                    if trading_client.get_clock().is_open:
                        tracked_candles, choppy_day_cnt = monitor_candles_close(tracked_candles, latest_close_time, latest_close, latest_high, latest_low, logger)
                        #choppy_day_cnt = is_choppy_day(slope_df, config.atr_threshold, config.price_range_threshold,
                                                #config.reverse_candle_threshold, config.low_volume_threshold, logger=logger, length=14)                            
                else:
                    # Define UTC time boundaries
                    start_utc_time = datetime_time(14, 30)  # 14:30 UTC
                    end_utc_time = datetime_time(21, 00)     # 21:00 UTC
                    #print(f"start_utc_time {start_utc_time} end_utc_time {end_utc_time} latest_close_time {latest_close_time.time()}")
                    if start_utc_time <= latest_close_time.time() <= end_utc_time:
                        tracked_candles, choppy_day_cnt = monitor_candles_close(tracked_candles, latest_close_time, latest_close, latest_high, latest_low, logger)
                        #is_choppy_day_flag = is_choppy_day(slope_df, config.atr_threshold, config.price_range_threshold,
                                                #config.reverse_candle_threshold, config.low_volume_threshold, logger=logger, length=14)
                logger.debug(f"Return choppy_day_cnt is : {choppy_day_cnt}")

            start_index += 1
            logger.debug(f"End time returned is : {endtime}")                
            if is_real_time_started:
                
                # Determine sleep time
                sleeptime = sleep_determination_extended(config, endtime - timedelta(minutes=config.get("min_data_age_threshold", 0, int)), 
                                                        latest_close_time, timeframe_minutes, trading_client,
                                                        market_hours, config.get("live_extra_sleep_seconds", 0.25, float), logger ) + config.get("live_extra_sleep_seconds", 0.25, float)

                print(f"Retrying in {sleeptime} seconds. Latest fetched bar time is {latest_close_time}")
                logger.warning(f"Retrying in {sleeptime} seconds. Latest fetched bar time is {latest_close_time}")
            else:
                print(f"Sleep configured is  {sleeptime} seconds. Latest fetched bar time is {latest_close_time}")
                logger.info(f"Sleep configured is {sleeptime} seconds. Latest fetched bar time is {latest_close_time}")
                
            time.sleep(sleeptime)

        except ValueError as ve:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_traceback = traceback.format_exc()
            logger.error(f"ValueError encountered at {error_time}: {ve}\nTraceback:\n{error_traceback}")

            print(f"ValueError encountered: {ve}")
            logger.info(f"Sleep configured is {config.get('error_sleep_seconds', 1, float)} seconds")
            time.sleep(config.get("error_sleep_seconds", 1, float))

        except Exception as e:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_traceback = traceback.format_exc()
            
            logger.error(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
            print(f"Unexpected error at {error_time}: {e}\nTraceback:\n{error_traceback}")
            logger.info(f"Sleep configured is {config.get('error_sleep_seconds', 1, float)} seconds")
            time.sleep(config.get("error_sleep_seconds", 1, float))

        finally:
            logger.info("------------------------------------")

def main(symbol: str, length: int, timeframe_minutes: int, slope_method: str, log_to_file: bool):
    
    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    file_config = ConfigLoader(config_file=f'config.ini')

    # Initialize DatabaseConfigLoader with credentials from config.ini
    db_config_loader = DatabaseConfigLoader(
        profile_id=1,
        db_host=file_config.db_host,
        db_user=file_config.db_user,
        db_password=file_config.db_password,
        db_name=file_config.db_name,
        tables=["dd_common_config", "dd_open_position_config"],
        refresh_interval=300
    )

    # Example usage:
    client = 'rootprofile'  # Change to 'client1' or any other client as needed
    #api_key = get_ssm_parameter(f'{client}_apikey')
    #api_secret = get_ssm_parameter(f'{client}_apisecret')

    api_key, api_secret = get_credentials("1")

    # Initialize logger with initial level from config
    #db_config_loader = DatabaseConfigLoader(0, tables=["common_config", "open_position_config"])
    logger = setup_logger(db_config_loader, log_to_file=log_to_file, file_name=os.path.join(logs_dir,"breakout_detective.log"))

    trading_client = initialize_trading_client(api_key, api_secret, logger)
    historicaldata_client = initialize_historical_data_client(api_key, api_secret, logger)
    redis_client = initialize_redis_client(file_config.redis_host, file_config.redis_port, file_config.redis_password, logger)

    # Pass the trading clients as a list to handle_positions
    handle_positions(symbol, length, timeframe_minutes, slope_method, trading_client, historicaldata_client, redis_client, logger, db_config_loader, file_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run main trading script for open positions.")
    parser.add_argument('--symbol', default="SPY", help="Stock symbol to trade")
    parser.add_argument('--length', type=int, default=15, help="Data length for analysis")
    parser.add_argument('--timeframe_minutes', type=int, default=3, help="Timeframe in minutes")
    parser.add_argument('--multiplier', type=float, default=1.0, help="Multiplier for slope calculation")
    parser.add_argument('--slope_method', default='Atr', choices=['Atr', 'Atr2', 'Atr3'], help="Method for slope calculation")
    parser.add_argument('--log_to_console', default=False, action='store_true', help="Log to console (default is to file)")
    parser.add_argument('--log_to_file', dest='log_to_console', action='store_false', help="Log to file instead of console")
    args = parser.parse_args()

    main(
        args.symbol, args.length, args.timeframe_minutes,
        args.slope_method, not args.log_to_console
    )