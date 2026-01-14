import logging
import pandas as pd
import pytz
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.requests import StockBarsRequest
from alpaca.common.enums import Sort
from datetime import datetime, timedelta, time
from logger_config import setup_logger
from alpaca.trading.requests import GetCalendarRequest
from alpaca.data.enums import DataFeed
import time
from customtimeit import time_it
import random

# Function to fetch the most recent bars as historical data
@time_it
def fetch_latest_data(symbol, trading_client, historicaldata_client, end_time,
                        length=1, timeframe_minutes=3, 
                        max_retries=3, base_delay=2,
                        logger=None):
    # Approximate start time based on 3-minute intervals and 70 bars
    approx_start_time = end_time - timedelta(minutes=length * timeframe_minutes + 1)
    logger.debug(f"Approximate start time based on length {length} candles: approx_start_time: {approx_start_time}, endtime: {end_time}")

    # Check if we have enough data in the last trading day; if not, fetch additional days
    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(timeframe_minutes, TimeFrameUnit.Minute),
        start=approx_start_time,
        end=end_time,
        sort=Sort.ASC,
        limit=length,
        feed=DataFeed.IEX  # Add this line to specify IEX data
    )
    #print(f"Requesting stock bars from {approx_start_time} to {end_time}")
    #logger.info(f"Requesting stock bars from {approx_start_time} to {end_time}")

    # Fetch bars
    bars = None

    for attempt in range(1, max_retries + 1):
        try:
            # Attempt to fetch stock bars
            bars = historicaldata_client.get_stock_bars(request_params)
        except (ConnectionResetError, Exception) as e:
            if attempt == max_retries:
                print(f"Max retries {max_retries} reached. Unable to fetch data: {e}")
                logger.error(f"Max retries {max_retries} reached. Unable to fetch data: {e}")
                return pd.DataFrame()
            
            # Log the retry attempt
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)  # Exponential backoff with jitter
            print(f"Attempt {attempt} failed to fetch bars. Retrying in {delay:.2f} seconds. Error: {e}")
            logger.warning(f"Attempt {attempt} failed to fetch bars. Retrying in {delay:.2f} seconds. Error: {e}")
            time.sleep(delay)
            
    logger.debug(f"Bars object: {bars}")
    logger.debug(f"Bars data keys: {bars.data.keys()}")
    
    if symbol not in bars.data:
        logger.error(f"Symbol {symbol} not found in fetched bars data.")
        return pd.DataFrame()

    # Create DataFrame from bar data
    bar_data = bars.data[symbol]  # Accessing bars for the specific symbol            
    logger.debug(f"Fetched {len(bar_data)} bars for {symbol}.")

    # Check if bars data is available
    if len(bar_data) < 1:
        logger.debug("No bars found for the requested timeframe. Will only go history if atleast 1 is returned. ")
        return pd.DataFrame()  # Return an empty DataFrame if no data is available

    elif len(bar_data) == length:
      
        # If you want to build a new DataFrame with specific columns:
        # Assuming bar_data has 'timestamp' as the index and not as a column
        # Extract 'timestamp' from MultiIndex and create a new column

        # Ensure we're working with a copy to avoid modifying a slice
        #bar_data['time'] = bar_data.index.get_level_values('timestamp')

        df = pd.DataFrame({
        'open': [bar.open for bar in bar_data],
        'close': [bar.close for bar in bar_data],
        'high': [bar.high for bar in bar_data],
        'low': [bar.low for bar in bar_data],
        'volume': [bar.volume for bar in bar_data],  # Add volume here
        'time': [bar.timestamp for bar in bar_data]
        })


        df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.tz_convert('UTC')
        #print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Fetched latest data:\n{df}")
        return df

    elif len(bar_data) < length:

        logger.debug("Need more bars, fetching from previous trading day(s)")

        # Create a GetCalendarRequest object for the past week
        calendar_request = GetCalendarRequest(
            start=(end_time.date() - timedelta(days=7)),
            end=end_time.date()
        )
        
        # Fetch the trading calendar for the specified date range
        calendar = trading_client.get_calendar(calendar_request)

        # Print the full calendar data for inspection
        logger.debug(f"Fetched calendar data: {calendar}")

        # Extract the last two trading days (you can adjust the slice if needed)
        #recent_trading_days = [day.date for day in calendar][-2:]  # Last trading day
        recent_trading_days = calendar[-2:]  # Select the last two entries from the calendar

        # Print the extracted recent trading days
        #print(f"Recent trading days: {recent_trading_days}")

        # You can also check the length of the calendar data to verify how many days were returned
        #print(f"Total number of trading days fetched: {len(calendar)}")

        # Set start time to the beginning of the second-to-last trading day
        approx_start_time = recent_trading_days[0].open.astimezone(pytz.timezone('America/New_York'))

        # Re-fetch with the adjusted start time
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(timeframe_minutes, TimeFrameUnit.Minute),
            start=approx_start_time,
            end=end_time,
            sort=Sort.ASC,
            feed=DataFeed.IEX  # Add this line to specify SIP data
        )

        
        # Fetch bars
        bar_data = None

        for attempt in range(1, max_retries + 1):
            try:
                # Attempt to fetch stock bars
                bar_data = historicaldata_client.get_stock_bars(request_params)
            except (ConnectionResetError, Exception) as e:
                if attempt == max_retries:
                    print(f"Max retries {max_retries} reached. Unable to fetch data: {e}")
                    logger.error(f"Max retries {max_retries} reached. Unable to fetch data: {e}")
                    return pd.DataFrame()
                
                # Log the retry attempt
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Attempt {attempt} failed to fetch historical bars. Retrying in {delay:.2f} seconds. Error: {e}")
                logger.warning(f"Attempt {attempt} failed to fetch historical bars. Retrying in {delay:.2f} seconds. Error: {e}")
                time.sleep(delay)
        
        #bar_data = historicaldata_client.get_stock_bars(request_params)
        df = bar_data.df

        # Get the last 70 bars
        recent_bars = df.tail(length)

        # Debug: Print timestamps of the fetched bars
        #for i, bar in enumerate(bar_data):
        #    print(f"Bar {i}: Time: {bar.timestamp}, Close: {bar.close}, High: {bar.high}, Low: {bar.low}")

        bar_data = recent_bars.copy()  # Accessing bars for the specific symbol            
       
        # If you want to build a new DataFrame with specific columns:
        # Assuming bar_data has 'timestamp' as the index and not as a column
        # Extract 'timestamp' from MultiIndex and create a new column


        # Ensure we're working with a copy to avoid modifying a slice
        bar_data['time'] = bar_data.index.get_level_values('timestamp')

        # Create the new DataFrame with 'time' and other columns
        latest_bars = pd.DataFrame({
            'open': bar_data['open'],
            'close': bar_data['close'],
            'high': bar_data['high'],
            'low': bar_data['low'],
            'volume': bar_data['volume'],
            'time': bar_data['time']
        })


        # Ensure 'time' column is in UTC and convert if necessary
        latest_bars['time'] = pd.to_datetime(latest_bars['time'], errors='coerce').dt.tz_convert('UTC')

        #print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Fetched latest data:\n{latest_bars}")
        return latest_bars
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data is available

# Function to fetch the most recent bars as historical data
@time_it
def fetch_daily_historicaldata(symbol, start_date_str, end_date_str, historicaldata_client, 
                            timeframe_minutes=3, length=1, start_index=0, logger=None):

    # Set pandas options to display all rows
    pd.set_option('display.max_rows', None)
    
    # Parse the start and end dates from string arguments
    start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    #end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    end_date = end_date_str
    
    # Initialize an empty DataFrame to store fetched bars
    df = pd.DataFrame()
    endofHistorydata = False

    # Define request parameters to fetch a batch of `length` candles starting from `start_index`
    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(timeframe_minutes, TimeFrameUnit.Minute),
        start=start_date,
        end=end_date,
        limit=length + start_index,  # Fetch enough bars to allow slicing from start_index
        feed=DataFeed.IEX  # Add this line to specify SIP data
    )

    logger.debug(f"Fetching historical data for {symbol} from {start_date} to {end_date} with limit={length + start_index}.")

    try:
        # Fetch bars data
        bars = historicaldata_client.get_stock_bars(request_params)
        if not bars.data or symbol not in bars.data:
            endofHistorydata = True
            return df, endofHistorydata
        
        # Extract bar data for the symbol
        bar_data = bars.data[symbol]
        
        logger.debug(f"Fetched {len(bar_data)} bars for {symbol} using Historical function.")

        # Create a DataFrame from the fetched bars
        data_df = pd.DataFrame({
            'open': [bar.open for bar in bar_data],
            'close': [bar.close for bar in bar_data],
            'high': [bar.high for bar in bar_data],
            'low': [bar.low for bar in bar_data],
            'volume': [bar.volume for bar in bar_data],
            'time': [bar.timestamp for bar in bar_data]
        })
        
        # Slice the DataFrame to get the window starting at `start_index` and of size `length`
        df = data_df.iloc[start_index:start_index + length]

        # Update endofHistorydata if fewer than `length` candles are returned
        if len(df) < length:
            print("End of historical data reached or insufficient data for required length.")
            logger.info("End of historical data reached or insufficient data for required length.")
            endofHistorydata = True

    except Exception as e:
        print(f"Error fetching bars for {symbol}: {str(e)}")
        logger.error(f"Error fetching bars for {symbol}: {str(e)}")
        endofHistorydata = True

    return df, endofHistorydata


def fetch_datafromfile(file_path, symbol, length=29, start_index=14, logger=None):    
    # Load data from the Excel file
    try:
        df = pd.read_excel(file_path)

        # Ensure the DataFrame has the expected structure
        if df.shape[1] < 5:
            print("Excel file must have at least three columns (Close, High, Low, time, volume).")
            logger.info("Excel file must have at least three columns (Close, High, Low, time, volume).")
            return pd.DataFrame(), True

        # Rename columns for clarity
        df.columns = ['open', 'close', 'high', 'low', 'time', 'volume']

        # Debug: Print the DataFrame shape and first few rows
        logger.debug(f"DataFrame shape: {df.shape} with index {start_index}")

        # Check if there’s enough data to slice
        if start_index > len(df):
            print("Reached the end of the file.")
            logger.info("Reached the end of the file.")
            return pd.DataFrame(), True  # Return an empty DataFrame as an end-of-file indicator
        #elif len(df) < start_index  :
        #    print(f"Not enough data to fetch the requested length. Returning available data from index {start_index}.")
        #    logger.info(f"Not enough data to fetch the requested length. Returning available data from index {start_index}.")
        #    sliced_df = df.iloc[start_index:].reset_index(drop=True)  # Return remaining data
        #    return sliced_df, True

        # Slice the DataFrame based on start_index and length
        if length == 1:
            # Fetch the exact record at start_index
            sliced_df = df.iloc[start_index-1:start_index].reset_index(drop=True)
        else:
            sliced_df = df.iloc[start_index:start_index + length].reset_index(drop=True)
        return sliced_df, False

    except Exception as e:
        print(f"Error reading from file: {str(e)}")
        logger.error(f"Error reading from file: {str(e)}")
        return pd.DataFrame(), True

