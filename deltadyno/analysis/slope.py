"""
Slope calculation for the DeltaDyno trading system.

This module provides functions to calculate slope values using
Average True Range (ATR) for dynamic support/resistance adjustment.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import talib

from deltadyno.data.fetcher import fetch_latest_data
from deltadyno.utils.timing import time_it


@time_it
def fetch_data_based_on_mode(
    history_mode: bool,
    real_data_mode: bool,
    timestamp: datetime,
    bar_count: int,
    trading_client,
    historicaldata_client,
    symbol: str,
    length: int,
    timeframe_minutes: int,
    start_index: int,
    max_retries: int,
    base_delay: float,
    data_feed: str = "IEX",
    logger=None
) -> Optional[pd.DataFrame]:
    """
    Fetch data for slope calculations based on the current operating mode.

    Args:
        history_mode: Whether historical data mode is active
        real_data_mode: Whether real-time data mode is active
        timestamp: Reference timestamp for data fetching
        bar_count: Number of bars to fetch
        trading_client: Alpaca trading client
        historicaldata_client: Alpaca historical data client
        symbol: Trading symbol
        length: Minimum required data length
        timeframe_minutes: Candle timeframe in minutes
        start_index: Starting index for data
        max_retries: Maximum retry attempts
        base_delay: Base delay for retries
        data_feed: Data feed type ("IEX" or "SIP"), default "IEX"
        logger: Logger instance

    Returns:
        DataFrame with fetched data, or None if insufficient data
    """
    logger.debug(f"Initiating fetch for slope data. Mode - Real: {real_data_mode}, History: {history_mode}")

    df = pd.DataFrame()

    if real_data_mode:
        logger.debug(f"Fetching real-time slope data for {bar_count} records from timestamp {timestamp}")
        df = fetch_latest_data(
            symbol=symbol,
            trading_client=trading_client,
            historicaldata_client=historicaldata_client,
            end_time=timestamp,
            length=bar_count,
            timeframe_minutes=timeframe_minutes,
            max_retries=max_retries,
            base_delay=base_delay,
            data_feed=data_feed,
            logger=logger
        )

    elif history_mode:
        # Add small offset for historical mode
        adjusted_timestamp = timestamp + timedelta(seconds=1)
        logger.debug(f"Fetching historical slope data for {bar_count} records from timestamp {adjusted_timestamp}")
        df = fetch_latest_data(
            symbol=symbol,
            trading_client=trading_client,
            historicaldata_client=historicaldata_client,
            end_time=adjusted_timestamp,
            length=bar_count,
            timeframe_minutes=timeframe_minutes,
            max_retries=max_retries,
            base_delay=base_delay,
            data_feed=data_feed,
            logger=logger
        )

    else:
        logger.warning("No valid mode specified for fetching slope data.")
        return None

    # Validate data sufficiency
    if df.empty or len(df) < length - 1:
        logger.warning(f"Insufficient data for slope calculation. Required: {length}, Found: {len(df)}.")
        return None

    df.reset_index(drop=True, inplace=True)

    logger.info(f"Slope based fetch:\n{str(df.tail())}")
    logger.debug(f"Slope based fetch (full):\n{str(df)}")

    return df


@time_it
def calculate_slope(
    slope_cal_df: pd.DataFrame,
    history_mode: bool,
    real_data_mode: bool,
    timestamp: datetime,
    slope_bar_count: int,
    trading_client,
    historicaldata_client,
    symbol: str,
    length: int,
    timeframe_minutes: int,
    max_retries: int,
    base_delay: float,
    method: str = "Atr",
    start_index: int = 14,
    data_feed: str = "IEX",
    logger=None
) -> Tuple[pd.DataFrame, Decimal]:
    """
    Calculate the slope value using Average True Range (ATR).

    The slope is used to adjust position bounds over time, creating
    dynamic support and resistance levels.

    Args:
        slope_cal_df: Existing DataFrame for slope calculations (if 100 rows)
        history_mode: Whether historical mode is active
        real_data_mode: Whether real-time mode is active
        timestamp: Reference timestamp
        slope_bar_count: Number of bars to use for slope calculation
        trading_client: Alpaca trading client
        historicaldata_client: Alpaca historical data client
        symbol: Trading symbol
        length: ATR calculation period
        timeframe_minutes: Candle timeframe
        max_retries: Maximum retry attempts
        base_delay: Base retry delay
        method: Slope calculation method (currently only 'Atr' supported)
        start_index: Starting index for data fetch
        data_feed: Data feed type ("IEX" or "SIP"), default "IEX"
        logger: Logger instance

    Returns:
        Tuple of (DataFrame used for calculation, calculated slope)
    """
    # Use existing data if we have enough bars
    if len(slope_cal_df) == 100:
        logger.debug("Using pre-calculated slope DataFrame")
        df = slope_cal_df
    else:
        df = fetch_data_based_on_mode(
            history_mode=history_mode,
            real_data_mode=real_data_mode,
            timestamp=timestamp,
            bar_count=slope_bar_count,
            trading_client=trading_client,
            historicaldata_client=historicaldata_client,
            symbol=symbol,
            length=length,
            timeframe_minutes=timeframe_minutes,
            start_index=start_index,
            max_retries=max_retries,
            base_delay=base_delay,
            data_feed=data_feed,
            logger=logger
        )

    if df is None:
        return pd.DataFrame(), Decimal("0.0")

    # Calculate ATR using TA-Lib
    atr_values = talib.ATR(
        high=df["high"].values,
        low=df["low"].values,
        close=df["close"].values,
        timeperiod=length
    )

    # Convert to Decimal for precision (handle NaN)
    last_atr = atr_values[-1]
    if np.isnan(last_atr):
        last_atr = 0.0
    last_atr_value = Decimal(str(last_atr))

    # Calculate slope as ATR divided by length
    slope = (last_atr_value / Decimal(length)).quantize(Decimal("0.0000000001"))

    logger.info(f"Calculated slope: {slope} using method {method}")
    return df, slope

