"""
Utility functions for the DeltaDyno trading system.

This module provides various helper functions for:
- Market hours determination
- API credentials management
- Option symbol generation
- Sleep time calculations
- Bar strength calculations
- Profit/loss calculations
"""

import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz
import boto3
from alpaca.data.requests import OptionLatestQuoteRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import GetCalendarRequest, GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus

from deltadyno.utils.timing import time_it
from deltadyno.constants import PUT, CALL


# =============================================================================
# AWS SSM Parameters
# =============================================================================

def get_ssm_parameter(parameter_name: str, region: str = "us-east-1") -> str:
    """
    Fetch a parameter value from AWS SSM Parameter Store.

    Args:
        parameter_name: Name of the parameter in SSM
        region: AWS region where the parameter is stored

    Returns:
        Parameter value as a string
    """
    ssm = boto3.client("ssm", region_name=region)
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


# =============================================================================
# Credentials Management
# =============================================================================

# Cache for loaded credentials
_credentials_cache = None


def _load_credentials_from_file() -> dict:
    """
    Load credentials from config/credentials.py file (for local development).

    This file is gitignored and should not be committed to version control.

    Returns:
        Dictionary of credentials, or empty dict if file doesn't exist
    """
    global _credentials_cache

    # Return cached credentials if already loaded
    if _credentials_cache is not None:
        return _credentials_cache

    try:
        # Try to load from config/credentials.py
        import sys
        import os
        config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config')
        credentials_path = os.path.join(config_dir, 'credentials.py')

        if os.path.exists(credentials_path):
            # Load credentials module
            import importlib.util
            spec = importlib.util.spec_from_file_location("credentials", credentials_path)
            credentials_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(credentials_module)
            _credentials_cache = getattr(credentials_module, 'credentials', {})
            return _credentials_cache
        else:
            _credentials_cache = {}
            return _credentials_cache
    except Exception:
        # If loading fails, return empty dict
        _credentials_cache = {}
        return _credentials_cache


def get_credentials(client_id: str) -> Tuple[str, str]:
    """
    Retrieve API credentials for a client.

    This function checks in the following order:
    1. Local credentials file (config/credentials.py) - for local development
    2. AWS SSM Parameter Store - for production environments

    Args:
        client_id: Client identifier (profile ID)

    Returns:
        Tuple of (api_key, api_secret)

    Raises:
        Exception: If credentials for the client are not found in either source
    """
    client_key = f"client_{client_id}"

    # First, try loading from local credentials file (for local testing)
    local_credentials = _load_credentials_from_file()
    if client_key in local_credentials and local_credentials[client_key]:
        creds = local_credentials[client_key]
        return creds["api_key"], creds["api_secret"]

    # Fall back to AWS SSM Parameter Store (for production)
    try:
        api_key = get_ssm_parameter(f'profile{client_id}_apikey')
        api_secret = get_ssm_parameter(f'profile{client_id}_apisecret')
        return api_key, api_secret
    except Exception as e:
        raise Exception(
            f"Credentials for client {client_id} not found! "
            f"Please configure either config/credentials.py (for local testing) or AWS SSM parameters (for production). "
            f"Error: {e}"
        )


def get_ssm_parameter(parameter_name: str, region: str = "us-east-1") -> str:
    """
    Fetch a parameter value from AWS SSM Parameter Store.

    Args:
        parameter_name: Name of the parameter in SSM
        region: AWS region where the parameter is stored

    Returns:
        Parameter value as a string
    """
    ssm = boto3.client("ssm", region_name=region)
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


# =============================================================================
# Market Hours
# =============================================================================

def get_market_hours(
    config,
    trading_client,
    logger,
    target_date: Optional[datetime.date] = None
) -> Optional[Dict[str, datetime]]:
    """
    Get market hours for a specific date.

    Returns a dictionary with pre-market, regular session, and after-hours
    times in UTC.

    Args:
        config: Configuration object with pre/post market hour settings
        trading_client: Alpaca trading client
        logger: Logger instance
        target_date: Date to get hours for (defaults to current UTC date)

    Returns:
        Dictionary with keys: pre_market_open, regular_open, regular_close, after_hours_close
        All times are in UTC. Returns None if no market data available.
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    logger.debug(f"target_date: {target_date}")

    # Fetch market calendar
    calendar_request = GetCalendarRequest(start=target_date, end=target_date)
    market_calendar = trading_client.get_calendar(calendar_request)

    if not market_calendar:
        logger.info(f"No market calendar data available for {target_date}.")
        return None

    # Extract open and close times
    market_open = market_calendar[0].open
    market_close = market_calendar[0].close

    # Localize to US/Eastern and convert to UTC
    eastern_tz = pytz.timezone("US/Eastern")
    market_open_localized = eastern_tz.localize(market_open)
    market_close_localized = eastern_tz.localize(market_close)

    market_open_utc = market_open_localized.astimezone(timezone.utc)
    market_close_utc = market_close_localized.astimezone(timezone.utc)

    # Calculate extended hours
    pre_market_hours = config.get("pre_market_hour", 100, int)
    pre_market_minutes = config.get("pre_market_minute", 100, int)
    post_market_hours = config.get("post_market_hour", 100, int)
    post_market_minutes = config.get("post_market_minute", 100, int)

    pre_market_open_utc = market_open_utc - timedelta(hours=pre_market_hours, minutes=pre_market_minutes)
    after_hours_close_utc = market_close_utc + timedelta(hours=post_market_hours, minutes=post_market_minutes)

    return {
        "pre_market_open": pre_market_open_utc,
        "regular_open": market_open_utc,
        "regular_close": market_close_utc,
        "after_hours_close": after_hours_close_utc,
    }


# =============================================================================
# Sleep Time Determination
# =============================================================================

@time_it
def sleep_determination_extended(
    config,
    current_time: datetime,
    latest_close_time: Optional[datetime],
    timeframe_minutes: int,
    trading_client,
    market_hours: Optional[Dict],
    live_extra_sleep_seconds: float,
    logger
) -> float:
    """
    Calculate optimal sleep time until the next data fetch.

    Accounts for market hours, including pre-market and after-hours sessions.

    Args:
        config: Configuration object
        current_time: Current timestamp (UTC)
        latest_close_time: Time of the most recent candle close
        timeframe_minutes: Candle timeframe in minutes
        trading_client: Alpaca trading client
        market_hours: Dictionary of market hours (from get_market_hours)
        live_extra_sleep_seconds: Additional sleep buffer
        logger: Logger instance

    Returns:
        Sleep time in seconds
    """
    previous_day_hours = None

    # Handle case where we're before market hours or market_hours is None
    if market_hours is None or current_time < market_hours["pre_market_open"]:
        previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        logger.debug(f"Fetching market hours for previous day: {previous_day}")
        previous_day_hours = get_market_hours(config, trading_client, logger, target_date=previous_day)

        # Find the most recent trading day
        while previous_day_hours is None:
            previous_day -= timedelta(days=1)
            logger.debug(f"Fetching market hours for {previous_day}")
            previous_day_hours = get_market_hours(config, trading_client, logger, target_date=previous_day)

            if previous_day_hours:
                logger.info(f"Using market hours for {previous_day}: {previous_day_hours}")
            else:
                logger.warning(f"Market hours not found for {previous_day}, checking previous day...")

        logger.info(f"Using previous day's market hours: {previous_day_hours}")

    # Determine which market hours to use
    if previous_day_hours and current_time <= previous_day_hours["after_hours_close"]:
        active_hours = previous_day_hours
        logger.debug(f"Current time {current_time} falls under previous day's trading hours.")
    elif previous_day_hours and current_time > previous_day_hours["after_hours_close"]:
        logger.debug("Market is closed today. Sleep for 1800 seconds")
        return 1800.0
    else:
        active_hours = market_hours
        logger.debug(f"Using current day's market hours: {market_hours}")

    # Extract time boundaries
    pre_market_open = active_hours["pre_market_open"]
    regular_close = active_hours["regular_close"]
    after_hours_close = active_hours["after_hours_close"]

    # Log time windows
    logger.debug(f"Current time: {current_time}")
    logger.debug(f"Pre-market open: {pre_market_open}")
    logger.debug(f"Regular market close: {regular_close}")
    logger.debug(f"After-hours close: {after_hours_close}")
    logger.debug(f"latest_close_time: {latest_close_time}")

    # Calculate next candle time
    next_candle_time = None
    if latest_close_time is not None:
        next_candle_time = latest_close_time + (2 * timedelta(minutes=timeframe_minutes))
    logger.debug(f"next_candle_time: {next_candle_time}")

    # Determine sleep duration based on market status
    if current_time < pre_market_open:
        # Before pre-market: sleep until pre-market opens
        sleep_seconds = (pre_market_open - current_time).total_seconds()
        logger.info(f"Market is closed. Sleep until pre-market opens at {pre_market_open} (UTC).")

    elif current_time > after_hours_close:
        # After after-hours: sleep until next day pre-market
        wake_up_time = pre_market_open + timedelta(days=1)
        sleep_seconds = (wake_up_time - current_time).total_seconds()
        logger.info(f"Market is closed. Sleep until tomorrow's pre-market at {wake_up_time} (UTC).")

    else:
        # During extended market hours
        if next_candle_time is not None:
            sleep_seconds = (next_candle_time - current_time).total_seconds()
        else:
            sleep_seconds = live_extra_sleep_seconds
        logger.info(f"Market is open. Sleep for {sleep_seconds}s until next candle at {next_candle_time} (UTC).")

    # Apply bounds to sleep time
    if sleep_seconds < 0:
        logger.warning("Next candle time has already passed. Adjusting sleep time to 1 second.")
        sleep_seconds = 1.0
    elif sleep_seconds > 1800:
        logger.warning("Sleep max 30 mins. Adjusting sleep time to 1800 seconds.")
        sleep_seconds = 1800.0

    return sleep_seconds


# =============================================================================
# Bar Strength Calculations
# =============================================================================

def calculate_bar_strength(
    latest_close: float,
    latest_open: float,
    latest_high: float,
    latest_low: float
) -> float:
    """
    Calculate directional bar strength (0 to 1).

    For bullish (green) bars: strength measures how close the close is to the high.
    For bearish (red) bars: strength measures how close the close is to the low.

    Args:
        latest_close: Closing price
        latest_open: Opening price
        latest_high: High price
        latest_low: Low price

    Returns:
        Strength value between 0.0 and 1.0
    """
    total_range = latest_high - latest_low

    if total_range == 0:
        return 0.0

    if latest_close > latest_open:
        # Bullish bar: how close to high
        strength = (latest_close - latest_low) / total_range
    elif latest_close < latest_open:
        # Bearish bar: how close to low (inverted)
        strength = (latest_high - latest_close) / total_range
    else:
        # Doji
        strength = 0.0

    return round(strength, 2)


# =============================================================================
# Option Symbol Utilities
# =============================================================================

def identify_option_type(symbol: str, logger) -> Optional[str]:
    """
    Identify whether an Alpaca option symbol is a Put or Call.

    Option symbol format: SYMBOL + YYMMDD + P/C + STRIKEXXXX
    The option type character is at position -9 (9th from end).

    Args:
        symbol: Alpaca option symbol
        logger: Logger instance

    Returns:
        CALL, PUT, or None if unable to determine
    """
    logger.info(f"Processing symbol: {symbol}")

    try:
        if len(symbol) >= 9:
            option_type_char = symbol[-9]
            logger.debug(f"Option type character: {option_type_char}")

            if option_type_char == "C":
                logger.info(f"Symbol {symbol} is a Call option")
                return CALL
            elif option_type_char == "P":
                logger.info(f"Symbol {symbol} is a Put option")
                return PUT
            else:
                print(f"Symbol {symbol} has invalid option type character: {option_type_char}")
                logger.warning(f"Symbol {symbol} has invalid option type character: {option_type_char}")
                return None
        else:
            print(f"Symbol {symbol} has invalid format: insufficient length")
            logger.warning(f"Symbol {symbol} has invalid format: insufficient length")
            return None

    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {e}")
        return None


def determine_expiration_date(
    now: datetime,
    open_position_expiry_trading_day: int,
    option_expiry_day_flip_to_next_trading_day: Optional[str],
    trading_client,
    logger
) -> Optional[str]:
    """
    Determine the option expiration date based on trading day offset.

    Args:
        now: Current datetime
        open_position_expiry_trading_day: Number of trading days until expiration
        option_expiry_day_flip_to_next_trading_day: Time (HH:MM) to flip to next day
        trading_client: Alpaca trading client
        logger: Logger instance

    Returns:
        Expiration date formatted as YYMMDD, or None on error
    """
    try:
        original_expiry_offset = open_position_expiry_trading_day

        # Normalize expiry offset
        if open_position_expiry_trading_day <= 0:
            open_position_expiry_trading_day = 10
            logger.info("open_position_expiry_trading_day was <= 0. Defaulting to 10.")
        elif open_position_expiry_trading_day > 100:
            open_position_expiry_trading_day = 100
            logger.info("open_position_expiry_trading_day exceeded 100. Capping to 100.")
        else:
            open_position_expiry_trading_day += 10

        # Fetch trading calendar
        calendar_request = GetCalendarRequest(
            start=now.date(),
            end=(now + timedelta(days=open_position_expiry_trading_day)).date()
        )
        calendar = trading_client.get_calendar(calendar_request)
        trading_days = [trading_day.date for trading_day in calendar]

        logger.debug(f"trading_days: {trading_days}, flip_time: {option_expiry_day_flip_to_next_trading_day}")

        # Determine base expiry date
        if not option_expiry_day_flip_to_next_trading_day:
            logger.warning("option_expiry_day_flip_to_next_trading_day is None/empty; using today's expiry.")
            today_expiry = trading_days[0]
        else:
            # Parse flip time
            time_parts = [int(part) for part in option_expiry_day_flip_to_next_trading_day.split(":")]
            logger.debug(f"time_parts: {time_parts}")

            flip_time = time(hour=time_parts[0], minute=time_parts[1])
            flip_datetime = now.replace(hour=flip_time.hour, minute=flip_time.minute, second=0, microsecond=0)

            if now < flip_datetime:
                today_expiry = trading_days[0]
            else:
                # Post-flip: use next trading day
                today_expiry = next((day for day in trading_days if day > now.date()), None)
                if today_expiry is None:
                    logger.error(f"No valid trading day found after {now.date()} for post-flip calculation.")
                    return None

        logger.debug(f"Expiry date: {today_expiry}, original_offset: {original_expiry_offset}")

        # Calculate target expiration
        try:
            expiration_date = trading_days[trading_days.index(today_expiry) + original_expiry_offset]
        except IndexError as e:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.error(f"Not enough trading days for offset {open_position_expiry_trading_day}: {error_time}: {e}")
            print(f"Not enough trading days for offset {open_position_expiry_trading_day}")
            return None

        # Format as YYMMDD
        formatted_expiration = expiration_date.strftime("%y%m%d")
        logger.debug(f"Expiration date: {expiration_date} (formatted: {formatted_expiration})")

        return formatted_expiration

    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_traceback = traceback.format_exc()
        logger.error(f"Error calculating expiration date at {error_time}: {e}\nTraceback:\n{error_traceback}")
        return None


def determine_strike_price(price: float, cents_to_rollover: int, logger) -> str:
    """
    Round price to strike price and format with required digits.

    Args:
        price: Current price
        cents_to_rollover: Cents threshold to round up to next dollar
        logger: Logger instance

    Returns:
        Strike price formatted as "XXXXX000" (5 digits + 3 zeros)
    """
    dollars_part = int(price)
    cents_part = round((price - dollars_part) * 100)

    if cents_part >= cents_to_rollover:
        dollars_part += 1

    # Format: 5 digits for dollars + "000" for cents
    formatted_dollars = f"{dollars_part:05}000"

    logger.debug(f"Strike price: {formatted_dollars} for price: {price}, rollover: {cents_to_rollover}")
    return formatted_dollars


def generate_option_symbol(
    symbol: str,
    open_position_expiry_trading_day: int,
    option_expiry_day_flip_to_next_trading_day: Optional[str],
    cents_to_rollover: int,
    price: float,
    option_type: str,
    now: datetime,
    trading_client,
    logger
) -> Optional[str]:
    """
    Generate an Alpaca option symbol.

    Format: SYMBOL + YYMMDD + P/C + STRIKEXXXX

    Args:
        symbol: Underlying symbol (e.g., 'SPY')
        open_position_expiry_trading_day: Trading days until expiration
        option_expiry_day_flip_to_next_trading_day: Time to flip to next day
        cents_to_rollover: Cents threshold for strike rounding
        price: Current price for strike calculation
        option_type: 'P' for put or 'C' for call
        now: Current datetime
        trading_client: Alpaca trading client
        logger: Logger instance

    Returns:
        Option symbol string, or None on error
    """
    expiration = determine_expiration_date(
        now, open_position_expiry_trading_day,
        option_expiry_day_flip_to_next_trading_day, trading_client, logger
    )

    if expiration is None:
        return None

    strike_price = determine_strike_price(price, cents_to_rollover, logger)
    option_symbol = f"{symbol}{expiration}{option_type}{strike_price}"

    logger.debug(
        f"Generating option symbol - Symbol: {symbol}, Expiration: {expiration}, "
        f"Type: {option_type}, Strike: {strike_price}"
    )
    logger.debug(f"Generated option symbol: {option_symbol}")

    return option_symbol


# =============================================================================
# SPY Performance Tracking
# =============================================================================

def get_spy_pct_change_since_open(
    config,
    trading_client,
    historicaldata_client,
    logger
) -> Optional[float]:
    """
    Calculate SPY percentage change since market open.

    Args:
        config: Configuration object
        trading_client: Alpaca trading client
        historicaldata_client: Alpaca historical data client
        logger: Logger instance

    Returns:
        Percentage change since open, or None if unavailable
    """
    market_hours = get_market_hours(config, trading_client, logger)

    logger.debug(f"Market hours are: {market_hours}")
    if not market_hours:
        logger.warning("No market hours provided; cannot compute % change since open.")
        return 0.0

    now_utc = datetime.now(timezone.utc)
    market_open_utc = market_hours["regular_open"]
    market_close_utc = market_hours["regular_close"]

    # If it's a non-trading day or we're before the regular open, return None
    if now_utc.date() != market_open_utc.date():
        logger.info(f"Today has no regular session (open={market_open_utc}).")
        return None
    if now_utc < market_open_utc:
        logger.info("Regular session has not opened yet.")
        return None

    # Fetch minute bars from open to now
    end_utc = min(now_utc, market_close_utc + timedelta(minutes=1))

    req = StockBarsRequest(
        symbol_or_symbols="SPY",
        timeframe=TimeFrame.Minute,
        start=market_open_utc,
        end=end_utc,
    )

    logger.info(f"market_open_utc: {market_open_utc}")
    logger.info(f"end_utc: {end_utc}")

    bars_df = historicaldata_client.get_stock_bars(req).df
    if bars_df.empty:
        logger.warning("No SPY minute bars found between regular open and now.")
        return 0.0

    # Handle MultiIndex
    spy_df = bars_df.xs("SPY") if isinstance(bars_df.index, pd.MultiIndex) else bars_df
    spy_df = spy_df.sort_index()

    first_bar = spy_df.iloc[0]
    last_bar = spy_df.iloc[-1]

    logger.info(f"first_bar: {first_bar}")
    logger.info(f"last_bar: {last_bar}")

    open_price = float(first_bar["open"])
    last_price = float(last_bar["close"])

    if open_price == 0:
        logger.warning("Open price is zero; cannot compute % change.")
        return 0.0

    pct_change = ((last_price - open_price) / open_price) * 100.0
    logger.info(
        f"SPY % change since regular open ({market_open_utc.isoformat()}): "
        f"open={open_price:.2f}, last={last_price:.2f}, change={pct_change:.2f}%"
    )
    return pct_change


def get_spy_day_percentage_change(historicaldata_client, logger) -> float:
    """
    Calculate SPY percentage change from yesterday's close.

    Args:
        historicaldata_client: Alpaca historical data client
        logger: Logger instance

    Returns:
        Percentage change from yesterday's close
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=5)

    request = StockBarsRequest(
        symbol_or_symbols="SPY",
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date
    )

    bars_df = historicaldata_client.get_stock_bars(request).df
    if bars_df.empty:
        logger.warning("No SPY day bars found.")
        return 0.0

    spy_df = bars_df.xs("SPY") if isinstance(bars_df.index, pd.MultiIndex) else bars_df
    spy_df = spy_df.sort_index()

    if len(spy_df) < 2:
        logger.warning("Not enough day bars to calculate change.")
        return 0.0

    logger.info(f"Fetched SPY daily bars:\n{spy_df}")

    # Check if latest bar is for today
    latest_bar_date = spy_df.index[-1].date()
    today_utc_date = datetime.utcnow().date()

    if latest_bar_date != today_utc_date:
        print(f"Latest SPY daily bar is not from today ({today_utc_date}), found {latest_bar_date}. Skipping.")
        logger.info(f"Latest SPY daily bar is not from today ({today_utc_date}), found {latest_bar_date}.")
        return 0.0

    yesterday_close = spy_df.iloc[-2]["close"]
    today_close = spy_df.iloc[-1]["close"]

    pct_change = ((today_close - yesterday_close) / yesterday_close) * 100
    logger.info(f"SPY % change: yesterday {yesterday_close:.2f} -> today {today_close:.2f}: {pct_change:.2f}%")
    return pct_change


# =============================================================================
# Profit/Loss Calculations
# =============================================================================

def get_realized_pnl(trading_client, logger, days_back: int = 0) -> float:
    """
    Calculate realized profit/loss for the past trading days.

    Args:
        trading_client: Alpaca trading client
        logger: Logger instance
        days_back: Number of days to look back (0 = today only)

    Returns:
        Realized P&L in cents (*100)
    """
    now_utc = datetime.now(timezone.utc)

    if days_back == 0:
        start_date = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_date = (now_utc - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)

    end_date = now_utc

    logger.info(f"Fetching orders from {start_date} to {end_date} (UTC)...")

    try:
        request_params = GetOrdersRequest(
            status=QueryOrderStatus.ALL,
            after=start_date.isoformat(),
            until=end_date.isoformat(),
            limit=500
        )
        orders = trading_client.get_orders(filter=request_params)
        orders_data = [order.model_dump() for order in orders]
        orders_df = pd.DataFrame(orders_data)

        if orders_df.empty or 'status' not in orders_df.columns:
            logger.info("No filled orders found.")
            return 0.0

        orders_df = orders_df[orders_df['status'] == 'filled'].sort_values(by="filled_at")
        logger.info(f"Fetched {len(orders_df)} filled orders.")

        holdings = defaultdict(lambda: [0.0, 0.0])  # symbol -> [qty, total_cost]
        realized_pnl = 0.0

        for _, row in orders_df.iterrows():
            symbol = row['symbol']
            side = row['side']
            qty = float(row['qty'])
            price = float(row['filled_avg_price'])

            if side == 'buy':
                holdings[symbol][0] += qty
                holdings[symbol][1] += qty * price
            elif side == 'sell':
                held_qty, held_cost = holdings[symbol]
                if held_qty == 0:
                    continue
                avg_cost = held_cost / held_qty
                pnl = qty * (price - avg_cost)
                realized_pnl += pnl
                holdings[symbol][0] -= qty
                holdings[symbol][1] -= avg_cost * qty

        logger.info(f"Final Realized PnL over last {days_back} day(s): ${realized_pnl:.2f}")
        return realized_pnl * 100

    except Exception as e:
        logger.error(f"Error fetching realized PnL: {e}")
        return 0.0


def get_daily_profit_loss(trading_client, logger) -> float:
    """
    Calculate today's profit/loss from account equity.

    Args:
        trading_client: Alpaca trading client
        logger: Logger instance

    Returns:
        Today's P&L in dollars
    """
    account = trading_client.get_account()

    equity = float(account.equity)
    last_equity = float(account.last_equity)

    daily_pnl = equity - last_equity
    daily_pnl_percent = (daily_pnl / last_equity) * 100 if last_equity != 0 else 0

    logger.info(f"Current Equity: ${equity:.2f}")
    logger.info(f"Previous Close Equity: ${last_equity:.2f}")
    logger.info(f"Today's PnL: ${daily_pnl:.2f} ({daily_pnl_percent:.2f}%)")
    print(f"Today's PnL: ${daily_pnl:.2f} ({daily_pnl_percent:.2f}%)")

    return daily_pnl


# =============================================================================
# Order Quantity Calculations
# =============================================================================

def adjust_order_quantities_per_fixed_amount(
    fetch_limit_orderqty: float,
    fetch_market_orderqty: float,
    option_price: float,
    buy_for_amount: float
) -> Tuple[int, int]:
    """
    Adjust order quantities to stay within a fixed budget.

    Args:
        fetch_limit_orderqty: Initial limit order quantity
        fetch_market_orderqty: Initial market order quantity
        option_price: Price per option contract
        buy_for_amount: Maximum amount to spend

    Returns:
        Tuple of (adjusted_limit_qty, adjusted_market_qty)
    """
    total_quantity = fetch_limit_orderqty + fetch_market_orderqty
    total_cost = total_quantity * option_price

    if total_cost == 0:
        return 0, 0

    scale_factor = buy_for_amount / total_cost

    limit_orderqty = round(fetch_limit_orderqty * scale_factor)
    market_orderqty = round(fetch_market_orderqty * scale_factor)

    adjusted_total_cost = (limit_orderqty + market_orderqty) * option_price

    if adjusted_total_cost > buy_for_amount:
        excess_units = (adjusted_total_cost - buy_for_amount) // option_price
        if market_orderqty >= excess_units:
            market_orderqty -= int(excess_units)
        elif limit_orderqty >= excess_units:
            limit_orderqty -= int(excess_units)

    return int(limit_orderqty), int(market_orderqty)


def adjust_order_quantities(
    limit_order_qty: int,
    market_order_qty: int,
    option_price: float,
    max_order_amount: float
) -> Tuple[int, int]:
    """
    Adjust order quantities to stay within maximum order amount.

    Args:
        limit_order_qty: Initial limit order quantity
        market_order_qty: Initial market order quantity
        option_price: Price per option contract
        max_order_amount: Maximum order amount in dollars

    Returns:
        Tuple of (adjusted_limit_qty, adjusted_market_qty)
    """
    total_cost = (limit_order_qty + market_order_qty) * option_price * 100

    if total_cost > max_order_amount:
        max_qty = max_order_amount // (option_price * 100)

        if max_qty < 1:
            max_qty = 1

        total_qty = limit_order_qty + market_order_qty

        if total_qty > 0:
            limit_order_qty = round((limit_order_qty / total_qty) * max_qty)
            market_order_qty = int(max_qty - limit_order_qty)

    return int(limit_order_qty), int(market_order_qty)


# =============================================================================
# Order Status and Option Quotes
# =============================================================================

def get_order_status(trading_client, order_id: str, logger=None):
    """
    Fetch order status from Alpaca API.

    Args:
        trading_client: Alpaca trading client
        order_id: Order ID to check
        logger: Optional logger instance

    Returns:
        Order status string, or None on error
    """
    try:
        order = trading_client.get_order_by_id(order_id)
        return order["status"] if isinstance(order, dict) else order.status
    except Exception as e:
        print(f"Failed to get order {order_id} status: {e}")
        if logger:
            logger.debug(f"Failed to get order {order_id} status: {e}")
        return None


def fetch_latest_option_quote(
    option_historicaldata_client,
    symbol: str,
    logger
) -> Optional[float]:
    """
    Fetch the latest option quote price.

    Args:
        option_historicaldata_client: Alpaca option historical data client
        symbol: Option symbol to fetch quote for
        logger: Logger instance

    Returns:
        Ask price for the option, or None on error
    """
    try:
        request_params = OptionLatestQuoteRequest(symbol_or_symbols=symbol)
        logger.debug("Requesting the latest option quote...")
        latest_quote = option_historicaldata_client.get_option_latest_quote(request_params)
        logger.debug("Latest quote fetched successfully.")
        logger.debug(f"Quote -> {latest_quote}")

        option_price = latest_quote[symbol].ask_price
        logger.info(f"The current ask price for {symbol} is: {option_price}")
        return option_price
    except Exception as e:
        logger.error(f"Error occurred while fetching the option quote for {symbol}: {e}")
        return None


# =============================================================================
# Utility Functions
# =============================================================================

def log_exception(context: str, exception: Exception, logger) -> None:
    """
    Log an exception with context and detailed traceback.

    Args:
        context: Description of what was being attempted
        exception: The exception that was raised
        logger: Logger instance
    """
    error_details = traceback.format_exc()
    print(f"{context}: {exception}")
    logger.error(f"{context}: {exception}\nDetails:\n{error_details}")

