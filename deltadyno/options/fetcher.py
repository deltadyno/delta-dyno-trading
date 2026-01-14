"""
Option Chain Fetcher.

Fetches option symbols for configured underlying tickers using the
Alpaca Options API.
"""

import logging
from datetime import datetime
from typing import List, Optional

from alpaca.data import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest

logger = logging.getLogger(__name__)


def fetch_options_for_symbols(
    symbols: List[str],
    api_key: str,
    api_secret: str,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[str] = None
) -> List[str]:
    """
    Fetch all option symbols for the given underlying tickers.
    
    Queries the Alpaca option chain API for each ticker and collects
    all available option symbols within the specified date range.
    
    Args:
        symbols: List of underlying ticker symbols (e.g., ['SPY', 'TSLA'])
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        start_date: Minimum expiration date (defaults to today)
        end_date: Maximum expiration date as string 'YYYY-MM-DD' (optional)
    
    Returns:
        List of OCC option symbols
    """
    try:
        logger.debug("Initializing OptionHistoricalDataClient...")
        
        option_client = OptionHistoricalDataClient(api_key, api_secret)
        all_symbols: List[str] = []
        
        # Default start date to today if not provided
        if start_date is None:
            start_date = datetime.now().date()
        
        for symbol in symbols:
            try:
                fetched_symbols = _fetch_options_for_single_symbol(
                    option_client, symbol, start_date, end_date
                )
                all_symbols.extend(fetched_symbols)
            except Exception as e:
                logger.error(f"Error fetching options for {symbol}: {e}")
                # Continue with other symbols even if one fails
                continue
        
        unique_count = len(set(all_symbols))
        logger.info(f"Total option symbols fetched: {unique_count}")
        
        return all_symbols
        
    except Exception as e:
        logger.error(f"Error initializing option client: {e}")
        return []


def _fetch_options_for_single_symbol(
    option_client: OptionHistoricalDataClient,
    symbol: str,
    start_date: datetime.date,
    end_date: Optional[str]
) -> List[str]:
    """
    Fetch option symbols for a single underlying ticker.
    
    Args:
        option_client: Initialized Alpaca option client
        symbol: Underlying ticker symbol
        start_date: Minimum expiration date
        end_date: Maximum expiration date as string 'YYYY-MM-DD'
    
    Returns:
        List of OCC option symbols for this ticker
    """
    logger.info(f"Fetching option chain for {symbol}...")
    
    # Build request parameters
    request_params = {
        "underlying_symbol": symbol,
        "expiration_date_gte": start_date,
    }
    
    if end_date:
        request_params["expiration_date_lte"] = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    request = OptionChainRequest(**request_params)
    option_chain = option_client.get_option_chain(request)
    
    current_symbols = list(option_chain.keys())
    symbol_count = len(current_symbols)
    
    logger.debug(f"Fetched {symbol_count} option symbols for {symbol}")
    
    return current_symbols
