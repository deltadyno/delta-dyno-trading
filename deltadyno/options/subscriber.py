"""
Option Trade Stream Subscriber.

Handles subscription to Alpaca option trade streams for configured symbols.
"""

import logging
from typing import Callable, List, Any

logger = logging.getLogger(__name__)


def subscribe_to_trades(
    option_stream: Any,
    option_symbols: List[str],
    handler: Callable
) -> bool:
    """
    Subscribe to option trade streams for the specified symbols.
    
    Subscribes the provided handler to receive trade updates for all
    option symbols in the list. The handler will be called asynchronously
    for each incoming trade.
    
    Args:
        option_stream: Alpaca OptionDataStream instance
        option_symbols: List of OCC option symbols to subscribe to
        handler: Async callback function to handle incoming trades
    
    Returns:
        True if all subscriptions succeeded, False if an error occurred
    """
    try:
        symbol_count = len(option_symbols)
        logger.info(f"Subscribing to trades for {symbol_count} option symbols...")
        
        for ticker in option_symbols:
            option_stream.subscribe_trades(handler, ticker)
        
        logger.info("Subscription to all symbols completed.")
        return True
        
    except Exception as e:
        logger.error(f"Error during trade subscription: {e}")
        return False


def unsubscribe_from_trades(
    option_stream: Any,
    option_symbols: List[str]
) -> bool:
    """
    Unsubscribe from option trade streams for the specified symbols.
    
    Args:
        option_stream: Alpaca OptionDataStream instance
        option_symbols: List of OCC option symbols to unsubscribe from
    
    Returns:
        True if all unsubscriptions succeeded, False if an error occurred
    """
    try:
        symbol_count = len(option_symbols)
        logger.info(f"Unsubscribing from trades for {symbol_count} option symbols...")
        
        for ticker in option_symbols:
            option_stream.unsubscribe_trades(ticker)
        
        logger.info("Unsubscription from all symbols completed.")
        return True
        
    except Exception as e:
        logger.error(f"Error during trade unsubscription: {e}")
        return False
