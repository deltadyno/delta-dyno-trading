"""
Option Trade Stream Handler.

Core stream handling module that:
1. Initializes and manages the Alpaca OptionDataStream
2. Processes incoming option trade events
3. Filters trades by premium threshold
4. Routes qualifying trades to Redis and DB queue

CRITICAL: This module preserves exact message ordering and ensures
all qualifying trades are written to both Redis and DB.
"""

import asyncio
import logging
from datetime import datetime, timezone
from queue import Queue
from typing import Any, Dict, Optional

from alpaca.data.live import OptionDataStream
from alpaca.data.enums import OptionsFeed

logger = logging.getLogger(__name__)

# =============================================================================
# Module State (Thread-safe globals)
# =============================================================================

# Option data stream instance
_option_stream: Optional[OptionDataStream] = None

# Redis client and queue (injected at startup)
_redis_client = None
_redis_queue_name: Optional[str] = None

# Trade buffer for batch DB writes
_trade_buffer: Queue = Queue()

# Premium threshold (loaded from config)
_premium_threshold: int = 500


# =============================================================================
# Initialization
# =============================================================================

def init_option_stream(api_key: str, api_secret: str) -> OptionDataStream:
    """
    Initialize the Alpaca option data stream.
    
    Args:
        api_key: Alpaca API key
        api_secret: Alpaca API secret
    
    Returns:
        Initialized OptionDataStream instance
    """
    global _option_stream
    _option_stream = OptionDataStream(api_key, api_secret, feed=OptionsFeed.OPRA)
    logger.info("Option data stream initialized with OPRA feed")
    return _option_stream


def get_option_stream() -> Optional[OptionDataStream]:
    """Get the current option data stream instance."""
    return _option_stream


def set_redis_client(client: Any, queue_name: str) -> None:
    """
    Inject the Redis client and queue name for message publishing.
    
    Must be called before stream processing begins.
    
    Args:
        client: Redis client instance (async or sync)
        queue_name: Redis stream/queue name for option messages
    """
    global _redis_client, _redis_queue_name
    _redis_client = client
    _redis_queue_name = queue_name
    logger.debug(f"Redis client configured for queue: {queue_name}")


def set_premium_threshold(threshold: int) -> None:
    """
    Set the minimum premium threshold for trade filtering.
    
    Args:
        threshold: Minimum premium value (in dollars) to process a trade
    """
    global _premium_threshold
    _premium_threshold = threshold
    logger.debug(f"Premium threshold set to: ${threshold}")


def get_trade_buffer() -> Queue:
    """Get the trade buffer queue for batch DB writes."""
    return _trade_buffer


# =============================================================================
# Symbol Parsing
# =============================================================================

def get_strike_price(option_symbol: str) -> float:
    """
    Extract strike price from OCC option symbol.
    
    The last 8 characters represent the strike price * 1000.
    
    Args:
        option_symbol: Full OCC option symbol
    
    Returns:
        Strike price as float
    """
    strike_price_str = option_symbol[-8:]
    return float(strike_price_str) / 1000


def parse_option_symbol(symbol: str) -> Dict[str, str]:
    """
    Parse an OCC option symbol into its components.
    
    OCC Symbol format: UNDERLYING + YYMMDD + C/P + STRIKE
    Example: TSLA250117C00445000
    
    Args:
        symbol: Full OCC option symbol
    
    Returns:
        Dictionary with parsed components:
        - Underlying: Ticker symbol
        - Expiration Date: YYMMDD format
        - Option Type: 'C' or 'P'
        - Strike Price: Formatted to 2 decimal places
    """
    try:
        # Find where the date starts (first '2' for 202X years)
        date_start = symbol.find('2')
        
        underlying = symbol[:date_start]
        expiration_date = symbol[date_start:date_start + 6]
        option_type = symbol[date_start + 6]
        strike_price = get_strike_price(symbol)
        
        return {
            "Underlying": underlying,
            "Expiration Date": expiration_date,
            "Option Type": option_type,
            "Strike Price": f"{strike_price:.2f}"
        }
    except Exception as e:
        logger.error(f"Error parsing symbol {symbol}: {e}")
        return {
            "Underlying": "Error",
            "Expiration Date": "Error",
            "Option Type": "Error",
            "Strike Price": "Error"
        }


def _exp_yymmdd_to_iso(yymmdd: str) -> str:
    """
    Convert OCC-style YYMMDD to ISO 'YYYY-MM-DD' format.
    
    Assumes 2000-based years (e.g., '251017' -> '2025-10-17').
    
    Args:
        yymmdd: Expiration date in YYMMDD format
    
    Returns:
        Expiration date in YYYY-MM-DD format
    """
    yy = int(yymmdd[0:2])
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])
    year = 2000 + yy
    return f"{year:04d}-{mm:02d}-{dd:02d}"


# =============================================================================
# Redis Publishing
# =============================================================================

def push_to_redis(message: Dict[str, Any]) -> bool:
    """
    Publish a normalized trade message to Redis stream.
    
    Uses XADD for Redis Stream (not LPUSH to a list).
    
    Args:
        message: Normalized trade data dictionary
    
    Returns:
        True if publish succeeded, False otherwise
    """
    try:
        if not (_redis_client and _redis_queue_name):
            logger.warning("Redis not configured; skipping push_to_redis.")
            return False
        
        message_id = _redis_client.xadd(_redis_queue_name, message)
        if message_id:
            logger.debug(f"Published to Redis: id={message_id}")
            return True
        else:
            logger.warning(f"Failed to publish to stream {_redis_queue_name}")
            return False
            
    except Exception as e:
        logger.error(f"Redis XADD failed: {e}")
        return False


# =============================================================================
# Trade Processing
# =============================================================================

def queue_trade(trade_dict: Dict[str, Any]) -> None:
    """
    Add a trade to the buffer queue for batch DB insertion.
    
    Args:
        trade_dict: Trade data dictionary ready for DB insert
    """
    _trade_buffer.put(trade_dict)


def write_to_db(data: Any, premium: float) -> None:
    """
    Process trade data for DB and Redis persistence.
    
    CRITICAL: This function maintains the exact ordering and side effects:
    1. Parse the option symbol
    2. Build DB-formatted trade data
    3. Queue for batch DB insert
    4. Build normalized message for Redis
    5. Push to Redis stream
    
    Args:
        data: Raw trade data from Alpaca stream
        premium: Calculated premium value
    """
    parsed_symbol = parse_option_symbol(data.symbol)
    timestamp = (data.timestamp or datetime.now(timezone.utc)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Build trade data for database
    trade_data = {
        "DateTime": timestamp,
        "Symbol": data.symbol,
        "Ticker": parsed_symbol["Underlying"],
        "ExpirationDate": parsed_symbol["Expiration Date"],
        "OptionType": parsed_symbol["Option Type"],
        "StrikePrice": parsed_symbol["Strike Price"],
        "Price": data.price,
        "Size": data.size,
        "Premium": premium
    }
    
    # Queue for batch DB insert (non-blocking)
    queue_trade(trade_data)
    logger.debug(f"Queued trade for DB: {data.symbol}")
    
    # Build normalized message for Redis
    ts_iso_z = (data.timestamp or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    exp_iso = _exp_yymmdd_to_iso(parsed_symbol["Expiration Date"])
    
    normalized = {
        "ts": ts_iso_z,
        "ticker": parsed_symbol["Underlying"],
        "occ": data.symbol,
        "cp": parsed_symbol["Option Type"],
        "strike": parsed_symbol["Strike Price"],
        "exp": exp_iso,
        "price": data.price,
        "size": data.size,
        "premium": premium
    }
    
    # Publish to Redis stream
    push_to_redis(normalized)


async def option_trade_handler(data: Any) -> None:
    """
    Async handler for incoming option trade events.
    
    CRITICAL: This is the core trade processing function that:
    1. Extracts trade data from the stream event
    2. Calculates premium (price * size * 100)
    3. Filters by premium threshold
    4. Routes qualifying trades to DB and Redis
    
    The ordering and behavior of this function MUST NOT be changed.
    
    Args:
        data: Raw trade data from Alpaca OptionDataStream
    """
    try:
        logger.debug(f"Received trade: {data.symbol}")
        
        # Extract trade details
        trade_price = data.price or 0
        trade_size = data.size or 0
        symbol = data.symbol or 'Unknown'
        trade_timestamp = data.timestamp if data.timestamp else datetime.now(timezone.utc)
        
        # Calculate premium: price * size * 100 (options are for 100 shares)
        premium = round(trade_price * trade_size * 100, 2)
        
        # Filter by premium threshold
        if premium > _premium_threshold:
            logger.info(
                f"HIGH PREMIUM: {symbol} | "
                f"Price: ${trade_price:.2f} | Size: {trade_size:.0f} | "
                f"Premium: ${premium:,.2f}"
            )
            write_to_db(data, premium)
            
    except Exception as e:
        logger.error(f"Error processing trade data: {e}")


# =============================================================================
# Stream Execution
# =============================================================================

async def run_stream() -> None:
    """
    Run the option data stream indefinitely.
    
    This is the main async loop that keeps the stream connection alive.
    Should be called after subscriptions are set up.
    """
    global _option_stream
    
    try:
        logger.info("Starting option data stream...")
        
        stream_task = asyncio.create_task(_option_stream._run_forever())
        await asyncio.gather(stream_task)
        
    except Exception as e:
        logger.error(f"Error in stream: {e}")
