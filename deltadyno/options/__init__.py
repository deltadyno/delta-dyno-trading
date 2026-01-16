"""
Options Streaming Module for DeltaDyno.

This module handles real-time option trade streaming, filtering by premium threshold,
and routing qualifying trades to both Redis and MySQL for downstream consumption.

Components:
- stream_handler: Core stream handling and trade processing
- fetcher: Option chain fetching for configured tickers
- subscriber: Stream subscription management
- persistence: Database write operations with batch support
- config: Option-specific configuration loading
"""

from deltadyno.options.stream_handler import (
    init_option_stream,
    get_option_stream,
    option_trade_handler,
    run_stream,
    set_redis_client,
    set_premium_threshold,
    get_trade_buffer,
)
from deltadyno.options.fetcher import fetch_options_for_symbols
from deltadyno.options.subscriber import subscribe_to_trades
from deltadyno.options.persistence import (
    insert_trade,
    insert_trades_batch,
    get_db_engine,
    initialize_persistence,
)
from deltadyno.options.config import OptionStreamConfig

__all__ = [
    # Stream handling
    "init_option_stream",
    "get_option_stream",
    "option_trade_handler",
    "run_stream",
    "set_redis_client",
    "set_premium_threshold",
    "get_trade_buffer",
    # Fetching
    "fetch_options_for_symbols",
    # Subscription
    "subscribe_to_trades",
    # Persistence
    "insert_trade",
    "insert_trades_batch",
    "get_db_engine",
    "initialize_persistence",
    # Configuration
    "OptionStreamConfig",
]

