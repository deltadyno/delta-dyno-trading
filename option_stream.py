#!/usr/bin/env python3
"""
Option Stream Entry Point.

Main entry point for the DeltaDyno option streaming system.
Monitors real-time option trades, filters by premium threshold,
and routes qualifying trades to Redis and MySQL for downstream processing.

Usage:
    python option_stream.py <profile_id>
    
Example:
    python option_stream.py 1

The profile_id is used to load API credentials for the specified trading profile.
"""

import argparse
import asyncio
import logging
import os
import sys
import threading
import time
from logging.handlers import RotatingFileHandler

from redis.asyncio import Redis

from deltadyno.options import (
    OptionStreamConfig,
    init_option_stream,
    get_option_stream,
    get_trade_buffer,
    option_trade_handler,
    run_stream,
    set_redis_client,
    set_premium_threshold,
    fetch_options_for_symbols,
    subscribe_to_trades,
    insert_trades_batch,
    initialize_persistence,
)
from deltadyno.utils.helpers import get_credentials

# Module logger
logger = logging.getLogger(__name__)


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(log_file: str = "logs/option_stream.log", console_level: int = logging.INFO) -> logging.Logger:
    """
    Configure logging with rotating file handler.
    
    - File handler: DEBUG level (all logs go to file)
    - Console handler: INFO level (only important logs to terminal)
    
    Args:
        log_file: Path to the log file
        console_level: Minimum log level for console output
    
    Returns:
        Configured logger instance
    """
    # Ensure logs directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler - captures DEBUG and above
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Console handler - captures INFO and above (less verbose)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Allow all levels, handlers will filter
    
    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger


# =============================================================================
# Background Workers
# =============================================================================

def db_batch_writer(config: OptionStreamConfig) -> None:
    """
    Background thread that batches DB writes for efficiency.
    
    Collects trades from the buffer queue and inserts them in batches,
    reducing database connection overhead under high message volume.
    
    Args:
        config: Option stream configuration
    """
    batch_size = config.db_batch_size
    interval = config.db_batch_interval_seconds
    trade_buffer = get_trade_buffer()
    
    logger.debug(f"DB batch writer started: batch_size={batch_size}, interval={interval}s")
    
    while True:
        try:
            batch = []
            
            # Collect trades up to batch size
            while not trade_buffer.empty() and len(batch) < batch_size:
                batch.append(trade_buffer.get())
            
            # Insert batch if we have trades
            if batch:
                insert_trades_batch(batch)
                logger.debug(f"Batch inserted {len(batch)} trades")
            
            # Sleep before next batch
            time.sleep(interval)
            
        except Exception as e:
            logger.error(f"Error in DB batch writer: {e}")
            time.sleep(interval)


# =============================================================================
# Redis Initialization
# =============================================================================

def initialize_redis_client(config: OptionStreamConfig) -> Redis:
    """
    Initialize and return an async Redis client.
    
    Args:
        config: Option stream configuration
    
    Returns:
        Configured Redis client instance
    """
    logger.info("Initializing Redis Client...")
    
    client = Redis(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        decode_responses=True
    )
    
    return client


# =============================================================================
# Main Entry Point
# =============================================================================

def main(profile_id: str, config_file: str = "config/config.ini") -> None:
    """
    Main entry point for option streaming.
    
    Args:
        profile_id: Trading profile ID for API credentials
        config_file: Path to configuration file
    """
    # Setup logging
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("Starting Option Stream program...")
    logger.info("=" * 60)
    
    # Load configuration
    # Try base config.ini first (for backward compatibility), then config/config.ini
    if os.path.exists("config.ini"):
        config = OptionStreamConfig(config_file="config.ini")
    else:
        config = OptionStreamConfig(config_file=config_file)
    
    logger.info(f"Configuration loaded: tickers={config.tickers}, premium_threshold=${config.premium_threshold}")
    
    # Initialize persistence layer
    initialize_persistence(config)
    logger.info("Database persistence initialized")
    
    # Get API credentials
    try:
        api_key, api_secret = get_credentials(profile_id)
        logger.info(f"Loaded credentials for profile {profile_id}")
    except Exception as e:
        logger.error(f"Failed to load credentials for profile {profile_id}: {e}")
        sys.exit(1)
    
    # Initialize option stream with dynamic API key/secret
    init_option_stream(api_key, api_secret)
    set_premium_threshold(config.premium_threshold)
    
    # Start background DB writer thread
    db_thread = threading.Thread(
        target=db_batch_writer,
        args=(config,),
        daemon=True,
        name="DBBatchWriter"
    )
    db_thread.start()
    logger.debug("Background DB batch writer thread started")
    
    # Fetch option symbols for configured tickers
    logger.info(f"Fetching option chains for: {', '.join(config.tickers)}")
    fetched_symbols = fetch_options_for_symbols(
        symbols=config.tickers,
        api_key=api_key,
        api_secret=api_secret,
        start_date=config.start_date,
        end_date=config.end_date
    )
    
    if not fetched_symbols:
        logger.error("No option symbols fetched. Exiting program.")
        sys.exit(1)
    
    logger.info(f"Fetched {len(fetched_symbols)} option symbols")
    
    # Initialize Redis client
    redis_client = initialize_redis_client(config)
    set_redis_client(redis_client, config.redis_stream_queue_name)
    logger.info(f"Redis configured for stream: {config.redis_stream_queue_name}")
    
    # Subscribe to trades
    stream = get_option_stream()
    subscribe_to_trades(stream, fetched_symbols, option_trade_handler)
    
    logger.info("=" * 60)
    logger.info("Option stream is now running. Press Ctrl+C to stop.")
    logger.info("=" * 60)
    
    # Run the stream
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("Detected running event loop. Using alternative approach.")
            loop.create_task(run_stream())
        else:
            asyncio.run(run_stream())
    except RuntimeError as e:
        logger.error(f"RuntimeError in event loop: {e}")
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the DeltaDyno option streaming system."
    )
    parser.add_argument(
        "profile_id",
        type=str,
        help="Trading profile ID for API credentials"
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to configuration file (default: config/config.ini)"
    )
    
    args = parser.parse_args()
    
    main(profile_id=args.profile_id, config_file=args.config)
