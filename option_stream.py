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


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(log_file: str = "logs/option_stream.log") -> logging.Logger:
    """
    Configure logging with rotating file handler.
    
    Args:
        log_file: Path to the log file
    
    Returns:
        Configured logger instance
    """
    # Ensure logs directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Set up rotating log file handler
    log_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3
    )
    
    # Log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    
    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)
    logger.addHandler(console_handler)
    
    return logger


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
    
    logging.info(f"DB batch writer started: batch_size={batch_size}, interval={interval}s")
    
    while True:
        try:
            batch = []
            
            # Collect trades up to batch size
            while not trade_buffer.empty() and len(batch) < batch_size:
                batch.append(trade_buffer.get())
            
            # Insert batch if we have trades
            if batch:
                insert_trades_batch(batch)
                logging.debug(f"Batch inserted {len(batch)} trades")
            
            # Sleep before next batch
            time.sleep(interval)
            
        except Exception as e:
            logging.error(f"Error in DB batch writer: {e}")
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
    print("Initializing Redis Client...")
    logging.info("Initializing Redis Client...")
    
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
    logger = setup_logging()
    
    print("Starting Option Stream program...")
    logging.info("Starting Option Stream program...")
    
    # Load configuration
    # Try base config.ini first (for backward compatibility), then config/config.ini
    if os.path.exists("config.ini"):
        config = OptionStreamConfig(config_file="config.ini")
    else:
        config = OptionStreamConfig(config_file=config_file)
    
    logging.info(f"Loaded configuration: tickers={config.tickers}, premium_threshold={config.premium_threshold}")
    
    # Initialize persistence layer
    initialize_persistence(config)
    
    # Get API credentials
    try:
        api_key, api_secret = get_credentials(profile_id)
        logging.info(f"Loaded credentials for profile {profile_id}")
    except Exception as e:
        logging.error(f"Failed to load credentials for profile {profile_id}: {e}")
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
    logging.info("Background DB batch writer started")
    
    # Fetch option symbols for configured tickers
    fetched_symbols = fetch_options_for_symbols(
        symbols=config.tickers,
        api_key=api_key,
        api_secret=api_secret,
        start_date=config.start_date,
        end_date=config.end_date
    )
    
    if not fetched_symbols:
        logging.warning("No option symbols fetched. Exiting program.")
        print("No option symbols fetched. Exiting program.")
        sys.exit(1)
    
    logging.info(f"Fetched {len(fetched_symbols)} option symbols")
    
    # Initialize Redis client
    redis_client = initialize_redis_client(config)
    set_redis_client(redis_client, config.redis_stream_queue_name)
    print("Redis queue is setup")
    logging.info(f"Redis configured for stream: {config.redis_stream_queue_name}")
    
    # Subscribe to trades
    stream = get_option_stream()
    subscribe_to_trades(stream, fetched_symbols, option_trade_handler)
    
    # Run the stream
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logging.warning("Detected running event loop. Using alternative approach.")
            loop.create_task(run_stream())
        else:
            asyncio.run(run_stream())
    except RuntimeError as e:
        logging.error(f"RuntimeError in event loop: {e}")
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user")
        print("\nShutdown requested. Exiting...")


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

