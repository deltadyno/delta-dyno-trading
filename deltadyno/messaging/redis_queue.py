"""
Redis queue operations for publishing breakout messages.

This module handles communication with Redis for publishing trading signals
and breakout notifications to downstream consumers.
"""

import json
import traceback
from datetime import datetime
from typing import Optional


def breakout_to_queue(
    symbol: str,
    direction: str,
    bar_strength: float,
    close_time: datetime,
    close_price: float,
    candle_size: float,
    volume: int,
    choppy_day_count: int,
    logger,
    redis_client,
    queue_name: str
) -> bool:
    """
    Publish a breakout signal to the Redis queue.

    Args:
        symbol: Trading symbol (e.g., 'SPY')
        direction: Breakout direction ('upward', 'downward', 'reverse_upward', 'reverse_downward')
        bar_strength: Calculated strength of the bar (0.0 to 1.0)
        close_time: Timestamp of the candle close
        close_price: Closing price of the candle
        candle_size: High-low range of the candle
        volume: Trading volume
        choppy_day_count: Count indicating choppy market conditions
        logger: Logger instance for logging operations
        redis_client: Redis client connection
        queue_name: Name of the Redis stream/queue

    Returns:
        bool: True if message was successfully published, False otherwise
    """
    try:
        # Format close_time for serialization
        close_time_str = close_time.isoformat() if isinstance(close_time, datetime) else str(close_time)

        # Construct the breakout message payload
        message = {
            "symbol": symbol,
            "direction": direction,
            "bar_strength": str(bar_strength),
            "close_time": close_time_str,
            "close_price": str(close_price),
            "candle_size": str(candle_size),
            "volume": str(volume),
            "choppy_day_count": str(choppy_day_count),
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info(f"Publishing breakout message to Redis: {message}")

        # Add message to Redis stream
        message_id = redis_client.xadd(queue_name, message)

        logger.info(f"Breakout message published successfully with ID: {message_id}")
        print(f"Breakout signal published: {direction} for {symbol} at {close_price}")

        return True

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"Failed to publish breakout message: {e}\nTraceback:\n{error_traceback}")
        print(f"Failed to publish breakout message: {e}")
        return False


def publish_position_close(
    symbol: str,
    direction: str,
    close_price: float,
    logger,
    redis_client,
    queue_name: str
) -> bool:
    """
    Publish a position close signal to the Redis queue.

    Args:
        symbol: Trading symbol
        direction: Close direction ('reverse_upward' or 'reverse_downward')
        close_price: Price at which position should be closed
        logger: Logger instance
        redis_client: Redis client connection
        queue_name: Name of the Redis stream/queue

    Returns:
        bool: True if message was successfully published, False otherwise
    """
    try:
        message = {
            "symbol": symbol,
            "direction": direction,
            "close_price": str(close_price),
            "action": "close_position",
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info(f"Publishing position close message to Redis: {message}")

        message_id = redis_client.xadd(queue_name, message)

        logger.info(f"Position close message published with ID: {message_id}")
        return True

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"Failed to publish position close message: {e}\nTraceback:\n{error_traceback}")
        return False

