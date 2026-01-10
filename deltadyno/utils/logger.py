"""
Logging configuration for the DeltaDyno trading system.

This module provides centralized logger setup with support for:
- File-based logging with rotation
- Console logging
- Dynamic log level updates from configuration
"""

import logging
from logging.handlers import RotatingFileHandler
from typing import Optional


def setup_logger(
    config_loader,
    log_to_file: bool = True,
    file_name: str = "trading.log"
) -> logging.Logger:
    """
    Set up and configure the application logger.

    Creates a logger with either file or console output, using the
    log level specified in the configuration.

    Args:
        config_loader: Configuration loader with get_log_level() method
        log_to_file: If True, log to file; otherwise log to console
        file_name: Log file path (used when log_to_file is True)

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger("TradingLogger")
    logger.setLevel(config_loader.get_log_level())

    # Clear any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Configure handler based on output destination
    if log_to_file:
        handler = RotatingFileHandler(
            file_name,
            maxBytes=5 * 1024 * 1024,  # 5 MB per file
            backupCount=3  # Keep 3 backup files
        )
    else:
        handler = logging.StreamHandler()

    # Set log format
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def update_logger_level(logger: logging.Logger, config_loader) -> None:
    """
    Dynamically update the logger's level from configuration.

    This allows log level changes without restarting the application.

    Args:
        logger: Logger instance to update
        config_loader: Configuration loader with get_log_level() method
    """
    new_level = config_loader.get_log_level()
    current_level = logging.getLevelName(logger.level)

    if current_level != new_level:
        logger.setLevel(new_level)
        logger.info(f"Log level changed from {current_level} to {new_level}")

