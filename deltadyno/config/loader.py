"""
File-based configuration loader for the DeltaDyno trading system.

This module loads configuration from an INI file, providing access to
static configuration values like database credentials and Redis settings.
"""

import configparser
from typing import Optional


class ConfigLoader:
    """
    Load configuration from an INI file.

    This class provides access to configuration values defined in a
    standard INI file format with sections and key-value pairs.

    Attributes:
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_password: Redis authentication password
        redis_stream_name_breakout_message: Redis stream name for breakout messages
        max_retries: Maximum retry attempts for API calls
        base_delay: Base delay for exponential backoff
        db_host: MySQL database hostname
        db_user: MySQL database username
        db_password: MySQL database password
        db_name: MySQL database name
    """

    def __init__(self, config_file: str = "config/config.ini"):
        """
        Initialize the configuration loader.

        Args:
            config_file: Path to the INI configuration file
        """
        self.config_file = config_file
        self.config = configparser.ConfigParser()

        # Load configuration
        self._load_common_config()

    def _load_common_config(self) -> None:
        """Load configuration settings from the INI file."""
        self.config.read(self.config_file)

        # Redis configuration
        self.redis_host: Optional[str] = self.config.get(
            "Common", "redis_host", fallback=None
        )
        self.redis_port: Optional[str] = self.config.get(
            "Common", "redis_port", fallback=None
        )
        self.redis_password: Optional[str] = self.config.get(
            "Common", "redis_password", fallback=None
        )
        self.redis_stream_name_breakout_message: Optional[str] = self.config.get(
            "Common", "redis_stream_name_breakout_message", fallback=None
        )

        # Retry configuration
        self.max_retries: Optional[int] = self.config.getint(
            "Common", "max_retries", fallback=3
        )
        self.base_delay: Optional[int] = self.config.getint(
            "Common", "base_delay", fallback=2
        )

        # Database configuration
        self.db_host: Optional[str] = self.config.get(
            "Common", "db_host", fallback=None
        )
        self.db_port: Optional[int] = self.config.getint(
            "Common", "db_port", fallback=3306
        )
        self.db_user: Optional[str] = self.config.get(
            "Common", "db_user", fallback=None
        )
        self.db_password: Optional[str] = self.config.get(
            "Common", "db_password", fallback=None
        )
        self.db_name: Optional[str] = self.config.get(
            "Common", "db_name", fallback=None
        )
        self.db_table_trade_stream: Optional[str] = self.config.get(
            "Common", "db_table_trade_stream", fallback="dd_trade_stream"
        )

        # Redis stream names
        self.redis_stream_name_options_flow: Optional[str] = self.config.get(
            "Common", "redis_stream_name_options_flow", fallback="options_flow:v1"
        )

        # Data feed configuration
        self.data_feed: Optional[str] = self.config.get(
            "Common", "data_feed", fallback="IEX"
        )

    def get(self, section: str, key: str, fallback: Optional[str] = None) -> Optional[str]:
        """
        Get a configuration value.

        Args:
            section: INI file section name
            key: Configuration key within the section
            fallback: Default value if key is not found

        Returns:
            Configuration value or fallback
        """
        return self.config.get(section, key, fallback=fallback)

    def getint(self, section: str, key: str, fallback: Optional[int] = None) -> Optional[int]:
        """
        Get an integer configuration value.

        Args:
            section: INI file section name
            key: Configuration key within the section
            fallback: Default value if key is not found

        Returns:
            Integer configuration value or fallback
        """
        return self.config.getint(section, key, fallback=fallback)

    def getfloat(self, section: str, key: str, fallback: Optional[float] = None) -> Optional[float]:
        """
        Get a float configuration value.

        Args:
            section: INI file section name
            key: Configuration key within the section
            fallback: Default value if key is not found

        Returns:
            Float configuration value or fallback
        """
        return self.config.getfloat(section, key, fallback=fallback)

    def getboolean(self, section: str, key: str, fallback: Optional[bool] = None) -> Optional[bool]:
        """
        Get a boolean configuration value.

        Args:
            section: INI file section name
            key: Configuration key within the section
            fallback: Default value if key is not found

        Returns:
            Boolean configuration value or fallback
        """
        return self.config.getboolean(section, key, fallback=fallback)

