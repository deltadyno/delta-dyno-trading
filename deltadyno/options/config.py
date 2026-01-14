"""
Option Stream Configuration.

Loads option streaming specific configuration from config.ini,
integrating with the existing DeltaDyno configuration system.
"""

import configparser
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class OptionStreamConfig:
    """
    Configuration container for option streaming parameters.
    
    Loads configuration from config.ini and provides typed access
    to all option streaming settings.
    """
    
    def __init__(self, config_file: str = "config/config.ini"):
        """
        Initialize configuration from the specified config file.
        
        Args:
            config_file: Path to the configuration file
        """
        self._config = configparser.ConfigParser()
        self._config_path = self._resolve_config_path(config_file)
        self._load_config()
        self._parse_settings()
    
    def _resolve_config_path(self, config_file: str) -> Path:
        """Resolve the config file path, checking multiple locations."""
        # Try the provided path first
        path = Path(config_file)
        if path.exists():
            return path
        
        # Try relative to current working directory
        cwd_path = Path.cwd() / config_file
        if cwd_path.exists():
            return cwd_path
        
        # Try the base config.ini in root (for backward compatibility)
        root_config = Path.cwd() / "config.ini"
        if root_config.exists():
            return root_config
        
        # Fall back to the original path (will fail gracefully)
        logger.warning(f"Config file not found at {config_file}, using defaults")
        return path
    
    def _load_config(self) -> None:
        """Load the configuration file."""
        if self._config_path.exists():
            self._config.read(str(self._config_path))
            logger.info(f"Loaded option stream config from {self._config_path}")
        else:
            logger.warning(f"Config file not found: {self._config_path}")
    
    def _parse_settings(self) -> None:
        """Parse all configuration settings."""
        # Date range settings
        self.days_forward: int = self._get_int("dates", "days_forward", 365)
        self.start_date: datetime.date = datetime.now().date()
        self.end_date: str = (self.start_date + timedelta(days=self.days_forward)).strftime('%Y-%m-%d')
        
        # Options settings
        self.premium_threshold: int = self._get_int("options", "premium_threshold", 500)
        self.tickers: List[str] = self._get_list("options", "tickers", ["SPY", "TSLA"])
        self.tweet_interval_minutes: int = self._get_int("options", "tweet_interval_minutes", 5)
        
        # Database settings
        self.db_host: str = self._get_str("database", "host", "localhost")
        self.db_port: int = self._get_int("database", "port", 3306)
        self.db_user: str = self._get_str("database", "user", "root")
        self.db_password: str = self._get_str("database", "password", "")
        self.db_name: str = self._get_str("database", "database", "deltadyno")
        self.db_table_name: str = self._get_str("database", "table_name", "dd_trade_stream")
        
        # Redis settings
        self.redis_host: str = self._get_str("redis", "redis_host", "localhost")
        self.redis_port: int = self._get_int("redis", "redis_port", 6379)
        self.redis_password: str = self._get_str("redis", "redis_password", "")
        self.redis_stream_queue_name: str = self._get_str("redis", "stream_queue_name", "options_flow:v1")
        
        # Batch processing settings
        self.db_batch_size: int = self._get_int("options", "db_batch_size", 20)
        self.db_batch_interval_seconds: float = self._get_float("options", "db_batch_interval_seconds", 2.0)
        
        logger.debug(f"Option stream config: tickers={self.tickers}, premium_threshold={self.premium_threshold}")
    
    def _get_str(self, section: str, key: str, default: str = "") -> str:
        """Get a string configuration value."""
        try:
            return self._config.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
    
    def _get_int(self, section: str, key: str, default: int = 0) -> int:
        """Get an integer configuration value."""
        try:
            return self._config.getint(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return default
    
    def _get_float(self, section: str, key: str, default: float = 0.0) -> float:
        """Get a float configuration value."""
        try:
            return self._config.getfloat(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return default
    
    def _get_list(self, section: str, key: str, default: List[str] = None) -> List[str]:
        """Get a comma-separated list configuration value."""
        if default is None:
            default = []
        try:
            value = self._config.get(section, key)
            return [item.strip() for item in value.split(",") if item.strip()]
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
    
    @property
    def db_connection_string(self) -> str:
        """Generate the SQLAlchemy database connection string."""
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    def reload(self) -> None:
        """Reload configuration from disk."""
        self._load_config()
        self._parse_settings()
        logger.info("Option stream configuration reloaded")

