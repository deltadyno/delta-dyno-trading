"""
Option Stream Configuration.

Loads option streaming specific configuration from config.ini,
integrating with the existing DeltaDyno configuration system.

Uses the shared [Common] section for database and Redis settings,
and the [options] section for option-specific settings.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from deltadyno.config.loader import ConfigLoader

logger = logging.getLogger(__name__)


class OptionStreamConfig:
    """
    Configuration container for option streaming parameters.
    
    Extends the base ConfigLoader to add option-specific settings
    while reusing shared database and Redis configuration.
    """
    
    def __init__(self, config_file: str = "config/config.ini"):
        """
        Initialize configuration from the specified config file.
        
        Args:
            config_file: Path to the configuration file
        """
        # Load base configuration (database, redis, etc.)
        self._base_config = ConfigLoader(config_file=config_file)
        self._parse_option_settings()
    
    def _parse_option_settings(self) -> None:
        """Parse option-specific configuration settings."""
        config = self._base_config.config
        
        # Date range settings (from [options] section)
        self.days_forward: int = config.getint("options", "days_forward", fallback=365)
        self.start_date: datetime.date = datetime.now().date()
        self.end_date: str = (self.start_date + timedelta(days=self.days_forward)).strftime('%Y-%m-%d')
        
        # Options settings
        self.premium_threshold: int = config.getint("options", "premium_threshold", fallback=500)
        self.tickers: List[str] = self._get_list("options", "tickers", ["SPY", "TSLA"])
        self.tweet_interval_minutes: int = config.getint("options", "tweet_interval_minutes", fallback=5)
        
        # Batch processing settings
        self.db_batch_size: int = config.getint("options", "db_batch_size", fallback=20)
        self.db_batch_interval_seconds: float = config.getfloat("options", "db_batch_interval_seconds", fallback=2.0)
        
        logger.debug(f"Option stream config: tickers={self.tickers}, premium_threshold={self.premium_threshold}")
    
    def _get_list(self, section: str, key: str, default: List[str] = None) -> List[str]:
        """Get a comma-separated list configuration value."""
        if default is None:
            default = []
        try:
            value = self._base_config.config.get(section, key)
            return [item.strip() for item in value.split(",") if item.strip()]
        except Exception:
            return default
    
    # ==========================================================================
    # Shared Configuration Properties (from [Common] section)
    # ==========================================================================
    
    @property
    def db_host(self) -> str:
        """Database hostname."""
        return self._base_config.db_host or "localhost"
    
    @property
    def db_port(self) -> int:
        """Database port."""
        return self._base_config.db_port or 3306
    
    @property
    def db_user(self) -> str:
        """Database username."""
        return self._base_config.db_user or "root"
    
    @property
    def db_password(self) -> str:
        """Database password."""
        return self._base_config.db_password or ""
    
    @property
    def db_name(self) -> str:
        """Database name."""
        return self._base_config.db_name or "deltadyno"
    
    @property
    def db_table_name(self) -> str:
        """Trade stream table name."""
        return self._base_config.db_table_trade_stream or "dd_trade_stream"
    
    @property
    def redis_host(self) -> str:
        """Redis hostname."""
        return self._base_config.redis_host or "localhost"
    
    @property
    def redis_port(self) -> int:
        """Redis port."""
        port = self._base_config.redis_port
        return int(port) if port else 6379
    
    @property
    def redis_password(self) -> str:
        """Redis password."""
        return self._base_config.redis_password or ""
    
    @property
    def redis_stream_queue_name(self) -> str:
        """Redis stream name for options flow."""
        return self._base_config.redis_stream_name_options_flow or "options_flow:v1"
    
    @property
    def db_connection_string(self) -> str:
        """Generate the SQLAlchemy database connection string."""
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    def reload(self) -> None:
        """Reload configuration from disk."""
        self._base_config = ConfigLoader(config_file=self._base_config.config_file)
        self._parse_option_settings()
        logger.info("Option stream configuration reloaded")
