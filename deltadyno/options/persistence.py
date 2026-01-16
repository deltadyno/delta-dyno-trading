"""
Option Trade Persistence Layer.

Handles database operations for option trade data with support for
both individual inserts and efficient batch operations.
"""

import logging
from typing import Dict, List, Any, Optional

from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Module-level engine and table references (lazily initialized)
_engine: Optional[Engine] = None
_metadata: Optional[MetaData] = None
_options_trades_table: Optional[Table] = None


def _get_default_config():
    """Get default configuration for database connection."""
    from deltadyno.options.config import OptionStreamConfig
    return OptionStreamConfig()


def get_db_engine(config=None) -> Engine:
    """
    Get or create the SQLAlchemy database engine.
    
    Uses connection pooling for efficient database access under high load.
    
    Args:
        config: Optional OptionStreamConfig instance. If not provided,
                loads from default config.ini location.
    
    Returns:
        SQLAlchemy Engine instance
    """
    global _engine, _metadata, _options_trades_table
    
    if _engine is None:
        if config is None:
            config = _get_default_config()
        
        # Create engine with connection pooling for high throughput
        _engine = create_engine(
            config.db_connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        _metadata = MetaData()
        
        # Define table structure (matches existing schema)
        _options_trades_table = Table(
            config.db_table_name,
            _metadata,
            Column('DateTime', DateTime),
            Column('Symbol', String(50)),
            Column('Ticker', String(20)),
            Column('ExpirationDate', String(10)),
            Column('OptionType', String(1)),
            Column('StrikePrice', String(10)),
            Column('Price', Float),
            Column('Size', Float),
            Column('Premium', Float)
        )
        
        # Create table if it doesn't exist
        _metadata.create_all(_engine)
        logger.debug(f"Database engine initialized for table: {config.db_table_name}")
    
    return _engine


def get_trades_table() -> Table:
    """Get the options trades table object."""
    global _options_trades_table
    if _options_trades_table is None:
        get_db_engine()  # This will initialize the table
    return _options_trades_table


def insert_trade(trade_data: Dict[str, Any]) -> bool:
    """
    Insert a single trade record into the database.
    
    Args:
        trade_data: Dictionary containing trade fields:
            - DateTime: Trade timestamp
            - Symbol: Full option symbol
            - Ticker: Underlying ticker
            - ExpirationDate: Option expiration date
            - OptionType: 'C' or 'P'
            - StrikePrice: Strike price
            - Price: Trade price
            - Size: Trade size
            - Premium: Calculated premium
    
    Returns:
        True if insert succeeded, False otherwise
    """
    try:
        engine = get_db_engine()
        table = get_trades_table()
        
        with engine.connect() as conn:
            conn.execute(table.insert().values(**trade_data))
            conn.commit()
            logger.debug(f"Inserted trade into DB: {trade_data.get('Symbol')}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"DB insert error: {str(e)}", exc_info=True)
        return False


def insert_trades_batch(trades: List[Dict[str, Any]]) -> bool:
    """
    Insert multiple trade records in a single batch operation.
    
    This is more efficient than individual inserts for high-volume
    streaming scenarios.
    
    Args:
        trades: List of trade data dictionaries
    
    Returns:
        True if batch insert succeeded, False otherwise
    """
    if not trades:
        return True
    
    try:
        engine = get_db_engine()
        table = get_trades_table()
        
        with engine.begin() as conn:
            conn.execute(table.insert(), trades)
        
        logger.debug(f"Batch inserted {len(trades)} trades into DB")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Batch insert error: {str(e)}", exc_info=True)
        return False


def initialize_persistence(config=None) -> None:
    """
    Explicitly initialize the persistence layer.
    
    Call this during application startup to ensure database
    connection and tables are ready before stream processing begins.
    
    Args:
        config: Optional OptionStreamConfig instance
    """
    engine = get_db_engine(config)
    logger.debug(f"Persistence layer initialized: {engine.url.database}")
