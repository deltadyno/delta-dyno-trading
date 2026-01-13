"""
Storage backends for telemetry data.

This module handles both MySQL (persistent) and Redis (real-time) storage.

Performance optimizations for scaling to 100s of profiles:
- MySQL connection pooling (prevents connection exhaustion)
- Redis connection pooling (efficient resource usage)
- Bulk INSERT operations (5-10x faster writes)
"""

import json
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import pooling
import redis
from redis import ConnectionPool

from deltadyno.telemetry.models import (
    BreakoutMetric,
    BreakoutOutcome,
    DrawdownMetric,
    EquityMetric,
    OrderConversionMetric,
    OrderMetric,
    SystemHealthMetric,
    TradePerformance
)


# Global connection pools (shared across all TelemetryStorage instances)
_mysql_pool: Optional[pooling.MySQLConnectionPool] = None
_mysql_pool_lock = threading.Lock()

_redis_pool: Optional[ConnectionPool] = None
_redis_pool_lock = threading.Lock()


class TelemetryStorage:
    """
    Hybrid storage backend for telemetry data.
    
    Uses MySQL for persistent storage and Redis for real-time caching.
    
    Performance optimizations:
    - MySQL connection pooling (shared pool across all instances)
    - Redis connection pooling (shared pool across all instances)
    - Bulk INSERT operations for high throughput
    """
    
    def __init__(
        self,
        db_host: str,
        db_user: str,
        db_password: str,
        db_name: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        redis_ttl_seconds: int = 3600,  # 1 hour default TTL for Redis data
        mysql_pool_size: int = 50,  # Connection pool size for MySQL
        mysql_pool_name: str = "telemetry_pool",  # Pool name
        redis_pool_size: int = 100  # Connection pool size for Redis
    ):
        """
        Initialize telemetry storage with connection pooling.
        
        Args:
            db_host: MySQL database host
            db_user: MySQL database user
            db_password: MySQL database password
            db_name: MySQL database name
            redis_host: Redis server host
            redis_port: Redis server port
            redis_password: Redis password (optional)
            redis_ttl_seconds: TTL for Redis keys in seconds
            mysql_pool_size: Size of MySQL connection pool (default: 50)
            mysql_pool_name: Name for MySQL connection pool
            redis_pool_size: Size of Redis connection pool (default: 100)
        """
        # Store connection details for fallback
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        
        # MySQL connection pool (shared across instances)
        global _mysql_pool
        with _mysql_pool_lock:
            if _mysql_pool is None:
                pool_config = {
                    'pool_name': mysql_pool_name,
                    'pool_size': mysql_pool_size,
                    'pool_reset_session': True,
                    'host': db_host,
                    'user': db_user,
                    'password': db_password,
                    'database': db_name,
                    'autocommit': False
                }
                _mysql_pool = pooling.MySQLConnectionPool(**pool_config)
                print(f"Created MySQL connection pool with {mysql_pool_size} connections")
        
        self.mysql_pool = _mysql_pool
        
        # Redis connection pool (shared across instances)
        global _redis_pool
        with _redis_pool_lock:
            if _redis_pool is None:
                _redis_pool = ConnectionPool(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    max_connections=redis_pool_size,
                    decode_responses=True
                )
                print(f"Created Redis connection pool with {redis_pool_size} connections")
        
        self.redis_pool = _redis_pool
        self.redis_client = redis.Redis(connection_pool=self.redis_pool)
        self.redis_ttl = redis_ttl_seconds
        
        # Initialize database schema
        self._initialize_schema()
    
    def _get_db_connection(self) -> mysql.connector.MySQLConnection:
        """Get connection from MySQL connection pool."""
        try:
            return self.mysql_pool.get_connection()
        except Exception as e:
            print(f"Error getting connection from pool: {e}")
            # Fallback to direct connection if pool fails
            return mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name
            )
    
    def _initialize_schema(self) -> None:
        """Initialize database tables if they don't exist."""
        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Create telemetry_metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dd_telemetry_metrics (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    profile_id INT NOT NULL,
                    metric_type VARCHAR(50) NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DECIMAL(15, 4) NOT NULL,
                    window_type VARCHAR(20) NOT NULL,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_profile_metric (profile_id, metric_type, metric_name),
                    INDEX idx_window (window_start, window_end)
                )
            """)
            
            # Create trade_performance table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dd_trade_performance (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    profile_id INT NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    trade_type VARCHAR(20) NOT NULL,
                    entry_price DECIMAL(10, 4),
                    exit_price DECIMAL(10, 4),
                    quantity INT,
                    pnl DECIMAL(10, 2),
                    pnl_pct DECIMAL(6, 2),
                    slippage DECIMAL(10, 4),
                    entry_time TIMESTAMP,
                    exit_time TIMESTAMP,
                    duration_seconds INT,
                    bar_strength DECIMAL(5, 3),
                    direction VARCHAR(20),
                    exit_reason VARCHAR(50),
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_profile_trade (profile_id, entry_time),
                    INDEX idx_symbol (symbol, entry_time)
                )
            """)
            
            # Create system_health table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dd_system_health (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    profile_id INT NOT NULL,
                    script_name VARCHAR(50) NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DECIMAL(15, 4),
                    status VARCHAR(20),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata JSON,
                    INDEX idx_profile_script (profile_id, script_name, timestamp)
                )
            """)
            
            conn.commit()
            cursor.close()
            
        except Exception as e:
            print(f"Warning: Could not initialize telemetry schema: {e}")
        finally:
            if conn and conn.is_connected():
                conn.close()  # Return connection to pool
    
    # =========================================================================
    # Redis Methods (Real-time)
    # =========================================================================
    
    def _redis_key(self, prefix: str, profile_id: int, *args: str) -> str:
        """Generate Redis key."""
        parts = [prefix, str(profile_id)] + list(args)
        return ":".join(parts)
    
    def store_realtime_equity(self, metric: EquityMetric) -> None:
        """Store real-time equity metric in Redis."""
        key = self._redis_key("telemetry:equity", metric.profile_id, "latest")
        data = metric.model_dump_json()
        self.redis_client.setex(key, self.redis_ttl, data)
    
    def get_realtime_equity(self, profile_id: int) -> Optional[EquityMetric]:
        """Get latest equity metric from Redis."""
        key = self._redis_key("telemetry:equity", profile_id, "latest")
        data = self.redis_client.get(key)
        if data:
            return EquityMetric.model_validate_json(data)
        return None
    
    def store_api_latency(self, profile_id: int, script_name: str, latency_ms: float) -> None:
        """Store API latency measurement in Redis (using sorted set for statistics)."""
        key = self._redis_key("telemetry:latency", profile_id, script_name)
        timestamp = datetime.utcnow().timestamp()
        self.redis_client.zadd(key, {str(latency_ms): timestamp})
        self.redis_client.expire(key, self.redis_ttl)
        
        # Keep only last 1000 measurements
        self.redis_client.zremrangebyrank(key, 0, -1001)
    
    def get_api_latency_stats(self, profile_id: int, script_name: str) -> Dict[str, float]:
        """Get API latency statistics from Redis."""
        key = self._redis_key("telemetry:latency", profile_id, script_name)
        values = self.redis_client.zrange(key, 0, -1, withscores=False)
        
        if not values:
            return {}
        
        latencies = [float(v) for v in values]
        latencies.sort()
        
        count = len(latencies)
        return {
            "count": count,
            "avg": sum(latencies) / count,
            "min": min(latencies),
            "max": max(latencies),
            "p50": latencies[count // 2] if count > 0 else 0,
            "p95": latencies[int(count * 0.95)] if count > 1 else latencies[0],
            "p99": latencies[int(count * 0.99)] if count > 1 else latencies[0],
        }
    
    def store_system_health(self, metric: SystemHealthMetric) -> None:
        """Store system health metric in Redis."""
        key = self._redis_key("telemetry:health", metric.profile_id, metric.script_name, "latest")
        data = metric.model_dump_json()
        self.redis_client.setex(key, self.redis_ttl, data)
    
    def get_system_health(self, profile_id: int, script_name: str) -> Optional[SystemHealthMetric]:
        """Get latest system health from Redis."""
        key = self._redis_key("telemetry:health", profile_id, script_name, "latest")
        data = self.redis_client.get(key)
        if data:
            return SystemHealthMetric.model_validate_json(data)
        return None
    
    # =========================================================================
    # MySQL Methods (Persistent)
    # =========================================================================
    
    def store_trade_performance(self, trade: TradePerformance) -> None:
        """Store trade performance record in MySQL (single insert)."""
        self.store_trade_performance_bulk([trade])
    
    def store_trade_performance_bulk(self, trades: List[TradePerformance]) -> None:
        """
        Store multiple trade performance records in MySQL using bulk INSERT.
        
        This is 5-10x faster than individual inserts for high throughput.
        
        Args:
            trades: List of TradePerformance objects to insert
        """
        if not trades:
            return
        
        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Prepare bulk insert values
            values = []
            for trade in trades:
                values.append((
                    trade.profile_id,
                    trade.symbol,
                    trade.trade_type,
                    float(trade.entry_price),
                    float(trade.exit_price),
                    trade.quantity,
                    float(trade.pnl),
                    trade.pnl_pct,
                    float(trade.slippage) if trade.slippage else None,
                    trade.entry_time,
                    trade.exit_time,
                    trade.duration_seconds,
                    trade.bar_strength,
                    trade.direction,
                    trade.exit_reason,
                    json.dumps(trade.metadata)
                ))
            
            # Bulk insert
            cursor.executemany("""
                INSERT INTO dd_trade_performance (
                    profile_id, symbol, trade_type, entry_price, exit_price,
                    quantity, pnl, pnl_pct, slippage, entry_time, exit_time,
                    duration_seconds, bar_strength, direction, exit_reason, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
            
            conn.commit()
            cursor.close()
            
        except Exception as e:
            print(f"Error storing trade performance (bulk): {e}")
        finally:
            if conn and conn.is_connected():
                conn.close()  # Return connection to pool
    
    def store_aggregated_metric(
        self,
        profile_id: int,
        metric_type: str,
        metric_name: str,
        metric_value: Decimal,
        window_type: str,
        window_start: datetime,
        window_end: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Store aggregated metric in MySQL (single insert)."""
        self.store_aggregated_metric_bulk([{
            'profile_id': profile_id,
            'metric_type': metric_type,
            'metric_name': metric_name,
            'metric_value': metric_value,
            'window_type': window_type,
            'window_start': window_start,
            'window_end': window_end,
            'metadata': metadata
        }])
    
    def store_aggregated_metric_bulk(self, metrics: List[Dict[str, Any]]) -> None:
        """
        Store multiple aggregated metrics in MySQL using bulk INSERT.
        
        Args:
            metrics: List of metric dictionaries with keys:
                profile_id, metric_type, metric_name, metric_value,
                window_type, window_start, window_end, metadata
        """
        if not metrics:
            return
        
        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Prepare bulk insert values
            values = []
            for metric in metrics:
                values.append((
                    metric['profile_id'],
                    metric['metric_type'],
                    metric['metric_name'],
                    float(metric['metric_value']),
                    metric['window_type'],
                    metric['window_start'],
                    metric['window_end'],
                    json.dumps(metric.get('metadata') or {})
                ))
            
            # Bulk insert with ON DUPLICATE KEY UPDATE
            # Note: MySQL doesn't support bulk ON DUPLICATE KEY UPDATE easily,
            # so we use individual inserts for metrics (they're less frequent than trades)
            for value in values:
                cursor.execute("""
                    INSERT INTO dd_telemetry_metrics (
                        profile_id, metric_type, metric_name, metric_value,
                        window_type, window_start, window_end, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        metric_value = VALUES(metric_value),
                        metadata = VALUES(metadata)
                """, value)
            
            conn.commit()
            cursor.close()
            
        except Exception as e:
            print(f"Error storing aggregated metrics (bulk): {e}")
        finally:
            if conn and conn.is_connected():
                conn.close()  # Return connection to pool
    
    def get_metrics(
        self,
        profile_id: int,
        metric_type: Optional[str] = None,
        metric_name: Optional[str] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query aggregated metrics from MySQL."""
        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = "SELECT * FROM dd_telemetry_metrics WHERE profile_id = %s"
            params = [profile_id]
            
            if metric_type:
                query += " AND metric_type = %s"
                params.append(metric_type)
            
            if metric_name:
                query += " AND metric_name = %s"
                params.append(metric_name)
            
            if window_start:
                query += " AND window_end >= %s"
                params.append(window_start)
            
            if window_end:
                query += " AND window_start <= %s"
                params.append(window_end)
            
            query += " ORDER BY window_start DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            print(f"Error querying metrics: {e}")
            return []
        finally:
            if conn and conn.is_connected():
                conn.close()  # Return connection to pool
    
    def get_trade_performance(
        self,
        profile_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        symbol: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query trade performance records from MySQL."""
        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = "SELECT * FROM dd_trade_performance WHERE profile_id = %s"
            params = [profile_id]
            
            if start_time:
                query += " AND entry_time >= %s"
                params.append(start_time)
            
            if end_time:
                query += " AND entry_time <= %s"
                params.append(end_time)
            
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)
            
            query += " ORDER BY entry_time DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            print(f"Error querying trade performance: {e}")
            return []
        finally:
            if conn and conn.is_connected():
                conn.close()  # Return connection to pool

