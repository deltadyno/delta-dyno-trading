"""
Data models for telemetry metrics.

This module defines Pydantic models for type-safe telemetry data structures.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


# =============================================================================
# Breakout Metrics
# =============================================================================

class BreakoutMetric(BaseModel):
    """Metrics for breakout detection and execution."""
    
    profile_id: int
    symbol: str
    direction: str  # 'upward', 'downward', 'reverse_upward', 'reverse_downward'
    bar_strength: float = Field(ge=0.0, le=1.0)
    close_price: Decimal
    candle_size: Decimal
    volume: int
    timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BreakoutOutcome(BaseModel):
    """Track outcome of a breakout signal."""
    
    profile_id: int
    signal_id: Optional[str] = None
    symbol: str
    entry_price: Optional[Decimal] = None
    exit_price: Optional[Decimal] = None
    pnl: Optional[Decimal] = None
    slippage: Optional[Decimal] = None
    success: Optional[bool] = None  # True if profitable
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Equity Metrics
# =============================================================================

class EquityMetric(BaseModel):
    """Real-time equity and PnL metrics."""
    
    profile_id: int
    timestamp: datetime
    account_equity: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    total_pnl: Decimal
    margin_used: Decimal
    margin_available: Decimal
    margin_utilization_pct: float = Field(ge=0.0, le=100.0)
    open_positions_count: int
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DrawdownMetric(BaseModel):
    """Maximum drawdown calculation."""
    
    profile_id: int
    window_start: datetime
    window_end: datetime
    peak_equity: Decimal
    trough_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: float
    current_equity: Decimal
    recovery_status: str  # 'recovered', 'drawdown', 'new_peak'


# =============================================================================
# Order Metrics
# =============================================================================

class OrderMetric(BaseModel):
    """Metrics for order execution and monitoring."""
    
    profile_id: int
    order_id: Optional[str] = None
    symbol: str
    order_type: str  # 'limit', 'market'
    side: str  # 'buy', 'sell'
    status: str  # 'pending', 'filled', 'canceled', 'expired', 'converted'
    quantity: int
    limit_price: Optional[Decimal] = None
    filled_price: Optional[Decimal] = None
    filled_quantity: Optional[int] = None
    slippage: Optional[Decimal] = None
    api_latency_ms: Optional[float] = None
    timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)


class OrderConversionMetric(BaseModel):
    """Track limit order to market order conversions."""
    
    profile_id: int
    timestamp: datetime
    total_limit_orders: int
    converted_to_market: int
    canceled: int
    expired: int
    conversion_rate: float
    avg_time_to_conversion_seconds: float
    metadata: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# System Health Metrics
# =============================================================================

class SystemHealthMetric(BaseModel):
    """System health and performance metrics."""
    
    profile_id: int
    script_name: str  # 'breakout_detector', 'order_monitor', 'equity_monitor'
    timestamp: datetime
    status: str  # 'healthy', 'degraded', 'error'
    
    # API Metrics
    api_latency_avg_ms: Optional[float] = None
    api_latency_p95_ms: Optional[float] = None
    api_latency_p99_ms: Optional[float] = None
    api_rate_limit_remaining: Optional[int] = None
    api_rate_limit_limit: Optional[int] = None
    api_error_count: int = 0
    
    # System Metrics
    cpu_usage_pct: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    error_count: int = 0
    warning_count: int = 0
    
    metadata: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Trade Performance
# =============================================================================

class TradePerformance(BaseModel):
    """Individual trade performance record."""
    
    profile_id: int
    symbol: str
    trade_type: str  # 'breakout', 'equity_close', 'stop_loss', 'choppy_day_close'
    entry_price: Decimal
    exit_price: Decimal
    quantity: int
    pnl: Decimal
    pnl_pct: float
    slippage: Decimal
    entry_time: datetime
    exit_time: datetime
    duration_seconds: int
    
    # Breakout-specific
    bar_strength: Optional[float] = None
    direction: Optional[str] = None
    
    # Exit reason
    exit_reason: Optional[str] = None  # 'profit_target', 'stop_loss', 'choppy_day', 'time_based'
    
    metadata: Dict[str, Any] = Field(default_factory=dict)

