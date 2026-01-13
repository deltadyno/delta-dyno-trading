"""
Centralized TelemetryManager for collecting and storing telemetry data.

This manager provides a non-blocking interface for telemetry collection
that can be used throughout the trading system.
"""

import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional
from queue import Queue

from deltadyno.telemetry.storage import TelemetryStorage
from deltadyno.telemetry.models import (
    BreakoutMetric,
    BreakoutOutcome,
    DrawdownMetric,
    EquityMetric,
    OrderMetric,
    SystemHealthMetric,
    TradePerformance
)


class TelemetryManager:
    """
    Centralized telemetry manager with async/non-blocking writes.
    
    This manager batches writes to avoid blocking trading loops and provides
    a simple interface for collecting metrics from various scripts.
    """
    
    def __init__(
        self,
        storage: TelemetryStorage,
        batch_size: int = 10,
        flush_interval_seconds: float = 5.0,
        enabled: bool = True
    ):
        """
        Initialize telemetry manager.
        
        Args:
            storage: TelemetryStorage instance
            batch_size: Number of metrics to batch before writing
            flush_interval_seconds: Maximum time between writes
            enabled: Whether telemetry collection is enabled
        """
        self.storage = storage
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.enabled = enabled
        
        # Async write queue
        self.write_queue: Queue = Queue()
        self.flush_lock = threading.Lock()
        
        # Start background writer thread if enabled
        if enabled:
            self._start_background_writer()
    
    def _start_background_writer(self) -> None:
        """Start background thread for async metric writes."""
        def writer():
            batch = []
            last_flush = time.time()
            
            while True:
                try:
                    # Get item from queue with timeout
                    try:
                        item = self.write_queue.get(timeout=1.0)
                        batch.append(item)
                    except:
                        item = None
                    
                    # Flush if batch is full or timeout reached
                    now = time.time()
                    should_flush = (
                        len(batch) >= self.batch_size or
                        (item is None and len(batch) > 0 and (now - last_flush) >= self.flush_interval)
                    )
                    
                    if should_flush and batch:
                        self._flush_batch(batch)
                        batch = []
                        last_flush = now
                        
                except Exception as e:
                    print(f"Error in telemetry writer thread: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
    
    def _flush_batch(self, batch: list) -> None:
        """Flush a batch of metrics to storage."""
        for item in batch:
            try:
                item_type, data = item
                
                if item_type == "trade_performance":
                    self.storage.store_trade_performance(data)
                elif item_type == "equity_realtime":
                    self.storage.store_realtime_equity(data)
                elif item_type == "system_health":
                    self.storage.store_system_health(data)
                elif item_type == "aggregated_metric":
                    self.storage.store_aggregated_metric(**data)
                    
            except Exception as e:
                print(f"Error flushing telemetry batch item: {e}")
    
    # =========================================================================
    # Breakout Metrics
    # =========================================================================
    
    def record_breakout_signal(
        self,
        profile_id: int,
        symbol: str,
        direction: str,
        bar_strength: float,
        close_price: Decimal,
        candle_size: Decimal,
        volume: int,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record a breakout signal detection."""
        if not self.enabled:
            return
        
        metric = BreakoutMetric(
            profile_id=profile_id,
            symbol=symbol,
            direction=direction,
            bar_strength=bar_strength,
            close_price=close_price,
            candle_size=candle_size,
            volume=volume,
            timestamp=timestamp or datetime.utcnow(),
            metadata=metadata or {}
        )
        
        # Store in Redis for real-time access (synchronous, fast)
        # Full history will be aggregated later
        try:
            key = f"telemetry:breakout:{profile_id}:signals"
            data = metric.model_dump_json()
            self.storage.redis_client.lpush(key, data)
            self.storage.redis_client.ltrim(key, 0, 999)  # Keep last 1000 signals
            self.storage.redis_client.expire(key, self.storage.redis_ttl)
        except Exception as e:
            print(f"Error recording breakout signal: {e}")
    
    def record_breakout_outcome(
        self,
        profile_id: int,
        symbol: str,
        entry_price: Decimal,
        exit_price: Decimal,
        quantity: int,
        entry_time: datetime,
        exit_time: datetime,
        bar_strength: Optional[float] = None,
        direction: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record the outcome of a breakout trade."""
        if not self.enabled:
            return
        
        # Calculate PnL and slippage
        pnl = (exit_price - entry_price) * quantity
        slippage = abs(exit_price - entry_price) / entry_price if entry_price else Decimal(0)
        
        trade = TradePerformance(
            profile_id=profile_id,
            symbol=symbol,
            trade_type="breakout",
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            pnl=pnl,
            pnl_pct=float((pnl / (entry_price * quantity)) * 100) if entry_price and quantity else 0.0,
            slippage=slippage,
            entry_time=entry_time,
            exit_time=exit_time,
            duration_seconds=int((exit_time - entry_time).total_seconds()),
            bar_strength=bar_strength,
            direction=direction,
            exit_reason=metadata.get("exit_reason") if metadata else None,
            metadata=metadata or {}
        )
        
        # Queue for async write
        self.write_queue.put(("trade_performance", trade))
    
    # =========================================================================
    # Equity Metrics
    # =========================================================================
    
    def record_equity_update(
        self,
        profile_id: int,
        account_equity: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal,
        margin_used: Decimal,
        margin_available: Decimal,
        open_positions_count: int,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record equity and PnL update."""
        if not self.enabled:
            return
        
        total_pnl = unrealized_pnl + realized_pnl
        margin_utilization = (margin_used / account_equity * 100) if account_equity > 0 else 0.0
        
        metric = EquityMetric(
            profile_id=profile_id,
            timestamp=timestamp or datetime.utcnow(),
            account_equity=account_equity,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            total_pnl=total_pnl,
            margin_used=margin_used,
            margin_available=margin_available,
            margin_utilization_pct=margin_utilization,
            open_positions_count=open_positions_count,
            metadata=metadata or {}
        )
        
        # Store in Redis for real-time access
        self.storage.store_realtime_equity(metric)
        
        # Also queue for MySQL aggregation
        self.write_queue.put(("equity_realtime", metric))
    
    def calculate_drawdown(
        self,
        profile_id: int,
        window_start: datetime,
        window_end: datetime,
        equity_history: list
    ) -> Optional[DrawdownMetric]:
        """Calculate maximum drawdown for a time window."""
        if not equity_history or len(equity_history) < 2:
            return None
        
        equity_values = [float(eq['equity']) for eq in equity_history]
        peak = max(equity_values)
        peak_idx = equity_values.index(peak)
        
        # Find minimum after peak
        if peak_idx < len(equity_values) - 1:
            trough = min(equity_values[peak_idx:])
        else:
            trough = peak
        
        max_dd = peak - trough
        max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0
        
        current_equity = equity_values[-1]
        recovery_status = "new_peak" if current_equity >= peak else ("recovered" if current_equity >= peak * 0.95 else "drawdown")
        
        return DrawdownMetric(
            profile_id=profile_id,
            window_start=window_start,
            window_end=window_end,
            peak_equity=Decimal(str(peak)),
            trough_equity=Decimal(str(trough)),
            max_drawdown=Decimal(str(max_dd)),
            max_drawdown_pct=max_dd_pct,
            current_equity=Decimal(str(current_equity)),
            recovery_status=recovery_status
        )
    
    # =========================================================================
    # Order Metrics
    # =========================================================================
    
    def record_order_metric(
        self,
        profile_id: int,
        symbol: str,
        order_type: str,
        side: str,
        status: str,
        quantity: int,
        timestamp: Optional[datetime] = None,
        order_id: Optional[str] = None,
        limit_price: Optional[Decimal] = None,
        filled_price: Optional[Decimal] = None,
        filled_quantity: Optional[int] = None,
        slippage: Optional[Decimal] = None,
        api_latency_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record order execution metric."""
        if not self.enabled:
            return
        
        metric = OrderMetric(
            profile_id=profile_id,
            order_id=order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            status=status,
            quantity=quantity,
            limit_price=limit_price,
            filled_price=filled_price,
            filled_quantity=filled_quantity,
            slippage=slippage,
            api_latency_ms=api_latency_ms,
            timestamp=timestamp or datetime.utcnow(),
            metadata=metadata or {}
        )
        
        # Store order in Redis for real-time tracking
        try:
            if order_id:
                key = f"telemetry:orders:{profile_id}:{order_id}"
                data = metric.model_dump_json()
                self.storage.redis_client.setex(key, self.storage.redis_ttl, data)
        except Exception as e:
            print(f"Error recording order metric: {e}")
        
        # Record API latency if provided
        if api_latency_ms is not None:
            script_name = metadata.get("script_name", "unknown") if metadata else "unknown"
            self.storage.store_api_latency(profile_id, script_name, api_latency_ms)
    
    # =========================================================================
    # System Health Metrics
    # =========================================================================
    
    def record_api_latency(
        self,
        profile_id: int,
        script_name: str,
        latency_ms: float
    ) -> None:
        """Record API call latency."""
        if not self.enabled:
            return
        
        self.storage.store_api_latency(profile_id, script_name, latency_ms)
    
    def record_system_health(
        self,
        profile_id: int,
        script_name: str,
        status: str,
        api_latency_avg_ms: Optional[float] = None,
        api_rate_limit_remaining: Optional[int] = None,
        api_rate_limit_limit: Optional[int] = None,
        error_count: int = 0,
        warning_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record system health metrics."""
        if not self.enabled:
            return
        
        # Get latency stats if available
        latency_stats = self.storage.get_api_latency_stats(profile_id, script_name)
        
        metric = SystemHealthMetric(
            profile_id=profile_id,
            script_name=script_name,
            timestamp=datetime.utcnow(),
            status=status,
            api_latency_avg_ms=api_latency_avg_ms or latency_stats.get("avg"),
            api_latency_p95_ms=latency_stats.get("p95"),
            api_latency_p99_ms=latency_stats.get("p99"),
            api_rate_limit_remaining=api_rate_limit_remaining,
            api_rate_limit_limit=api_rate_limit_limit,
            api_error_count=error_count,
            error_count=error_count,
            warning_count=warning_count,
            metadata=metadata or {}
        )
        
        # Store in Redis for real-time access
        self.storage.store_system_health(metric)
        
        # Also queue for MySQL storage
        self.write_queue.put(("system_health", metric))
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def get_realtime_equity(self, profile_id: int) -> Optional[EquityMetric]:
        """Get latest equity metric."""
        return self.storage.get_realtime_equity(profile_id)
    
    def get_system_health(self, profile_id: int, script_name: str) -> Optional[SystemHealthMetric]:
        """Get latest system health."""
        return self.storage.get_system_health(profile_id, script_name)
    
    def get_api_latency_stats(self, profile_id: int, script_name: str) -> Dict[str, float]:
        """Get API latency statistics."""
        return self.storage.get_api_latency_stats(profile_id, script_name)
    
    def get_metrics(
        self,
        profile_id: int,
        metric_type: Optional[str] = None,
        metric_name: Optional[str] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None
    ) -> list:
        """Query aggregated metrics."""
        return self.storage.get_metrics(
            profile_id, metric_type, metric_name, window_start, window_end
        )
    
    def get_trade_performance(
        self,
        profile_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        symbol: Optional[str] = None
    ) -> list:
        """Query trade performance records."""
        return self.storage.get_trade_performance(
            profile_id, start_time, end_time, symbol
        )

