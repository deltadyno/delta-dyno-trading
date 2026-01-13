"""Metrics API endpoints."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from deltadyno.telemetry.manager import TelemetryManager


router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


# Response models
class BreakoutMetricsResponse(BaseModel):
    profile_id: int
    success_rate: Optional[float] = None
    avg_slippage: Optional[float] = None
    avg_profit_per_trade: Optional[float] = None
    total_signals: int = 0
    profitable_trades: int = 0
    losing_trades: int = 0


class EquityMetricsResponse(BaseModel):
    profile_id: int
    current_equity: Optional[float] = None
    total_pnl: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    realized_pnl: Optional[float] = None
    margin_utilization_pct: Optional[float] = None
    max_drawdown: Optional[float] = None
    max_drawdown_pct: Optional[float] = None


class OrderMetricsResponse(BaseModel):
    profile_id: int
    total_orders: int = 0
    filled_orders: int = 0
    canceled_orders: int = 0
    avg_api_latency_ms: Optional[float] = None
    conversion_rate: Optional[float] = None


class SystemHealthResponse(BaseModel):
    profile_id: int
    script_name: str
    status: str
    api_latency_avg_ms: Optional[float] = None
    api_latency_p95_ms: Optional[float] = None
    api_latency_p99_ms: Optional[float] = None
    rate_limit_remaining: Optional[int] = None
    error_count: int = 0


def register_routes(app, telemetry_manager: TelemetryManager):
    """Register metrics routes."""
    
    @router.get("/breakout/{profile_id}", response_model=BreakoutMetricsResponse)
    def get_breakout_metrics(
        profile_id: int,
        days: int = Query(7, ge=1, le=365, description="Number of days to analyze")
    ):
        """Get breakout detection metrics."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        # Get trade performance for breakout trades
        trades = telemetry_manager.get_trade_performance(
            profile_id=profile_id,
            start_time=start_time,
            end_time=end_time
        )
        
        breakout_trades = [t for t in trades if t.get('trade_type') == 'breakout']
        
        if not breakout_trades:
            return BreakoutMetricsResponse(profile_id=profile_id)
        
        profitable = [t for t in breakout_trades if t.get('pnl', 0) > 0]
        losing = [t for t in breakout_trades if t.get('pnl', 0) <= 0]
        
        success_rate = len(profitable) / len(breakout_trades) * 100 if breakout_trades else 0
        avg_slippage = sum(t.get('slippage', 0) for t in breakout_trades) / len(breakout_trades) if breakout_trades else 0
        avg_profit = sum(t.get('pnl', 0) for t in profitable) / len(profitable) if profitable else 0
        
        return BreakoutMetricsResponse(
            profile_id=profile_id,
            success_rate=success_rate,
            avg_slippage=float(avg_slippage) if avg_slippage else None,
            avg_profit_per_trade=float(avg_profit) if avg_profit else None,
            total_signals=len(breakout_trades),
            profitable_trades=len(profitable),
            losing_trades=len(losing)
        )
    
    @router.get("/equity/{profile_id}", response_model=EquityMetricsResponse)
    def get_equity_metrics(profile_id: int):
        """Get equity and risk metrics."""
        # Get real-time equity
        equity = telemetry_manager.get_realtime_equity(profile_id)
        
        if equity:
            return EquityMetricsResponse(
                profile_id=profile_id,
                current_equity=float(equity.account_equity),
                total_pnl=float(equity.total_pnl),
                unrealized_pnl=float(equity.unrealized_pnl),
                realized_pnl=float(equity.realized_pnl),
                margin_utilization_pct=equity.margin_utilization_pct
            )
        
        return EquityMetricsResponse(profile_id=profile_id)
    
    @router.get("/orders/{profile_id}", response_model=OrderMetricsResponse)
    def get_order_metrics(
        profile_id: int,
        script_name: Optional[str] = Query(None, description="Filter by script name")
    ):
        """Get order execution metrics."""
        # Get API latency stats
        if script_name:
            latency_stats = telemetry_manager.get_api_latency_stats(profile_id, script_name)
        else:
            # Aggregate across all scripts
            scripts = ['breakout_detector', 'order_monitor', 'equity_monitor']
            all_latencies = []
            for script in scripts:
                stats = telemetry_manager.get_api_latency_stats(profile_id, script)
                if stats.get('avg'):
                    all_latencies.append(stats['avg'])
            latency_stats = {'avg': sum(all_latencies) / len(all_latencies)} if all_latencies else {}
        
        return OrderMetricsResponse(
            profile_id=profile_id,
            avg_api_latency_ms=latency_stats.get('avg')
        )
    
    @router.get("/system/{profile_id}", response_model=list[SystemHealthResponse])
    def get_system_health(
        profile_id: int,
        script_name: Optional[str] = Query(None, description="Filter by script name")
    ):
        """Get system health metrics."""
        scripts = [script_name] if script_name else ['breakout_detector', 'order_monitor', 'equity_monitor']
        
        results = []
        for script in scripts:
            health = telemetry_manager.get_system_health(profile_id, script)
            if health:
                latency_stats = telemetry_manager.get_api_latency_stats(profile_id, script)
                results.append(SystemHealthResponse(
                    profile_id=profile_id,
                    script_name=script,
                    status=health.status,
                    api_latency_avg_ms=latency_stats.get('avg'),
                    api_latency_p95_ms=latency_stats.get('p95'),
                    api_latency_p99_ms=latency_stats.get('p99'),
                    rate_limit_remaining=health.api_rate_limit_remaining,
                    error_count=health.error_count
                ))
        
        return results

