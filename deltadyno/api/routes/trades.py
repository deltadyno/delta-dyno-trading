"""Trade performance API endpoints."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from deltadyno.telemetry.manager import TelemetryManager


router = APIRouter(prefix="/api/v1/trades", tags=["trades"])


def register_routes(app, telemetry_manager: TelemetryManager):
    """Register trade routes."""
    
    @router.get("/{profile_id}")
    def get_trades(
        profile_id: int,
        days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
        symbol: Optional[str] = Query(None, description="Filter by symbol"),
        limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
    ):
        """Get trade performance records."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        trades = telemetry_manager.get_trade_performance(
            profile_id=profile_id,
            start_time=start_time,
            end_time=end_time,
            symbol=symbol
        )
        
        # Convert Decimal to float for JSON serialization
        for trade in trades:
            for key, value in trade.items():
                if hasattr(value, '__float__'):  # Decimal type
                    trade[key] = float(value)
        
        return trades[:limit]
    
    @router.get("/{profile_id}/performance")
    def get_trade_performance_summary(
        profile_id: int,
        days: int = Query(30, ge=1, le=365)
    ):
        """Get aggregated trade performance summary."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        trades = telemetry_manager.get_trade_performance(
            profile_id=profile_id,
            start_time=start_time,
            end_time=end_time
        )
        
        if not trades:
            return {
                "profile_id": profile_id,
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0
            }
        
        total_pnl = sum(float(t.get('pnl', 0)) for t in trades)
        winning = [t for t in trades if float(t.get('pnl', 0)) > 0]
        losing = [t for t in trades if float(t.get('pnl', 0)) <= 0]
        
        return {
            "profile_id": profile_id,
            "total_trades": len(trades),
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "total_pnl": total_pnl,
            "avg_pnl": total_pnl / len(trades) if trades else 0.0,
            "win_rate": len(winning) / len(trades) * 100 if trades else 0.0,
            "avg_win": sum(float(t.get('pnl', 0)) for t in winning) / len(winning) if winning else 0.0,
            "avg_loss": sum(float(t.get('pnl', 0)) for t in losing) / len(losing) if losing else 0.0
        }

