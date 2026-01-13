"""
Integration helpers for adding telemetry to existing scripts.

This module provides helper functions and decorators to easily add
telemetry collection to existing code.
"""

import functools
import time
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

from deltadyno.telemetry.manager import TelemetryManager


# Global telemetry manager instance (will be initialized by scripts)
_global_telemetry_manager: Optional[TelemetryManager] = None


def set_telemetry_manager(manager: TelemetryManager) -> None:
    """Set the global telemetry manager instance."""
    global _global_telemetry_manager
    _global_telemetry_manager = manager


def get_telemetry_manager() -> Optional[TelemetryManager]:
    """Get the global telemetry manager instance."""
    return _global_telemetry_manager


@contextmanager
def measure_api_latency(
    profile_id: int,
    script_name: str,
    operation_name: str = "api_call"
):
    """
    Context manager to measure API call latency.
    
    Usage:
        with measure_api_latency(profile_id, "order_monitor", "place_order"):
            # API call code here
            trading_client.submit_order(...)
    """
    start_time = time.time()
    error = None
    
    try:
        yield
    except Exception as e:
        error = e
        raise
    finally:
        latency_ms = (time.time() - start_time) * 1000
        
        manager = get_telemetry_manager()
        if manager:
            manager.record_api_latency(profile_id, script_name, latency_ms)


def record_order_metric_decorator(
    profile_id: int,
    script_name: str,
    symbol: Optional[str] = None
):
    """
    Decorator to automatically record order metrics for a function.
    
    Usage:
        @record_order_metric_decorator(profile_id, "order_monitor")
        def place_order(...):
            # Order placement code
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = None
            error = None
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error = e
                raise
            finally:
                latency_ms = (time.time() - start_time) * 1000
                
                manager = get_telemetry_manager()
                if manager and result:
                    # Try to extract order information from result
                    # This is a generic implementation - adapt based on your order objects
                    try:
                        order_id = getattr(result, 'id', None) or getattr(result, 'order_id', None)
                        order_type = getattr(result, 'order_type', 'unknown')
                        side = getattr(result, 'side', 'unknown')
                        quantity = getattr(result, 'qty', None) or getattr(result, 'quantity', None)
                        filled_price = getattr(result, 'filled_avg_price', None)
                        
                        if order_id:
                            manager.record_order_metric(
                                profile_id=profile_id,
                                symbol=symbol or "UNKNOWN",
                                order_type=order_type,
                                side=side,
                                status="pending",
                                quantity=quantity or 0,
                                order_id=str(order_id),
                                filled_price=Decimal(str(filled_price)) if filled_price else None,
                                api_latency_ms=latency_ms,
                                metadata={"script_name": script_name}
                            )
                    except Exception as e:
                        print(f"Error recording order metric: {e}")
        
        return wrapper
    return decorator

