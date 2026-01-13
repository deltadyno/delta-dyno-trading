"""
API module for DeltaDyno telemetry system.

Provides REST endpoints for accessing telemetry data from external frontend.
"""

from deltadyno.api.server import create_app, get_telemetry_manager

__all__ = ['create_app', 'get_telemetry_manager']

