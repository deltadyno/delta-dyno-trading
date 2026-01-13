"""FastAPI server for telemetry API."""

import os
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deltadyno.config.loader import ConfigLoader
from deltadyno.telemetry.manager import TelemetryManager
from deltadyno.telemetry.storage import TelemetryStorage

from deltadyno.api.middleware.cors import get_cors_config
from deltadyno.api.routes import metrics, trades


# Global telemetry manager instance
_telemetry_manager: Optional[TelemetryManager] = None


def get_telemetry_manager() -> TelemetryManager:
    """Get or create telemetry manager singleton."""
    global _telemetry_manager
    
    if _telemetry_manager is None:
        # Load configuration
        config_path = 'config/config.ini'
        if not os.path.exists(config_path):
            config_path = '/home/ec2-user/deltadynocode/config.ini'
        
        config = ConfigLoader(config_file=config_path)
        
        # Create storage
        storage = TelemetryStorage(
            db_host=config.db_host,
            db_user=config.db_user,
            db_password=config.db_password,
            db_name=config.db_name,
            redis_host=config.redis_host,
            redis_port=config.redis_port,
            redis_password=config.redis_password
        )
        
        # Create manager
        _telemetry_manager = TelemetryManager(storage=storage, enabled=True)
    
    return _telemetry_manager


def create_app(
    telemetry_manager: Optional[TelemetryManager] = None,
    cors_origins: Optional[list] = None
) -> FastAPI:
    """
    Create FastAPI application.
    
    Args:
        telemetry_manager: Optional TelemetryManager instance. If None, creates one.
        cors_origins: List of allowed CORS origins
    
    Returns:
        FastAPI application instance
    """
    # Get or create telemetry manager
    if telemetry_manager is None:
        telemetry_manager = get_telemetry_manager()
    
    # Create FastAPI app
    app = FastAPI(
        title="DeltaDyno Telemetry API",
        description="API for accessing trading system telemetry and metrics",
        version="1.0.0"
    )
    
    # Add CORS middleware
    cors_config = get_cors_config(cors_origins)
    app.add_middleware(
        CORSMiddleware,
        **cors_config
    )
    
    # Register routes
    metrics.register_routes(app, telemetry_manager)
    trades.register_routes(app, telemetry_manager)
    
    # Health check endpoint
    @app.get("/health")
    def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "service": "deltadyno-telemetry"}
    
    @app.get("/")
    def root():
        """Root endpoint."""
        return {
            "service": "DeltaDyno Telemetry API",
            "version": "1.0.0",
            "docs": "/docs"
        }
    
    return app


# For running with uvicorn
if __name__ == "__main__":
    import uvicorn
    
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000)

