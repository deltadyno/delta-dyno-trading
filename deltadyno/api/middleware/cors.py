"""CORS middleware configuration."""

from typing import List, Optional


def get_cors_config(allowed_origins: Optional[List[str]] = None) -> dict:
    """
    Get CORS configuration for FastAPI.
    
    Args:
        allowed_origins: List of allowed origins. If None, allows all origins.
                        Default includes common frontend URLs.
    
    Returns:
        Dictionary with CORS configuration
    """
    if allowed_origins is None:
        # Default: Allow common frontend origins
        allowed_origins = [
            "http://localhost:3000",
            "http://localhost:3001",
            "https://deltadyno.github.io",
            "https://*.vercel.app",
            "https://*.netlify.app",
        ]
    
    return {
        "allow_origins": allowed_origins,
        "allow_credentials": True,
        "allow_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["*"],
    }

