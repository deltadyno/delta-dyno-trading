"""
API Server Entry Point

Run the telemetry API server using:
    python api_server.py

Or with uvicorn:
    uvicorn api_server:app --host 0.0.0.0 --port 8000
"""

from deltadyno.api.server import create_app

# Create the FastAPI application
app = create_app()

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

