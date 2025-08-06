# EMERGENCY FIX: Disable PyMongo C extensions to prevent memory corruption
import os
os.environ['PYMONGO_FORCE_PURE_PYTHON'] = '1'
# Additional safety measures for Python 3.13
os.environ['PYTHONMALLOC'] = 'malloc'
os.environ['PYTHONASYNCIODEBUG'] = '0'

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routes_auth import router as auth_router
from routes_vessel_insights import router as vessel_insights_router
from routes_zone_port_insights import router as zone_port_insights_router
from routes_zone_port_notifications import router as zone_port_notifications_router
from routes_vessel_notifications import router as vessel_notifications_router
from routes_voyage_insights import router as voyage_insights_router
from routes_user import router as user_router
from routes_notifications import router as notifications_router
import logging

# Configure logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Global exception handler for memory-related errors
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_msg = str(exc)
    if any(keyword in error_msg.lower() for keyword in ['malloc', 'corruption', 'memory', 'segmentation']):
        logger.error(f"Memory-related error caught: {error_msg}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error - memory issue detected", "error_type": "memory_error"}
        )
    logger.error(f"Unhandled exception: {error_msg}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error_type": "general_error"}
    )

# Allow all origins, methods, and headers for CORS (adjust as needed for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify ["http://localhost:3000"] etc.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(vessel_insights_router)
app.include_router(zone_port_insights_router)
app.include_router(zone_port_notifications_router)
app.include_router(vessel_notifications_router)
app.include_router(voyage_insights_router)
app.include_router(user_router)
app.include_router(notifications_router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Insights API server!"}
