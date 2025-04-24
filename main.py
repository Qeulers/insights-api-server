from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes_auth import router as auth_router
from routes_vessel_insights import router as vessel_insights_router
from routes_zone_port_insights import router as zone_port_insights_router
from routes_zone_port_notifications import router as zone_port_notifications_router
from routes_vessel_notifications import router as vessel_notifications_router
from routes_voyage_insights import router as voyage_insights_router

app = FastAPI()

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

@app.get("/")
def read_root():
    return {"message": "Welcome to the Insights API server!"}
