from fastapi import FastAPI
from routes_auth import router as auth_router
from routes_vessel_insights import router as vessel_insights_router
from routes_zone_port_insights import router as zone_port_insights_router

app = FastAPI()

app.include_router(auth_router)
app.include_router(vessel_insights_router)
app.include_router(zone_port_insights_router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Insights API server!"}
