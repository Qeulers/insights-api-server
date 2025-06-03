import os
from fastapi import APIRouter, HTTPException, Request, Body
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, Optional

router = APIRouter(prefix="/notifications", tags=["Notifications"])

def get_zone_port_notifications_collection():
    load_dotenv()
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_NOTIFICATIONS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_ZONE_PORT_NOTIFICATIONS")
    
    if not all([mongo_url, db_name, collection_name]):
        raise HTTPException(
            status_code=500,
            detail="MongoDB configuration for zone port notifications is missing."
        )
    
    client = MongoClient(mongo_url)
    db = client[db_name]
    return db[collection_name]

def get_vessel_notifications_collection():
    load_dotenv()
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_NOTIFICATIONS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_VESSEL_NOTIFICATIONS")
    
    if not all([mongo_url, db_name, collection_name]):
        raise HTTPException(
            status_code=500,
            detail="MongoDB configuration for vessel notifications is missing."
        )
    
    client = MongoClient(mongo_url)
    db = client[db_name]
    return db[collection_name]

@router.post("/webhook/zone-port-event")
async def handle_zone_port_webhook(notification_data: Dict[str, Any] = Body(...)):
    try:
        collection = get_zone_port_notifications_collection()
        notification_data["received_at"] = datetime.utcnow()
        result = collection.insert_one(notification_data)
        return {
            "status": "success",
            "message": "Notification stored successfully",
            "notification_id": str(result.inserted_id)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process notification: {str(e)}"
        )

@router.post("/webhook/vessel-event")
async def handle_vessel_webhook(notification_data: Dict[str, Any] = Body(...)):
    try:
        collection = get_vessel_notifications_collection()
        notification_data["received_at"] = datetime.utcnow()
        result = collection.insert_one(notification_data)
        return {
            "status": "success",
            "message": "Vessel notification stored successfully",
            "notification_id": str(result.inserted_id)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process vessel notification: {str(e)}"
        )
