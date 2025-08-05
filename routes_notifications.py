import os
import httpx
from fastapi import APIRouter, HTTPException, Request, Body, Depends, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

router = APIRouter(prefix="/notifications", tags=["Notifications"])

async def check_user_logged_in(user_id: str = Query(..., description="User ID for authentication"), request: Request = None):
    """Dependency to check if user is logged in."""
    if not request:
        raise HTTPException(status_code=500, detail="Request object not available")
        
    base_url = str(request.base_url)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{base_url}users/{user_id}/is-logged-in")
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="User not found.")
            if resp.status_code != 200 or not resp.json().get("is_logged_in", False):
                raise HTTPException(status_code=401, detail="User is not logged in.")
            return user_id
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to check user login: {str(e)}")

# Global MongoDB client - reuse connections
_mongo_client = None

def get_mongo_client():
    global _mongo_client
    if _mongo_client is None:
        load_dotenv()
        mongo_url = os.getenv("MONGO_URL")
        if not mongo_url:
            raise HTTPException(status_code=500, detail="MongoDB URL is missing.")
        _mongo_client = MongoClient(mongo_url, maxPoolSize=50, minPoolSize=5)
    return _mongo_client

def get_zone_port_notifications_collection():
    load_dotenv()
    db_name = os.getenv("MONGO_DB_NAME_NOTIFICATIONS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_ZONE_PORT_NOTIFICATIONS")
    
    if not all([db_name, collection_name]):
        raise HTTPException(
            status_code=500,
            detail="MongoDB configuration for zone port notifications is missing."
        )
    
    client = get_mongo_client()
    db = client[db_name]
    return db[collection_name]

def get_vessel_notifications_collection():
    load_dotenv()
    db_name = os.getenv("MONGO_DB_NAME_NOTIFICATIONS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_VESSEL_NOTIFICATIONS")
    
    if not all([db_name, collection_name]):
        raise HTTPException(
            status_code=500,
            detail="MongoDB configuration for vessel notifications is missing."
        )
    
    client = get_mongo_client()
    db = client[db_name]
    return db[collection_name]

@router.post("/webhook/zone-port-event")
async def handle_zone_port_webhook(notification_data: Dict[str, Any] = Body(...)):
    try:
        collection = get_zone_port_notifications_collection()
        notification_data["received_at"] = datetime.utcnow()
        # Extract user_id and auto_screen from custom_reference
        custom_ref = notification_data.get("custom_reference")
        if custom_ref and "|" in custom_ref:
            try:
                user_id_part, auto_screen_part = [x.strip() for x in custom_ref.split("|", 1)]
                notification_data["user_id"] = user_id_part
                notification_data["auto_screen"] = auto_screen_part.upper() == "TRUE"
            except Exception:
                notification_data["user_id"] = None
                notification_data["auto_screen"] = None
        else:
            notification_data["user_id"] = None
            notification_data["auto_screen"] = None
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
        # Extract user_id and auto_screen from custom_reference
        custom_ref = notification_data.get("custom_reference")
        if custom_ref:
            try:
                notification_data["user_id"] = custom_ref
            except Exception:
                notification_data["user_id"] = None
        else:
            notification_data["user_id"] = None
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


class SubscriptionIDs(BaseModel):
    subscription_ids: List[str]


@router.post("/zone-port-notifications")
async def get_zone_port_notifications(
    subscription_ids: SubscriptionIDs,
    user_id: str = Depends(check_user_logged_in),
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of notifications to return")
):
    """
    Retrieve zone/port notifications for the given list of subscription IDs.
    """
    try:
        collection = get_zone_port_notifications_collection()
        
        # Query for documents where subscription_id is in the provided list
        cursor = collection.find({
            "subscription_id": {"$in": subscription_ids.subscription_ids}
        }).sort("received_at", -1).limit(limit)
        
        # Convert cursor to list and format the response
        notifications = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            doc['_id'] = str(doc['_id'])
            notifications.append(doc)
            
        return {
            "status": "success",
            "count": len(notifications),
            "notifications": notifications
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve zone/port notifications: {str(e)}"
        )


@router.post("/vessel-notifications")
async def get_vessel_notifications(
    subscription_ids: SubscriptionIDs,
    user_id: str = Depends(check_user_logged_in),
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of notifications to return")
):
    """
    Retrieve vessel notifications for the given list of subscription IDs.
    """
    try:
        collection = get_vessel_notifications_collection()
        
        # Query for documents where subscription_id is in the provided list
        cursor = collection.find({
            "subscription_id": {"$in": subscription_ids.subscription_ids}
        }).sort("received_at", -1).limit(limit)
        
        # Convert cursor to list and format the response
        notifications = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            doc['_id'] = str(doc['_id'])
            notifications.append(doc)
            
        return {
            "status": "success",
            "count": len(notifications),
            "notifications": notifications
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve vessel notifications: {str(e)}"
        )
