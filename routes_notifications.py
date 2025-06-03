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


class SubscriptionIDs(BaseModel):
    subscription_ids: List[str]


@router.post("/zone-port-notifications/by-subscriptions")
async def get_zone_port_notifications_by_subscriptions(
    subscription_ids: SubscriptionIDs,
    user_id: str = Depends(check_user_logged_in)
):
    """
    Retrieve all zone/port notifications for the given list of subscription IDs.
    """
    try:
        collection = get_zone_port_notifications_collection()
        
        # Query for documents where subscription_id is in the provided list
        cursor = collection.find({
            "subscription_id": {"$in": subscription_ids.subscription_ids}
        })
        
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


@router.post("/vessel-notifications/by-subscriptions")
async def get_vessel_notifications_by_subscriptions(
    subscription_ids: SubscriptionIDs,
    user_id: str = Depends(check_user_logged_in)
):
    """
    Retrieve all vessel notifications for the given list of subscription IDs.
    """
    try:
        collection = get_vessel_notifications_collection()
        
        # Query for documents where subscription_id is in the provided list
        cursor = collection.find({
            "subscription_id": {"$in": subscription_ids.subscription_ids}
        })
        
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
