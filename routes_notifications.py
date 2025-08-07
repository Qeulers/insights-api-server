import os
import httpx
from fastapi import APIRouter, HTTPException, Request, Body, Depends, Query, BackgroundTasks
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

# Global MongoDB client - reuse connections with safety measures
_mongo_client = None
_client_lock = None

def get_mongo_client():
    global _mongo_client, _client_lock
    
    # Initialize lock on first call
    if _client_lock is None:
        import threading
        _client_lock = threading.Lock()
    
    with _client_lock:
        if _mongo_client is None:
            try:
                load_dotenv()
                mongo_url = os.getenv("MONGO_URL")
                if not mongo_url:
                    raise HTTPException(status_code=500, detail="MongoDB URL is missing.")
                
                # Use safer connection settings for Python 3.13
                _mongo_client = MongoClient(
                    mongo_url, 
                    maxPoolSize=20,  # Reduced pool size for stability
                    minPoolSize=2,
                    maxIdleTimeMS=30000,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=10000,
                    socketTimeoutMS=20000,
                    retryWrites=False  # Disable for stability
                )
                # Test connection
                _mongo_client.admin.command('ping')
            except Exception as e:
                _mongo_client = None
                raise HTTPException(status_code=500, detail=f"MongoDB connection failed: {str(e)}")
    
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

import logging

async def screen_vessel_and_update_notification(notification_data: Dict[str, Any], inserted_id, collection):
    import asyncio
    load_dotenv()
    logger = logging.getLogger("screen_vessel_and_update_notification")
    logger.info(f"Starting screening for inserted_id={inserted_id}")
    PTE_BASE_URL = os.getenv("PTE_BASE_URL")
    PTE_API_KEY = os.getenv("PTE_API_KEY")
    PTE_API_USERNAME = os.getenv("PTE_API_USERNAME")
    if not (PTE_BASE_URL and PTE_API_KEY and PTE_API_USERNAME):
        logger.error("Missing PTE API config. Skipping screening.")
        return
    try:
        imo_number = str(notification_data["notification"]["vessel_information"]["imo"])
        logger.info(f"Extracted IMO number: {imo_number}")
    except Exception as e:
        logger.error(f"Invalid payload, could not extract IMO: {e}")
        return
    registration_url = f"{PTE_BASE_URL}/registration?api_key={PTE_API_KEY}&username={PTE_API_USERNAME}"
    registration_payload = {"registered_name": imo_number}
    transaction_id = None

    # Keep the client open for registration and polling
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            logger.info(f"Registering vessel with payload: {registration_payload}")
            reg_resp = await client.post(registration_url, json=registration_payload)
            reg_resp.raise_for_status()
            reg_data = reg_resp.json()
            transaction_id = reg_data.get("transaction_id")
            logger.info(f"Registration successful, transaction_id: {transaction_id}")
        except Exception as e:
            logger.error(f"Registration failed: {e}")
            return
        if not transaction_id:
            logger.error("No transaction_id returned from registration. Skipping.")
            return
        poll_url = f"{PTE_BASE_URL}/transaction?id={transaction_id}&api_key={PTE_API_KEY}&username={PTE_API_USERNAME}"
        screening_status = "PENDING"
        poll_resp_obj = None
        for poll_count in range(40):  # ~2min max
            try:
                poll_resp = await client.get(poll_url)
                poll_resp.raise_for_status()
                poll_data = poll_resp.json()
                objects = poll_data.get("objects", [])
                logger.info(f"Polling attempt {poll_count+1}: objects={bool(objects)}, screening_status={screening_status}")
                if objects:
                    poll_resp_obj = objects[0]
                    screening_status = poll_resp_obj.get("screening_status", "PENDING")
                    logger.info(f"Screening status: {screening_status}")
                    if screening_status != "PENDING":
                        break
            except Exception as e:
                logger.warning(f"Polling error (attempt {poll_count+1}): {e}")
                await asyncio.sleep(3)
                continue
            await asyncio.sleep(3)
        if not poll_resp_obj or screening_status == "PENDING":
            logger.error("Polling timed out or failed to complete screening.")
            return
        try:
            screen_results = poll_resp_obj.get("screen_results", [])
            logger.info(f"Extracted screen_results: {screen_results}")
            def get_status(check):
                for sr in screen_results:
                    if sr.get("check") == check:
                        return sr.get("status")
                return None
            screening_results = {
                "transaction_id": transaction_id,
                "overall_severity": screening_status,
                "company_sanctions": get_status("COMPANY_SANCTIONS"),
                "ship_sanctions": get_status("SANCTIONS"),
                "ship_movement": get_status("SHIP_MOVE_HIST"),
                "psc": get_status("PSC_HISTORY"),
            }
            logger.info(f"Updating notification document {inserted_id} with screening_results: {screening_results}")
            update_result = collection.update_one({"_id": inserted_id}, {"$set": {"screening_results": screening_results}})
            logger.info(f"MongoDB update result: matched_count={update_result.matched_count}, modified_count={update_result.modified_count}")
        except Exception as e:
            logger.error(f"Exception during screening result extraction or MongoDB update: {e}", exc_info=True)
            return

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
async def handle_zone_port_webhook(notification_data: Dict[str, Any] = Body(...), background_tasks: BackgroundTasks = None):
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
        # Launch screening in background if auto_screen is true
        if notification_data.get("auto_screen"):
            if background_tasks is not None:
                background_tasks.add_task(screen_vessel_and_update_notification, notification_data, result.inserted_id, collection)
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


@router.get("/zone-port-notifications")
async def get_zone_port_notifications(
    user_id: str = Depends(check_user_logged_in),
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of notifications to return")
):
    """
    Retrieve all zone/port notifications for the user with the given user_id.
    """
    try:
        collection = get_zone_port_notifications_collection()
        
        # Query for documents where user_id matches the provided user_id
        cursor = collection.find({
            "user_id": user_id
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


@router.get("/vessel-notifications")
async def get_vessel_notifications(
    user_id: str = Depends(check_user_logged_in),
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of notifications to return")
):
    """
    Retrieve all vessel notifications for the user with the given user_id.
    """
    try:
        collection = get_vessel_notifications_collection()
        
        # Query for documents where user_id matches the provided user_id
        cursor = collection.find({
            "user_id": user_id
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
