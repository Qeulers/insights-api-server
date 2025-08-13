import os
import httpx
import logging
import asyncio
import json
import weakref
import html
from fastapi import APIRouter, HTTPException, Request, Body, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from pydantic import BaseModel
from contextlib import asynccontextmanager

router = APIRouter(prefix="/notifications", tags=["Notifications"])

logger = logging.getLogger("notifications")
sse_logger = logging.getLogger("sse")
logging.basicConfig(level=logging.INFO)

# SSE Connection Management
class SSEConnectionManager:
    def __init__(self):
        self.active_connections: Set[asyncio.Queue] = set()
        self.connection_lock = asyncio.Lock()
        self.max_connections = 100  # Limit concurrent connections
        self.heartbeat_interval = 30  # seconds
        self.connection_timeout = 300  # 5 minutes
        
    async def connect(self, websocket_queue: asyncio.Queue) -> bool:
        """Add a new SSE connection"""
        async with self.connection_lock:
            if len(self.active_connections) >= self.max_connections:
                sse_logger.warning(f"Connection limit reached ({self.max_connections}). Rejecting new connection.")
                return False
            
            self.active_connections.add(websocket_queue)
            sse_logger.info(f"New SSE connection established. Active connections: {len(self.active_connections)}")
            return True
    
    async def disconnect(self, websocket_queue: asyncio.Queue):
        """Remove an SSE connection"""
        async with self.connection_lock:
            self.active_connections.discard(websocket_queue)
            sse_logger.info(f"SSE connection closed. Active connections: {len(self.active_connections)}")
    
    async def broadcast_notification(self, notification_data: Dict[str, Any]):
        """Broadcast notification to all connected clients"""
        if not self.active_connections:
            sse_logger.debug("No active SSE connections to broadcast to")
            return
        
        # Sanitize notification data to prevent XSS
        sanitized_data = self._sanitize_notification(notification_data)
        message = self._format_sse_message(sanitized_data)
        
        sse_logger.info(f"Broadcasting notification to {len(self.active_connections)} connections")
        
        # Create a copy of connections to avoid modification during iteration
        connections_copy = list(self.active_connections)
        
        for connection_queue in connections_copy:
            try:
                # Use put_nowait to avoid blocking if queue is full
                connection_queue.put_nowait(message)
            except asyncio.QueueFull:
                sse_logger.warning("Connection queue full, removing stale connection")
                await self.disconnect(connection_queue)
            except Exception as e:
                sse_logger.error(f"Error broadcasting to connection: {e}")
                await self.disconnect(connection_queue)
    
    def _sanitize_notification(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize notification data to prevent XSS attacks"""
        def sanitize_value(value):
            if isinstance(value, str):
                return html.escape(value)
            elif isinstance(value, dict):
                return {k: sanitize_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [sanitize_value(item) for item in value]
            else:
                return value
        
        return sanitize_value(data)
    
    def _format_sse_message(self, data: Dict[str, Any]) -> str:
        """Format data as SSE message"""
        try:
            json_data = json.dumps(data, default=str)
            return f"data: {json_data}\n\n"
        except Exception as e:
            sse_logger.error(f"Error formatting SSE message: {e}")
            return "data: {\"error\": \"Failed to format notification\"}\n\n"
    
    async def send_heartbeat(self):
        """Send heartbeat to all connections"""
        if not self.active_connections:
            return
        
        heartbeat_message = "data: {\"type\": \"heartbeat\", \"timestamp\": \"" + datetime.utcnow().isoformat() + "\"}\n\n"
        
        connections_copy = list(self.active_connections)
        for connection_queue in connections_copy:
            try:
                connection_queue.put_nowait(heartbeat_message)
            except (asyncio.QueueFull, Exception) as e:
                sse_logger.warning(f"Heartbeat failed for connection: {e}")
                await self.disconnect(connection_queue)

# Global SSE manager instance
sse_manager = SSEConnectionManager()

# Background task for heartbeat
async def heartbeat_task():
    """Background task to send periodic heartbeats"""
    while True:
        try:
            await asyncio.sleep(sse_manager.heartbeat_interval)
            await sse_manager.send_heartbeat()
        except Exception as e:
            sse_logger.error(f"Heartbeat task error: {e}")
            await asyncio.sleep(5)  # Brief pause before retrying

# Start heartbeat task when module loads
asyncio.create_task(heartbeat_task())

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


async def broadcast_notification_to_sse(notification_data: Dict[str, Any], inserted_id):
    """Broadcast notification to SSE clients"""
    try:
        # Prepare notification for broadcasting
        broadcast_data = {
            "type": "zone_port_notification",
            "notification_id": str(inserted_id),
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": notification_data.get("user_id"),
            "data": notification_data
        }
        
        # Broadcast to all connected SSE clients
        await sse_manager.broadcast_notification(broadcast_data)
        sse_logger.info(f"Broadcasted notification {inserted_id} to SSE clients")
        
    except Exception as e:
        sse_logger.error(f"Failed to broadcast notification {inserted_id}: {e}")

async def screen_vessel_and_update_notification_with_broadcast(notification_data: Dict[str, Any], inserted_id, collection):
    """Screen vessel and update notification, then broadcast to SSE clients"""
    # First run the original screening function
    await screen_vessel_and_update_notification(notification_data, inserted_id, collection)
    
    # After screening is complete, fetch the updated notification and broadcast it
    try:
        updated_notification = collection.find_one({"_id": inserted_id})
        if updated_notification:
            # Convert ObjectId to string for JSON serialization
            updated_notification["_id"] = str(updated_notification["_id"])
            await broadcast_notification_to_sse(updated_notification, inserted_id)
        else:
            logger.warning(f"Could not find updated notification {inserted_id} for broadcasting")
    except Exception as e:
        sse_logger.error(f"Failed to broadcast updated notification {inserted_id}: {e}")

async def screen_vessel_and_update_notification(notification_data: Dict[str, Any], inserted_id, collection):
    import asyncio
    load_dotenv()
    logger = logging.getLogger("screen_vessel_and_update_notification")
    logger.info(f"[screening] Starting screening for inserted_id={inserted_id}")
    PTE_BASE_URL = os.getenv("PTE_BASE_URL")
    PTE_API_KEY = os.getenv("PTE_API_KEY")
    PTE_API_USERNAME = os.getenv("PTE_API_USERNAME")
    logger.info(f"[screening] PTE_BASE_URL: {PTE_BASE_URL}, PTE_API_KEY: {'set' if PTE_API_KEY else 'not set'}, PTE_API_USERNAME: {PTE_API_USERNAME}")
    if not (PTE_BASE_URL and PTE_API_KEY and PTE_API_USERNAME):
        logger.error("[screening] Missing PTE API config. Skipping screening.")
        return
    try:
        logger.info(f"[screening] notification_data: {notification_data}")
        imo_number = str(notification_data.get("notification", {}).get("vessel_information", {}).get("imo") or notification_data.get("vessel_information", {}).get("imo"))
        logger.info(f"[screening] Extracted IMO number: {imo_number}")
    except Exception as e:
        logger.error(f"[screening] Invalid payload, could not extract IMO: {e}")
        return
    registration_url = f"{PTE_BASE_URL}/registration?api_key={PTE_API_KEY}&username={PTE_API_USERNAME}"
    registration_payload = {"registered_name": imo_number}
    transaction_id = None
    logger.info(f"[screening] Registration URL: {registration_url}")
    logger.info(f"[screening] Registration payload: {registration_payload}")
    # Keep the client open for registration and polling
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            logger.info(f"[screening] Registering vessel...")
            reg_resp = await client.post(registration_url, json=registration_payload)
            reg_resp.raise_for_status()
            reg_data = reg_resp.json()
            transaction_id = reg_data.get("transaction_id")
            logger.info(f"[screening] Registration successful, transaction_id: {transaction_id}")
        except Exception as e:
            logger.error(f"[screening] Registration failed: {e}")
            return
        if not transaction_id:
            logger.error("[screening] No transaction_id returned from registration. Skipping.")
            return
        poll_url = f"{PTE_BASE_URL}/transaction?id={transaction_id}&api_key={PTE_API_KEY}&username={PTE_API_USERNAME}"
        screening_status = "PENDING"
        poll_resp_obj = None
        logger.info(f"[screening] Polling for screening results at: {poll_url}")
        
        # Graduated polling intervals over 5 minutes:
        # First minute: 3 seconds (20 attempts)
        # Second minute: 5 seconds (12 attempts) 
        # Remaining 3 minutes: 10 seconds (18 attempts)
        # Total: 50 attempts over 5 minutes
        total_attempts = 50
        
        for poll_count in range(total_attempts):
            # Determine polling interval based on attempt number
            if poll_count < 20:  # First minute: 3 second intervals
                poll_interval = 3
                phase = "first minute"
            elif poll_count < 32:  # Second minute: 5 second intervals
                poll_interval = 5
                phase = "second minute"
            else:  # Remaining time: 10 second intervals
                poll_interval = 10
                phase = "remaining duration"
                
            try:
                poll_resp = await client.get(poll_url)
                poll_resp.raise_for_status()
                poll_data = poll_resp.json()
                objects = poll_data.get("objects", [])
                logger.info(f"[screening] Polling attempt {poll_count+1}/{total_attempts} ({phase}): objects={bool(objects)}, screening_status={screening_status}")
                if objects:
                    poll_resp_obj = objects[0]
                    screening_status = poll_resp_obj.get("screening_status", "PENDING")
                    logger.info(f"[screening] Screening status: {screening_status}")
                    if screening_status != "PENDING":
                        break
            except Exception as e:
                logger.warning(f"[screening] Polling error (attempt {poll_count+1}/{total_attempts}): {e}")
                await asyncio.sleep(poll_interval)
                continue
            await asyncio.sleep(poll_interval)
        if not poll_resp_obj or screening_status == "PENDING":
            logger.error("[screening] Polling timed out or failed to complete screening.")
            return
        try:
            screen_results = poll_resp_obj.get("screen_results", [])
            logger.info(f"[screening] Extracted screen_results: {screen_results}")
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
            logger.info(f"[screening] Updating notification document {inserted_id} with screening_results: {screening_results}")
            update_result = collection.update_one({"_id": inserted_id}, {"$set": {"screening_results": screening_results}})
            logger.info(f"[screening] MongoDB update result: matched_count={update_result.matched_count}, modified_count={update_result.modified_count}")
        except Exception as e:
            logger.error(f"[screening] Exception during screening result extraction or MongoDB update: {e}", exc_info=True)
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
    logger.info("Received zone-port webhook event.")
    try:
        collection = get_zone_port_notifications_collection()
        logger.info("Obtained zone port notifications collection.")
        notification_data["received_at"] = datetime.utcnow()
        logger.info(f"Added received_at timestamp: {notification_data['received_at']}")
        # Extract user_id and auto_screen from custom_reference
        custom_ref = notification_data.get("custom_reference")
        logger.info(f"custom_reference: {custom_ref}")
        if custom_ref and "|" in custom_ref:
            try:
                user_id_part, auto_screen_part = [x.strip() for x in custom_ref.split("|", 1)]
                notification_data["user_id"] = user_id_part
                notification_data["auto_screen"] = auto_screen_part.upper() == "TRUE"
                logger.info(f"Parsed user_id: {user_id_part}, auto_screen: {notification_data['auto_screen']}")
            except Exception as e:
                logger.error(f"Failed to parse custom_reference: {e}")
                notification_data["user_id"] = None
                notification_data["auto_screen"] = None
        else:
            notification_data["user_id"] = None
            notification_data["auto_screen"] = None
            logger.info("No valid custom_reference found; set user_id and auto_screen to None.")
        logger.info(f"Notification data to insert: {notification_data}")
        result = collection.insert_one(notification_data)
        logger.info(f"Inserted notification with _id: {result.inserted_id}")
        # Launch screening in background if auto_screen is true
        if notification_data.get("auto_screen"):
            if background_tasks is not None:
                logger.info(f"auto_screen is True, adding background screening task for _id: {result.inserted_id}")
                background_tasks.add_task(screen_vessel_and_update_notification_with_broadcast, notification_data, result.inserted_id, collection)
            else:
                logger.warning("auto_screen is True but background_tasks is None; screening not started.")
        else:
            logger.info("auto_screen is False or not set; skipping background screening.")
            # Broadcast notification immediately if no screening is needed
            await broadcast_notification_to_sse(notification_data, result.inserted_id)
        return {
            "status": "success",
            "message": "Notification stored successfully",
            "notification_id": str(result.inserted_id)
        }
    except Exception as e:
        logger.error(f"Failed to process notification: {e}", exc_info=True)
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
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of notifications to return"),
    created_at_start: Optional[str] = Query(None, description="Filter notifications with created_at >= this UTC ISO timestamp")
):
    """
    Retrieve all zone/port notifications for the user with the given user_id.
    """
    try:
        collection = get_zone_port_notifications_collection()
        
        # Build query filter
        query_filter = {"user_id": user_id}
        
        # Add timestamp filter if provided
        if created_at_start:
            try:
                # Since created_at is stored as a string, we can do string comparison
                # Ensure the input format matches the stored format
                start_datetime = datetime.fromisoformat(created_at_start.replace('Z', '+00:00'))
                # Convert back to the same string format as stored in the database
                start_datetime_str = start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
                query_filter["created_at"] = {"$gte": start_datetime_str}
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid created_at_start format. Expected UTC ISO timestamp (e.g., '2023-01-01T00:00:00Z')"
                )
        
        # Query for documents with the built filter
        cursor = collection.find(query_filter).sort("created_at", -1).limit(limit)
        
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


@router.get("/zone-port-events/stream")
async def zone_port_events_stream(
    request: Request,
    user_id: str = Query(..., description="User ID for authentication")
):
    """
    Server-Sent Events (SSE) endpoint for real-time zone/port notifications.
    
    Clients should connect to this endpoint to receive real-time notifications.
    The connection will send periodic heartbeats to keep the connection alive.
    
    Recommended client implementation:
    - Use EventSource API in JavaScript
    - Implement exponential backoff for reconnection (start with 1s, max 30s)
    - Handle 'heartbeat' events to maintain connection
    - Parse 'notification' events for actual data
    
    Example JavaScript client:
    ```javascript
    const eventSource = new EventSource('/notifications/zone-port-events/stream?user_id=your_user_id');
    
    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        if (data.type === 'heartbeat') {
            console.log('Heartbeat received');
        } else {
            console.log('Notification received:', data);
            // Handle notification data
        }
    };
    
    eventSource.onerror = function(event) {
        console.error('SSE error:', event);
        // Implement exponential backoff reconnection
    };
    ```
    """
    # Validate user authentication
    try:
        await check_user_logged_in(user_id, request)
    except HTTPException as e:
        sse_logger.warning(f"SSE connection rejected for user {user_id}: {e.detail}")
        raise e
    
    # Create a queue for this connection
    connection_queue = asyncio.Queue(maxsize=50)  # Limit queue size to prevent memory issues
    
    # Try to register the connection
    if not await sse_manager.connect(connection_queue):
        raise HTTPException(
            status_code=503,
            detail="Server at capacity. Please try again later."
        )
    
    async def event_generator():
        """Generate SSE events for this connection"""
        try:
            sse_logger.info(f"Starting SSE stream for user {user_id}")
            
            # Send initial connection confirmation
            initial_message = {
                "type": "connection_established",
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "message": "SSE connection established successfully"
            }
            yield f"data: {json.dumps(initial_message)}\n\n"
            
            # Main event loop
            while True:
                try:
                    # Wait for messages with timeout
                    message = await asyncio.wait_for(
                        connection_queue.get(), 
                        timeout=sse_manager.connection_timeout
                    )
                    yield message
                    
                except asyncio.TimeoutError:
                    sse_logger.info(f"SSE connection timeout for user {user_id}")
                    break
                    
                except asyncio.CancelledError:
                    sse_logger.info(f"SSE connection cancelled for user {user_id}")
                    break
                    
        except Exception as e:
            sse_logger.error(f"SSE stream error for user {user_id}: {e}")
        finally:
            # Clean up connection
            await sse_manager.disconnect(connection_queue)
            sse_logger.info(f"SSE stream ended for user {user_id}")
    
    # Return streaming response with proper SSE headers
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )
