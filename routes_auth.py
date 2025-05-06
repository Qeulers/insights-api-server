import httpx
from fastapi import APIRouter, Request, Response, Depends, HTTPException
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone

EXTERNAL_BASE_URL = "https://api.polestar-production.com"
router = APIRouter(prefix="/account/v1/auth", tags=["Authentication"])

# Helper to get MongoDB collection for users
def get_users_collection():
    load_dotenv()
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_USERS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_USERS")
    if not all([mongo_url, db_name, collection_name]):
        raise Exception("MongoDB configuration is missing.")
    client = MongoClient(mongo_url)
    db = client[db_name]
    return db[collection_name]

async def proxy_request(request: Request, endpoint: str) -> Response:
    method = request.method
    url = f"{EXTERNAL_BASE_URL}{endpoint}"
    headers = dict(request.headers)
    # Remove host header to avoid conflicts
    headers.pop("host", None)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                content=await request.body()
            )
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.post("/signin")
async def signin(request: Request):
    # Forward the request to the upstream auth service
    method = request.method
    url = f"{EXTERNAL_BASE_URL}/account/v1/auth/signin"
    headers = dict(request.headers)
    headers.pop("host", None)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                content=await request.body()
            )
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})

    # If signin failed, return immediately
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
            media_type=resp.headers.get("content-type")
        )

    # Parse response and extract user info
    try:
        payload = resp.json()
        # Adjust this extraction to match the actual structure of your signin response
        user_data = {
            "user_id": payload.get("user_id"),
            "username": payload.get("username"),
            "first_name": payload.get("first_name"),
            "last_name": payload.get("last_name"),
            "email": payload.get("email"),
            "role": payload.get("role"),
            "account_id": payload.get("account_id"),
        }
        if all(user_data.values()):
            collection = get_users_collection()
            existing = collection.find_one({"user_id": user_data["user_id"]})
            now_utc = datetime.now(timezone.utc).isoformat()
            try:
                if not existing:
                    user_doc = {
                        **user_data,
                        "is_logged_in": True,
                        "last_login": now_utc,
                        "settings": {
                            "saved_entities": [],
                            "recent_searches": []
                        }
                    }
                    collection.insert_one(user_doc)
                else:
                    # User exists, update login state and last_login
                    update_result = collection.update_one(
                        {"user_id": user_data["user_id"]},
                        {"$set": {"is_logged_in": True, "last_login": now_utc}}
                    )
                    if update_result.modified_count == 0 and update_result.matched_count == 0:
                        raise Exception("Failed to update user login state.")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"User creation or update failed: {str(e)}")
    except Exception as e:
        # Log or handle error, but do not block signin response
        pass

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.put("/access-token-refresh")
async def refresh_token(request: Request):
    return await proxy_request(request, "/account/v1/auth/access-token-refresh")

@router.post("/password-reset/otp")
async def send_password_reset_otp(request: Request):
    return await proxy_request(request, "/account/v1/auth/password-reset/otp")

@router.post("/password-reset")
async def reset_password(request: Request):
    return await proxy_request(request, "/account/v1/auth/password-reset")
