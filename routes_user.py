from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone
import os

router = APIRouter()

# Helper to get MongoDB collection for users
def get_users_collection():
    load_dotenv()
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_USERS")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_USERS")
    if not all([mongo_url, db_name, collection_name]):
        raise HTTPException(status_code=500, detail="MongoDB configuration is missing.")
    client = MongoClient(mongo_url)
    db = client[db_name]
    return db[collection_name]

# POST /users: create a new user
@router.post("/users", status_code=status.HTTP_201_CREATED)
async def create_user(user: dict):
    collection = get_users_collection()
    user_id = user.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required.")
    # Check if user exists
    if collection.find_one({"user_id": user_id}):
        raise HTTPException(status_code=409, detail="User already exists.")
    # Prepare user doc
    now_utc = datetime.now(timezone.utc).isoformat()
    user_doc = {
        "user_id": user["user_id"],
        "username": user["username"],
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "email": user["email"],
        "role": user["role"],
        "account_id": user["account_id"],
        "is_logged_in": True,
        "last_login": now_utc,
        "settings": {
            "saved_entities": [],
            "recent_searches": []
        }
    }
    collection.insert_one(user_doc)
    user_doc["_id"] = str(user_doc.get("_id", ""))
    return JSONResponse(content=user_doc, status_code=201)

# GET /users/{user_id}: retrieve a user
@router.get("/users/{user_id}")
async def get_user_by_id(user_id: str):
    collection = get_users_collection()
    result = collection.find_one({"user_id": user_id.strip()})
    if not result:
        # Case-insensitive fallback
        result = collection.find_one({"user_id": {"$regex": f"^{user_id.strip()}$", "$options": "i"}})
    if not result:
        raise HTTPException(status_code=404, detail="User not found.")
    result["_id"] = str(result["_id"])
    return result
