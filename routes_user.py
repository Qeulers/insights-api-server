from fastapi import APIRouter, HTTPException, status, Request, Response
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

# GET /users: retrieve all users
@router.get("/users")
async def get_all_users():
    collection = get_users_collection()
    users = list(collection.find())
    for user in users:
        user["_id"] = str(user["_id"])
    return users

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

# DELETE /users/{user_id}: remove a user
@router.delete("/users/{user_id}", status_code=204)
async def delete_user(user_id: str):
    collection = get_users_collection()
    result = collection.delete_one({"user_id": user_id.strip()})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found.")
    return Response(status_code=204)

# POST /users/{user_id}/logout: set is_logged_in to false
@router.post("/users/{user_id}/logout")
async def logout_user(user_id: str):
    collection = get_users_collection()
    result = collection.update_one(
        {"user_id": user_id.strip()},
        {"$set": {"is_logged_in": False}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"message": f"User {user_id} logged out successfully."}

# POST /users/{user_id}/login: set is_logged_in to true and update last_login
@router.post("/users/{user_id}/login")
async def login_user(user_id: str):
    collection = get_users_collection()
    now_utc = datetime.now(timezone.utc).isoformat()
    result = collection.update_one(
        {"user_id": user_id.strip()},
        {"$set": {"is_logged_in": True, "last_login": now_utc}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"message": f"User {user_id} logged in successfully."}

# GET /users/{user_id}/settings: retrieve user settings only
@router.get("/users/{user_id}/settings")
async def get_user_settings(user_id: str, request: Request):
    import httpx
    # Check if user is logged in
    base_url = str(request.base_url)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{base_url}users/{user_id}/is-logged-in")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to check user login: {str(e)}")
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found.")
        if resp.status_code != 200 or not resp.json().get("is_logged_in", False):
            raise HTTPException(status_code=401, detail="User is not logged in.")
    collection = get_users_collection()
    user = collection.find_one({"user_id": user_id.strip()})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    settings = user.get("settings", {})
    return settings

# PUT /users/{user_id}/saved-entities: edit saved_entities array
@router.put("/users/{user_id}/saved-entities")
async def edit_saved_entities(user_id: str, request: Request):
    import httpx
    from fastapi import status
    # Check if user is logged in
    base_url = str(request.base_url)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{base_url}users/{user_id}/is-logged-in")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to check user login: {str(e)}")
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found.")
        if resp.status_code != 200 or not resp.json().get("is_logged_in", False):
            raise HTTPException(status_code=401, detail="User is not logged in.")
    collection = get_users_collection()
    body = await request.json()
    add = body.get("add", [])
    remove = body.get("remove", [])

    # Find the user
    user = collection.find_one({"user_id": user_id.strip()})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    # Ensure settings and saved_entities exist
    settings = user.get("settings", {})
    saved_entities = settings.get("saved_entities", [])

    # Remove entities in 'remove'
    remove_set = set((e["id"], e["type"]) for e in remove if "id" in e and "type" in e)
    saved_entities = [e for e in saved_entities if (e.get("id"), e.get("type")) not in remove_set]

    # Add entities in 'add' if not already present
    existing_set = set((e.get("id"), e.get("type")) for e in saved_entities)
    from datetime import datetime, timezone
    for entity in add:
        if "id" in entity and "type" in entity:
            key = (entity["id"], entity["type"])
            if key not in existing_set:
                # Add saved_at timestamp
                entity_with_timestamp = dict(entity)
                entity_with_timestamp["saved_at"] = datetime.now(timezone.utc).isoformat()
                saved_entities.append(entity_with_timestamp)
                existing_set.add(key)

    # Update user document
    settings["saved_entities"] = saved_entities
    result = collection.update_one(
        {"user_id": user_id.strip()},
        {"$set": {"settings.saved_entities": saved_entities}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found during update.")

    return {"saved_entities": saved_entities}

# GET /users/{user_id}/is-logged-in: check if user is logged in
@router.get("/users/{user_id}/is-logged-in")
async def is_user_logged_in(user_id: str):
    collection = get_users_collection()
    user = collection.find_one({"user_id": user_id.strip()})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"is_logged_in": bool(user.get("is_logged_in", False))}
