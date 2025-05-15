import httpx
from fastapi import APIRouter, Request, Response, Query, HTTPException
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional
import json
import os
from dotenv import load_dotenv
from utils.api_helpers import paginate_all_data, build_params

load_dotenv()

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

EXTERNAL_BASE_URL = "https://api.polestar-production.com/vessel-insights"
PTE_API_BASE_URL = "https://api.polestar-production.com/purpletrac/v1"
router = APIRouter(prefix="/vessel-insights", tags=["Vessel Insights"])

@router.get("/vessel-characteristics/{imo}")
async def vessel_characteristics(request: Request, imo: int, flatten_json: Optional[bool] = False):
    headers = dict(request.headers)
    headers.pop("host", None)
    # Only allow requests with Authorization header
    if "authorization" not in {k.lower() for k in headers}:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization header"})
    params = dict(request.query_params)
    params.pop("flatten_json", None)  # Don't forward our custom param
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-characteristics/{imo}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    # Only flatten on 200 and if flatten_json is true
    if flatten_json and resp.status_code == 200:
        try:
            payload = resp.json()
            if "data" in payload:
                flat = flatten_dict(payload["data"])
                return JSONResponse(content=flat, status_code=200)
        except Exception:
            pass  # If flatten fails, fall through and return raw
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )


@router.get("/vessels/search")
async def search_vessels(
    request: Request,
    user_id: str = Query(..., description="User ID for authentication"),
    limit: int = Query(500, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    imo_number__startswith: Optional[str] = Query(None, description="Filter by IMO number prefix."),
    ship_name__istartswith: Optional[str] = Query(None, description="Filter by ship name prefix (case-insensitive)."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate results."),
    flatten_json: Optional[bool] = Query(False, description="If true, remove meta and rename objects to data.")
):
    """
    Search vessels using the Polestar PurpleTrac SISShip endpoint. Requires user authentication.
    """
    # Check if user is logged in
    # Check if user is logged in (internal endpoint, absolute URL)
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

    # Prepare upstream API params
    username = os.getenv("PTE_API_USERNAME")
    api_key = os.getenv("PTE_API_KEY")
    if not username or not api_key:
        raise HTTPException(status_code=500, detail="Polestar API credentials not configured.")

    params = build_params(
        username=username,
        api_key=api_key,
        limit=limit,
        offset=offset,
        imo_number__startswith=imo_number__startswith,
        ship_name__istartswith=ship_name__istartswith
    )
    upstream_url = f"{PTE_API_BASE_URL}/sisship"

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(upstream_url, params=page_params)

    def extract_objects(payload):
        # Returns the 'objects' array from the upstream response
        return payload.get("objects", [])

    if all_data:
        try:
            meta, all_objects, _ = await paginate_all_data(
                fetch_page, limit, offset, "total_count", extract_objects
            )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        if flatten_json:
            return JSONResponse(content={"data": all_objects}, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": all_objects}, status_code=200)

    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(upstream_url, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            objects = payload.get("objects", [])
            return JSONResponse(content={"data": objects}, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
