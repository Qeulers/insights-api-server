import httpx
from fastapi import APIRouter, Request, Response, Query
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional
import json

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
router = APIRouter(prefix="/vessel-insights", tags=["Vessel Insights"])

@router.get("/vessel-characteristics")
async def vessel_characteristics(request: Request, imo: str = Query(...), flatten_json: Optional[bool] = False):
    headers = dict(request.headers)
    headers.pop("host", None)
    # Only allow requests with Authorization header
    if "authorization" not in {k.lower() for k in headers}:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization header"})
    params = dict(request.query_params)
    params.pop("flatten_json", None)  # Don't forward our custom param
    params["imo"] = imo
    url = f"{EXTERNAL_BASE_URL}/vessel-characteristics"
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
