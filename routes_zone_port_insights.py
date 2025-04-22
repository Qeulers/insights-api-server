import httpx
from fastapi import APIRouter, Request, Response, Query
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional

EXTERNAL_BASE_URL = "https://api.polestar-production.com/zone-port-insights"
router = APIRouter(prefix="/zone-port-insights", tags=["Zone Port Insights"])

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# /v1/zones search endpoint
@router.get("/zones")
async def search_zones(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    name_contains: Optional[str] = Query(None, description="Name or partial name of the port/zone to search for."),
    unlocode: Optional[str] = Query(None, description="Filter by port UNLOCODE."),
    country_code: Optional[str] = Query(None, description="Filter by three-letter country code."),
    sub_division_code: Optional[str] = Query(None, description="Filter by sub division code."),
    wpi_number: Optional[int] = Query(None, description="Filter by World Port Index number."),
    type: Optional[str] = Query(None, description="Type of zone/port. See API docs for allowed values."),
    sub_type: Optional[str] = Query(None, description="Sub type of zone/port. See API docs for allowed values."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten each object in the data array and return only the data array content.")
):
    headers = dict(request.headers)
    headers.pop("host", None)
    if "authorization" not in {k.lower() for k in headers}:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization header"})
    params = {
        "limit": limit,
        "offset": offset,
    }
    if name_contains is not None:
        params["name_contains"] = name_contains
    if unlocode is not None:
        params["unlocode"] = unlocode
    if country_code is not None:
        params["country_code"] = country_code
    if sub_division_code is not None:
        params["sub_division_code"] = sub_division_code
    if wpi_number is not None:
        params["wpi_number"] = wpi_number
    if type is not None:
        params["type"] = type
    if sub_type is not None:
        params["sub_type"] = sub_type
    url = f"{EXTERNAL_BASE_URL}/v1/zones"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    # Only flatten on 200 and if flatten_json is true
    if flatten_json and resp.status_code == 200:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], list):
                flat_data = [flatten_dict(obj) for obj in payload["data"]]
                return JSONResponse(content=flat_data, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

# /v1/zone-and-port-traffic/{id_type}/{id} endpoint
@router.get("/zone-and-port-traffic/{id_type}/{id}")
async def zone_port_traffic(
    request: Request,
    id_type: str,
    id: str,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the vessels in the port."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the vessels in the port."),
    event_type: str = Query(None, description="Filter on specific zone or port events. If omitted, all events will be considered."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and zone_port_information and return them as a flat list.")
):
    headers = dict(request.headers)
    headers.pop("host", None)
    if "authorization" not in {k.lower() for k in headers}:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization header"})
    params = {
        "limit": limit,
        "offset": offset,
    }
    if timestamp_start is not None:
        params["timestamp_start"] = timestamp_start
    if timestamp_end is not None:
        params["timestamp_end"] = timestamp_end
    if event_type is not None:
        params["event_type"] = event_type
    url = f"{EXTERNAL_BASE_URL}/v1/zone-and-port-traffic/{id_type}/{id}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    # Only flatten on 200 and if flatten_json is true
    if flatten_json and resp.status_code == 200:
        try:
            payload = resp.json()
            if (
                "data" in payload
                and isinstance(payload["data"], dict)
                and "events" in payload["data"]
                and isinstance(payload["data"]["events"], list)
            ):
                zone_port_info = flatten_dict(payload["data"].get("zone_port_information", {}))
                flat_events = [
                    {**flatten_dict(event), **zone_port_info}
                    for event in payload["data"]["events"]
                ]
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
