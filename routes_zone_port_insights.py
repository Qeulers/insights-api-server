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

def flatten_zone_port_traffic_response(payload):
    # Returns a list of flattened events, each merged with flattened zone_port_information
    if (
        "data" in payload
        and isinstance(payload["data"], dict)
        and "events" in payload["data"]
        and isinstance(payload["data"]["events"], list)
    ):
        zone_port_info = flatten_dict(payload["data"].get("zone_port_information", {}), parent_key="zone_port_information")
        return [
            {**flatten_dict(event), **zone_port_info}
            for event in payload["data"]["events"]
        ]
    return None

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
    flatten_json: Optional[bool] = Query(False, description="If true, flatten each object in the data array and return only the data array content."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all data array objects into a single response.")
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

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)

    if all_data:
        all_data_list = []
        current_offset = offset
        total_count = None
        first_meta = None
        while True:
            resp = await fetch_page(current_offset)
            if resp.status_code != 200:
                break
            payload = resp.json()
            if first_meta is None:
                first_meta = payload.get("meta", {})
                total_count = first_meta.get("total_count")
            data_batch = payload.get("data", [])
            all_data_list.extend(data_batch)
            current_offset += limit
            if not data_batch or (total_count is not None and current_offset >= total_count):
                break
        if flatten_json:
            flat_data = [flatten_dict(obj) for obj in all_data_list]
            return JSONResponse(content=flat_data, status_code=200)
        else:
            return JSONResponse(content={"meta": first_meta, "data": all_data_list}, status_code=200)
    # Not all_data: normal single page logic
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
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and zone_port_information and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response.")
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

    async def fetch_zone_port_traffic_page(headers, params, url):
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=params)

    if all_data:
        all_events = []
        current_offset = offset
        total_count = None
        first_meta = None
        zone_port_info_raw = None
        zone_port_info_flat = None
        while True:
            page_params = params.copy()
            page_params["offset"] = current_offset
            resp = await fetch_zone_port_traffic_page(headers, page_params, url)
            if resp.status_code != 200:
                break
            payload = resp.json()
            if first_meta is None:
                first_meta = payload.get("meta", {})
                total_count = first_meta.get("total_count")
            # Extract both raw and flat zone_port_information
            if zone_port_info_raw is None:
                zone_port_info_raw = payload.get("data", {}).get("zone_port_information")
                zone_port_info_flat = flatten_dict(zone_port_info_raw, parent_key="zone_port_information") if zone_port_info_raw else {}
            events_batch = payload.get("data", {}).get("events")
            if not isinstance(events_batch, list):
                events_batch = []
            all_events.extend(events_batch)
            current_offset += limit
            if not events_batch or (total_count is not None and current_offset >= total_count):
                break
        if flatten_json:
            flat_events = [
                {**flatten_dict(event), **(zone_port_info_flat or {})}
                for event in all_events
            ]
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": first_meta, "data": {"zone_port_information": zone_port_info_raw, "events": all_events}}, status_code=200)
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    if flatten_json and resp.status_code == 200:
        try:
            payload = resp.json()
            flat_events = flatten_zone_port_traffic_response(payload)
            if flat_events is not None:
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
