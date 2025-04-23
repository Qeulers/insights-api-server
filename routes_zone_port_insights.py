import httpx
from fastapi import APIRouter, Request, Response, Query, HTTPException
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional
from utils.api_helpers import extract_and_validate_headers, build_params, paginate_all_data

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
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset, name_contains=name_contains, unlocode=unlocode,
        country_code=country_code, sub_division_code=sub_division_code, wpi_number=wpi_number,
        type=type, sub_type=sub_type
    )
    url = f"{EXTERNAL_BASE_URL}/v1/zones"

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)

    if all_data:
        meta, all_data_list, _ = await paginate_all_data(fetch_page, limit, offset, "total_count", lambda p: p.get("data", []))
        if flatten_json:
            flat_data = [flatten_dict(obj) for obj in all_data_list]
            return JSONResponse(content=flat_data, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": all_data_list}, status_code=200)
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
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
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end, event_type=event_type
    )
    url = f"{EXTERNAL_BASE_URL}/v1/zone-and-port-traffic/{id_type}/{id}"

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)

    if all_data:
        meta, all_events, extra_info = await paginate_all_data(
            fetch_page, limit, offset, "total_count", lambda p: p.get("data", {}).get("events", [])
        )
        zone_port_info_raw = extra_info.get("zone_port_information")
        zone_port_info_flat = flatten_dict(zone_port_info_raw, parent_key="zone_port_information") if zone_port_info_raw else {}
        if flatten_json:
            flat_events = [
                {**flatten_dict(event), **zone_port_info_flat}
                for event in all_events
            ]
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"zone_port_information": zone_port_info_raw, "events": all_events}}, status_code=200)
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
