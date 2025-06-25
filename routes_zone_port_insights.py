import httpx
from fastapi import APIRouter, Request, Response, Query, HTTPException
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional
from utils.api_helpers import extract_and_validate_headers, build_params, paginate_all_data
from utils.vessel_type_mapping import vessel_type_matches_lvl3, validate_lvl3_values
from pymongo import MongoClient
from dotenv import load_dotenv
import os

EXTERNAL_BASE_URL = "https://api.polestar-production.com/zone-port-insights"
router = APIRouter(prefix="/zone-port-insights", tags=["Zone Port Insights"])

@router.get("/polygons/{zone_id}", response_class=JSONResponse)
def get_zone_polygon(zone_id: str):
    """
    Returns the WKT polygon for a zone by zone_id.
    Handles both string and BSON UUID storage.
    """
    from fastapi import status
    from bson import ObjectId
    import uuid
    load_dotenv()
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_ZONES")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_ZONES_POLYGONS")
    if not all([mongo_url, db_name, collection_name]):
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": "MongoDB configuration missing in .env"})
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        collection = db[collection_name]
        import uuid
        from bson.binary import Binary
        try:
            uuid_val = uuid.UUID(zone_id)
        except Exception:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": f"Invalid zone_id: '{zone_id}' is not a valid UUID"})
        result = collection.find_one({"zone_id": Binary.from_uuid(uuid_val)})
        if not result:
            return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": f"Zone with id '{zone_id}' not found"})
        wkt = result.get("geometry_wkt")
        if not wkt:
            return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": f"geometry_wkt not found for zone '{zone_id}'"})
        return JSONResponse(content={"zone_id": zone_id, "geometry_wkt": wkt}, status_code=200)
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": str(e)})
    finally:
        try:
            client.close()
        except Exception:
            pass


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

def filter_vessels_by_params(vessels, flag_country_code=None, incl_vessel_type_lvl3=None, excl_vessel_type_lvl3=None, imo=None, mmsi=None):
    filtered = vessels
    # Filter by flag_country_code
    if flag_country_code:
        allowed_set = set(code.strip().upper() for code in flag_country_code.split(",") if code.strip())
        filtered = [
            v for v in filtered
            if (
                isinstance(v, dict)
                and isinstance(v.get("vessel_information"), dict)
                and (str(v["vessel_information"].get("flag_code", "")).upper() in allowed_set)
            )
        ]
    # Validate and filter by vessel_type_level3 (include)
    incl_lvl3_set = set(val.strip().lower() for val in incl_vessel_type_lvl3.split(",") if val.strip()) if incl_vessel_type_lvl3 else set()
    excl_lvl3_set = set(val.strip().lower() for val in excl_vessel_type_lvl3.split(",") if val.strip()) if excl_vessel_type_lvl3 else set()
    if incl_lvl3_set and excl_lvl3_set:
        overlap = incl_lvl3_set & excl_lvl3_set
        if overlap:
            raise HTTPException(status_code=400, detail=f"Values cannot be in both incl_vessel_type_lvl3 and excl_vessel_type_lvl3: {sorted(overlap)}")
    valid_incl_lvl3 = validate_lvl3_values(incl_lvl3_set) if incl_lvl3_set else set()
    valid_excl_lvl3 = validate_lvl3_values(excl_lvl3_set) if excl_lvl3_set else set()
    if valid_incl_lvl3:
        filtered = [
            v for v in filtered
            if (
                isinstance(v, dict)
                and isinstance(v.get("vessel_information"), dict)
                and vessel_type_matches_lvl3(v["vessel_information"].get("vessel_type"), valid_incl_lvl3)
            )
        ]
    if valid_excl_lvl3:
        filtered = [
            v for v in filtered
            if not (
                isinstance(v, dict)
                and isinstance(v.get("vessel_information"), dict)
                and vessel_type_matches_lvl3(v["vessel_information"].get("vessel_type"), valid_excl_lvl3)
            )
        ]
    # Filter by IMO
    if imo:
        allowed_imo_set = set(val.strip() for val in imo.split(",") if val.strip())
        filtered = [
            v for v in filtered
            if (
                isinstance(v, dict)
                and isinstance(v.get("vessel_information"), dict)
                and (str(v["vessel_information"].get("imo", "")) in allowed_imo_set)
            )
        ]
    # Filter by MMSI
    if mmsi:
        allowed_mmsi_set = set(val.strip() for val in mmsi.split(",") if val.strip())
        filtered = [
            v for v in filtered
            if (
                isinstance(v, dict)
                and isinstance(v.get("vessel_information"), dict)
                and (str(v["vessel_information"].get("mmsi", "")) in allowed_mmsi_set)
            )
        ]
    return filtered

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
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all data array objects into a single response."),
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
        try:
            meta, all_data_list, _ = await paginate_all_data(fetch_page, limit, offset, "total_count", lambda p: p.get("data", []))
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
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

# /v1/zones/{id} endpoint
@router.get("/zones/{zone_id}")
async def get_zone_by_id(request: Request, zone_id: str, user_id: str = Query(..., description="User ID for authentication")):
    # Check if user is logged in
    import httpx
    from fastapi import status
    base_url = str(request.base_url)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{base_url}users/{user_id}/is-logged-in")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to check user login: {str(e)}")
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found.")
        elif resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to check user login.")
        is_logged_in = resp.json().get("is_logged_in", False)
        if not is_logged_in:
            raise HTTPException(status_code=403, detail="User not logged in.")
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_ZONES")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_ZONES")
    if not all([mongo_url, db_name, collection_name]):
        raise HTTPException(status_code=500, detail="MongoDB configuration is missing.")
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    zone_id_stripped = zone_id.strip()
    result = collection.find_one({"zone_id": zone_id_stripped})
    if not result:
        result = collection.find_one({"zone_id": {"$regex": f"^{zone_id_stripped}$", "$options": "i"}})
    if not result:
        raise HTTPException(status_code=404, detail="Zone not found.")
    result["_id"] = str(result["_id"])
    return result

# /zone-port-insights/zones/bulk-request endpoint
from fastapi import Body
from typing import List

@router.post("/zones/bulk-request")
async def bulk_get_zones_by_id(
    request: Request,
    user_id: str = Query(..., description="User ID for authentication"),
    zone_ids: List[str] = Body(..., description="List of zone IDs to fetch")
):
    import httpx
    from fastapi import status
    base_url = str(request.base_url)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{base_url}users/{user_id}/is-logged-in")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to check user login: {str(e)}")
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found.")
        elif resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to check user login.")
        is_logged_in = resp.json().get("is_logged_in", False)
        if not is_logged_in:
            raise HTTPException(status_code=403, detail="User not logged in.")
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB_NAME_ZONES")
    collection_name = os.getenv("MONGO_COLLECTION_NAME_ZONES")
    if not all([mongo_url, db_name, collection_name]):
        raise HTTPException(status_code=500, detail="MongoDB configuration is missing.")
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    found_zones = []
    seen_ids = set()
    for zid in zone_ids:
        zid_stripped = zid.strip()
        if zid_stripped in seen_ids:
            continue  # avoid duplicate queries
        seen_ids.add(zid_stripped)
        zone = collection.find_one({"zone_id": zid_stripped})
        if not zone:
            zone = collection.find_one({"zone_id": {"$regex": f"^{zid_stripped}$", "$options": "i"}})
        if zone:
            zone["_id"] = str(zone["_id"])
            found_zones.append(zone)
    return found_zones

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
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response."),
    flag_country_code: Optional[str] = Query(None, description="Comma separated list of three letter country codes to filter vessel_information.flag_code on."),
    incl_vessel_type_lvl3: Optional[str] = Query(None, description="Comma separated list of vessel_type_level3 to filter vessel_information.vessel_type on."),
    excl_vessel_type_lvl3: Optional[str] = Query(None, description="Comma separated list of vessel_type_level3 to EXCLUDE vessel_information.vessel_type on."),
    imo: Optional[str] = Query(None, description="Comma separated list of IMO numbers to filter vessel_information.imo on."),
    mmsi: Optional[str] = Query(None, description="Comma separated list of MMSI numbers to filter vessel_information.mmsi on."),
    excl_port_information: Optional[bool] = Query(False, description="If true, exclude all fields with the zone_port_information_ prefix from the response when flatten_json is true."),
    excl_event_details: Optional[bool] = Query(False, description="If true, exclude select fields with the event_details_ prefix from the response when flatten_json is true."),
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end, event_type=event_type
    )
    url = f"{EXTERNAL_BASE_URL}/v1/zone-and-port-traffic/{id_type}/{id}"

    # --- Validate incl/excl mutual exclusivity and parse sets ---
    incl_lvl3_set = set(val.strip().lower() for val in incl_vessel_type_lvl3.split(",") if val.strip()) if incl_vessel_type_lvl3 else set()
    excl_lvl3_set = set(val.strip().lower() for val in excl_vessel_type_lvl3.split(",") if val.strip()) if excl_vessel_type_lvl3 else set()
    overlap = incl_lvl3_set & excl_lvl3_set
    if overlap:
        raise HTTPException(status_code=400, detail=f"Values cannot be in both incl_vessel_type_lvl3 and excl_vessel_type_lvl3: {sorted(overlap)}")
    valid_incl_lvl3 = validate_lvl3_values(incl_lvl3_set) if incl_lvl3_set else set()
    valid_excl_lvl3 = validate_lvl3_values(excl_lvl3_set) if excl_lvl3_set else set()

    def filter_events(events, allowed_codes, allowed_incl_lvl3, allowed_excl_lvl3, allowed_imo, allowed_mmsi):
        filtered = events
        # Filter by flag_country_code
        if allowed_codes:
            allowed_set = set(code.strip().upper() for code in allowed_codes.split(",") if code.strip())
            filtered = [
                e for e in filtered
                if (
                    isinstance(e, dict)
                    and isinstance(e.get("vessel_information"), dict)
                    and (str(e["vessel_information"].get("flag_code", "")).upper() in allowed_set)
                )
            ]
        # Filter by vessel_type_level3 (include)
        if allowed_incl_lvl3:
            filtered = [
                e for e in filtered
                if (
                    isinstance(e, dict)
                    and isinstance(e.get("vessel_information"), dict)
                    and vessel_type_matches_lvl3(e["vessel_information"].get("vessel_type"), allowed_incl_lvl3)
                )
            ]
        # Filter by vessel_type_level3 (exclude)
        if allowed_excl_lvl3:
            filtered = [
                e for e in filtered
                if not (
                    isinstance(e, dict)
                    and isinstance(e.get("vessel_information"), dict)
                    and vessel_type_matches_lvl3(e["vessel_information"].get("vessel_type"), allowed_excl_lvl3)
                )
            ]
        # Filter by IMO
        if allowed_imo:
            allowed_imo_set = set(val.strip() for val in allowed_imo.split(",") if val.strip())
            filtered = [
                e for e in filtered
                if (
                    isinstance(e, dict)
                    and isinstance(e.get("vessel_information"), dict)
                    and (str(e["vessel_information"].get("imo", "")) in allowed_imo_set)
                )
            ]
        # Filter by MMSI
        if allowed_mmsi:
            allowed_mmsi_set = set(val.strip() for val in allowed_mmsi.split(",") if val.strip())
            filtered = [
                e for e in filtered
                if (
                    isinstance(e, dict)
                    and isinstance(e.get("vessel_information"), dict)
                    and (str(e["vessel_information"].get("mmsi", "")) in allowed_mmsi_set)
                )
            ]
        return filtered

    def filter_flattened_fields(flat_events):
        """
        Remove fields according to excl_port_information and excl_event_details from each event dict in flat_events.
        """
        if not isinstance(flat_events, list):
            return flat_events
        event_details_exclude = {
            "event_details_course",
            "event_details_heading",
            "event_details_navigational_status_status",
            "event_details_navigational_status_code",
            "event_details_reported_destination",
            "event_details_reported_eta",
            "event_details_speed",
        }
        filtered = []
        for obj in flat_events:
            if not isinstance(obj, dict):
                filtered.append(obj)
                continue
            filtered_obj = obj.copy()
            if excl_port_information:
                filtered_obj = {k: v for k, v in filtered_obj.items() if not k.startswith("zone_port_information_")}
            if excl_event_details:
                filtered_obj = {k: v for k, v in filtered_obj.items() if not (k in event_details_exclude)}
            filtered.append(filtered_obj)
        return filtered

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)

    if all_data:
        try:
            # Fetch the first page to inspect meta['total_count'] before aggregating
            first_page_resp = await fetch_page(offset)
            first_page_json = first_page_resp.json()
            meta = first_page_json.get('meta', {})
            total_count = meta.get('total_count')
            if total_count is not None and total_count > 30000:
                return JSONResponse(
                    status_code=413,
                    content={
                        "error": f"The requested data set is too large to process (total_count: {total_count}, max allowed: 30000). Please narrow your query parameters."
                    }
                )
            # If OK, proceed with normal pagination
            meta, all_events, extra_info = await paginate_all_data(
                fetch_page, limit, offset, "total_count", lambda p: p.get("data", {}).get("events", [])
            )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        zone_port_info_raw = extra_info.get("zone_port_information")
        zone_port_info_flat = flatten_dict(zone_port_info_raw, parent_key="zone_port_information") if zone_port_info_raw else {}
        # Filter events with all logic
        filtered_events = filter_events(all_events, flag_country_code, valid_incl_lvl3, valid_excl_lvl3, imo, mmsi)
        if flatten_json:
            flat_events = [
                {**flatten_dict(event), **zone_port_info_flat}
                for event in filtered_events
            ]
            # Apply exclusion filters
            flat_events = filter_flattened_fields(flat_events)
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"zone_port_information": zone_port_info_raw, "events": filtered_events}}, status_code=200)
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    if resp.status_code == 200:
        try:
            payload = resp.json()
            # Filter events with all logic
            if (
                "data" in payload and isinstance(payload["data"], dict)
                and "events" in payload["data"] and isinstance(payload["data"]["events"], list)
            ):
                events = payload["data"]["events"]
                filtered_events = filter_events(events, flag_country_code, valid_incl_lvl3, valid_excl_lvl3, imo, mmsi)
                payload["data"]["events"] = filtered_events
            if flatten_json:
                flat_events = flatten_zone_port_traffic_response(payload)
                if flat_events is not None:
                    # Apply exclusion filters
                    flat_events = filter_flattened_fields(flat_events)
                    return JSONResponse(content=flat_events, status_code=200)
            else:
                return JSONResponse(content=payload, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

# /v1/vessels-in-zone-or-port/{id_type}/{id} endpoint
@router.get("/vessels-in-zone-or-port/{id_type}/{id}")
async def vessels_in_zone_or_port(
    request: Request,
    id_type: str,
    id: str,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    flatten_json: Optional[bool] = Query(False, description="If true, return only the vessels array, discarding meta and zone_port_information."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all vessels array objects into a single response."),
    flag_country_code: Optional[str] = Query(None, description="Comma separated list of three letter country codes to filter vessel_information.flag_code on."),
    incl_vessel_type_lvl3: Optional[str] = Query(None, description="Comma separated list of vessel_type_level3 to filter vessel_information.vessel_type on."),
    excl_vessel_type_lvl3: Optional[str] = Query(None, description="Comma separated list of vessel_type_level3 to EXCLUDE vessel_information.vessel_type on."),
    imo: Optional[str] = Query(None, description="Comma separated list of IMO numbers to filter vessel_information.imo on."),
    mmsi: Optional[str] = Query(None, description="Comma separated list of MMSI numbers to filter vessel_information.mmsi on."),
):
    headers = extract_and_validate_headers(request)
    params = build_params(limit=limit, offset=offset)
    url = f"{EXTERNAL_BASE_URL}/v1/vessels-in-zone-or-port/{id_type}/{id}"

    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)

    if all_data:
        try:
            meta, all_vessels, extra_info = await paginate_all_data(
                fetch_page, limit, offset, "total_count", lambda p: p.get("data", {}).get("vessels", [])
            )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        filtered_vessels = filter_vessels_by_params(
            all_vessels,
            flag_country_code=flag_country_code,
            incl_vessel_type_lvl3=incl_vessel_type_lvl3,
            excl_vessel_type_lvl3=excl_vessel_type_lvl3,
            imo=imo,
            mmsi=mmsi,
        )
        if flatten_json:
            return JSONResponse(content=filtered_vessels, status_code=200)
        else:
            zone_port_info = extra_info.get("zone_port_information") if extra_info else None
            return JSONResponse(content={"meta": meta, "data": {"zone_port_information": zone_port_info, "vessels": filtered_vessels}}, status_code=200)

    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    if resp.status_code == 200:
        try:
            payload = resp.json()
            vessels = []
            if "data" in payload and isinstance(payload["data"], dict) and "vessels" in payload["data"]:
                vessels = payload["data"]["vessels"]
            filtered_vessels = filter_vessels_by_params(
                vessels,
                flag_country_code=flag_country_code,
                incl_vessel_type_lvl3=incl_vessel_type_lvl3,
                excl_vessel_type_lvl3=excl_vessel_type_lvl3,
                imo=imo,
                mmsi=mmsi,
            )
            if flatten_json:
                return JSONResponse(content=filtered_vessels, status_code=200)
            else:
                payload["data"]["vessels"] = filtered_vessels
                return JSONResponse(content=payload, status_code=200)
        except Exception:
            pass  # fallback to raw response
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
