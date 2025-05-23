import httpx
from fastapi import APIRouter, Request, Response, Path, Query
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional
from utils.api_helpers import paginate_all_data, extract_and_validate_headers, build_params
from routes_zone_port_insights import flatten_dict
from copy import deepcopy

def flatten_positional_discrepancy_event(event):
    """Custom flattening function for positional discrepancy events.
    
    This function handles the special case of zones and ports arrays in the started/stopped
    objects, flattening the centroid coordinates and removing description fields.
    """
    # Make a deep copy to avoid modifying the original event
    event_copy = deepcopy(event)
    
    # Process started zones if they exist
    if "started" in event_copy and "zones" in event_copy["started"] and isinstance(event_copy["started"]["zones"], list):
        for i, zone in enumerate(event_copy["started"]["zones"]):
            # Remove description field
            if "description" in zone:
                del zone["description"]
            # Flatten centroid coordinates
            if "centroid" in zone and isinstance(zone["centroid"], dict):
                if "latitude" in zone["centroid"]:
                    zone["latitude"] = zone["centroid"]["latitude"]
                if "longitude" in zone["centroid"]:
                    zone["longitude"] = zone["centroid"]["longitude"]
                del zone["centroid"]
    
    # Process started ports if they exist
    if "started" in event_copy and "ports" in event_copy["started"] and isinstance(event_copy["started"]["ports"], list):
        for i, port in enumerate(event_copy["started"]["ports"]):
            # Remove description field
            if "description" in port:
                del port["description"]
            # Flatten centroid coordinates
            if "centroid" in port and isinstance(port["centroid"], dict):
                if "latitude" in port["centroid"]:
                    port["latitude"] = port["centroid"]["latitude"]
                if "longitude" in port["centroid"]:
                    port["longitude"] = port["centroid"]["longitude"]
                del port["centroid"]
    
    # Process stopped zones if they exist
    if "stopped" in event_copy and "zones" in event_copy["stopped"] and isinstance(event_copy["stopped"]["zones"], list):
        for i, zone in enumerate(event_copy["stopped"]["zones"]):
            # Remove description field
            if "description" in zone:
                del zone["description"]
            # Flatten centroid coordinates
            if "centroid" in zone and isinstance(zone["centroid"], dict):
                if "latitude" in zone["centroid"]:
                    zone["latitude"] = zone["centroid"]["latitude"]
                if "longitude" in zone["centroid"]:
                    zone["longitude"] = zone["centroid"]["longitude"]
                del zone["centroid"]
    
    # Process stopped ports if they exist
    if "stopped" in event_copy and "ports" in event_copy["stopped"] and isinstance(event_copy["stopped"]["ports"], list):
        for i, port in enumerate(event_copy["stopped"]["ports"]):
            # Remove description field
            if "description" in port:
                del port["description"]
            # Flatten centroid coordinates
            if "centroid" in port and isinstance(port["centroid"], dict):
                if "latitude" in port["centroid"]:
                    port["latitude"] = port["centroid"]["latitude"]
                if "longitude" in port["centroid"]:
                    port["longitude"] = port["centroid"]["longitude"]
                del port["centroid"]
    
    # Now flatten the modified event
    return flatten_dict(event_copy)

router = APIRouter(prefix="/voyage-insights", tags=["Voyage Insights"])
EXTERNAL_BASE_URL = "https://api.polestar-production.com/voyage-insights"

async def proxy_request(request: Request, path: str, path_params: dict = None):
    path_params = path_params or {}
    url = f"{EXTERNAL_BASE_URL}{path.format(**path_params)}"
    headers = dict(request.headers)
    headers.pop("host", None)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                "GET",
                url,
                params=request.query_params,
                headers=headers,
            )
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.get("/vessel-zone-and-port-events/{imo}")
async def vessel_zone_and_port_events(
    request: Request, 
    imo: str = Path(...),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the events."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the events."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response.")
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end
    )
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-zone-and-port-events/{imo}"
    
    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)
    
    if all_data:
        # Create a container for extra_info that can be accessed from the nested function
        extra_info_container = {}
        
        # Define a custom extractor that also captures vessel_information
        def extract_data_and_vessel_info(payload):
            if "data" in payload and isinstance(payload["data"], dict):
                if "vessel_information" in payload["data"]:
                    extra_info_container["vessel_information"] = payload["data"]["vessel_information"]
                return payload["data"].get("events", [])
            return []
        
        meta, all_events, _ = await paginate_all_data(
            fetch_page, limit, offset, "total_count", extract_data_and_vessel_info
        )
        
        # Get vessel_information from our container
        vessel_info = extra_info_container.get("vessel_information", {})
        vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
        
        if flatten_json:
            # For STS pairings, we need to handle the paired_vessel field specially
            flat_events = []
            for event in all_events:
                # Create a flattened event
                flat_event = flatten_dict(event)
                # Add vessel information
                flat_event.update(vessel_info_flat)
                flat_events.append(flat_event)
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"vessel_information": vessel_info, "events": all_events}}, status_code=200)
    
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], dict) and "events" in payload["data"]:
                events = payload["data"]["events"]
                vessel_info = payload["data"].get("vessel_information", {})
                vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
                
                # For STS pairings, we need to handle the paired_vessel field specially
                flat_events = []
                for event in events:
                    # Create a flattened event
                    flat_event = flatten_dict(event)
                    # Add vessel information
                    flat_event.update(vessel_info_flat)
                    flat_events.append(flat_event)
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.get("/vessel-ais-reporting-gaps/{imo}")
async def vessel_ais_reporting_gaps(
    request: Request, 
    imo: str = Path(...),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the events."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the events."),
    gap_threshold_gte: int = Query(None, ge=1, description="Minimum gap duration in hours to include in results. Must be an integer greater than or equal to 1."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response.")
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end,
        gap_threshold_gte=gap_threshold_gte
    )
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-ais-reporting-gaps/{imo}"
    
    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)
    
    if all_data:
        # Create a container for extra_info that can be accessed from the nested function
        extra_info_container = {}
        
        # Define a custom extractor that also captures vessel_information
        def extract_data_and_vessel_info(payload):
            if "data" in payload and isinstance(payload["data"], dict):
                if "vessel_information" in payload["data"]:
                    extra_info_container["vessel_information"] = payload["data"]["vessel_information"]
                return payload["data"].get("events", [])
            return []
        
        meta, all_events, _ = await paginate_all_data(
            fetch_page, limit, offset, "total_count", extract_data_and_vessel_info
        )
        
        # Get vessel_information from our container
        vessel_info = extra_info_container.get("vessel_information", {})
        vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
        
        if flatten_json:
            # For STS pairings, we need to handle the paired_vessel field specially
            flat_events = []
            for event in all_events:
                # Create a flattened event
                flat_event = flatten_dict(event)
                # Add vessel information
                flat_event.update(vessel_info_flat)
                flat_events.append(flat_event)
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"vessel_information": vessel_info, "events": all_events}}, status_code=200)
    
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], dict) and "events" in payload["data"]:
                events = payload["data"]["events"]
                vessel_info = payload["data"].get("vessel_information", {})
                vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
                
                # For STS pairings, we need to handle the paired_vessel field specially
                flat_events = []
                for event in events:
                    # Create a flattened event
                    flat_event = flatten_dict(event)
                    # Add vessel information
                    flat_event.update(vessel_info_flat)
                    flat_events.append(flat_event)
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.get("/vessel-sts-pairings/{imo}")
async def vessel_sts_pairings(
    request: Request, 
    imo: str = Path(...),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the events."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the events."),
    sts_type: str = Query(None, description="Comma-separated list of STS types to filter on. Valid values are CARGO, BUNKERING, FISHING, UNKNOWN. If omitted, all types will be considered."),
    sts_duration_gte: int = Query(None, ge=1, description="Minimum STS duration in minutes to include in results. Must be an integer greater than or equal to 1."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response.")
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end,
        sts_type=sts_type, sts_duration_gte=sts_duration_gte
    )
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-sts-pairings/{imo}"
    
    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)
    
    if all_data:
        # Create a container for extra_info that can be accessed from the nested function
        extra_info_container = {}
        
        # Define a custom extractor that also captures vessel_information
        def extract_data_and_vessel_info(payload):
            if "data" in payload and isinstance(payload["data"], dict):
                if "vessel_information" in payload["data"]:
                    extra_info_container["vessel_information"] = payload["data"]["vessel_information"]
                return payload["data"].get("events", [])
            return []
        
        meta, all_events, _ = await paginate_all_data(
            fetch_page, limit, offset, "total_count", extract_data_and_vessel_info
        )
        
        # Get vessel_information from our container
        vessel_info = extra_info_container.get("vessel_information", {})
        vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
        
        if flatten_json:
            # For STS pairings, we need to handle the paired_vessel field specially
            flat_events = []
            for event in all_events:
                # Create a flattened event
                flat_event = flatten_dict(event)
                # Add vessel information
                flat_event.update(vessel_info_flat)
                flat_events.append(flat_event)
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"vessel_information": vessel_info, "events": all_events}}, status_code=200)
    
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], dict) and "events" in payload["data"]:
                events = payload["data"]["events"]
                vessel_info = payload["data"].get("vessel_information", {})
                vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
                
                # For STS pairings, we need to handle the paired_vessel field specially
                flat_events = []
                for event in events:
                    # Create a flattened event
                    flat_event = flatten_dict(event)
                    # Add vessel information
                    flat_event.update(vessel_info_flat)
                    flat_events.append(flat_event)
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.get("/vessel-positional-discrepancy/{imo}")
async def vessel_positional_discrepancy(
    request: Request, 
    imo: str = Path(...),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the events."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the events."),
    duration_hours_gte: int = Query(None, ge=1, description="Minimum event duration in hours to include in results. Must be an integer greater than or equal to 1."),
    event_types: str = Query(None, description="Comma-separated list of event types to filter on. Valid values are SAME_POSITION, BOX_FORMATION, CIRCLE, MMSI_OUTLIER. If omitted, all types will be considered."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all events and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all events into a single response.")
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end,
        duration_hours_gte=duration_hours_gte, event_types=event_types
    )
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-positional-discrepancy/{imo}"
    
    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)
    
    if all_data:
        # Create a container for extra_info that can be accessed from the nested function
        extra_info_container = {}
        
        # Define a custom extractor that also captures vessel_information
        def extract_data_and_vessel_info(payload):
            if "data" in payload and isinstance(payload["data"], dict):
                if "vessel_information" in payload["data"]:
                    extra_info_container["vessel_information"] = payload["data"]["vessel_information"]
                return payload["data"].get("events", [])
            return []
        try:
            meta, all_events, _ = await paginate_all_data(
                fetch_page, limit, offset, "total_count", extract_data_and_vessel_info
            )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        # Get vessel_information from our container
        vessel_info = extra_info_container.get("vessel_information", {})
        vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
        
        if flatten_json:
            # For positional discrepancy, we need to handle the complex nested structure
            flat_events = []
            for event in all_events:
                # Use our custom flattening function for positional discrepancy events
                flat_event = flatten_positional_discrepancy_event(event)
                # Add vessel information
                flat_event.update(vessel_info_flat)
                flat_events.append(flat_event)
            return JSONResponse(content=flat_events, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"vessel_information": vessel_info, "events": all_events}}, status_code=200)
    
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], dict) and "events" in payload["data"]:
                events = payload["data"]["events"]
                vessel_info = payload["data"].get("vessel_information", {})
                vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
                
                # For positional discrepancy, we need to handle the complex nested structure
                flat_events = []
                for event in events:
                    # Use our custom flattening function for positional discrepancy events
                    flat_event = flatten_positional_discrepancy_event(event)
                    # Add vessel information
                    flat_event.update(vessel_info_flat)
                    flat_events.append(flat_event)
                return JSONResponse(content=flat_events, status_code=200)
        except Exception:
            pass  # fallback to raw response
    
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.get("/vessel-port-state-control/{imo}")
async def vessel_port_state_control(
    request: Request, 
    imo: str = Path(...),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    timestamp_start: str = Query(None, description="The start date and time in UTC from which to get the inspections."),
    timestamp_end: str = Query(None, description="The end date and time in UTC for which to get the inspections."),
    flatten_json: Optional[bool] = Query(False, description="If true, flatten all inspections and return them as a flat list."),
    all_data: Optional[bool] = Query(False, description="If true, fetch all pages and aggregate all inspections into a single response.")
):
    headers = extract_and_validate_headers(request)
    params = build_params(
        limit=limit, offset=offset,
        timestamp_start=timestamp_start, timestamp_end=timestamp_end
    )
    url = f"{EXTERNAL_BASE_URL}/v1/vessel-port-state-control/{imo}"
    
    async def fetch_page(offset_value):
        page_params = params.copy()
        page_params["offset"] = offset_value
        async with httpx.AsyncClient() as client:
            return await client.get(url, headers=headers, params=page_params)
    
    if all_data:
        # Create a container for extra_info that can be accessed from the nested function
        extra_info_container = {}
        
        # Define a custom extractor that also captures vessel_information
        def extract_data_and_vessel_info(payload):
            if "data" in payload and isinstance(payload["data"], dict):
                if "vessel_information" in payload["data"]:
                    extra_info_container["vessel_information"] = payload["data"]["vessel_information"]
                return payload["data"].get("inspections", [])
            return []
        try:
            meta, all_inspections, _ = await paginate_all_data(
                fetch_page, limit, offset, "total_count", extract_data_and_vessel_info
            )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        # Get vessel_information from our container
        vessel_info = extra_info_container.get("vessel_information", {})
        vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
        
        if flatten_json:
            flat_inspections = []
            for inspection in all_inspections:
                # Create a flattened inspection
                flat_inspection = flatten_dict(inspection)
                # Add vessel information
                flat_inspection.update(vessel_info_flat)
                flat_inspections.append(flat_inspection)
            return JSONResponse(content=flat_inspections, status_code=200)
        else:
            return JSONResponse(content={"meta": meta, "data": {"vessel_information": vessel_info, "inspections": all_inspections}}, status_code=200)
    
    # Not all_data: normal single page logic
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    
    if resp.status_code == 200 and flatten_json:
        try:
            payload = resp.json()
            if "data" in payload and isinstance(payload["data"], dict) and "inspections" in payload["data"]:
                inspections = payload["data"]["inspections"]
                vessel_info = payload["data"].get("vessel_information", {})
                vessel_info_flat = flatten_dict(vessel_info, parent_key="vessel_information") if vessel_info else {}
                
                flat_inspections = []
                for inspection in inspections:
                    # Create a flattened inspection
                    flat_inspection = flatten_dict(inspection)
                    # Add vessel information
                    flat_inspection.update(vessel_info_flat)
                    flat_inspections.append(flat_inspection)
                return JSONResponse(content=flat_inspections, status_code=200)
        except Exception:
            pass  # fallback to raw response
    
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
