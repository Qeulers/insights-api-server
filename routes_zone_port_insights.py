import httpx
from fastapi import APIRouter, Request, Response, Query
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY

EXTERNAL_BASE_URL = "https://api.polestar-production.com/zone-port-insights"
router = APIRouter(prefix="/zone-port-insights", tags=["Zone Port Insights"])

# /v1/zones search endpoint
@router.get("/zones")
async def search_zones(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="The number of records to skip. Works with limit."),
    name_contains: str = Query(None, description="Name or partial name of the port/zone to search for."),
    unlocode: str = Query(None, description="Filter by port UNLOCODE."),
    country_code: str = Query(None, description="Filter by three-letter country code."),
    sub_division_code: str = Query(None, description="Filter by sub division code."),
    wpi_number: int = Query(None, description="Filter by World Port Index number."),
    type: str = Query(None, description="Type of zone/port. See API docs for allowed values."),
    sub_type: str = Query(None, description="Sub type of zone/port. See API docs for allowed values.")
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
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

# /v1/zone-and-port-traffic/{id_type}/{id} endpoint
@router.get("/v1/zone-and-port-traffic/{id_type}/{id}")
async def zone_port_traffic(request: Request, id_type: str, id: str):
    headers = dict(request.headers)
    headers.pop("host", None)
    if "authorization" not in {k.lower() for k in headers}:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization header"})
    params = dict(request.query_params)
    url = f"{EXTERNAL_BASE_URL}/v1/zone-and-port-traffic/{id_type}/{id}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )
