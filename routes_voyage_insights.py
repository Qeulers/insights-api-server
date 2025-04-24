import httpx
from fastapi import APIRouter, Request, Response, Path
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY

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
async def vessel_zone_and_port_events(request: Request, imo: str = Path(...)):
    return await proxy_request(request, "/v1/vessel-zone-and-port-events/{imo}", {"imo": imo})

@router.get("/vessel-ais-reporting-gaps/{imo}")
async def vessel_ais_reporting_gaps(request: Request, imo: str = Path(...)):
    return await proxy_request(request, "/v1/vessel-ais-reporting-gaps/{imo}", {"imo": imo})

@router.get("/vessel-sts-pairings/{imo}")
async def vessel_sts_pairings(request: Request, imo: str = Path(...)):
    return await proxy_request(request, "/v1/vessel-sts-pairings/{imo}", {"imo": imo})

@router.get("/vessel-positional-discrepancy/{imo}")
async def vessel_positional_discrepancy(request: Request, imo: str = Path(...)):
    return await proxy_request(request, "/v1/vessel-positional-discrepancy/{imo}", {"imo": imo})

@router.get("/vessel-port-state-control/{imo}")
async def vessel_port_state_control(request: Request, imo: str = Path(...)):
    return await proxy_request(request, "/v1/vessel-port-state-control/{imo}", {"imo": imo})
