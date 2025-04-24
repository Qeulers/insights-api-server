import httpx
from fastapi import APIRouter, Request, Response, Path
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional

router = APIRouter(prefix="/notifications", tags=["Zone Port Notifications"])
EXTERNAL_BASE_URL = "https://api.polestar-production.com/notifications"

async def proxy_request(request: Request, method: str, path: str, path_params: dict = None):
    path_params = path_params or {}
    url = f"{EXTERNAL_BASE_URL}{path.format(**path_params)}"
    headers = dict(request.headers)
    headers.pop("host", None)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method,
                url,
                params=request.query_params,
                content=await request.body() if method in ("POST", "PUT") else None,
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

@router.get("/zones-and-ports")
async def get_zones_and_ports(request: Request):
    return await proxy_request(request, "GET", "/v1/zones-and-ports")

@router.post("/zones-and-ports")
async def post_zones_and_ports(request: Request):
    return await proxy_request(request, "POST", "/v1/zones-and-ports")

@router.get("/zones-and-ports/{id}")
async def get_zone_and_port(request: Request, id: str = Path(...)):
    return await proxy_request(request, "GET", "/v1/zones-and-ports/{id}", {"id": id})

@router.put("/zones-and-ports/{id}")
async def put_zone_and_port(request: Request, id: str = Path(...)):
    return await proxy_request(request, "PUT", "/v1/zones-and-ports/{id}", {"id": id})

@router.delete("/zones-and-ports/{id}")
async def delete_zone_and_port(request: Request, id: str = Path(...)):
    return await proxy_request(request, "DELETE", "/v1/zones-and-ports/{id}", {"id": id})

@router.get("/zones-and-ports/{id}/notifications")
async def get_zone_and_port_notifications(request: Request, id: str = Path(...)):
    return await proxy_request(request, "GET", "/v1/zones-and-ports/{id}/notifications", {"id": id})
