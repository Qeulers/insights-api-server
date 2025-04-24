import httpx
from fastapi import APIRouter, Request, Response, Path
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY
from typing import Optional

router = APIRouter(prefix="/notifications", tags=["Vessel Notifications"])
EXTERNAL_BASE_URL = "https://event-notification-service-api.polestar-production.com/notifications"

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

@router.get("/vessels")
async def get_vessels(request: Request):
    return await proxy_request(request, "GET", "/v1/vessels")

@router.post("/vessels")
async def post_vessels(request: Request):
    return await proxy_request(request, "POST", "/v1/vessels")

@router.get("/vessels/{id}")
async def get_vessel(request: Request, id: str = Path(...)):
    return await proxy_request(request, "GET", "/v1/vessels/{id}", {"id": id})

@router.put("/vessels/{id}")
async def put_vessel(request: Request, id: str = Path(...)):
    return await proxy_request(request, "PUT", "/v1/vessels/{id}", {"id": id})

@router.delete("/vessels/{id}")
async def delete_vessel(request: Request, id: str = Path(...)):
    return await proxy_request(request, "DELETE", "/v1/vessels/{id}", {"id": id})

@router.get("/vessels/{id}/notifications")
async def get_vessel_notifications(request: Request, id: str = Path(...)):
    return await proxy_request(request, "GET", "/v1/vessels/{id}/notifications", {"id": id})
