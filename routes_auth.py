import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from starlette.status import HTTP_502_BAD_GATEWAY

EXTERNAL_BASE_URL = "https://api.polestar-production.com"
router = APIRouter(prefix="/account/v1/auth", tags=["Authentication"])

async def proxy_request(request: Request, endpoint: str) -> Response:
    method = request.method
    url = f"{EXTERNAL_BASE_URL}{endpoint}"
    headers = dict(request.headers)
    # Remove host header to avoid conflicts
    headers.pop("host", None)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                content=await request.body()
            )
    except httpx.RequestError as e:
        return JSONResponse(status_code=HTTP_502_BAD_GATEWAY, content={"detail": str(e)})
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() != "content-encoding"},
        media_type=resp.headers.get("content-type")
    )

@router.post("/signin")
async def signin(request: Request):
    return await proxy_request(request, "/account/v1/auth/signin")

@router.put("/access-token-refresh")
async def refresh_token(request: Request):
    return await proxy_request(request, "/account/v1/auth/access-token-refresh")

@router.post("/password-reset/otp")
async def send_password_reset_otp(request: Request):
    return await proxy_request(request, "/account/v1/auth/password-reset/otp")

@router.post("/password-reset")
async def reset_password(request: Request):
    return await proxy_request(request, "/account/v1/auth/password-reset")
