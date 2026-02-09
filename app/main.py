# app/main.py
from fastapi import FastAPI, Request, Depends
from app.api.v1.router import api_router
from app.core.config import settings
from app.core.security import get_current_user
from app.core.rate_limit import rate_limit
from app.core.observability import log_request
import time

app = FastAPI(
    title=settings.API_NAME,
    version=settings.API_VERSION
)

@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    return response

app.include_router(
    api_router,
    prefix="/api/v1",
    dependencies=[Depends(get_current_user)]
)
