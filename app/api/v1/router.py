# app/api/v1/router.py
from fastapi import APIRouter
from .fast_api import router as fast_router
from .qoe_api import router as qoe_router
from .xpt_api import router as xpt_router

api_router = APIRouter()

api_router.include_router(fast_router, prefix="/fast", tags=["fast"])
api_router.include_router(qoe_router, prefix="/qoe", tags=["qoe"])
api_router.include_router(xpt_router, prefix="/xpt", tags=["xpt"])
