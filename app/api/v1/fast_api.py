from fastapi import APIRouter, Depends
from app.core.security import get_current_user

router = APIRouter()

@router.get("/health")
def fast_health():
    return {"status": "fast api ok"}
