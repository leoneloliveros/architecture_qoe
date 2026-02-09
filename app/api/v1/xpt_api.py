from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def xpt_health():
    return {"status": "xpt api ok"}
