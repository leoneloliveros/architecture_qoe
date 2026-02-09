from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def qoe_health():
    return {"status": "qoe api ok"}
