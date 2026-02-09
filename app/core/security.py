from fastapi import Header, HTTPException
from app.core.config import settings

def get_current_user(x_api_key: str = Header(...)):
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {
        "user_id": "internal-client",
        "plan": "standard",
        "rate_limit": settings.RATE_LIMIT_REQUESTS
    }
