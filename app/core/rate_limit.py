import time
from fastapi import HTTPException

_request_store = {}

def apply_rate_limit(user: dict, window: int):
    user_id = user["user_id"]
    limit = user["rate_limit"]

    now = time.time()
    window_start = now - window

    requests = _request_store.get(user_id, [])
    requests = [r for r in requests if r > window_start]

    if len(requests) >= limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded"
        )

    requests.append(now)
    _request_store[user_id] = requests
