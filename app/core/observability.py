# app/core/observability.py
import time
import logging

logger = logging.getLogger("api")
logging.basicConfig(level=logging.INFO)

def log_request(path: str, user_id: str, start_time: float):
    duration = time.time() - start_time
    logger.info(
        f"path={path} user={user_id} duration={duration:.3f}s"
    )
