# app/core/config.py
import os

class Settings:
    API_NAME = os.getenv("API_NAME", "Plataforma APIs Negocio")
    API_VERSION = os.getenv("API_VERSION", "1.0.0")

    AUTH_MODE = os.getenv("AUTH_MODE", "api_key")  
    API_KEY = os.getenv("INTERNAL_API_KEY")

    RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
    RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # segundos

settings = Settings()
