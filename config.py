from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    KALSHI_ENV: str = "PROD"
    KALSHI_EMAIL: str

    # RSA Credentials
    KALSHI_API_KEY_ID: str
    KALSHI_PRIVATE_KEY_PATH: str

    # Allow extra fields in .env (like old password/timeout settings) without crashing
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
