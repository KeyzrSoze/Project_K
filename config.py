import os
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

    def model_post_init(self, __context):
        key_path = os.path.expanduser(self.KALSHI_PRIVATE_KEY_PATH)
        if not os.path.isabs(key_path):
            root = os.getenv("PROJECT_K_ROOT")
            if root:
                root = os.path.abspath(os.path.expanduser(root))
            else:
                root = os.path.abspath(os.path.dirname(__file__))
            key_path = os.path.abspath(os.path.join(root, key_path))
        self.KALSHI_PRIVATE_KEY_PATH = key_path
