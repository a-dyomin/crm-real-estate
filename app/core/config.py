from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "CRE CRM"
    app_env: str = "dev"
    database_url: str = "sqlite:///./crm_phase1.db"
    api_prefix: str = "/api/v1"
    secret_key: str = "change-me"
    access_token_ttl_minutes: int = 720
    default_admin_email: str = "admin@crecrm.app"
    default_admin_password: str = "admin123"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
