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
    media_dir: str = "./media"
    telephony_webhook_token: str = ""
    openai_api_key: str = ""
    openai_base_url: str = "https://api.openai.com/v1"
    transcription_model: str = "whisper-1"
    parser_scheduler_enabled: bool = True
    parser_poll_interval_minutes: int = 1440
    parser_request_timeout_sec: int = 25
    parser_max_items_per_source: int = 10000
    parser_detail_fetch_limit: int = 10
    parser_mirror_fallback_enabled: bool = True
    parser_mirror_base_url: str = "https://r.jina.ai/http://"
    avito_api_base_url: str = "https://api.avito.ru"
    avito_token_url: str = "https://api.avito.ru/token"
    avito_client_id: str = ""
    avito_client_secret: str = ""
    avito_user_id: str = ""
    avito_request_timeout_sec: int = 25
    telegram_api_id: str = ""
    telegram_api_hash: str = ""
    telegram_session_string: str = ""
    telegram_channel_discovery_limit: int = 10000
    telegram_search_limit_per_query: int = 10000
    telegram_search_days_back: int = 30

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
