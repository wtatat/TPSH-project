from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class Settings:
    bot_token: str
    openrouter_api_key: str
    openrouter_model: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    videos_json_path: str
    schema_sql_path: str
    db_connect_retries: int
    db_connect_retry_delay_seconds: float
    llm_timeout_seconds: int
    llm_max_retries: int
    llm_retry_base_delay_seconds: float
    llm_retry_max_delay_seconds: float

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def load_settings() -> Settings:
    settings = Settings(
        bot_token=os.getenv("BOT_TOKEN", "").strip(),
        openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "").strip(),
        openrouter_model=os.getenv(
            "OPENROUTER_MODEL", "qwen/qwen3-235b-a22b-thinking-2507"
        ).strip(),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost").strip(),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "videos_db").strip(),
        postgres_user=os.getenv("POSTGRES_USER", "postgres").strip(),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres").strip(),
        videos_json_path=os.getenv("VIDEOS_JSON_PATH", "videos.json").strip(),
        schema_sql_path=os.getenv("SCHEMA_SQL_PATH", "sql/schema.sql").strip(),
        db_connect_retries=int(os.getenv("DB_CONNECT_RETRIES", "30")),
        db_connect_retry_delay_seconds=float(
            os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "2")
        ),
        llm_timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "90")),
        llm_max_retries=int(os.getenv("LLM_MAX_RETRIES", "4")),
        llm_retry_base_delay_seconds=float(
            os.getenv("LLM_RETRY_BASE_DELAY_SECONDS", "1")
        ),
        llm_retry_max_delay_seconds=float(
            os.getenv("LLM_RETRY_MAX_DELAY_SECONDS", "8")
        ),
    )

    missing = []
    if not settings.bot_token:
        missing.append("BOT_TOKEN")
    if not settings.openrouter_api_key:
        missing.append("OPENROUTER_API_KEY")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    return settings
