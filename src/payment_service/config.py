from typing import Literal

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PAYMENTS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
    )

    database_url: PostgresDsn = Field(repr=False)
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=5, ge=0)
    db_pool_timeout: float = Field(default=10, gt=0)

    @field_validator("database_url")
    @classmethod
    def require_asyncpg(cls, url: PostgresDsn) -> PostgresDsn:
        if url.scheme != "postgresql+asyncpg":
            raise ValueError("database_url must use the postgresql+asyncpg driver")
        return url


class WorkerSettings(DatabaseSettings):
    rabbitmq_url: AmqpDsn = Field(repr=False)
    rabbitmq_connection_timeout: float = Field(default=10, gt=0)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    processing_retry_base: float = Field(default=1, ge=0.1, le=60)
    webhook_timeout: float = Field(default=10, gt=0, le=60)
    webhook_private_hosts: list[str] = Field(default_factory=list)
    outbox_poll_interval: float = Field(default=1, ge=0.1, le=60)
    outbox_publish_timeout: float = Field(default=10, gt=0, le=60)
    outbox_retry_base: float = Field(default=1, ge=0.1, le=60)
    outbox_retry_max: float = Field(default=60, ge=60, le=3600)


class Settings(WorkerSettings):
    model_config = SettingsConfigDict(extra="forbid")

    api_key: SecretStr = Field(min_length=1)
