from pathlib import Path

import pytest
from pydantic import ValidationError

from payment_service.config import DatabaseSettings, Settings, WorkerSettings


@pytest.mark.usefixtures("settings_environment")
def test_settings_read_environment_without_exposing_credentials() -> None:
    settings = Settings()
    assert settings.api_key.get_secret_value() == "test-api-key"
    assert settings.database_url.scheme == "postgresql+asyncpg"
    assert "test-api-key" not in repr(settings)
    assert "payments:payments" not in repr(settings)


@pytest.mark.usefixtures("settings_environment")
@pytest.mark.parametrize("variable", ["API_KEY", "DATABASE_URL", "RABBITMQ_URL"])
def test_required_settings(variable: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(f"PAYMENTS_{variable}")
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.usefixtures("settings_environment")
@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("API_KEY", ""),
        ("DATABASE_URL", "postgresql://payments:payments@localhost/payments"),
        ("RABBITMQ_URL", "https://localhost/"),
        ("DB_POOL_SIZE", "0"),
        ("DB_MAX_OVERFLOW", "-1"),
        ("DB_POOL_TIMEOUT", "0"),
        ("RABBITMQ_CONNECTION_TIMEOUT", "0"),
        ("LOG_LEVEL", "INVALID"),
        ("GATEWAY_TIMEOUT", "0"),
        ("GATEWAY_TIMEOUT", "61"),
        ("OUTBOX_POLL_INTERVAL", "0"),
        ("OUTBOX_PUBLISH_TIMEOUT", "0"),
        ("OUTBOX_RETRY_BASE", "0"),
        ("OUTBOX_RETRY_MAX", "0"),
    ],
)
def test_invalid_settings(variable: str, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"PAYMENTS_{variable}", value)
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.usefixtures("settings_environment")
def test_environment_overrides_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("PAYMENTS_DB_POOL_SIZE=3\n", encoding="utf-8")
    assert Settings().db_pool_size == 3
    monkeypatch.setenv("PAYMENTS_DB_POOL_SIZE", "7")
    assert Settings().db_pool_size == 7


@pytest.mark.usefixtures("settings_environment")
def test_database_settings_do_not_require_api_or_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PAYMENTS_API_KEY")
    monkeypatch.delenv("PAYMENTS_RABBITMQ_URL")
    assert DatabaseSettings().database_url.scheme == "postgresql+asyncpg"


@pytest.mark.usefixtures("settings_environment")
def test_worker_settings_do_not_require_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PAYMENTS_API_KEY")
    assert WorkerSettings().outbox_publish_timeout == 10
