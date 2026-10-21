from pathlib import Path

import pytest


@pytest.fixture
def settings_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in (
        "DB_POOL_SIZE",
        "DB_MAX_OVERFLOW",
        "DB_POOL_TIMEOUT",
        "RABBITMQ_CONNECTION_TIMEOUT",
        "LOG_LEVEL",
    ):
        monkeypatch.delenv(f"PAYMENTS_{name}", raising=False)
    monkeypatch.setenv("PAYMENTS_API_KEY", "test-api-key")
    monkeypatch.setenv(
        "PAYMENTS_DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost/payments"
    )
    monkeypatch.setenv("PAYMENTS_RABBITMQ_URL", "amqp://payments:payments@localhost/")
