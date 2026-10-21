import shutil
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    if shutil.which("docker") is None:
        pytest.skip("PostgreSQL integration tests require Docker")
    probe = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=15)
    if probe.returncode != 0:
        pytest.skip("PostgreSQL integration tests require a running Docker daemon")
    container = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--detach",
            "--publish",
            "127.0.0.1::5432",
            "--env",
            "POSTGRES_PASSWORD=integration-test-only",
            "--env",
            "POSTGRES_DB=payments_test",
            "postgres:17-bookworm",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    ).stdout.strip()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            readiness = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                capture_output=True,
                timeout=5,
            )
            if readiness.returncode == 0:
                break
            time.sleep(0.2)
        else:
            pytest.fail("Test PostgreSQL did not become ready within 30 seconds")
        address = subprocess.run(
            ["docker", "port", container, "5432/tcp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        yield f"postgresql+asyncpg://postgres:integration-test-only@{address}/payments_test"
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container], check=True, capture_output=True, timeout=15
        )


@pytest.fixture(scope="session")
def migration_config(postgres_url: str) -> Config:
    repository = Path(__file__).resolve().parents[2]
    config = Config(str(repository / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


@pytest.fixture(scope="session")
def migrated_database(migration_config: Config) -> None:
    command.upgrade(migration_config, "head")


@pytest.fixture
async def engine(postgres_url: str, migrated_database: None) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE outbox, payments"))
        yield engine
    finally:
        await engine.dispose()
