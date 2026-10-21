import asyncio

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def test_migrations_round_trip(migration_config: Config, migrated_database: None) -> None:
    async def table_names() -> list[str]:
        url = migration_config.get_main_option("sqlalchemy.url")
        assert url is not None
        engine = create_async_engine(url)
        try:
            async with engine.connect() as connection:
                return await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        finally:
            await engine.dispose()

    command.check(migration_config)
    command.downgrade(migration_config, "base")
    assert asyncio.run(table_names()) == ["alembic_version"]
    command.upgrade(migration_config, "head")
    assert set(asyncio.run(table_names())) == {"alembic_version", "payments", "outbox"}
    command.check(migration_config)
