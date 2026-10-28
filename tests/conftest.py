import asyncio
import json
from collections.abc import AsyncIterator
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
        "PROCESSING_RETRY_BASE",
        "GATEWAY_TIMEOUT",
        "WEBHOOK_TIMEOUT",
        "WEBHOOK_PRIVATE_HOSTS",
        "OUTBOX_POLL_INTERVAL",
        "OUTBOX_PUBLISH_TIMEOUT",
        "OUTBOX_RETRY_BASE",
        "OUTBOX_RETRY_MAX",
    ):
        monkeypatch.delenv(f"PAYMENTS_{name}", raising=False)
    monkeypatch.setenv("PAYMENTS_API_KEY", "test-api-key")
    monkeypatch.setenv(
        "PAYMENTS_DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost/payments"
    )
    monkeypatch.setenv("PAYMENTS_RABBITMQ_URL", "amqp://payments:payments@localhost/")


@pytest.fixture
async def receiver() -> AsyncIterator[tuple[int, list[dict[str, object]]]]:
    requests: list[dict[str, object]] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode()
            size = int(
                next(
                    line.split(":", 1)[1]
                    for line in headers.splitlines()
                    if line.lower().startswith("content-length:")
                )
            )
            body = json.loads(await reader.readexactly(size))
            requests.append({"headers": headers, "body": body})
            status = "503 Unavailable" if "/fail " in headers else "204 No Content"
            if "/recover " in headers and len(requests) == 1:
                status = "503 Unavailable"
            if "/redirect " in headers:
                status = "302 Found"
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Length: 0\r\nLocation: /hook\r\n\r\n".encode()
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server:
        yield server.sockets[0].getsockname()[1], requests
