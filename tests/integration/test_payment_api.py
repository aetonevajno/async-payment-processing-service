import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx2 import ASGITransport, AsyncClient
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.types import Receive, Scope, Send

from payment_service.api.app import create_app
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.db.session import create_session_factory

pytestmark = pytest.mark.integration


@pytest.fixture
def payment_body() -> dict[str, object]:
    return {
        "amount": "1234567890123456.78",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42, "gift": False},
        "webhook_url": "https://merchant.example/webhook",
    }


@pytest.fixture
async def client(
    engine: AsyncEngine,
    postgres_url: str,
    settings_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[AsyncClient]:
    monkeypatch.setenv("PAYMENTS_DATABASE_URL", postgres_url)
    app = create_app()
    async with app.router.lifespan_context(app) as state:
        assert state is not None

        async def app_with_state(scope: Scope, receive: Receive, send: Send) -> None:
            scope["state"] = dict(state)
            await app(scope, receive, send)

        async with AsyncClient(
            transport=ASGITransport(app=app_with_state),
            base_url="http://testserver",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "order-42"},
        ) as client:
            yield client


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
async def test_create_and_get_payment(
    client: AsyncClient, engine: AsyncEngine, payment_body: dict[str, object], currency: str
) -> None:
    payment_body["currency"] = currency
    created = await client.post("/api/v1/payments", json=payment_body)
    assert created.status_code == 202
    accepted = created.json()
    assert set(accepted) == {"payment_id", "status", "created_at"}
    assert accepted["status"] == "pending"
    assert datetime.fromisoformat(accepted["created_at"]).utcoffset() is not None
    response = await client.get(f"/api/v1/payments/{accepted['payment_id']}")
    assert response.status_code == 200
    assert response.json() == payment_body | accepted | {
        "idempotency_key": "order-42",
        "processed_at": None,
    }
    async with create_session_factory(engine)() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
        assert event.payment_id == UUID(accepted["payment_id"])
        assert event.event_type == "payment.created"
        assert event.payload == {"payment_id": accepted["payment_id"]}
        assert event.published_at is None


async def test_get_missing_payment(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid4()}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


async def test_replay_returns_existing_payment_without_another_event(
    client: AsyncClient, engine: AsyncEngine, payment_body: dict[str, object]
) -> None:
    first = await client.post("/api/v1/payments", json=payment_body)
    second = await client.post("/api/v1/payments", json=payment_body)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    async with create_session_factory(engine)() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("amount", "10.00"),
        ("currency", "USD"),
        ("description", "Changed order"),
        ("metadata", {"order_id": 42, "gift": 0}),
        ("webhook_url", "https://another.example/webhook"),
    ],
)
async def test_key_with_different_body_conflicts(
    client: AsyncClient, payment_body: dict[str, object], field: str, replacement: object
) -> None:
    first = await client.post("/api/v1/payments", json=payment_body)
    assert first.status_code == 202
    second = await client.post("/api/v1/payments", json=payment_body | {field: replacement})
    assert second.status_code == 409
    saved = await client.get(f"/api/v1/payments/{first.json()['payment_id']}")
    assert saved.json()[field] == payment_body[field]


async def test_equivalent_normalized_body_is_idempotent(
    client: AsyncClient, payment_body: dict[str, object]
) -> None:
    first = await client.post("/api/v1/payments", json=payment_body | {"amount": "10.0"})
    second = await client.post(
        "/api/v1/payments",
        json=payment_body
        | {
            "amount": "10.00",
            "metadata": {"gift": False, "order_id": 42},
            "webhook_url": "https://MERCHANT.example:443/webhook",
        },
    )
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()


@pytest.mark.parametrize("different_body", [False, True])
async def test_concurrent_requests_create_one_payment_and_event(
    client: AsyncClient, engine: AsyncEngine, payment_body: dict[str, object], different_body: bool
) -> None:
    first, second = await asyncio.gather(
        client.post("/api/v1/payments", json=payment_body),
        client.post(
            "/api/v1/payments",
            json=payment_body | ({"description": "Another order"} if different_body else {}),
        ),
    )
    assert sorted([first.status_code, second.status_code]) == (
        [202, 409] if different_body else [202, 202]
    )
    if not different_body:
        assert first.json() == second.json()
    async with create_session_factory(engine)() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_replay_does_not_reset_completed_payment(
    client: AsyncClient, engine: AsyncEngine, payment_body: dict[str, object]
) -> None:
    first = await client.post("/api/v1/payments", json=payment_body)
    processed_at = datetime.now(UTC)
    async with create_session_factory(engine).begin() as session:
        await session.execute(
            update(Payment).values(status=PaymentStatus.SUCCEEDED, processed_at=processed_at)
        )
    second = await client.post("/api/v1/payments", json=payment_body)
    assert second.status_code == 202
    assert second.json() == first.json() | {"status": "succeeded"}
    fetched = await client.get(f"/api/v1/payments/{first.json()['payment_id']}")
    assert datetime.fromisoformat(fetched.json()["processed_at"]) == processed_at


async def test_outbox_error_rolls_back_api_creation(
    client: AsyncClient,
    engine: AsyncEngine,
    payment_body: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT test_outbox_failure CHECK (false)")
        )
    try:
        response = await client.post("/api/v1/payments", json=payment_body)
        assert response.status_code == 500
        assert response.json() == {"detail": "Database operation failed"}
        assert "Database operation failed (IntegrityError)" in caplog.text
        assert "Failing row contains" not in caplog.text
        async with create_session_factory(engine)() as session:
            assert await session.scalar(select(func.count()).select_from(Payment)) == 0
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE outbox DROP CONSTRAINT test_outbox_failure"))
    retried = await client.post("/api/v1/payments", json=payment_body)
    assert retried.status_code == 202


async def test_metadata_defaults_to_empty_object(
    client: AsyncClient, payment_body: dict[str, object]
) -> None:
    del payment_body["metadata"]
    response = await client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 202
    fetched = await client.get(f"/api/v1/payments/{response.json()['payment_id']}")
    assert fetched.json()["metadata"] == {}
