import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.db.models import Currency, OutboxEvent, Payment, PaymentStatus
from payment_service.db.session import create_session_factory

pytestmark = pytest.mark.integration


def new_payment(key: str = "order-1") -> Payment:
    return Payment(
        amount=Decimal("1234567890123456.78"),
        currency=Currency.RUB,
        description="Test payment",
        payment_metadata={"order": {"id": 42}, "tags": ["test"]},
        idempotency_key=key,
        webhook_url="https://merchant.example/webhook",
    )


async def test_payment_and_outbox_commit_together(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = new_payment()
        session.add(payment)
        await session.flush()
        event = OutboxEvent(
            payment_id=payment.id,
            event_type="payment.created",
            payload={"payment_id": str(payment.id)},
        )
        session.add(event)
    async with sessions() as session:
        saved_payment = await session.get(Payment, payment.id)
        saved_event = await session.get(OutboxEvent, event.id)
        assert saved_payment is not None
        assert saved_event is not None
        assert saved_payment.amount == Decimal("1234567890123456.78")
        assert saved_payment.payment_metadata == {"order": {"id": 42}, "tags": ["test"]}
        assert saved_payment.status is PaymentStatus.PENDING
        assert saved_payment.processed_at is None
        assert saved_payment.created_at.utcoffset() == timedelta(0)
        assert saved_event.available_at.utcoffset() == timedelta(0)
        assert saved_event.payment_id == saved_payment.id
        assert saved_event.attempt == 1
        assert saved_event.publish_attempts == 0
        assert saved_event.published_at is None


async def test_outbox_failure_rolls_back_payment(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    with pytest.raises(IntegrityError, match="ck_outbox_attempt_positive"):
        async with sessions.begin() as session:
            payment = new_payment()
            session.add(payment)
            await session.flush()
            session.add(
                OutboxEvent(
                    payment_id=payment.id, event_type="payment.created", payload={}, attempt=0
                )
            )
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_concurrent_idempotency_key_allows_one_transaction(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    ready = asyncio.Barrier(2)

    async def insert_payment() -> bool:
        try:
            async with sessions.begin() as session:
                payment = new_payment("same-key")
                session.add(payment)
                await ready.wait()
                await session.flush()
                session.add(
                    OutboxEvent(payment_id=payment.id, event_type="payment.created", payload={})
                )
        except IntegrityError as error:
            assert "uq_payments_idempotency_key" in str(error)
            return False
        return True

    outcomes = await asyncio.gather(insert_payment(), insert_payment())
    assert sorted(outcomes) == [False, True]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-1"), Decimal("NaN")])
async def test_invalid_amount_rejected(engine: AsyncEngine, amount: Decimal) -> None:
    sessions = create_session_factory(engine)
    with pytest.raises(IntegrityError, match="ck_payments_amount_positive"):
        async with sessions.begin() as session:
            payment = new_payment()
            payment.amount = amount
            session.add(payment)


@pytest.mark.parametrize(
    ("assignment", "constraint"),
    [
        ("currency = 'GBP'", "ck_payments_currency"),
        ("status = 'unknown'", "ck_payments_status"),
        ("status = 'succeeded'", "ck_payments_status_processed_at"),
        ("processed_at = now()", "ck_payments_status_processed_at"),
        ("idempotency_key = '   '", "ck_payments_idempotency_key_not_blank"),
        ("metadata = '[]'::jsonb", "ck_payments_metadata_object"),
        (
            "status = 'succeeded', processed_at = created_at - interval '1 second'",
            "ck_payments_processed_after_creation",
        ),
    ],
)
async def test_database_enforces_payment_contract(
    engine: AsyncEngine, assignment: str, constraint: str
) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        session.add(new_payment())
    with pytest.raises(IntegrityError, match=constraint):
        async with engine.begin() as connection:
            await connection.execute(text(f"UPDATE payments SET {assignment}"))


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_terminal_status_and_timezone_round_trip(
    engine: AsyncEngine, status: PaymentStatus
) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = new_payment()
        session.add(payment)
    processed_at = datetime.now(timezone(timedelta(hours=3)))
    async with sessions.begin() as session:
        await session.execute(update(Payment).values(status=status, processed_at=processed_at))
    async with sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved is not None
        assert saved.status is status
        assert saved.processed_at == processed_at


async def test_outbox_rejects_missing_payment(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    with pytest.raises(IntegrityError, match="fk_outbox_payment_id_payments"):
        async with sessions.begin() as session:
            session.add(OutboxEvent(payment_id=uuid4(), event_type="payment.created", payload={}))


async def test_duplicate_attempt_rejected_but_next_attempt_allowed(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = new_payment()
        session.add(payment)
        await session.flush()
        session.add(OutboxEvent(payment_id=payment.id, event_type="payment.created", payload={}))
    with pytest.raises(IntegrityError, match="uq_outbox_logical_attempt"):
        async with sessions.begin() as session:
            session.add(
                OutboxEvent(payment_id=payment.id, event_type="payment.created", payload={})
            )
    async with sessions.begin() as session:
        session.add(
            OutboxEvent(payment_id=payment.id, event_type="payment.created", payload={}, attempt=2)
        )
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2


async def test_outbox_can_be_claimed_with_skip_locked(engine: AsyncEngine) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = new_payment()
        session.add(payment)
        await session.flush()
        session.add_all(
            [
                OutboxEvent(
                    payment_id=payment.id, event_type="payment.created", payload={}, attempt=n
                )
                for n in (1, 2)
            ]
        )
    query = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now())
        .order_by(OutboxEvent.available_at, OutboxEvent.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    async with sessions.begin() as first, sessions.begin() as second:
        first_event = (await first.scalars(query)).one()
        second_event = (await second.scalars(query)).one()
        assert first_event.id != second_event.id
