import asyncio
from datetime import UTC, datetime, timedelta
from functools import partial
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.db.outbox import LeaseLost
from payment_service.db.session import create_session_factory
from payment_service.messaging.outbox import publish_next
from payment_service.payments.processing import EventInProgress, process_event
from tests.integration.test_consumer import Calls, next_event

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("phase", ["processing", "webhook", "publish"])
async def test_external_calls_release_connections_and_row_locks(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, phase: str
) -> None:
    single_connection = create_async_engine(engine.url, pool_size=1, max_overflow=0)
    sessions = create_session_factory(single_connection)
    calls = Calls()
    if phase == "webhook":
        await process_event(event_id, sessions, calls.gateway, calls.webhook, worker_settings)
        event_id = await next_event(engine, "payment.webhook", 1)

    async def check_locks() -> None:
        async with asyncio.timeout(1), sessions.begin() as session:
            event = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.id == event_id)
                    .with_for_update(nowait=True)
                )
            ).one()
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update(nowait=True)
            )
            assert payment is not None

    async def gateway(payment: Payment) -> PaymentStatus:
        await check_locks()
        return PaymentStatus.SUCCEEDED

    async def webhook(payment: Payment) -> None:
        await check_locks()

    async def publish(event: OutboxEvent) -> None:
        await check_locks()

    try:
        if phase == "publish":
            assert await publish_next(sessions, publish, worker_settings)
        else:
            await process_event(event_id, sessions, gateway, webhook, worker_settings)
    finally:
        await single_connection.dispose()


@pytest.mark.parametrize("phase", ["processing", "webhook", "publish"])
async def test_active_claim_defers_duplicate_without_another_external_call(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, phase: str
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    entered = asyncio.Event()
    release = asyncio.Event()
    external_calls = 0
    if phase == "webhook":
        await process_event(event_id, sessions, calls.gateway, calls.webhook, worker_settings)
        event_id = await next_event(engine, "payment.webhook", 1)

    async def gateway(payment: Payment) -> PaymentStatus:
        await webhook(payment)
        return PaymentStatus.SUCCEEDED

    async def webhook(payment: Payment) -> None:
        nonlocal external_calls
        external_calls += 1
        entered.set()
        await release.wait()

    async def publish(event: OutboxEvent) -> None:
        nonlocal external_calls
        external_calls += 1
        entered.set()
        await release.wait()

    process = partial(process_event, event_id, sessions, gateway, webhook, worker_settings)
    publishing = partial(publish_next, sessions, publish, worker_settings)
    task = asyncio.create_task(publishing() if phase == "publish" else process())
    try:
        async with asyncio.timeout(3):
            await entered.wait()
            if phase == "publish":
                assert not await publishing()
            else:
                with pytest.raises(EventInProgress):
                    await process()
            assert external_calls == 1
    finally:
        release.set()
        await task


@pytest.mark.parametrize("phase", ["processing", "publish"])
async def test_expired_claim_is_recovered_and_stale_owner_cannot_save(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, phase: str
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def gateway(payment: Payment) -> PaymentStatus:
        entered.set()
        await release.wait()
        return PaymentStatus.FAILED

    async def publish(event: OutboxEvent) -> None:
        entered.set()
        await release.wait()

    async def confirmed(event: OutboxEvent) -> None:
        return None

    task = asyncio.create_task(
        publish_next(sessions, publish, worker_settings)
        if phase == "publish"
        else process_event(event_id, sessions, gateway, calls.webhook, worker_settings)
    )
    try:
        async with asyncio.timeout(3):
            await entered.wait()
            async with sessions.begin() as session:
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id == event_id)
                    .values({f"{phase}_until": datetime.now(UTC) - timedelta(seconds=1)})
                )
            if phase == "publish":
                assert await publish_next(sessions, confirmed, worker_settings)
            else:
                await process_event(
                    event_id, sessions, calls.gateway, calls.webhook, worker_settings
                )
    finally:
        release.set()
        with pytest.raises(LeaseLost):
            await task

    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        payment = (await session.scalars(select(Payment))).one()
        assert event is not None
        if phase == "publish":
            assert event.published_at is not None and event.publish_attempts == 1
        else:
            assert payment.status == PaymentStatus.SUCCEEDED
            assert payment.processing_attempts == 1
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2


@pytest.mark.parametrize("phase", ["processing", "publish"])
async def test_abandoned_claim_survives_restart_and_expires(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, phase: str
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent).values(
                {
                    f"{phase}_token": uuid4(),
                    f"{phase}_until": datetime.now(UTC) - timedelta(seconds=1),
                }
            )
        )
    if phase == "publish":

        async def confirmed(event: OutboxEvent) -> None:
            return None

        assert await publish_next(sessions, confirmed, worker_settings)
    else:
        await process_event(event_id, sessions, calls.gateway, calls.webhook, worker_settings)
        assert calls.gateway_calls == 1
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.processing_token is None and event.processing_until is None
        assert event.publish_token is None and event.publish_until is None


@pytest.mark.parametrize("phase", ["processing", "webhook", "publish"])
async def test_timed_out_external_call_schedules_retry_and_releases_claim(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, phase: str
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    settings = worker_settings.model_copy(
        update={"gateway_timeout": 0.01, "webhook_timeout": 0.01, "outbox_publish_timeout": 0.01}
    )
    if phase == "webhook":
        await process_event(event_id, sessions, calls.gateway, calls.webhook, settings)
        event_id = await next_event(engine, "payment.webhook", 1)

    async def gateway(payment: Payment) -> PaymentStatus:
        await asyncio.sleep(1)
        return PaymentStatus.SUCCEEDED

    async def webhook(payment: Payment) -> None:
        await asyncio.sleep(1)

    async def publish(event: OutboxEvent) -> None:
        await asyncio.sleep(1)

    if phase == "publish":
        assert await publish_next(sessions, publish, settings)
    else:
        await process_event(event_id, sessions, gateway, webhook, settings)
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        payment = (await session.scalars(select(Payment))).one()
        assert event is not None
        assert event.processing_token is event.publish_token is None
        if phase == "publish":
            assert event.published_at is None and event.last_error == "TimeoutError"
            assert event.available_at > datetime.now(UTC)
        else:
            assert (payment.processing_attempts, payment.webhook_attempts) == (
                1,
                1 if phase == "webhook" else 0,
            )
            retry = (
                await session.scalars(select(OutboxEvent).where(OutboxEvent.attempt == 2))
            ).one()
            assert retry.available_at > datetime.now(UTC)
