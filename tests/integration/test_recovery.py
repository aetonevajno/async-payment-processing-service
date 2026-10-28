import asyncio
import logging
from functools import partial
from uuid import UUID

import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.db.session import create_session_factory
from payment_service.messaging.outbox import publish_next
from payment_service.messaging.publisher import publish_event
from payment_service.payments.processing import process_event
from payment_service.payments.webhook import deliver_webhook
from payment_service.workers.consumer import run_consumer
from tests.integration.test_consumer import Calls, next_event

pytestmark = pytest.mark.integration


async def test_http_success_before_failed_commit_is_safely_repeated(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    receiver: tuple[int, list[dict[str, object]]],
) -> None:
    port, requests = receiver
    settings = worker_settings.model_copy(update={"webhook_private_hosts": ["127.0.0.1"]})
    sessions = create_session_factory(engine)
    calls = Calls()
    process = partial(
        process_event,
        sessions=sessions,
        gateway=calls.gateway,
        webhook=partial(deliver_webhook, settings=settings),
        settings=settings,
    )
    await process(event_id)
    webhook_id = await next_event(engine, "payment.webhook", 1)
    async with sessions.begin() as session:
        payment = (await session.scalars(select(Payment))).one()
        payment.webhook_url = f"http://127.0.0.1:{port}/hook"
        await session.execute(
            text(
                "ALTER TABLE payments ADD CONSTRAINT test_block_delivery "
                "CHECK (webhook_delivered_at IS NULL)"
            )
        )
    try:
        with pytest.raises(IntegrityError):
            await process(webhook_id)
        async with sessions() as session:
            payment = (await session.scalars(select(Payment))).one()
            assert payment.status == PaymentStatus.SUCCEEDED
            assert payment.webhook_attempts == 0 and payment.webhook_delivered_at is None
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                text("ALTER TABLE payments DROP CONSTRAINT test_block_delivery")
            )
    await process(webhook_id)
    await process(event_id)
    assert calls.gateway_calls == 1
    assert len(requests) == 2 and requests[0]["body"] == requests[1]["body"]
    async with sessions() as session:
        payment = (await session.scalars(select(Payment))).one()
        assert payment.webhook_attempts == 1 and payment.webhook_delivered_at


async def test_retry_insertion_failure_rolls_back_attempt(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    calls.gateway_fails = True
    process = partial(
        process_event,
        sessions=sessions,
        gateway=calls.gateway,
        webhook=calls.webhook,
        settings=worker_settings,
    )
    async with engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT test_no_retry CHECK (attempt = 1)")
        )
    try:
        with pytest.raises(IntegrityError):
            await process(event_id)
        async with sessions() as session:
            payment = (await session.scalars(select(Payment))).one()
            assert payment.processing_attempts == 0 and payment.last_processing_error is None
            assert len((await session.scalars(select(OutboxEvent))).all()) == 1
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE outbox DROP CONSTRAINT test_no_retry"))
    await process(event_id)
    assert await next_event(engine, "payment.created", 2)


async def test_consumer_recovers_after_database_rejects_connections(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    broker: RabbitBroker,
) -> None:
    failure_seen = asyncio.Event()

    class FailureSignal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "Consumer transaction failed" in record.getMessage():
                failure_seen.set()

    signal = FailureSignal()
    consumer_logger = logging.getLogger("payment_service.workers.consumer")
    sessions = create_session_factory(engine)
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        await publish_event(event, broker=broker, settings=worker_settings)
    admin_engine = create_async_engine(engine.url.set(database="postgres"))
    try:
        consumer_logger.addHandler(signal)
        async with admin_engine.connect() as administration:
            await administration.execute(
                text("ALTER DATABASE payments_test ALLOW_CONNECTIONS false")
            )
            await administration.commit()
            consumer = asyncio.create_task(run_consumer(worker_settings))
            try:
                async with asyncio.timeout(10):
                    await failure_seen.wait()
                await administration.execute(
                    text("ALTER DATABASE payments_test ALLOW_CONNECTIONS true")
                )
                await administration.commit()
                async with asyncio.timeout(12):
                    while True:
                        async with sessions() as session:
                            payment = (await session.scalars(select(Payment))).one()
                            if payment.processed_at:
                                assert payment.processing_attempts == 1
                                break
                        await asyncio.sleep(0.05)
            finally:
                await administration.execute(
                    text("ALTER DATABASE payments_test ALLOW_CONNECTIONS true")
                )
                await administration.commit()
                consumer.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await consumer
    finally:
        consumer_logger.removeHandler(signal)
        await admin_engine.dispose()


async def test_pending_webhook_survives_consumer_restart(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    broker: RabbitBroker,
    receiver: tuple[int, list[dict[str, object]]],
) -> None:
    port, requests = receiver
    settings = worker_settings.model_copy(update={"webhook_private_hosts": ["127.0.0.1"]})
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = (await session.scalars(select(Payment))).one()
        payment.webhook_url = f"http://127.0.0.1:{port}/recover"
    send = partial(publish_event, broker=broker, settings=settings)
    consumer = asyncio.create_task(run_consumer(settings))
    try:
        async with asyncio.timeout(12):
            while True:
                await publish_next(sessions, send, settings)
                async with sessions() as session:
                    payment = (await session.scalars(select(Payment))).one()
                    if payment.webhook_attempts == 1:
                        assert payment.webhook_delivered_at is None
                        processed_at = payment.processed_at
                        break
                await asyncio.sleep(0.05)
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
    consumer = asyncio.create_task(run_consumer(settings))
    try:
        async with asyncio.timeout(10):
            while True:
                await publish_next(sessions, send, settings)
                async with sessions() as session:
                    payment = (await session.scalars(select(Payment))).one()
                    if payment.webhook_delivered_at:
                        assert payment.processing_attempts == 1
                        assert payment.processed_at == processed_at
                        assert payment.webhook_attempts == 2
                        break
                await asyncio.sleep(0.05)
        assert len(requests) == 2 and requests[0]["body"] == requests[1]["body"]
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
