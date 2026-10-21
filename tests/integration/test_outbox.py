import asyncio
import json
import subprocess
from datetime import UTC, datetime, timedelta
from functools import partial
from uuid import UUID

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import ProbableAuthenticationError
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from pydantic import AmqpDsn
from sqlalchemy import func, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.db.session import create_session_factory
from payment_service.messaging.broker import create_broker
from payment_service.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_payment_topology,
)
from payment_service.workers.outbox import publish_event, publish_next, run_publisher

pytestmark = pytest.mark.integration


async def test_confirmed_publication(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    send = partial(publish_event, broker=broker, settings=worker_settings)
    assert await publish_next(sessions, send, worker_settings)
    assert not await publish_next(sessions, send, worker_settings)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    message = await queue.get(timeout=2)
    assert message is not None
    assert message.message_id == str(event_id)
    assert message.delivery_mode is DeliveryMode.PERSISTENT
    assert message.type == "payment.created"
    assert message.headers == {"attempt": 1}
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert json.loads(message.body) == {"payment_id": str(event.payment_id)}
        assert message.correlation_id == str(event.payment_id)
        assert event.published_at is not None
        assert event.publish_attempts == 1
        assert event.last_error is None
    await message.ack()


async def test_unroutable_event_is_retried_after_binding_restored(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.unbind(PAYMENTS_EXCHANGE.name, routing_key=PAYMENTS_QUEUE.routing_key)
    send = partial(publish_event, broker=broker, settings=worker_settings)
    assert await publish_next(sessions, send, worker_settings)
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.last_error == "PublishError"
        assert event.available_at > datetime.now(UTC)
    assert not await publish_next(sessions, send, worker_settings)
    await declare_payment_topology(broker)
    async with sessions.begin() as session:
        await session.execute(update(OutboxEvent).values(available_at=func.now()))
    assert await publish_next(sessions, send, worker_settings)
    message = await queue.get(timeout=2)
    assert message is not None
    await message.ack()
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is not None
        assert event.publish_attempts == 2
        assert event.last_error is None


async def test_cancellation_after_publish_causes_safe_duplicate(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)

    async def interrupted_publish(event: OutboxEvent) -> None:
        await publish_event(event, broker=broker, settings=worker_settings)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await publish_next(sessions, interrupted_publish, worker_settings)
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is None
        assert event.publish_attempts == 0
    assert await publish_next(
        sessions, partial(publish_event, broker=broker, settings=worker_settings), worker_settings
    )
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    for _ in range(2):
        message = await queue.get(timeout=2)
        assert message is not None
        assert message.message_id == str(event_id)
        await message.ack()


async def test_database_failure_after_confirmation_keeps_event(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    async with engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT test_no_publish CHECK (published_at IS NULL)")
        )
    try:
        with pytest.raises(IntegrityError, match="test_no_publish"):
            await publish_next(
                sessions,
                partial(publish_event, broker=broker, settings=worker_settings),
                worker_settings,
            )
        async with sessions() as session:
            event = await session.get(OutboxEvent, event_id)
            assert event is not None
            assert event.published_at is None
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("ALTER TABLE outbox DROP CONSTRAINT test_no_publish"))
    assert await publish_next(
        sessions, partial(publish_event, broker=broker, settings=worker_settings), worker_settings
    )
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    for _ in range(2):
        message = await queue.get(timeout=2)
        assert message is not None
        assert message.message_id == str(event_id)
        await message.ack()


async def test_two_publishers_skip_each_others_locked_events(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        first = await session.get(OutboxEvent, event_id)
        assert first is not None
        session.add(
            OutboxEvent(
                payment_id=first.payment_id,
                event_type=first.event_type,
                payload=first.payload,
                attempt=2,
            )
        )
    ready = asyncio.Barrier(2)

    async def synchronized_publish(event: OutboxEvent) -> None:
        await ready.wait()
        await publish_event(event, broker=broker, settings=worker_settings)

    async with asyncio.timeout(5):
        outcomes = await asyncio.gather(
            publish_next(sessions, synchronized_publish, worker_settings),
            publish_next(sessions, synchronized_publish, worker_settings),
        )
        assert all(outcomes)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    message_ids = set()
    for _ in range(2):
        message = await queue.get(timeout=2)
        assert message is not None
        message_ids.add(message.message_id)
        await message.ack()
    assert len(message_ids) == 2


async def test_deferred_events_are_not_published(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        await session.execute(
            update(OutboxEvent).values(available_at=datetime.now(UTC) + timedelta(days=1))
        )
    assert not await publish_next(
        sessions, partial(publish_event, broker=broker, settings=worker_settings), worker_settings
    )
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    assert await queue.get(fail=False) is None


async def test_retry_backoff_is_capped(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        await session.execute(update(OutboxEvent).values(publish_attempts=100000))

    async def unavailable(event: OutboxEvent) -> None:
        raise ConnectionError("test outage")

    before = datetime.now(UTC)
    assert await publish_next(sessions, unavailable, worker_settings)
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is None
        assert event.last_error == "ConnectionError"
        assert (
            before + timedelta(seconds=60)
            <= event.available_at
            <= datetime.now(UTC) + timedelta(seconds=60)
        )


async def test_broker_outage_does_not_lose_event(
    engine: AsyncEngine,
    broker: RabbitBroker,
    worker_settings: WorkerSettings,
    event_id: UUID,
    rabbitmq_server: tuple[str, str],
) -> None:
    sessions = create_session_factory(engine)
    container = rabbitmq_server[0]
    await asyncio.to_thread(
        subprocess.run,
        ["docker", "exec", container, "rabbitmqctl", "stop_app"],
        check=True,
        capture_output=True,
        timeout=30,
    )
    try:
        assert await publish_next(
            sessions,
            partial(
                publish_event,
                broker=broker,
                settings=worker_settings.model_copy(update={"outbox_publish_timeout": 0.2}),
            ),
            worker_settings,
        )
        async with sessions() as session:
            event = await session.get(OutboxEvent, event_id)
            assert event is not None
            assert event.published_at is None
    finally:
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "exec", container, "rabbitmqctl", "start_app"],
            check=True,
            capture_output=True,
            timeout=30,
        )
    # A fresh connection also exercises restart recovery without relying on in-memory state.
    recovered = create_broker(worker_settings)
    try:
        await recovered.connect()
        await declare_payment_topology(recovered)
        async with sessions.begin() as session:
            await session.execute(update(OutboxEvent).values(available_at=func.now()))
        assert await publish_next(
            sessions,
            partial(publish_event, broker=recovered, settings=worker_settings),
            worker_settings,
        )
        queue = await recovered.declare_queue(PAYMENTS_QUEUE)
        message = await queue.get(timeout=2)
        assert message is not None
        assert message.message_id == str(event_id)
        await message.ack()
    finally:
        await recovered.stop()


async def test_worker_loop_can_be_cancelled_after_publishing(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    task = asyncio.create_task(run_publisher(worker_settings))
    try:
        async with asyncio.timeout(5):
            while True:
                async with sessions() as session:
                    event = await session.get(OutboxEvent, event_id)
                    assert event is not None
                    if event.published_at is not None:
                        break
                await asyncio.sleep(0.05)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_missing_publisher_confirmation_is_not_success(
    engine: AsyncEngine, broker: RabbitBroker, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    sessions = create_session_factory(engine)
    unconfirmed_broker = RabbitBroker(
        str(worker_settings.rabbitmq_url), default_channel=Channel(publisher_confirms=False)
    )
    try:
        await unconfirmed_broker.connect()
        await declare_payment_topology(unconfirmed_broker)
        await publish_next(
            sessions,
            partial(publish_event, broker=unconfirmed_broker, settings=worker_settings),
            worker_settings,
        )
    finally:
        await unconfirmed_broker.stop()
    async with sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is None
        assert event.last_error == "PublishNotConfirmed"


async def test_invalid_broker_credentials_fail_without_endless_retry(
    worker_settings: WorkerSettings,
) -> None:
    invalid = worker_settings.model_copy(
        update={
            "rabbitmq_url": AmqpDsn(
                str(worker_settings.rabbitmq_url).replace("integration-test-only", "wrong")
            )
        }
    )
    async with asyncio.timeout(5):
        with pytest.raises(ProbableAuthenticationError):
            await run_publisher(invalid)
