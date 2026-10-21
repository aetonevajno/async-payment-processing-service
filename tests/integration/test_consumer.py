import asyncio
import logging
from datetime import UTC, datetime
from functools import partial
from uuid import UUID

import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.db.session import create_session_factory
from payment_service.messaging.topology import DEAD_QUEUE
from payment_service.payments.gateway import GatewayUnavailable
from payment_service.payments.webhook import WebhookDeliveryError
from payment_service.workers.consumer import run_consumer
from payment_service.workers.outbox import publish_event, publish_next
from payment_service.workers.processing import process_event

pytestmark = pytest.mark.integration


class Calls:
    def __init__(self) -> None:
        self.gateway_calls = 0
        self.webhook_calls = 0
        self.gateway_fails = False
        self.webhook_fails = False
        self.status = PaymentStatus.SUCCEEDED

    async def gateway(self, payment: Payment) -> PaymentStatus:
        self.gateway_calls += 1
        if self.gateway_fails:
            raise GatewayUnavailable()
        return self.status

    async def webhook(self, payment: Payment) -> None:
        self.webhook_calls += 1
        if self.webhook_fails:
            raise WebhookDeliveryError("http_503")


async def next_event(engine: AsyncEngine, event_type: str, attempt: int) -> UUID:
    async with create_session_factory(engine).begin() as session:
        event = (
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == event_type, OutboxEvent.attempt == attempt
                )
            )
        ).one()
        event.available_at = datetime.now(UTC)
        return event.id


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_business_outcome_and_duplicate_delivery(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID, status: PaymentStatus
) -> None:
    sessions = create_session_factory(engine)
    calls = Calls()
    calls.status = status
    process = partial(
        process_event,
        sessions=sessions,
        gateway=calls.gateway,
        webhook=calls.webhook,
        settings=worker_settings,
    )
    await asyncio.gather(process(event_id), process(event_id))
    webhook_id = await next_event(engine, "payment.webhook", 1)
    await asyncio.gather(process(webhook_id), process(webhook_id))
    assert calls.gateway_calls == calls.webhook_calls == 1
    async with sessions() as session:
        payment = (await session.scalars(select(Payment))).one()
        assert payment.status == status
        assert payment.processed_at and payment.webhook_delivered_at
        assert payment.processing_attempts == payment.webhook_attempts == 1


@pytest.mark.parametrize("phase", ["processing", "webhook"])
async def test_three_attempts_then_durable_dlq(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    phase: str,
    broker: RabbitBroker,
) -> None:
    calls = Calls()
    calls.gateway_fails = phase == "processing"
    calls.webhook_fails = phase == "webhook"
    sessions = create_session_factory(engine)
    process = partial(
        process_event,
        sessions=sessions,
        gateway=calls.gateway,
        webhook=calls.webhook,
        settings=worker_settings,
    )
    if phase == "webhook":
        await process(event_id)
    kind = "payment.created" if phase == "processing" else "payment.webhook"
    for attempt in range(1, 4):
        current_id = await next_event(engine, kind, attempt)
        await process(current_id)
        await process(current_id)
        if attempt < 3:
            async with sessions() as session:
                scheduled = (
                    await session.scalars(
                        select(OutboxEvent).where(
                            OutboxEvent.event_type == kind, OutboxEvent.attempt == attempt + 1
                        )
                    )
                ).one()
                assert (scheduled.available_at - datetime.now(UTC)).total_seconds() > 0
    async with sessions() as session:
        payment = (await session.scalars(select(Payment))).one()
        assert payment.processing_exhausted == (phase == "processing")
        assert payment.webhook_exhausted == (phase == "webhook")
        assert calls.gateway_calls == (3 if phase == "processing" else 1)
        assert calls.webhook_calls == (0 if phase == "processing" else 3)
        dead = (
            await session.scalars(
                select(OutboxEvent).where(OutboxEvent.event_type == "payment.dead")
            )
        ).one()
        queue = await broker.declare_queue(DEAD_QUEUE)
        await queue.purge()
        await publish_event(dead, broker=broker, settings=worker_settings)
        message = await queue.get(timeout=2)
        assert message is not None and message.message_id == str(dead.id)
        await message.ack()


async def test_webhook_recovery_does_not_repeat_gateway(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    calls = Calls()
    sessions = create_session_factory(engine)
    process = partial(
        process_event,
        sessions=sessions,
        gateway=calls.gateway,
        webhook=calls.webhook,
        settings=worker_settings,
    )
    await process(event_id)
    calls.webhook_fails = True
    await process(await next_event(engine, "payment.webhook", 1))
    calls.webhook_fails = False
    await process(await next_event(engine, "payment.webhook", 2))
    await process(event_id)
    assert calls.gateway_calls == 1 and calls.webhook_calls == 2


async def test_cancelled_gateway_rolls_back_attempt(
    engine: AsyncEngine, worker_settings: WorkerSettings, event_id: UUID
) -> None:
    async def cancelled(payment: Payment) -> PaymentStatus:
        raise asyncio.CancelledError()

    sessions = create_session_factory(engine)
    with pytest.raises(asyncio.CancelledError):
        await process_event(event_id, sessions, cancelled, Calls().webhook, worker_settings)
    async with sessions() as session:
        payment = (await session.scalars(select(Payment))).one()
        assert payment.processing_attempts == 0
        assert payment.status == PaymentStatus.PENDING


async def test_real_consumer_with_outbox(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    broker: RabbitBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = Calls()
    delivered = asyncio.Event()

    async def webhook(payment: Payment, settings: WorkerSettings) -> None:
        await calls.webhook(payment)
        delivered.set()

    monkeypatch.setattr("payment_service.workers.consumer.simulate_payment", calls.gateway)
    monkeypatch.setattr("payment_service.workers.consumer.deliver_webhook", webhook)
    consumer = asyncio.create_task(run_consumer(worker_settings))
    sessions = create_session_factory(engine)
    send = partial(publish_event, broker=broker, settings=worker_settings)
    try:
        async with asyncio.timeout(10):
            while not delivered.is_set():
                await publish_next(sessions, send, worker_settings)
                await asyncio.sleep(0.05)
            while True:
                async with sessions() as session:
                    payment = (await session.scalars(select(Payment))).one()
                    if payment.webhook_delivered_at:
                        break
                await asyncio.sleep(0.05)
        assert calls.gateway_calls == calls.webhook_calls == 1
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer


async def test_consumer_delivers_real_http(
    engine: AsyncEngine,
    worker_settings: WorkerSettings,
    event_id: UUID,
    broker: RabbitBroker,
    receiver: tuple[int, list[dict[str, object]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    port, requests = receiver
    settings = worker_settings.model_copy(update={"webhook_private_hosts": ["127.0.0.1"]})
    sessions = create_session_factory(engine)
    async with sessions.begin() as session:
        payment = (await session.scalars(select(Payment))).one()
        payment.webhook_url = f"http://127.0.0.1:{port}/hook?token=merchant-secret"
    consumer = asyncio.create_task(run_consumer(settings))
    send = partial(publish_event, broker=broker, settings=settings)
    try:
        async with asyncio.timeout(12):
            while True:
                await publish_next(sessions, send, settings)
                async with sessions() as session:
                    payment = (await session.scalars(select(Payment))).one()
                    if payment.webhook_delivered_at:
                        assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
                        assert payment.processing_attempts == payment.webhook_attempts == 1
                        break
                await asyncio.sleep(0.05)
        assert "merchant-secret" not in caplog.text
        assert len(requests) == 1
        body = requests[0]["body"]
        assert isinstance(body, dict)
        assert body["payment_id"] == str(payment.id)
        assert body["status"] == payment.status.value
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer


async def test_poison_message_is_confirmed_to_dlq(
    worker_settings: WorkerSettings,
    broker: RabbitBroker,
) -> None:
    from aio_pika import Message

    from payment_service.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE

    queue = await broker.declare_queue(DEAD_QUEUE)
    await queue.purge()
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    consumer = asyncio.create_task(run_consumer(worker_settings))
    try:
        await exchange.publish(
            Message(b"{invalid json", content_type="application/json", message_id="invalid-id"),
            routing_key=PAYMENTS_QUEUE.routing_key,
        )
        async with asyncio.timeout(10):
            while True:
                message = await queue.get(fail=False)
                if message:
                    assert b"invalid_message_id" in message.body
                    assert message.message_id == "invalid-id"
                    await message.ack()
                    break
                await asyncio.sleep(0.05)
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
