from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.payments.gateway import GatewayUnavailable
from payment_service.payments.webhook import WebhookDeliveryError

type Gateway = Callable[[Payment], Awaitable[PaymentStatus]]
type Webhook = Callable[[Payment], Awaitable[None]]


class InvalidEvent(Exception):
    """The message does not identify a supported durable event"""


def schedule_event(
    session: AsyncSession,
    payment: Payment,
    event_type: str,
    attempt: int,
    delay: float = 0,
    reason: str | None = None,
) -> None:
    session.add(
        OutboxEvent(
            payment_id=payment.id,
            event_type=event_type,
            attempt=attempt,
            payload={"payment_id": str(payment.id), **({"reason": reason} if reason else {})},
            available_at=datetime.now(UTC) + timedelta(seconds=delay),
        )
    )


async def process_gateway(
    session: AsyncSession,
    payment: Payment,
    event: OutboxEvent,
    gateway: Gateway,
    settings: WorkerSettings,
) -> None:
    if payment.status != PaymentStatus.PENDING or payment.processing_exhausted:
        return
    if event.attempt != payment.processing_attempts + 1:
        return
    payment.processing_attempts = event.attempt
    try:
        status = await gateway(payment)
        if status not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
            raise ValueError("Gateway returned a nonterminal outcome")
    except GatewayUnavailable as error:
        payment.last_processing_error = type(error).__name__
        payment.processing_exhausted = event.attempt == 3
        schedule_event(
            session,
            payment,
            "payment.dead" if payment.processing_exhausted else "payment.created",
            event.attempt if payment.processing_exhausted else event.attempt + 1,
            0 if event.attempt == 3 else settings.processing_retry_base * 2 ** (event.attempt - 1),
            "processing:" + type(error).__name__,
        )
    else:
        payment.status = status
        payment.processed_at = datetime.now(UTC)
        payment.last_processing_error = None
        schedule_event(session, payment, "payment.webhook", 1)


async def process_webhook(
    session: AsyncSession,
    payment: Payment,
    event: OutboxEvent,
    webhook: Webhook,
    settings: WorkerSettings,
) -> None:
    if payment.status == PaymentStatus.PENDING:
        raise InvalidEvent("webhook_before_processing")
    if payment.webhook_delivered_at or payment.webhook_exhausted:
        return
    if event.attempt != payment.webhook_attempts + 1:
        return
    payment.webhook_attempts = event.attempt
    try:
        await webhook(payment)
    except WebhookDeliveryError as error:
        payment.last_webhook_error = str(error)
        payment.webhook_exhausted = event.attempt == 3
        schedule_event(
            session,
            payment,
            "payment.dead" if payment.webhook_exhausted else "payment.webhook",
            event.attempt if payment.webhook_exhausted else event.attempt + 1,
            0 if event.attempt == 3 else settings.processing_retry_base * 2 ** (event.attempt - 1),
            "webhook:" + str(error),
        )
    else:
        payment.webhook_delivered_at = datetime.now(UTC)
        payment.last_webhook_error = None


async def process_event(
    event_id: UUID,
    sessions: async_sessionmaker[AsyncSession],
    gateway: Gateway,
    webhook: Webhook,
    settings: WorkerSettings,
) -> None:
    async with sessions.begin() as session:
        event = await session.get(OutboxEvent, event_id)
        if event is None or event.event_type not in {"payment.created", "payment.webhook"}:
            raise InvalidEvent("unknown_event")
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise InvalidEvent("unknown_payment")
        if event.event_type == "payment.created":
            await process_gateway(session, payment, event, gateway, settings)
        else:
            await process_webhook(session, payment, event, webhook, settings)
