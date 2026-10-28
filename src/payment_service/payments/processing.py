import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment, PaymentStatus
from payment_service.db.outbox import LeaseLost, OutboxRepository
from payment_service.db.payments import PaymentRepository
from payment_service.payments.gateway import GatewayUnavailable
from payment_service.payments.webhook import WebhookDeliveryError

logger = logging.getLogger(__name__)

type Gateway = Callable[[Payment], Awaitable[PaymentStatus]]
type Webhook = Callable[[Payment], Awaitable[None]]


class InvalidEvent(Exception):
    """The message does not identify a supported durable event"""


class EventInProgress(Exception):
    """A delivery must be retried while another worker owns the event"""


def needs_attempt(payment: Payment, event: OutboxEvent) -> bool:
    if event.event_type == "payment.created":
        return (
            payment.status == PaymentStatus.PENDING
            and not payment.processing_exhausted
            and event.attempt == payment.processing_attempts + 1
        )
    if payment.status == PaymentStatus.PENDING:
        raise InvalidEvent("webhook_before_processing")
    return (
        payment.webhook_delivered_at is None
        and not payment.webhook_exhausted
        and event.attempt == payment.webhook_attempts + 1
    )


def schedule_retry(
    outbox: OutboxRepository,
    event: OutboxEvent,
    reason: str,
    settings: WorkerSettings,
) -> None:
    exhausted = event.attempt == 3
    outbox.schedule(
        event.payment_id,
        "payment.dead" if exhausted else event.event_type,
        event.attempt if exhausted else event.attempt + 1,
        0 if exhausted else settings.processing_retry_base * 2 ** (event.attempt - 1),
        reason,
    )
    logger.warning("Payment %s attempt %s failed: %s", event.payment_id, event.attempt, reason)


async def call_gateway(
    payment: Payment, gateway: Gateway, settings: WorkerSettings
) -> PaymentStatus:
    try:
        async with asyncio.timeout(settings.gateway_timeout):
            status = await gateway(payment)
    except TimeoutError as error:
        raise GatewayUnavailable("gateway_timeout") from error
    if status not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
        raise ValueError("Gateway returned a nonterminal outcome")
    return status


def complete_attempt(
    payment: Payment,
    event: OutboxEvent,
    outbox: OutboxRepository,
    status: PaymentStatus | None,
    failure: str | None,
    settings: WorkerSettings,
) -> None:
    if event.event_type == "payment.created":
        payment.processing_attempts = event.attempt
        payment.last_processing_error = failure
        if failure is not None:
            payment.processing_exhausted = event.attempt == 3
            schedule_retry(outbox, event, "processing:" + failure, settings)
        else:
            assert status is not None
            payment.status = status
            payment.processed_at = datetime.now(UTC)
            outbox.schedule(payment.id, "payment.webhook")
    else:
        payment.webhook_attempts = event.attempt
        payment.last_webhook_error = failure
        if failure is not None:
            payment.webhook_exhausted = event.attempt == 3
            schedule_retry(outbox, event, "webhook:" + failure, settings)
        else:
            payment.webhook_delivered_at = datetime.now(UTC)


async def process_event(
    event_id: UUID,
    sessions: async_sessionmaker[AsyncSession],
    gateway: Gateway,
    webhook: Webhook,
    settings: WorkerSettings,
) -> None:
    token = uuid4()
    async with sessions.begin() as session:
        event = await OutboxRepository(session).get(event_id, lock=True)
        if event is None or event.event_type not in {"payment.created", "payment.webhook"}:
            raise InvalidEvent("unknown_event")
        payment = await PaymentRepository(session).get(event.payment_id, lock=True)
        if payment is None:
            raise InvalidEvent("unknown_payment")
        if not needs_attempt(payment, event):
            return
        if event.processing_until is not None and event.processing_until > datetime.now(UTC):
            raise EventInProgress(str(event_id))
        event.processing_token = token
        event.processing_until = datetime.now(UTC) + timedelta(
            seconds=max(settings.gateway_timeout, settings.webhook_timeout) + 5
        )

    try:
        status = None
        failure = None
        if event.event_type == "payment.created":
            try:
                status = await call_gateway(payment, gateway, settings)
            except GatewayUnavailable as error:
                failure = type(error).__name__
        else:
            try:
                async with asyncio.timeout(settings.webhook_timeout):
                    await webhook(payment)
            except (WebhookDeliveryError, TimeoutError) as error:
                failure = str(error) if isinstance(error, WebhookDeliveryError) else "TimeoutError"

        async with sessions.begin() as session:
            outbox = OutboxRepository(session)
            current_event = await outbox.get(event_id, lock=True)
            if current_event is None or current_event.processing_token != token:
                raise LeaseLost(str(event_id))
            current_payment = await PaymentRepository(session).get(event.payment_id, lock=True)
            if current_payment is None:
                raise InvalidEvent("unknown_payment")
            complete_attempt(current_payment, current_event, outbox, status, failure, settings)
    finally:
        async with sessions.begin() as session:
            await OutboxRepository(session).release_processing(event_id, token)
