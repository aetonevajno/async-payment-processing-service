import asyncio

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.messaging.topology import (
    DEAD_EXCHANGE,
    DEAD_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
)


class PublishNotConfirmed(Exception):
    """RabbitMQ did not acknowledge responsibility for a message"""


async def publish_event(
    event: OutboxEvent, *, broker: RabbitBroker, settings: WorkerSettings
) -> None:
    if event.event_type not in {"payment.created", "payment.webhook", "payment.dead"}:
        raise ValueError(f"Unsupported outbox event type: {event.event_type}")
    dead = event.event_type == "payment.dead"
    async with asyncio.timeout(settings.outbox_publish_timeout):
        confirmation = await broker.publish(
            event.payload,
            exchange=DEAD_EXCHANGE if dead else PAYMENTS_EXCHANGE,
            routing_key=DEAD_QUEUE.routing_key if dead else PAYMENTS_QUEUE.routing_key,
            persist=True,
            mandatory=True,
            timeout=settings.outbox_publish_timeout,
            message_id=str(event.id),
            correlation_id=str(event.payment_id),
            message_type=event.event_type,
            headers={"attempt": event.attempt},
        )
    if not isinstance(confirmation, Basic.Ack):
        raise PublishNotConfirmed()
