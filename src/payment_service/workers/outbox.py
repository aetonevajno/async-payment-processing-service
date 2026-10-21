import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from functools import partial

from aio_pika.exceptions import (
    AMQPException,
    ChannelInvalidStateError,
    IncompatibleProtocolError,
    ProbableAuthenticationError,
)
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.db.session import create_engine, create_session_factory
from payment_service.messaging.broker import create_broker
from payment_service.messaging.topology import (
    DEAD_EXCHANGE,
    DEAD_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_payment_topology,
)

logger = logging.getLogger(__name__)
type PublishEvent = Callable[[OutboxEvent], Awaitable[None]]


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


async def publish_next(
    sessions: async_sessionmaker[AsyncSession], publish: PublishEvent, settings: WorkerSettings
) -> bool:
    async with sessions.begin() as session:
        event = await session.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now())
            .order_by(OutboxEvent.available_at, OutboxEvent.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        event.publish_attempts += 1
        try:
            await publish(event)
        except (AMQPException, ChannelInvalidStateError, OSError, PublishNotConfirmed) as error:
            delay = min(
                settings.outbox_retry_max,
                settings.outbox_retry_base * 2 ** min(event.publish_attempts - 1, 16),
            )
            event.available_at = datetime.now(UTC) + timedelta(seconds=delay)
            event.last_error = type(error).__name__
            logger.warning("Outbox event %s postponed: %s", event.id, event.last_error)
        else:
            event.published_at = datetime.now(UTC)
            event.last_error = None
    return True


async def run_publisher(settings: WorkerSettings) -> None:
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    broker = create_broker(settings)
    publish = partial(publish_event, broker=broker, settings=settings)
    try:
        while True:
            try:
                await broker.connect()
                await declare_payment_topology(broker)
                break
            except (ProbableAuthenticationError, IncompatibleProtocolError):
                raise
            except (OSError, ChannelInvalidStateError) as error:
                logger.warning("RabbitMQ connection unavailable: %s", type(error).__name__)
                await broker.stop()
                await asyncio.sleep(settings.outbox_poll_interval)
        while True:
            try:
                if await publish_next(sessions, publish, settings):
                    continue
            except (DBAPIError, PoolTimeoutError, OSError) as error:
                if isinstance(error, DBAPIError):
                    sqlstate = str(getattr(error.orig, "sqlstate", ""))
                    if not error.connection_invalidated and not sqlstate.startswith(
                        ("08", "53", "57")
                    ):
                        # Invalid SQL and schema errors require operator action.
                        raise
                logger.warning("Outbox database unavailable: %s", type(error).__name__)
            await asyncio.sleep(settings.outbox_poll_interval)
    finally:
        try:
            await broker.stop()
        finally:
            await engine.dispose()


async def main() -> None:
    settings = WorkerSettings()
    logging.basicConfig(level=settings.log_level)
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run_publisher(settings)
    except asyncio.CancelledError as error:
        logger.info("Outbox publisher stopped: %s", type(error).__name__)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    asyncio.run(main())
