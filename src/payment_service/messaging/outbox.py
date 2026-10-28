import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.db.outbox import LeaseLost, OutboxRepository
from payment_service.messaging.publisher import PublishNotConfirmed

logger = logging.getLogger(__name__)
type PublishEvent = Callable[[OutboxEvent], Awaitable[None]]


async def publish_next(
    sessions: async_sessionmaker[AsyncSession], publish: PublishEvent, settings: WorkerSettings
) -> bool:
    async with sessions.begin() as session:
        event = await OutboxRepository(session).claim_publication(
            settings.outbox_publish_timeout + 5
        )
    if event is None:
        return False
    token = event.publish_token
    assert token is not None
    try:
        failure = None
        try:
            async with asyncio.timeout(settings.outbox_publish_timeout):
                await publish(event)
        except (AMQPException, ChannelInvalidStateError, OSError, PublishNotConfirmed) as error:
            failure = type(error).__name__

        async with sessions.begin() as session:
            current_event = await OutboxRepository(session).get(event.id, lock=True)
            if current_event is None or current_event.publish_token != token:
                raise LeaseLost(str(event.id))
            current_event.publish_attempts += 1
            current_event.last_error = failure
            if failure is not None:
                delay = min(
                    settings.outbox_retry_max,
                    settings.outbox_retry_base * 2 ** min(current_event.publish_attempts - 1, 16),
                )
                current_event.available_at = datetime.now(UTC) + timedelta(seconds=delay)
                logger.warning("Outbox event %s postponed: %s", event.id, failure)
            else:
                current_event.published_at = datetime.now(UTC)
    finally:
        async with sessions.begin() as session:
            await OutboxRepository(session).release_publication(event.id, token)
    return True
