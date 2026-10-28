from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent


class LeaseLost(Exception):
    """Another worker has reclaimed this event"""


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, event_id: UUID, *, lock: bool = False) -> OutboxEvent | None:
        statement = select(OutboxEvent).where(OutboxEvent.id == event_id)
        if lock:
            statement = statement.with_for_update()
        return (await self.session.scalars(statement)).one_or_none()

    def schedule(
        self,
        payment_id: UUID,
        event_type: str,
        attempt: int = 1,
        delay: float = 0,
        reason: str | None = None,
    ) -> None:
        self.session.add(
            OutboxEvent(
                payment_id=payment_id,
                event_type=event_type,
                attempt=attempt,
                payload={"payment_id": str(payment_id), **({"reason": reason} if reason else {})},
                available_at=datetime.now(UTC) + timedelta(seconds=delay),
            )
        )

    async def claim_publication(self, lease_seconds: float) -> OutboxEvent | None:
        event = await self.session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.available_at <= func.now(),
                or_(OutboxEvent.publish_until.is_(None), OutboxEvent.publish_until <= func.now()),
            )
            .order_by(OutboxEvent.available_at, OutboxEvent.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is not None:
            event.publish_token = uuid4()
            event.publish_until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        return event

    async def release_processing(self, event_id: UUID, token: UUID) -> None:
        await self.session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.processing_token == token)
            .values(processing_token=None, processing_until=None)
        )

    async def release_publication(self, event_id: UUID, token: UUID) -> None:
        await self.session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.publish_token == token)
            .values(publish_token=None, publish_until=None)
        )
