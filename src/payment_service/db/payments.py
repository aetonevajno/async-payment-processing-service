from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import Payment
from payment_service.payments.schemas import PaymentCreate


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None:
        statement = select(Payment).where(Payment.id == payment_id)
        if lock:
            statement = statement.with_for_update()
        return (await self.session.scalars(statement)).one_or_none()

    async def insert(self, request: PaymentCreate, idempotency_key: str) -> Payment | None:
        return await self.session.scalar(
            insert(Payment)
            .values(
                idempotency_key=idempotency_key,
                amount=request.amount,
                currency=request.currency,
                description=request.description,
                payment_metadata=request.metadata,
                webhook_url=str(request.webhook_url),
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )

    async def find_matching_request(
        self, request: PaymentCreate, idempotency_key: str
    ) -> Payment | None:
        return (
            await self.session.scalars(
                select(Payment).where(
                    Payment.idempotency_key == idempotency_key,
                    Payment.amount == request.amount,
                    Payment.currency == request.currency,
                    Payment.description == request.description,
                    Payment.payment_metadata == request.metadata,
                    Payment.webhook_url == str(request.webhook_url),
                )
            )
        ).one_or_none()
