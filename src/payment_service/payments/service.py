from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent, Payment
from payment_service.payments.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """An idempotency key already belongs to a different payment request"""


async def create_payment(
    session: AsyncSession,
    payment_request: PaymentCreate,
    *,
    idempotency_key: str,
) -> Payment:
    async with session.begin():
        statement = (
            insert(Payment)
            .values(
                idempotency_key=idempotency_key,
                amount=payment_request.amount,
                currency=payment_request.currency,
                description=payment_request.description,
                payment_metadata=payment_request.metadata,
                webhook_url=str(payment_request.webhook_url),
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        payment = await session.scalar(statement)
        if payment is None:
            payment = await session.scalar(
                select(Payment).where(
                    Payment.idempotency_key == idempotency_key,
                    Payment.amount == payment_request.amount,
                    Payment.currency == payment_request.currency,
                    Payment.description == payment_request.description,
                    Payment.payment_metadata == payment_request.metadata,
                    Payment.webhook_url == str(payment_request.webhook_url),
                )
            )
            if payment is None:
                raise IdempotencyConflict(idempotency_key)
        else:
            session.add(
                OutboxEvent(
                    payment_id=payment.id,
                    event_type="payment.created",
                    payload={"payment_id": str(payment.id)},
                )
            )
    return payment
