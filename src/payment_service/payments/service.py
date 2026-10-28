from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import Payment
from payment_service.db.outbox import OutboxRepository
from payment_service.db.payments import PaymentRepository
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
        payments = PaymentRepository(session)
        payment = await payments.insert(payment_request, idempotency_key)
        if payment is None:
            payment = await payments.find_matching_request(payment_request, idempotency_key)
            if payment is None:
                raise IdempotencyConflict(idempotency_key)
        else:
            OutboxRepository(session).schedule(payment.id, "payment.created")
    return payment
