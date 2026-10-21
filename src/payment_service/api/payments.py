from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.api.dependencies import get_session
from payment_service.api.schemas import PaymentAccepted, PaymentDetails
from payment_service.db.models import Payment
from payment_service.payments.schemas import PaymentCreate
from payment_service.payments.service import IdempotencyConflict, create_payment

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def post_payment(
    payment_request: PaymentCreate,
    session: DatabaseSession,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$")
    ],
) -> PaymentAccepted:
    try:
        payment = await create_payment(
            session,
            payment_request,
            idempotency_key=idempotency_key,
        )
    except IdempotencyConflict as error:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key already used with a different request"
        ) from error

    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}")
async def get_payment(payment_id: UUID, session: DatabaseSession) -> PaymentDetails:
    payment = await session.get(Payment, payment_id)

    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")

    return PaymentDetails(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
