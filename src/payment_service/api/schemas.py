from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from payment_service.db.models import Currency, JsonValue, PaymentStatus


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
