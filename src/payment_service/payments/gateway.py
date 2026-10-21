import asyncio
import random

from payment_service.db.models import Payment, PaymentStatus


class GatewayUnavailable(Exception):
    """A technical failure independent of the business outcome"""


async def simulate_payment(payment: Payment) -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED
