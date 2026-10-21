import asyncio
import ipaddress
import socket

import httpx2

from payment_service.config import WorkerSettings
from payment_service.db.models import Payment


class WebhookDeliveryError(Exception):
    """Delivery failed without changing the payment outcome"""


async def resolve_destination(url: httpx2.URL, private_hosts: list[str]) -> str:
    addresses = await asyncio.get_running_loop().getaddrinfo(
        url.host, url.port or (443 if url.scheme == "https" else 80), type=socket.SOCK_STREAM
    )
    if not addresses:
        raise WebhookDeliveryError("dns_empty")
    candidates = [str(address[4][0]) for address in addresses]
    if url.host not in private_hosts and any(
        not ipaddress.ip_address(address).is_global for address in candidates
    ):
        raise WebhookDeliveryError("destination_blocked")
    return candidates[0]


async def deliver_webhook(payment: Payment, settings: WorkerSettings) -> None:
    url = httpx2.URL(payment.webhook_url)
    if url.scheme not in {"http", "https"} or url.userinfo or url.fragment:
        raise WebhookDeliveryError("invalid_destination")
    try:
        async with asyncio.timeout(settings.webhook_timeout):
            address = await resolve_destination(url, settings.webhook_private_hosts)
            
            async with httpx2.AsyncClient(
                trust_env=False, follow_redirects=False, timeout=settings.webhook_timeout
            ) as client:
                async with client.stream(
                    "POST",
                    url.copy_with(host=address),
                    headers={
                        "Host": url.netloc.decode("ascii"),
                        "Idempotency-Key": str(payment.id),
                    },
                    extensions={"sni_hostname": url.host},
                    json={
                        "event_id": str(payment.id),
                        "payment_id": str(payment.id),
                        "status": payment.status.value,
                        "amount": str(payment.amount),
                        "currency": payment.currency.value,
                        "processed_at": payment.processed_at.isoformat()
                        if payment.processed_at
                        else None,
                    },
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise WebhookDeliveryError(f"http_{response.status_code}")
    except (httpx2.HTTPError, OSError, TimeoutError) as error:
        raise WebhookDeliveryError(type(error).__name__) from error
