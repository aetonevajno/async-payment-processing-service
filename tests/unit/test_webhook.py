import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx2
import pytest

from payment_service.config import WorkerSettings
from payment_service.db.models import Currency, Payment, PaymentStatus
from payment_service.payments.webhook import (
    WebhookDeliveryError,
    deliver_webhook,
    resolve_destination,
)


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("12.30"),
        currency=Currency.RUB,
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_url="http://127.0.0.1/hook",
    )


async def test_real_http_delivery_and_stable_identity(
    settings_environment: None, payment: Payment, receiver: tuple[int, list[dict[str, object]]]
) -> None:
    port, requests = receiver
    payment.webhook_url = f"http://127.0.0.1:{port}/hook"
    settings = WorkerSettings(webhook_private_hosts=["127.0.0.1"])
    await deliver_webhook(payment, settings)
    await deliver_webhook(payment, settings)
    assert len(requests) == 2
    assert requests[0]["body"] == requests[1]["body"]
    assert payment.processed_at is not None
    assert requests[0]["body"] == {
        "event_id": str(payment.id),
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "12.30",
        "currency": "RUB",
        "processed_at": payment.processed_at.isoformat(),
    }
    assert f"Host: 127.0.0.1:{port}" in str(requests[0]["headers"])
    assert f"Idempotency-Key: {payment.id}" in str(requests[0]["headers"])


@pytest.mark.parametrize("path,reason", [("fail", "http_503"), ("redirect", "http_302")])
async def test_http_failures_and_no_redirects(
    settings_environment: None,
    payment: Payment,
    receiver: tuple[int, list[dict[str, object]]],
    path: str,
    reason: str,
) -> None:
    port, requests = receiver
    payment.webhook_url = f"http://127.0.0.1:{port}/{path}"
    with pytest.raises(WebhookDeliveryError, match=reason):
        await deliver_webhook(payment, WorkerSettings(webhook_private_hosts=["127.0.0.1"]))
    assert len(requests) == 1


@pytest.mark.parametrize("host", ["127.0.0.1", "169.254.169.254", "10.0.0.1", "[::1]"])
async def test_private_destinations_blocked(host: str) -> None:
    with pytest.raises(WebhookDeliveryError, match="destination_blocked"):
        await resolve_destination(httpx2.URL(f"http://{host}/"), [])


async def test_total_timeout_includes_dns(
    settings_environment: None, payment: Payment, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def slow_resolution(url: httpx2.URL, private_hosts: list[str]) -> str:
        await asyncio.sleep(1)
        return "127.0.0.1"

    monkeypatch.setattr("payment_service.payments.webhook.resolve_destination", slow_resolution)
    with pytest.raises(WebhookDeliveryError, match="TimeoutError"):
        await deliver_webhook(payment, WorkerSettings(webhook_timeout=0.01))


async def test_pins_ip_preserving_hostname(
    settings_environment: None, payment: Payment, monkeypatch: pytest.MonkeyPatch
) -> None:
    payment.webhook_url = "https://merchant.example/hook"
    requests: list[httpx2.Request] = []
    original_client = httpx2.AsyncClient

    async def resolve(url: httpx2.URL, private_hosts: list[str]) -> str:
        return "93.184.215.14"

    def receive(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(204)

    def client(*, trust_env: bool, follow_redirects: bool, timeout: float) -> httpx2.AsyncClient:
        assert not trust_env and not follow_redirects
        return original_client(transport=httpx2.MockTransport(receive))

    monkeypatch.setattr("payment_service.payments.webhook.resolve_destination", resolve)
    monkeypatch.setattr(httpx2, "AsyncClient", client)
    await deliver_webhook(payment, WorkerSettings())
    assert requests[0].url.host == "93.184.215.14"
    assert requests[0].headers["Host"] == "merchant.example"
    assert requests[0].extensions["sni_hostname"] == "merchant.example"
