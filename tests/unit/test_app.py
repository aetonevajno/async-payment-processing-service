import socket
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from payment_service.api.app import create_app


@pytest.fixture
def client(settings_environment: None) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


def test_health(client: TestClient) -> None:
    response = client.get("/health", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
def test_health_requires_api_key(client: TestClient, headers: dict[str, str]) -> None:
    response = client.get("/health", headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}


@pytest.fixture
def payment_body() -> dict[str, object]:
    return {
        "amount": "10.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {},
        "webhook_url": "https://merchant.example/webhook",
    }


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
def test_payment_endpoints_require_api_key(
    client: TestClient, method: str, headers: dict[str, str]
) -> None:
    url = "/api/v1/payments" + ("/00000000-0000-0000-0000-000000000001" if method == "GET" else "")
    response = client.request(method, url, headers=headers)
    assert response.status_code == 401


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000.00"),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("amount", 10.01),
        ("amount", True),
        ("currency", "GBP"),
        ("metadata", []),
        ("webhook_url", "not-a-url"),
        ("webhook_url", "ftp://merchant.example/"),
        ("webhook_url", "https://user:password@merchant.example/"),
        ("webhook_url", "https://merchant.example/#fragment"),
        ("unexpected", "value"),
    ],
)
def test_invalid_payment_body(
    client: TestClient, payment_body: dict[str, object], field: str, value: object
) -> None:
    response = client.post(
        "/api/v1/payments",
        json=payment_body | {field: value},
        headers={"X-API-Key": "test-api-key", "Idempotency-Key": "order-42"},
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][:2] == ["body", field]
    assert "input" not in response.json()["detail"][0]


@pytest.mark.parametrize("key", [None, "", "   ", "x" * 256])
def test_idempotency_key_is_required_and_validated(
    client: TestClient, payment_body: dict[str, object], key: str | None
) -> None:
    headers = {"X-API-Key": "test-api-key"}
    if key is not None:
        headers["Idempotency-Key"] = key
    response = client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert response.status_code == 422


def test_invalid_payment_id(client: TestClient) -> None:
    response = client.get("/api/v1/payments/not-a-uuid", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 422


@pytest.mark.parametrize("body", ['{"amount":', '{"amount":NaN}', '{"metadata":{"value":NaN}}'])
def test_malformed_or_nonfinite_json_is_422(client: TestClient, body: str) -> None:
    response = client.post(
        "/api/v1/payments",
        content=body,
        headers={
            "X-API-Key": "test-api-key",
            "Idempotency-Key": "order-42",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 422


def test_database_connection_failure_is_503(
    settings_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    with socket.socket() as unavailable_server:
        unavailable_server.bind(("127.0.0.1", 0))
        port = unavailable_server.getsockname()[1]
        monkeypatch.setenv(
            "PAYMENTS_DATABASE_URL",
            f"postgresql+asyncpg://payments:payments@127.0.0.1:{port}/payments",
        )
        with TestClient(create_app()) as client:
            response = client.get(
                "/api/v1/payments/00000000-0000-0000-0000-000000000001",
                headers={"X-API-Key": "test-api-key"},
            )
        assert response.status_code == 503
        assert response.json() == {"detail": "Database temporarily unavailable"}
