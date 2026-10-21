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
