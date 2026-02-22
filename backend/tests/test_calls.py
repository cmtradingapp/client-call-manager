from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas.call import CallStatus, ClientCallResult
from app.schemas.client import ClientDetail


@pytest.fixture
def client():
    # Patches are applied before TestClient starts so the lifespan httpx client
    # is created but never actually used (the service functions are mocked).
    with TestClient(app) as c:
        yield c


def test_initiate_calls_success(client):
    with patch(
        "app.routers.calls.get_client_details", new_callable=AsyncMock
    ) as mock_detail, patch(
        "app.routers.calls.initiate_call", new_callable=AsyncMock
    ) as mock_call:
        mock_detail.return_value = ClientDetail(
            client_id="C-001",
            name="Test Client",
            status="active",
            phone_number="+15551234567",
        )
        mock_call.return_value = ClientCallResult(
            client_id="C-001",
            status=CallStatus.initiated,
            conversation_id="conv-abc123",
        )

        response = client.post("/api/calls/initiate", json={"client_ids": ["C-001"]})

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["status"] == "initiated"
    assert data["results"][0]["conversation_id"] == "conv-abc123"


def test_initiate_calls_missing_phone(client):
    with patch(
        "app.routers.calls.get_client_details", new_callable=AsyncMock
    ) as mock_detail:
        mock_detail.return_value = None

        response = client.post("/api/calls/initiate", json={"client_ids": ["C-999"]})

    assert response.status_code == 200
    data = response.json()
    assert data["results"][0]["status"] == "failed"
    assert data["results"][0]["error"] is not None


def test_initiate_calls_elevenlabs_failure(client):
    with patch(
        "app.routers.calls.get_client_details", new_callable=AsyncMock
    ) as mock_detail, patch(
        "app.routers.calls.initiate_call", new_callable=AsyncMock
    ) as mock_call:
        mock_detail.return_value = ClientDetail(
            client_id="C-002",
            name="Test Client 2",
            status="active",
            phone_number="+15559876543",
        )
        mock_call.return_value = ClientCallResult(
            client_id="C-002",
            status=CallStatus.failed,
            error="HTTP 401: Unauthorized",
        )

        response = client.post("/api/calls/initiate", json={"client_ids": ["C-002"]})

    assert response.status_code == 200
    data = response.json()
    assert data["results"][0]["status"] == "failed"
    assert "Unauthorized" in data["results"][0]["error"]


def test_initiate_calls_multiple_mixed(client):
    with patch(
        "app.routers.calls.get_client_details", new_callable=AsyncMock
    ) as mock_detail, patch(
        "app.routers.calls.initiate_call", new_callable=AsyncMock
    ) as mock_call:
        mock_detail.side_effect = [
            ClientDetail(
                client_id="C-001", name="Alice", status="active",
                phone_number="+15551111111",
            ),
            None,  # C-002 lookup fails
        ]
        mock_call.return_value = ClientCallResult(
            client_id="C-001",
            status=CallStatus.initiated,
            conversation_id="conv-xyz",
        )

        response = client.post(
            "/api/calls/initiate", json={"client_ids": ["C-001", "C-002"]}
        )

    assert response.status_code == 200
    results = {r["client_id"]: r for r in response.json()["results"]}
    assert results["C-001"]["status"] == "initiated"
    assert results["C-002"]["status"] == "failed"
