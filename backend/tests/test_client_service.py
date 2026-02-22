from unittest.mock import AsyncMock, patch

import pytest

from app.schemas.client import ClientDetail, ClientStatus, FilterParams
from app.services.client_service import _query_mssql, get_filtered_clients


@pytest.mark.asyncio
async def test_query_mssql_no_filters():
    with patch(
        "app.services.client_service.database.execute_query", new_callable=AsyncMock
    ) as mock_query:
        mock_query.return_value = [{"client_id": "C-001"}, {"client_id": "C-002"}]

        result = await _query_mssql(FilterParams())

        assert result == ["C-001", "C-002"]
        query_str = mock_query.call_args[0][0]
        assert "1=1" in query_str


@pytest.mark.asyncio
async def test_query_mssql_with_status_filter():
    with patch(
        "app.services.client_service.database.execute_query", new_callable=AsyncMock
    ) as mock_query:
        mock_query.return_value = [{"client_id": "C-003"}]

        result = await _query_mssql(FilterParams(status=ClientStatus.active))

        assert result == ["C-003"]
        query_str = mock_query.call_args[0][0]
        assert "status = ?" in query_str
        params = mock_query.call_args[0][1]
        assert "active" in params


@pytest.mark.asyncio
async def test_query_mssql_combined_filters():
    with patch(
        "app.services.client_service.database.execute_query", new_callable=AsyncMock
    ) as mock_query:
        mock_query.return_value = []

        from datetime import date

        await _query_mssql(
            FilterParams(
                date_from=date(2025, 1, 1),
                date_to=date(2025, 12, 31),
                status=ClientStatus.active,
                region="northeast",
                custom_field="premium",
            )
        )

        query_str = mock_query.call_args[0][0]
        assert "created_at >= ?" in query_str
        assert "created_at <= ?" in query_str
        assert "status = ?" in query_str
        assert "region = ?" in query_str
        assert "custom_field LIKE ?" in query_str
        params = mock_query.call_args[0][1]
        assert "%premium%" in params


@pytest.mark.asyncio
async def test_get_filtered_clients_enrichment():
    mock_http = AsyncMock()

    with patch(
        "app.services.client_service._query_mssql", new_callable=AsyncMock
    ) as mock_sql, patch(
        "app.services.client_service.get_client_details", new_callable=AsyncMock
    ) as mock_enrich:
        mock_sql.return_value = ["C-001", "C-002"]
        mock_enrich.side_effect = [
            ClientDetail(
                client_id="C-001", name="Alice", status="active",
                phone_number="+15551234567",
            ),
            ClientDetail(
                client_id="C-002", name="Bob", status="active",
                phone_number="+15559876543",
            ),
        ]

        result = await get_filtered_clients(mock_http, FilterParams())

        assert len(result) == 2
        assert result[0].client_id == "C-001"
        assert result[1].name == "Bob"


@pytest.mark.asyncio
async def test_get_filtered_clients_filters_none_results():
    mock_http = AsyncMock()

    with patch(
        "app.services.client_service._query_mssql", new_callable=AsyncMock
    ) as mock_sql, patch(
        "app.services.client_service.get_client_details", new_callable=AsyncMock
    ) as mock_enrich:
        mock_sql.return_value = ["C-001", "C-002"]
        mock_enrich.side_effect = [
            ClientDetail(
                client_id="C-001", name="Alice", status="active",
                phone_number="+15551234567",
            ),
            None,  # enrichment failed for C-002
        ]

        result = await get_filtered_clients(mock_http, FilterParams())

        assert len(result) == 1
        assert result[0].client_id == "C-001"


@pytest.mark.asyncio
async def test_get_filtered_clients_empty_mssql():
    mock_http = AsyncMock()

    with patch(
        "app.services.client_service._query_mssql", new_callable=AsyncMock
    ) as mock_sql:
        mock_sql.return_value = []

        result = await get_filtered_clients(mock_http, FilterParams())

        assert result == []
