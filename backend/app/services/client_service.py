import asyncio
import logging
from typing import List, Optional

import httpx

from app import database
from app.config import settings
from app.schemas.client import ClientDetail, FilterParams
from app.services.internal_api import get_client_details
from app.services.mock_data import filter_mock_clients

logger = logging.getLogger(__name__)

_ENRICH_SEMAPHORE = asyncio.Semaphore(10)


async def get_filtered_clients(
    http_client: httpx.AsyncClient, filters: FilterParams
) -> List[ClientDetail]:
    if settings.mock_mode:
        return filter_mock_clients(filters)

    client_ids = await _query_mssql(filters)
    if not client_ids:
        return []
    details = await _enrich_clients(http_client, client_ids)
    return [d for d in details if d is not None]


async def _query_mssql(filters: FilterParams) -> List[str]:
    conditions: list[str] = []
    params: list = []

    if filters.date_from:
        conditions.append("created_at >= ?")
        params.append(str(filters.date_from))

    if filters.date_to:
        conditions.append("created_at <= ?")
        params.append(str(filters.date_to))

    if filters.status:
        conditions.append("status = ?")
        params.append(filters.status.value)

    if filters.region:
        conditions.append("region = ?")
        params.append(filters.region)

    if filters.custom_field:
        conditions.append("custom_field LIKE ?")
        params.append(f"%{filters.custom_field}%")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT client_id FROM clients WHERE {where_clause}"

    rows = await database.execute_query(query, tuple(params))
    return [row["client_id"] for row in rows]


async def _enrich_single(
    http_client: httpx.AsyncClient, client_id: str
) -> Optional[ClientDetail]:
    async with _ENRICH_SEMAPHORE:
        return await get_client_details(http_client, client_id)


async def _enrich_clients(
    http_client: httpx.AsyncClient, client_ids: List[str]
) -> List[Optional[ClientDetail]]:
    tasks = [_enrich_single(http_client, cid) for cid in client_ids]
    return list(await asyncio.gather(*tasks))
