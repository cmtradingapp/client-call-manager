import logging
from typing import Optional

import httpx

from app.config import settings
from app.schemas.client import ClientDetail

logger = logging.getLogger(__name__)


async def get_client_details(
    client: httpx.AsyncClient, client_id: str
) -> Optional[ClientDetail]:
    if settings.mock_mode:
        from app.services.mock_data import get_mock_client
        return get_mock_client(client_id)

    try:
        response = await client.get(
            f"{settings.internal_api_base_url}/clients/{client_id}",
            headers={"Authorization": f"Bearer {settings.internal_api_key}"},
        )
        response.raise_for_status()
        data = response.json()
        return ClientDetail(
            client_id=data.get("client_id", client_id),
            name=data.get("name", ""),
            status=data.get("status", ""),
            region=data.get("region"),
            created_at=data.get("created_at"),
            phone_number=data.get("phone_number", ""),
            email=data.get("email"),
            account_manager=data.get("account_manager"),
        )
    except Exception as e:
        logger.error(f"Failed to fetch details for client {client_id}: {e}")
        return None
