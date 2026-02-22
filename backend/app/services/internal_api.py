import logging
from typing import Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


async def get_phone_number(client: httpx.AsyncClient, client_id: str) -> Optional[str]:
    """Fetch fullTelephone for a client from the CRM API."""
    if settings.mock_mode:
        return None

    try:
        response = await client.get(
            f"{settings.crm_api_base_url}//crm-api/user",
            params={"id": client_id},
            headers={"x-crm-api-token": settings.crm_api_token},
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json()
        result = data.get("result") if isinstance(data, dict) else None
        phone = result.get("fullTelephone") if isinstance(result, dict) else None
        return phone or None
    except Exception as e:
        logger.warning("CRM API failed for client %s: %s", client_id, e)
        return None
