import logging
import uuid

import httpx

from app.config import settings
from app.schemas.call import CallStatus, ClientCallResult

logger = logging.getLogger(__name__)

ELEVENLABS_OUTBOUND_URL = (
    "https://api.elevenlabs.io/v1/convai/conversations/outbound-call"
)


async def initiate_call(
    client: httpx.AsyncClient, client_id: str, phone_number: str
) -> ClientCallResult:
    if settings.mock_mode:
        logger.info(f"[MOCK] Simulating outbound call to {phone_number} for {client_id}")
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.initiated,
            conversation_id=f"mock-conv-{uuid.uuid4().hex[:8]}",
        )

    try:
        response = await client.post(
            ELEVENLABS_OUTBOUND_URL,
            json={
                "agent_id": settings.elevenlabs_agent_id,
                "to": phone_number,
                "from": settings.elevenlabs_from_number,
            },
            headers={
                "xi-api-key": settings.elevenlabs_api_key,
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        data = response.json()
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.initiated,
            conversation_id=data.get("conversation_id"),
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            f"ElevenLabs call failed for {client_id}: "
            f"{e.response.status_code} {e.response.text}"
        )
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.failed,
            error=f"HTTP {e.response.status_code}: {e.response.text}",
        )
    except Exception as e:
        logger.error(f"ElevenLabs call error for {client_id}: {e}")
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.failed,
            error=str(e),
        )
