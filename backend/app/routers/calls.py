import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.history_db import insert_call_history
from app.models.call_mapping import CallMapping
from app.pg_database import get_db
from app.schemas.call import CallRequest, CallResponse, CallStatus, ClientCallResult
from app.services.elevenlabs_service import initiate_call
from app.services.internal_api import get_crm_data

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/calls/initiate", response_model=CallResponse)
async def initiate_calls(request: Request, body: CallRequest, db: AsyncSession = Depends(get_db)) -> CallResponse:
    http_client = request.app.state.http_client

    async def call_one(client_id: str) -> ClientCallResult:
        crm = await get_crm_data(http_client, client_id)
        if not crm.phone:
            result = ClientCallResult(
                client_id=client_id,
                status=CallStatus.failed,
                error="Could not retrieve phone number for client",
            )
        else:
            result = await initiate_call(
                http_client,
                client_id,
                crm.phone,
                first_name=crm.first_name,
                email=crm.email,
                agent_id=body.agent_id,
                agent_phone_number_id=body.agent_phone_number_id,
            )
        await insert_call_history(
            client_id=client_id,
            client_name=crm.first_name,
            phone_number=crm.phone,
            conversation_id=result.conversation_id,
            status=result.status.value,
            error=result.error,
            agent_id=body.agent_id,
        )
        if result.conversation_id:
            db.add(CallMapping(conversation_id=result.conversation_id, account_id=client_id))
        return result

    results = await asyncio.gather(*[call_one(cid) for cid in body.client_ids])
    await db.commit()
    return CallResponse(results=list(results))


@router.get("/calls/history")
async def get_call_history(
    request: Request,
    agent_id: Optional[str] = Query(None),
    call_successful: Optional[str] = Query(None),
    page_size: int = Query(100, ge=1, le=100),
    cursor: Optional[str] = Query(None),
) -> Any:
    http_client = request.app.state.http_client
    params: dict[str, Any] = {"page_size": page_size}
    if agent_id:
        params["agent_id"] = agent_id
    if call_successful:
        params["call_successful"] = call_successful
    if cursor:
        params["cursor"] = cursor
    try:
        response = await http_client.get(
            "https://api.elevenlabs.io/v1/convai/conversations",
            params=params,
            headers={"xi-api-key": settings.elevenlabs_api_key},
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch ElevenLabs conversations: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch call history from ElevenLabs")
