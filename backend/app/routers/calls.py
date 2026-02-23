import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Query, Request

from app.history_db import insert_call_history, query_call_history
from app.schemas.call import CallRequest, CallResponse, CallStatus, ClientCallResult
from app.schemas.history import CallHistoryRecord
from app.services.elevenlabs_service import initiate_call
from app.services.internal_api import get_crm_data

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/calls/initiate", response_model=CallResponse)
async def initiate_calls(request: Request, body: CallRequest) -> CallResponse:
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
        return result

    results = await asyncio.gather(*[call_one(cid) for cid in body.client_ids])
    return CallResponse(results=list(results))


@router.get("/calls/history", response_model=list[CallHistoryRecord])
async def get_call_history(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[CallHistoryRecord]:
    records = await query_call_history(
        date_from=date_from,
        date_to=date_to,
        status=status,
        limit=limit,
        offset=offset,
    )
    return [CallHistoryRecord(**r) for r in records]
