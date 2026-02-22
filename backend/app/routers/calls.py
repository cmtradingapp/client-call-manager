import asyncio
import logging

from fastapi import APIRouter, Request

from app.schemas.call import CallRequest, CallResponse, CallStatus, ClientCallResult
from app.services.elevenlabs_service import initiate_call
from app.services.internal_api import get_phone_number

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/calls/initiate", response_model=CallResponse)
async def initiate_calls(request: Request, body: CallRequest) -> CallResponse:
    http_client = request.app.state.http_client

    async def call_one(client_id: str) -> ClientCallResult:
        phone = await get_phone_number(http_client, client_id)
        if not phone:
            return ClientCallResult(
                client_id=client_id,
                status=CallStatus.failed,
                error="Could not retrieve phone number for client",
            )
        return await initiate_call(http_client, client_id, phone)

    results = await asyncio.gather(*[call_one(cid) for cid in body.client_ids])
    return CallResponse(results=list(results))
