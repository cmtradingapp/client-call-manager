import csv
import io
from typing import List

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.call_mapping import CallMapping
from app.pg_database import get_db

router = APIRouter()


class LookupRequest(BaseModel):
    conversation_ids: List[str]


@router.post("/call-mappings/lookup")
async def lookup_mappings(
    body: LookupRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> dict:
    if not body.conversation_ids:
        return {"mappings": {}}
    result = await db.execute(
        select(CallMapping).where(CallMapping.conversation_id.in_(body.conversation_ids))
    )
    mappings = {m.conversation_id: m.account_id for m in result.scalars().all()}
    return {"mappings": mappings}


@router.post("/call-mappings/export-unknown")
async def export_unknown(
    body: LookupRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> StreamingResponse:
    """
    Given a list of conversation_ids that are 'unknown', returns a CSV
    of their account_ids so they can be re-uploaded for calling.
    """
    result = await db.execute(
        select(CallMapping).where(CallMapping.conversation_id.in_(body.conversation_ids))
    )
    mappings = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id"])
    for m in mappings:
        writer.writerow([m.account_id])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=unknown_calls.csv"},
    )
