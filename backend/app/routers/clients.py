from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Request

from app.schemas.client import ClientDetail, ClientStatus, FilterParams
from app.services.client_service import get_filtered_clients

router = APIRouter()


@router.get("/clients", response_model=List[ClientDetail])
async def list_clients(
    request: Request,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    status: Optional[ClientStatus] = None,
    region: Optional[str] = None,
    custom_field: Optional[str] = None,
) -> List[ClientDetail]:
    filters = FilterParams(
        date_from=date_from,
        date_to=date_to,
        status=status,
        region=region,
        custom_field=custom_field,
    )
    http_client = request.app.state.http_client
    return await get_filtered_clients(http_client, filters)
