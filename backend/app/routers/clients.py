from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Request

from app.schemas.client import ClientDetail, FilterParams
from app.services.client_service import get_filtered_clients

router = APIRouter()


@router.get("/clients", response_model=List[ClientDetail])
async def list_clients(
    request: Request,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    sales_status: Optional[int] = None,
    region: Optional[str] = None,
    custom_field: Optional[str] = None,
    sales_client_potential: Optional[int] = None,
    sales_client_potential_op: Optional[str] = None,
    language: Optional[str] = None,
    live: Optional[str] = None,
    ftd: Optional[str] = None,
) -> List[ClientDetail]:
    filters = FilterParams(
        date_from=date_from,
        date_to=date_to,
        sales_status=sales_status,
        region=region,
        custom_field=custom_field,
        sales_client_potential=sales_client_potential,
        sales_client_potential_op=sales_client_potential_op,
        language=language,
        live=live,
        ftd=ftd,
    )
    http_client = request.app.state.http_client
    return await get_filtered_clients(http_client, filters)
