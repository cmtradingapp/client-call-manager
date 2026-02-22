from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class ClientStatus(str, Enum):
    active = "active"
    inactive = "inactive"
    pending = "pending"


class FilterParams(BaseModel):
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    status: Optional[ClientStatus] = None
    region: Optional[str] = None
    custom_field: Optional[str] = None


class ClientSummary(BaseModel):
    client_id: str
    name: str
    status: str
    region: Optional[str] = None
    created_at: Optional[str] = None


class ClientDetail(BaseModel):
    client_id: str
    name: str
    status: str
    region: Optional[str] = None
    created_at: Optional[str] = None
    phone_number: str
    email: Optional[str] = None
    account_manager: Optional[str] = None
