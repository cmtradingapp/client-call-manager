from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class CallStatus(str, Enum):
    initiated = "initiated"
    failed = "failed"


class CallRequest(BaseModel):
    client_ids: List[str]


class ClientCallResult(BaseModel):
    client_id: str
    status: CallStatus
    conversation_id: Optional[str] = None
    error: Optional[str] = None


class CallResponse(BaseModel):
    results: List[ClientCallResult]
