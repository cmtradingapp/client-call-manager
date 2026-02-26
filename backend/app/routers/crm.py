import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.auth_deps import get_current_user
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Valid retention status keys (from CRM system)
VALID_STATUS_KEYS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    32, 33, 34, 35, 36, 37,
}


class RetentionStatusUpdate(BaseModel):
    status_key: int


@router.put("/clients/{account_id}/retention-status")
async def update_retention_status(
    account_id: str,
    body: RetentionStatusUpdate,
    request: Request,
    _: Any = Depends(get_current_user),
) -> dict:
    """Proxy endpoint to update a client's retention status via the CRM API."""
    if body.status_key not in VALID_STATUS_KEYS:
        raise HTTPException(status_code=400, detail=f"Invalid status_key: {body.status_key}")

    crm_url = f"{settings.crm_api_base_url}/crm-api/retention"
    params = {
        "userId": account_id,
        "retentionStatus": body.status_key,
    }
    headers = {}
    if settings.crm_api_token:
        headers["Authorization"] = f"Bearer {settings.crm_api_token}"

    logger.info(
        "CRM API call: updating retention status for account %s to %d",
        account_id,
        body.status_key,
    )

    try:
        http_client = request.app.state.http_client
        response = await http_client.post(crm_url, params=params, headers=headers)

        if response.status_code >= 400:
            detail = response.text[:500] if response.text else f"CRM API returned {response.status_code}"
            logger.error("CRM API error %d: %s", response.status_code, detail)
            raise HTTPException(
                status_code=502,
                detail=f"CRM API error ({response.status_code}): {detail}",
            )

        logger.info(
            "CRM API success: account %s retention status updated to %d",
            account_id,
            body.status_key,
        )
        return {
            "success": True,
            "message": f"Retention status updated to {body.status_key} for account {account_id}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("CRM API request failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")


class AddNoteBody(BaseModel):
    note: str


@router.post("/clients/{account_id}/note")
async def add_client_note(
    account_id: str,
    body: AddNoteBody,
    request: Request,
    _: Any = Depends(get_current_user),
) -> dict:
    """Proxy endpoint to add a note for a client via the CRM API."""
    if not body.note or not body.note.strip():
        raise HTTPException(status_code=400, detail="Note text cannot be empty")

    crm_url = f"{settings.crm_api_base_url}/crm-api/user-note"
    params = {
        "userId": account_id,
        "note": body.note.strip(),
    }
    headers = {}
    if settings.crm_api_token:
        headers["Authorization"] = f"Bearer {settings.crm_api_token}"

    logger.info("CRM API call: adding note for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.post(crm_url, params=params, headers=headers)

        if response.status_code >= 400:
            detail = response.text[:500] if response.text else f"CRM API returned {response.status_code}"
            logger.error("CRM API error %d: %s", response.status_code, detail)
            raise HTTPException(
                status_code=502,
                detail=f"CRM API error ({response.status_code}): {detail}",
            )

        logger.info("CRM API success: note added for account %s", account_id)
        return {
            "success": True,
            "message": f"Note added for account {account_id}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("CRM API request failed (add note): %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")


@router.get("/clients/{account_id}/crm-user")
async def get_crm_user(
    account_id: str,
    request: Request,
    _: Any = Depends(get_current_user),
) -> dict:
    """Proxy endpoint to fetch CRM user details (including phone number)."""
    crm_url = f"{settings.crm_api_base_url}/crm-api/user"
    params = {"userId": account_id}
    headers = {}
    if settings.crm_api_token:
        headers["Authorization"] = f"Bearer {settings.crm_api_token}"

    logger.info("CRM API call: fetching user details for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.get(crm_url, params=params, headers=headers)

        if response.status_code >= 400:
            detail = response.text[:500] if response.text else f"CRM API returned {response.status_code}"
            logger.error("CRM API error %d: %s", response.status_code, detail)
            raise HTTPException(
                status_code=502,
                detail=f"CRM API error ({response.status_code}): {detail}",
            )

        data = response.json()
        logger.info("CRM API success: fetched user details for account %s", account_id)
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("CRM API request failed (get user): %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")
