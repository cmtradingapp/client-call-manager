import logging
from typing import Any

import httpx
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


def _crm_headers() -> dict[str, str]:
    """Build common headers for all CRM API requests."""
    return {
        "x-crm-api-token": settings.crm_api_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _handle_crm_response(response: httpx.Response, context: str) -> dict:
    """Inspect CRM response and raise appropriate HTTPException on error.

    The CRM API returns HTTP 200 for all responses. Errors are indicated
    by the JSON body: {"success": false, "error": {...}, "result": null}.

    For non-200 HTTP codes (rare), we map them to appropriate status codes.
    For 200 responses with success=false, we inspect the error details.
    """
    # Handle non-200 HTTP responses (rare but possible)
    if response.status_code >= 400:
        body_preview = response.text[:500] if response.text else ""
        logger.error("CRM API HTTP error [%s] %d: %s", context, response.status_code, body_preview)

        if response.status_code in (401, 403):
            raise HTTPException(
                status_code=502,
                detail="CRM authentication failed. Please contact an administrator.",
            )
        if response.status_code >= 500:
            raise HTTPException(
                status_code=503,
                detail="CRM service is temporarily unavailable. Please try again later.",
            )
        raise HTTPException(
            status_code=502,
            detail=f"CRM returned an unexpected error ({response.status_code}).",
        )

    # Parse JSON body
    try:
        data = response.json()
    except Exception:
        logger.error("CRM API returned non-JSON response [%s]: %s", context, response.text[:200])
        raise HTTPException(
            status_code=502,
            detail="CRM returned an unexpected response format.",
        )

    # Check CRM-level success flag
    if data.get("success") is False:
        error_info = data.get("error")
        if isinstance(error_info, dict):
            error_details = error_info.get("errorDetails", "")
            error_desc = error_info.get("errorDesc", "")
        elif isinstance(error_info, str):
            error_details = error_info
            error_desc = ""
        else:
            error_details = str(error_info) if error_info else ""
            error_desc = ""

        logger.error("CRM API business error [%s]: %s — %s", context, error_desc, error_details)

        # Detect "not found" pattern
        if "not found" in error_details.lower():
            raise HTTPException(
                status_code=404,
                detail=f"Client not found in CRM: {error_details}",
            )
        # Detect empty/invalid input pattern
        if "not valid" in error_details.lower() or "cannot be empty" in error_details.lower():
            raise HTTPException(
                status_code=400,
                detail=f"CRM validation error: {error_details}",
            )
        # Generic CRM error
        raise HTTPException(
            status_code=502,
            detail=f"CRM error: {error_details or error_desc or 'Unknown error'}",
        )

    return data


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
        raise HTTPException(
            status_code=400,
            detail=f"Invalid retention status key: {body.status_key}. Must be one of the valid CRM status codes.",
        )

    crm_url = f"{settings.crm_api_base_url}/crm-api/retention"
    params: dict[str, Any] = {
        "userId": account_id,
        "retentionStatus": body.status_key,
    }

    logger.info(
        "CRM API call: updating retention status for account %s to %d",
        account_id,
        body.status_key,
    )

    try:
        http_client = request.app.state.http_client
        response = await http_client.put(crm_url, params=params, headers=_crm_headers())
        _handle_crm_response(response, f"update_retention_status({account_id})")

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
    except httpx.TimeoutException:
        logger.error("CRM API timeout: update_retention_status(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
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
        raise HTTPException(status_code=400, detail="Note text cannot be empty.")

    crm_url = f"{settings.crm_api_base_url}/crm-api/user-note"
    params: dict[str, Any] = {
        "userId": account_id,
        "note": body.note.strip(),
    }

    logger.info("CRM API call: adding note for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.post(crm_url, params=params, headers=_crm_headers())
        _handle_crm_response(response, f"add_client_note({account_id})")

        logger.info("CRM API success: note added for account %s", account_id)
        return {
            "success": True,
            "message": f"Note added for account {account_id}",
        }
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("CRM API timeout: add_client_note(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
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
    params: dict[str, Any] = {"id": account_id}

    logger.info("CRM API call: fetching user details for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.get(crm_url, params=params, headers=_crm_headers())
        data = _handle_crm_response(response, f"get_crm_user({account_id})")

        # CRM wraps the user object in a "result" field
        result = data.get("result")
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Client not found in CRM. Please verify the account ID.",
            )

        logger.info("CRM API success: fetched user details for account %s", account_id)
        return result
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("CRM API timeout: get_crm_user(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
    except Exception as e:
        logger.error("CRM API request failed (get user): %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")
