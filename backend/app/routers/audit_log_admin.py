import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.models.audit_log import AuditLog
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

VALID_ACTION_TYPES = {"status_change", "note_added", "call_initiated", "whatsapp_opened"}


@router.get("/admin/audit-log")
async def list_audit_log(
    agent_username: Optional[str] = Query(None, description="Filter by agent username"),
    client_account_id: Optional[str] = Query(None, description="Filter by client account ID"),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    date_from: Optional[datetime] = Query(None, description="Filter from date (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="Filter to date (ISO 8601)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Return paginated audit log entries with optional filters. Admin-only."""
    query = select(AuditLog)
    count_query = select(func.count(AuditLog.id))

    # Apply filters
    if agent_username:
        query = query.where(AuditLog.agent_username == agent_username)
        count_query = count_query.where(AuditLog.agent_username == agent_username)
    if client_account_id:
        query = query.where(AuditLog.client_account_id == client_account_id)
        count_query = count_query.where(AuditLog.client_account_id == client_account_id)
    if action_type:
        query = query.where(AuditLog.action_type == action_type)
        count_query = count_query.where(AuditLog.action_type == action_type)
    if date_from:
        query = query.where(AuditLog.timestamp >= date_from)
        count_query = count_query.where(AuditLog.timestamp >= date_from)
    if date_to:
        query = query.where(AuditLog.timestamp <= date_to)
        count_query = count_query.where(AuditLog.timestamp <= date_to)

    # Total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginate (newest first)
    offset = (page - 1) * page_size
    query = query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(page_size)

    result = await db.execute(query)
    entries = result.scalars().all()

    return {
        "items": [
            {
                "id": e.id,
                "agent_id": e.agent_id,
                "agent_username": e.agent_username,
                "client_account_id": e.client_account_id,
                "action_type": e.action_type,
                "action_value": e.action_value,
                "timestamp": e.timestamp.isoformat(),
            }
            for e in entries
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }


async def _aggregate_period(db: AsyncSession, since: datetime) -> list[dict]:
    """Aggregate audit_log counts per agent since the given timestamp."""
    total_col = func.count().label("total")
    query = (
        select(
            AuditLog.agent_username,
            func.sum(case((AuditLog.action_type == "call_initiated", 1), else_=0)).label("calls"),
            func.sum(case((AuditLog.action_type == "note_added", 1), else_=0)).label("notes"),
            func.sum(case((AuditLog.action_type == "status_change", 1), else_=0)).label("status_changes"),
            func.sum(case((AuditLog.action_type == "whatsapp_opened", 1), else_=0)).label("whatsapp"),
            total_col,
        )
        .where(AuditLog.timestamp >= since)
        .group_by(AuditLog.agent_username)
        .order_by(total_col.desc())
    )
    result = await db.execute(query)
    rows = result.all()
    return [
        {
            "agent_username": row.agent_username,
            "calls": row.calls,
            "notes": row.notes,
            "status_changes": row.status_changes,
            "whatsapp": row.whatsapp,
            "total": row.total,
        }
        for row in rows
    ]


@router.get("/admin/activity-dashboard")
async def activity_dashboard(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Return per-agent activity stats for today, this week, this month. Admin-only."""
    now = datetime.now(timezone.utc)

    # Today: start of current UTC day
    start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # This week: Monday 00:00 UTC
    days_since_monday = now.weekday()  # Monday=0
    start_of_week = (now - timedelta(days=days_since_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # This month: 1st of current month 00:00 UTC
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    today_data = await _aggregate_period(db, start_of_today)
    week_data = await _aggregate_period(db, start_of_week)
    month_data = await _aggregate_period(db, start_of_month)

    return {
        "periods": {
            "today": today_data,
            "this_week": week_data,
            "this_month": month_data,
        },
        "last_updated": now.isoformat(),
    }
