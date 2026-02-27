import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.retention_task import RetentionTask
from app.pg_database import get_db

router = APIRouter()

# ---------------------------------------------------------------------------
# Column / operator maps
# ---------------------------------------------------------------------------

_TASK_COL_SQL: Dict[str, str] = {
    "balance":              "m.total_balance",
    "credit":               "m.total_credit",
    "equity":               "m.total_equity",
    "trade_count":          "m.trade_count",
    "total_profit":         "m.total_profit",
    "days_in_retention":    "(CURRENT_DATE - m.client_qualification_date)",
    "deposit_count":        "m.deposit_count",
    "total_deposit":        "m.total_deposit",
    "days_from_last_trade": "(CURRENT_DATE - m.last_trade_date::date)",
    "sales_potential":      "NULLIF(TRIM(m.sales_client_potential), '')::numeric",
    "age":                  "EXTRACT(year FROM AGE(m.birth_date))::numeric",
    "assigned_to":          "m.assigned_to",
    "live_equity":          "(m.total_balance + m.total_credit)",
    "max_open_trade":       "m.max_open_trade",
    "max_volume":           "m.max_volume",
    "turnover":             "CASE WHEN (m.total_balance + m.total_credit) != 0 THEN m.max_volume / (m.total_balance + m.total_credit) ELSE 0 END",
}

_OP_MAP: Dict[str, str] = {
    "eq":  "=",
    "gt":  ">",
    "lt":  "<",
    "gte": ">=",
    "lte": "<=",
}

_MV_ACTIVE = (
    "COALESCE(m.last_trade_date > CURRENT_DATE - make_interval(days => 35)"
    " OR m.last_deposit_time > CURRENT_DATE - make_interval(days => 35), false)"
)

_MV_ACTIVE_FTD = (
    f"(m.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_MV_ACTIVE})"
)


# ---------------------------------------------------------------------------
# WHERE-clause builder
# ---------------------------------------------------------------------------

def _build_task_where(
    conditions: List[Dict[str, Any]],
) -> Tuple[List[str], Dict[str, Any]]:
    where_list: List[str] = ["m.client_qualification_date IS NOT NULL"]
    params: Dict[str, Any] = {}

    for i, cond in enumerate(conditions):
        column = cond.get("column", "")
        op = cond.get("op", "eq")
        value = cond.get("value", "")

        if column == "active":
            if value == "true":
                where_list.append(f"({_MV_ACTIVE})")
            else:
                where_list.append(f"NOT ({_MV_ACTIVE})")
            continue

        if column == "active_ftd":
            if value == "true":
                where_list.append(f"({_MV_ACTIVE_FTD})")
            else:
                where_list.append(f"NOT ({_MV_ACTIVE_FTD})")
            continue

        sql_op = _OP_MAP.get(op, "=")

        if column == "days_from_last_trade":
            try:
                cast_value: Any = int(value)
            except (ValueError, TypeError):
                cast_value = value
            params[f"cond_{i}"] = cast_value
            where_list.append(
                f"m.last_trade_date IS NOT NULL"
                f" AND (CURRENT_DATE - m.last_trade_date::date) {sql_op} :cond_{i}"
            )
            continue

        sql_expr = _TASK_COL_SQL.get(column)
        if sql_expr is None:
            continue

        try:
            cast_value = float(value)
        except (ValueError, TypeError):
            cast_value = value

        params[f"cond_{i}"] = cast_value
        where_list.append(f"{sql_expr} {sql_op} :cond_{i}")

    return where_list, params


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ConditionIn(BaseModel):
    column: str
    op: str
    value: str


VALID_COLORS = {"red", "orange", "yellow", "green", "blue", "purple", "pink", "grey"}


class TaskCreate(BaseModel):
    name: str
    conditions: List[ConditionIn]
    color: Optional[str] = "grey"


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    conditions: Optional[List[ConditionIn]] = None
    color: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _trigger_task_assignments() -> None:
    """Fire-and-forget: recompute client_task_assignments after a task change."""
    from app.routers.etl import rebuild_task_assignments
    await rebuild_task_assignments()


def _task_out(task: RetentionTask) -> Dict[str, Any]:
    return {
        "id": task.id,
        "name": task.name,
        "conditions": json.loads(task.conditions),
        "color": task.color or "grey",
        "created_at": task.created_at.isoformat() if task.created_at else None,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/retention/tasks")
async def list_tasks(
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    result = await db.execute(
        select(RetentionTask).order_by(RetentionTask.created_at)
    )
    tasks = result.scalars().all()
    return [_task_out(t) for t in tasks]


@router.post("/retention/tasks", status_code=201)
async def create_task(
    body: TaskCreate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    color = (body.color or "grey").lower()
    if color not in VALID_COLORS:
        color = "grey"
    task = RetentionTask(
        name=body.name,
        conditions=json.dumps([c.model_dump() for c in body.conditions]),
        color=color,
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    asyncio.create_task(_trigger_task_assignments())
    return _task_out(task)


@router.put("/retention/tasks/{task_id}")
async def update_task(
    task_id: int,
    body: TaskUpdate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if body.name is not None:
        task.name = body.name
    if body.conditions is not None:
        task.conditions = json.dumps([c.model_dump() for c in body.conditions])
    if body.color is not None:
        color = body.color.lower()
        if color in VALID_COLORS:
            task.color = color
    await db.commit()
    await db.refresh(task)
    asyncio.create_task(_trigger_task_assignments())
    return _task_out(task)


@router.delete("/retention/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> None:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await db.delete(task)
    await db.commit()
    asyncio.create_task(_trigger_task_assignments())


@router.get("/retention/tasks/{task_id}/clients")
async def get_task_clients(
    task_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        conditions: List[Dict[str, Any]] = json.loads(task.conditions)
        where_list, params = _build_task_where(conditions)
        where_clause = " AND ".join(where_list)

        # Count
        count_sql = text(f"SELECT COUNT(*) FROM retention_mv m WHERE {where_clause}")
        count_result = await db.execute(count_sql, params)
        total: int = count_result.scalar() or 0

        # Paginated rows
        offset = (page - 1) * page_size
        data_params = {**params, "limit": page_size, "offset": offset}
        data_sql = text(
            f"SELECT"
            f"  m.accountid,"
            f"  m.total_balance    AS balance,"
            f"  m.total_credit     AS credit,"
            f"  m.total_equity     AS equity,"
            f"  m.trade_count,"
            f"  m.total_profit,"
            f"  m.last_trade_date,"
            f"  m.assigned_to,"
            f"  COALESCE("
            f"    m.last_trade_date > CURRENT_DATE - make_interval(days => 35)"
            f"    OR m.last_deposit_time > CURRENT_DATE - make_interval(days => 35),"
            f"    false"
            f"  ) AS active"
            f" FROM retention_mv m"
            f" WHERE {where_clause}"
            f" ORDER BY m.accountid"
            f" LIMIT :limit OFFSET :offset"
        )
        data_result = await db.execute(data_sql, data_params)
        rows = data_result.fetchall()

        # Collect assigned_to IDs to resolve agent names
        agent_ids = list({r.assigned_to for r in rows if r.assigned_to})
        agent_map: Dict[str, str] = {}
        if agent_ids:
            users_result = await db.execute(
                text(
                    "SELECT id, TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) AS full_name"
                    " FROM vtiger_users WHERE id = ANY(:ids)"
                ),
                {"ids": agent_ids},
            )
            for ur in users_result.fetchall():
                agent_map[str(ur.id)] = ur.full_name or ur.id

        clients = []
        for r in rows:
            clients.append(
                {
                    "accountid": r.accountid,
                    "balance": r.balance,
                    "credit": r.credit,
                    "equity": r.equity,
                    "trade_count": r.trade_count,
                    "total_profit": r.total_profit,
                    "last_trade_date": (
                        r.last_trade_date.isoformat() if r.last_trade_date else None
                    ),
                    "active": bool(r.active),
                    "agent_name": agent_map.get(str(r.assigned_to)) if r.assigned_to else None,
                }
            )

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": clients,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Query failed: {exc}") from exc
