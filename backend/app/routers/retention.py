from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.pg_database import get_db

router = APIRouter()

# active = had a trade (open_time) OR deposit in the last N days
_MV_ACTIVE = (
    "COALESCE("
    "m.last_trade_date > CURRENT_DATE - make_interval(days => :activity_days)"
    " OR m.last_deposit_time > CURRENT_DATE - make_interval(days => :activity_days)"
    ", false)"
)
_MV_ACTIVE_FTD = (
    f"(m.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_MV_ACTIVE})"
)

_SORT_COLS = {
    "accountid": "m.accountid",
    "client_qualification_date": "m.client_qualification_date",
    "days_in_retention": "(CURRENT_DATE - m.client_qualification_date)",
    "trade_count": "m.trade_count",
    "total_profit": "m.total_profit",
    "last_trade_date": "m.last_trade_date",
    "days_from_last_trade": "m.last_close_time",
    "active": _MV_ACTIVE,
    "active_ftd": _MV_ACTIVE_FTD,
    "deposit_count": "m.deposit_count",
    "total_deposit": "m.total_deposit",
    "balance": "m.total_balance",
    "credit": "m.total_credit",
    "sales_client_potential": "m.sales_client_potential",
    "age": "EXTRACT(year FROM AGE(m.birth_date))",
}

_OP_MAP = {"eq": "=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}


def _num_cond(op: str, expr: str, param: str) -> str | None:
    sql_op = _OP_MAP.get(op)
    return f"{expr} {sql_op} :{param}" if sql_op else None


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort_by: str = Query("accountid"),
    sort_dir: str = Query("asc"),
    accountid: str = Query(""),
    # numeric filters
    trade_count_op: str = Query(""),
    trade_count_val: float | None = Query(None),
    days_op: str = Query(""),
    days_val: float | None = Query(None),
    profit_op: str = Query(""),
    profit_val: float | None = Query(None),
    days_from_last_trade_op: str = Query(""),
    days_from_last_trade_val: float | None = Query(None),
    deposit_count_op: str = Query(""),
    deposit_count_val: float | None = Query(None),
    total_deposit_op: str = Query(""),
    total_deposit_val: float | None = Query(None),
    balance_op: str = Query(""),
    balance_val: float | None = Query(None),
    credit_op: str = Query(""),
    credit_val: float | None = Query(None),
    # date range filters
    qual_date_from: str = Query(""),
    qual_date_to: str = Query(""),
    last_trade_from: str = Query(""),
    last_trade_to: str = Query(""),
    # boolean filters
    active: str = Query(""),
    active_ftd: str = Query(""),
    # activity window
    activity_days: int = Query(35, ge=1, le=365),
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        sort_col = _SORT_COLS.get(sort_by, "m.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

        where: list[str] = ["m.client_qualification_date IS NOT NULL"]
        params: dict = {"activity_days": activity_days}

        if accountid:
            where.append("m.accountid ILIKE :accountid_pattern")
            params["accountid_pattern"] = f"%{accountid}%"

        if qual_date_from:
            where.append("m.client_qualification_date >= :qual_date_from")
            params["qual_date_from"] = qual_date_from
        if qual_date_to:
            where.append("m.client_qualification_date <= :qual_date_to")
            params["qual_date_to"] = qual_date_to

        if days_op and days_val is not None:
            cond = _num_cond(days_op, "(CURRENT_DATE - m.client_qualification_date)", "days_val")
            if cond:
                where.append(cond)
                params["days_val"] = int(days_val)

        if trade_count_op and trade_count_val is not None:
            cond = _num_cond(trade_count_op, "m.trade_count", "trade_count_val")
            if cond:
                where.append(cond)
                params["trade_count_val"] = int(trade_count_val)

        if profit_op and profit_val is not None:
            cond = _num_cond(profit_op, "m.total_profit", "profit_val")
            if cond:
                where.append(cond)
                params["profit_val"] = profit_val

        if last_trade_from:
            where.append("m.last_trade_date >= :last_trade_from")
            params["last_trade_from"] = last_trade_from
        if last_trade_to:
            where.append("m.last_trade_date <= :last_trade_to")
            params["last_trade_to"] = last_trade_to

        if days_from_last_trade_op and days_from_last_trade_val is not None:
            cond = _num_cond(days_from_last_trade_op, "(CURRENT_DATE - m.last_close_time::date)", "days_from_last_trade_val")
            if cond:
                where.append(f"m.last_close_time IS NOT NULL AND {cond}")
                params["days_from_last_trade_val"] = int(days_from_last_trade_val)

        if deposit_count_op and deposit_count_val is not None:
            cond = _num_cond(deposit_count_op, "m.deposit_count", "deposit_count_val")
            if cond:
                where.append(cond)
                params["deposit_count_val"] = int(deposit_count_val)

        if total_deposit_op and total_deposit_val is not None:
            cond = _num_cond(total_deposit_op, "m.total_deposit", "total_deposit_val")
            if cond:
                where.append(cond)
                params["total_deposit_val"] = total_deposit_val

        if balance_op and balance_val is not None:
            cond = _num_cond(balance_op, "m.total_balance", "balance_val")
            if cond:
                where.append(cond)
                params["balance_val"] = balance_val

        if credit_op and credit_val is not None:
            cond = _num_cond(credit_op, "m.total_credit", "credit_val")
            if cond:
                where.append(cond)
                params["credit_val"] = credit_val

        if active == "true":
            where.append(f"({_MV_ACTIVE})")
        elif active == "false":
            where.append(f"NOT ({_MV_ACTIVE})")

        if active_ftd == "true":
            where.append(f"({_MV_ACTIVE_FTD})")
        elif active_ftd == "false":
            where.append(f"NOT ({_MV_ACTIVE_FTD})")

        where_clause = " AND ".join(where)

        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM retention_mv m WHERE {where_clause}"),
            params,
        )
        total = count_result.scalar() or 0

        rows_result = await db.execute(
            text(f"""
                SELECT
                    m.accountid,
                    m.client_qualification_date,
                    (CURRENT_DATE - m.client_qualification_date) AS days_in_retention,
                    m.trade_count,
                    m.total_profit,
                    m.last_trade_date,
                    CASE WHEN m.last_close_time IS NOT NULL
                         THEN (CURRENT_DATE - m.last_close_time::date) END AS days_from_last_trade,
                    {_MV_ACTIVE} AS active,
                    {_MV_ACTIVE_FTD} AS active_ftd,
                    m.deposit_count,
                    m.total_deposit,
                    m.total_balance AS balance,
                    m.total_credit AS credit,
                    m.sales_client_potential,
                    CASE WHEN m.birth_date IS NOT NULL
                         THEN EXTRACT(year FROM AGE(m.birth_date))::int END AS age
                FROM retention_mv m
                WHERE {where_clause}
                ORDER BY {sort_col} {direction}
                LIMIT :limit OFFSET :offset
            """),
            {**params, "limit": page_size, "offset": (page - 1) * page_size},
        )
        rows = rows_result.mappings().all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [
                {
                    "accountid": str(r["accountid"]),
                    "client_qualification_date": r["client_qualification_date"].isoformat() if r["client_qualification_date"] else None,
                    "days_in_retention": int(r["days_in_retention"]) if r["days_in_retention"] is not None else None,
                    "trade_count": int(r["trade_count"]),
                    "total_profit": float(r["total_profit"]),
                    "last_trade_date": r["last_trade_date"].isoformat() if r["last_trade_date"] else None,
                    "days_from_last_trade": int(r["days_from_last_trade"]) if r["days_from_last_trade"] is not None else None,
                    "active": bool(r["active"]),
                    "active_ftd": bool(r["active_ftd"]),
                    "deposit_count": int(r["deposit_count"]),
                    "total_deposit": float(r["total_deposit"]),
                    "balance": float(r["balance"]),
                    "credit": float(r["credit"]),
                    "sales_client_potential": r["sales_client_potential"],
                    "age": int(r["age"]) if r["age"] is not None else None,
                }
                for r in rows
            ],
        }
    except Exception as e:
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
