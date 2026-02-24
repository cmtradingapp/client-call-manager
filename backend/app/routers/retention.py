from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.pg_database import get_db

router = APIRouter()

_SORT_COLS = {
    "accountid": "a.accountid",
    "client_qualification_date": "a.client_qualification_date",
    "trade_count": "trade_count",
    "days_in_retention": "days_in_retention",
    "total_profit": "total_profit",
    "last_trade_date": "last_trade_date",
    "active": "active",
    "active_ftd": "active_ftd",
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
    # text / numeric filters
    accountid: str = Query(""),
    trade_count_op: str = Query(""),
    trade_count_val: float | None = Query(None),
    days_op: str = Query(""),
    days_val: float | None = Query(None),
    profit_op: str = Query(""),
    profit_val: float | None = Query(None),
    # date range filter
    qual_date_from: str = Query(""),   # YYYY-MM-DD
    qual_date_to: str = Query(""),     # YYYY-MM-DD
    # last trade date range
    last_trade_from: str = Query(""),  # YYYY-MM-DD
    last_trade_to: str = Query(""),    # YYYY-MM-DD
    # boolean filters
    active: str = Query(""),        # "true" | "false" | ""
    active_ftd: str = Query(""),    # "true" | "false" | ""
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        sort_col = _SORT_COLS.get(sort_by, "a.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

        where: list[str] = ["a.client_qualification_date IS NOT NULL"]
        having: list[str] = []
        params: dict = {}

        if accountid:
            where.append("a.accountid ILIKE :accountid_pattern")
            params["accountid_pattern"] = f"%{accountid}%"

        if qual_date_from:
            where.append("a.client_qualification_date >= :qual_date_from")
            params["qual_date_from"] = qual_date_from
        if qual_date_to:
            where.append("a.client_qualification_date <= :qual_date_to")
            params["qual_date_to"] = qual_date_to

        if days_op and days_val is not None:
            cond = _num_cond(days_op, "(CURRENT_DATE - a.client_qualification_date)", "days_val")
            if cond:
                where.append(cond)
                params["days_val"] = int(days_val)

        if trade_count_op and trade_count_val is not None:
            cond = _num_cond(trade_count_op, "COUNT(t.ticket)", "trade_count_val")
            if cond:
                having.append(cond)
                params["trade_count_val"] = int(trade_count_val)

        if profit_op and profit_val is not None:
            cond = _num_cond(profit_op, "COALESCE(SUM(t.profit), 0)", "profit_val")
            if cond:
                having.append(cond)
                params["profit_val"] = profit_val

        _active_expr = "COALESCE(BOOL_OR(t.close_time IS NOT NULL AND t.close_time > CURRENT_DATE - INTERVAL '7 days'), false)"
        _ftd_expr = f"(a.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_active_expr})"

        if last_trade_from:
            having.append("MAX(t.close_time) >= :last_trade_from")
            params["last_trade_from"] = last_trade_from
        if last_trade_to:
            having.append("MAX(t.close_time) <= :last_trade_to")
            params["last_trade_to"] = last_trade_to

        if active == "true":
            having.append(f"{_active_expr} = true")
        elif active == "false":
            having.append(f"{_active_expr} = false")

        if active_ftd == "true":
            having.append(f"{_ftd_expr} = true")
        elif active_ftd == "false":
            having.append(f"{_ftd_expr} = false")

        where_clause = " AND ".join(where)
        having_clause = f"HAVING {' AND '.join(having)}" if having else ""

        base = f"""
            FROM ant_acc a
            INNER JOIN vtiger_trading_accounts vta ON a.accountid = vta.vtigeraccountid
            LEFT JOIN trades_mt4 t ON t.login = vta.login AND t.cmd IN (0, 1)
            WHERE {where_clause}
            GROUP BY a.accountid, a.client_qualification_date
            {having_clause}
        """

        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM (SELECT a.accountid {base}) _sub"),
            params,
        )
        total = count_result.scalar() or 0

        rows_result = await db.execute(
            text(f"""
                SELECT
                    a.accountid,
                    a.client_qualification_date,
                    (CURRENT_DATE - a.client_qualification_date) AS days_in_retention,
                    COUNT(t.ticket) AS trade_count,
                    COALESCE(SUM(t.profit), 0) AS total_profit,
                    MAX(t.close_time) AS last_trade_date,
                    {_active_expr} AS active,
                    {_ftd_expr} AS active_ftd
                {base}
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
                    "trade_count": int(r["trade_count"]),
                    "days_in_retention": int(r["days_in_retention"]) if r["days_in_retention"] is not None else None,
                    "total_profit": float(r["total_profit"]),
                    "last_trade_date": r["last_trade_date"].isoformat() if r["last_trade_date"] else None,
                    "active": bool(r["active"]),
                    "active_ftd": bool(r["active_ftd"]),
                }
                for r in rows
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
