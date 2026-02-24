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
    "days_in_retention": "(CURRENT_DATE - a.client_qualification_date)",
    "trade_count": "ta.trade_count",
    "total_profit": "ta.total_profit",
    "last_trade_date": "ta.last_trade_date",
    "days_from_last_trade": "ta.last_close_time",
    "active": "active",
    "active_ftd": "active_ftd",
    "deposit_count": "da.deposit_count",
    "total_deposit": "da.total_deposit",
    "balance": "ab.total_balance",
    "credit": "ab.total_credit",
}

_OP_MAP = {"eq": "=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}


def _num_cond(op: str, expr: str, param: str) -> str | None:
    sql_op = _OP_MAP.get(op)
    return f"{expr} {sql_op} :{param}" if sql_op else None


# qualifying_logins pre-filters to only accounts we care about,
# so the trade/deposit/balance CTEs only process relevant rows.
_CTES = """
WITH qualifying_logins AS (
    SELECT vta.login, a.accountid
    FROM ant_acc a
    INNER JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = a.accountid
    WHERE a.client_qualification_date IS NOT NULL
      AND a.client_qualification_date >= '2024-01-01'
),
trades_agg AS (
    SELECT
        ql.accountid,
        COUNT(t.ticket) AS trade_count,
        COALESCE(SUM(t.computed_profit), 0) AS total_profit,
        MAX(t.open_time) AS last_trade_date,
        MAX(t.close_time) AS last_close_time,
        COALESCE(BOOL_OR(
            t.open_time IS NOT NULL AND t.open_time > CURRENT_DATE - make_interval(days => :activity_days)
        ), false) AS has_recent_trade
    FROM qualifying_logins ql
    LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)
        AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN ('inactivity', 'zeroingusd', 'spread'))
    GROUP BY ql.accountid
),
deposits_agg AS (
    SELECT
        ql.accountid,
        COUNT(mtt.mttransactionsid) AS deposit_count,
        COALESCE(SUM(mtt.usdamount), 0) AS total_deposit,
        COALESCE(BOOL_OR(
            mtt.confirmation_time IS NOT NULL
            AND mtt.confirmation_time > CURRENT_DATE - make_interval(days => :activity_days)
        ), false) AS has_recent_deposit
    FROM qualifying_logins ql
    LEFT JOIN vtiger_mttransactions mtt ON mtt.login = ql.login
        AND mtt.transactionapproval = 'Approved'
        AND mtt.transactiontype = 'Deposit'
        AND (mtt.payment_method IS NULL OR mtt.payment_method != 'BonusProtectedPositionCashback')
    GROUP BY ql.accountid
),
balance_agg AS (
    SELECT
        ql.accountid,
        COALESCE(SUM(vta.balance), 0) AS total_balance,
        COALESCE(SUM(vta.credit), 0) AS total_credit
    FROM qualifying_logins ql
    INNER JOIN vtiger_trading_accounts vta ON vta.login = ql.login
    GROUP BY ql.accountid
)
"""

_JOINS = """
    FROM ant_acc a
    INNER JOIN trades_agg ta ON ta.accountid = a.accountid
    INNER JOIN deposits_agg da ON da.accountid = a.accountid
    INNER JOIN balance_agg ab ON ab.accountid = a.accountid
"""

_ACTIVE_EXPR = "(ta.has_recent_trade OR da.has_recent_deposit)"
_ACTIVE_FTD_EXPR = f"(a.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_ACTIVE_EXPR})"


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
        # Give this session more memory for hash aggregations
        await db.execute(text("SET work_mem = '256MB'"))

        sort_col = _SORT_COLS.get(sort_by, "a.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

        where: list[str] = ["a.client_qualification_date IS NOT NULL"]
        params: dict = {"activity_days": activity_days}

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
            cond = _num_cond(trade_count_op, "ta.trade_count", "trade_count_val")
            if cond:
                where.append(cond)
                params["trade_count_val"] = int(trade_count_val)

        if profit_op and profit_val is not None:
            cond = _num_cond(profit_op, "ta.total_profit", "profit_val")
            if cond:
                where.append(cond)
                params["profit_val"] = profit_val

        if last_trade_from:
            where.append("ta.last_trade_date >= :last_trade_from")
            params["last_trade_from"] = last_trade_from
        if last_trade_to:
            where.append("ta.last_trade_date <= :last_trade_to")
            params["last_trade_to"] = last_trade_to

        if days_from_last_trade_op and days_from_last_trade_val is not None:
            cond = _num_cond(days_from_last_trade_op, "(CURRENT_DATE - ta.last_close_time::date)", "days_from_last_trade_val")
            if cond:
                where.append(f"ta.last_close_time IS NOT NULL AND {cond}")
                params["days_from_last_trade_val"] = int(days_from_last_trade_val)

        if deposit_count_op and deposit_count_val is not None:
            cond = _num_cond(deposit_count_op, "da.deposit_count", "deposit_count_val")
            if cond:
                where.append(cond)
                params["deposit_count_val"] = int(deposit_count_val)

        if total_deposit_op and total_deposit_val is not None:
            cond = _num_cond(total_deposit_op, "da.total_deposit", "total_deposit_val")
            if cond:
                where.append(cond)
                params["total_deposit_val"] = total_deposit_val

        if balance_op and balance_val is not None:
            cond = _num_cond(balance_op, "ab.total_balance", "balance_val")
            if cond:
                where.append(cond)
                params["balance_val"] = balance_val

        if credit_op and credit_val is not None:
            cond = _num_cond(credit_op, "ab.total_credit", "credit_val")
            if cond:
                where.append(cond)
                params["credit_val"] = credit_val

        if active == "true":
            where.append(f"{_ACTIVE_EXPR} = true")
        elif active == "false":
            where.append(f"{_ACTIVE_EXPR} = false")

        if active_ftd == "true":
            where.append(f"{_ACTIVE_FTD_EXPR} = true")
        elif active_ftd == "false":
            where.append(f"{_ACTIVE_FTD_EXPR} = false")

        where_clause = " AND ".join(where)

        count_result = await db.execute(
            text(f"{_CTES} SELECT COUNT(*) FROM (SELECT a.accountid {_JOINS} WHERE {where_clause}) _sub"),
            params,
        )
        total = count_result.scalar() or 0

        rows_result = await db.execute(
            text(f"""
                {_CTES}
                SELECT
                    a.accountid,
                    a.client_qualification_date,
                    (CURRENT_DATE - a.client_qualification_date) AS days_in_retention,
                    ta.trade_count,
                    ta.total_profit,
                    ta.last_trade_date,
                    CASE WHEN ta.last_close_time IS NOT NULL
                         THEN (CURRENT_DATE - ta.last_close_time::date) ELSE NULL END AS days_from_last_trade,
                    {_ACTIVE_EXPR} AS active,
                    {_ACTIVE_FTD_EXPR} AS active_ftd,
                    da.deposit_count,
                    da.total_deposit,
                    ab.total_balance AS balance,
                    ab.total_credit AS credit
                {_JOINS}
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
                }
                for r in rows
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
