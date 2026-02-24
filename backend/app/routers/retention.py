from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.pg_database import get_db

router = APIRouter()

_SORT_COLS = {
    "accountid": "a.accountid",
    "trade_count": "trade_count",
    "days_in_retention": "days_in_retention",
    "total_profit": "total_profit",
}

_BASE_SELECT = """
    SELECT
        a.accountid,
        a.client_qualification_date,
        (CURRENT_DATE - a.client_qualification_date) AS days_in_retention,
        COUNT(t.ticket) AS trade_count,
        COALESCE(SUM(t.profit), 0) AS total_profit
    FROM ant_acc a
    INNER JOIN vtiger_trading_accounts vta ON a.accountid = vta.vtigeraccountid
    LEFT JOIN trades_mt4 t ON t.login = vta.login AND t.cmd IN (0, 1)
    WHERE a.client_qualification_date IS NOT NULL
      AND (:search = '' OR a.accountid ILIKE :search_pattern)
    GROUP BY a.accountid, a.client_qualification_date
"""

_COUNT_QUERY = """
    SELECT COUNT(DISTINCT a.accountid)
    FROM ant_acc a
    INNER JOIN vtiger_trading_accounts vta ON a.accountid = vta.vtigeraccountid
    WHERE a.client_qualification_date IS NOT NULL
      AND (:search = '' OR a.accountid ILIKE :search_pattern)
"""


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query(""),
    sort_by: str = Query("accountid"),
    sort_dir: str = Query("asc"),
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        sort_col = _SORT_COLS.get(sort_by, "a.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
        search_pattern = f"%{search}%" if search else ""

        params = {"search": search, "search_pattern": search_pattern}

        total_result = await db.execute(text(_COUNT_QUERY), params)
        total = total_result.scalar() or 0

        query = f"{_BASE_SELECT} ORDER BY {sort_col} {direction} LIMIT :limit OFFSET :offset"
        result = await db.execute(
            text(query),
            {**params, "limit": page_size, "offset": (page - 1) * page_size},
        )
        rows = result.mappings().all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [
                {
                    "accountid": str(r["accountid"]),
                    "trade_count": int(r["trade_count"]),
                    "days_in_retention": int(r["days_in_retention"]) if r["days_in_retention"] is not None else None,
                    "total_profit": float(r["total_profit"]),
                }
                for r in rows
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
