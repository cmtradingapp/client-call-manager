from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.pg_database import get_db

router = APIRouter()

_CLIENTS_QUERY = """
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
    GROUP BY a.accountid, a.client_qualification_date
    ORDER BY a.accountid
    LIMIT :limit OFFSET :offset
"""

_COUNT_QUERY = """
    SELECT COUNT(DISTINCT a.accountid)
    FROM ant_acc a
    INNER JOIN vtiger_trading_accounts vta ON a.accountid = vta.vtigeraccountid
    WHERE a.client_qualification_date IS NOT NULL
"""


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        total_result = await db.execute(text(_COUNT_QUERY))
        total = total_result.scalar() or 0

        result = await db.execute(
            text(_CLIENTS_QUERY),
            {"limit": page_size, "offset": (page - 1) * page_size},
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
