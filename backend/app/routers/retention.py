from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth_deps import get_current_user
from app.database import execute_query

router = APIRouter()

_QUERY = """
    WITH trade_counts AS (
        SELECT
            vta.vtigeraccountid,
            COUNT(t.login) AS trade_count
        FROM report.vtiger_trading_accounts vta
        INNER JOIN report.dealio_mt4trades t
            ON t.login = vta.login
        WHERE t.cmd IN (0, 1)
          AND t.symbol NOT IN ('Inactivity', 'ZeroingUSD', 'Spread')
        GROUP BY vta.vtigeraccountid
    )
    SELECT
        a.accountid,
        tc.trade_count,
        COUNT(*) OVER() AS total_count
    FROM report.ant_acc a
    INNER JOIN trade_counts tc ON a.accountid = tc.vtigeraccountid
    WHERE a.client_qualification_date IS NOT NULL
    ORDER BY a.accountid
    OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: Any = Depends(get_current_user),
) -> dict:
    try:
        rows = await execute_query(_QUERY, ((page - 1) * page_size, page_size))
        total = int(rows[0]["total_count"]) if rows else 0
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [{"accountid": str(r["accountid"]), "trade_count": int(r["trade_count"])} for r in rows],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
