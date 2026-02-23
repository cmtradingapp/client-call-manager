from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth_deps import get_current_user
from app.database import execute_query

router = APIRouter()

_DATA_QUERY = """
    SELECT
        a.accountid,
        COUNT(t.login) AS trade_count
    FROM report.ant_acc a
    LEFT JOIN report.vtiger_trading_accounts vta
        ON a.accountid = vta.vtigeraccountid
    LEFT JOIN report.dealio_mt4trades t
        ON t.login = vta.login
        AND t.cmd IN (0, 1)
        AND t.symbol NOT IN ('Inactivity', 'ZeroingUSD', 'Spread')
    WHERE a.client_qualification_date IS NOT NULL
    GROUP BY a.accountid
    HAVING COUNT(t.login) > 0
    ORDER BY a.accountid
    OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""

_COUNT_QUERY = """
    SELECT COUNT(DISTINCT a.accountid)
    FROM report.ant_acc a
    WHERE a.client_qualification_date IS NOT NULL
"""


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: Any = Depends(get_current_user),
) -> dict:
    try:
        offset = (page - 1) * page_size
        count_rows = await execute_query(_COUNT_QUERY)
        total = list(count_rows[0].values())[0] if count_rows else 0

        rows = await execute_query(_DATA_QUERY, (offset, page_size))
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [{"accountid": str(r["accountid"]), "trade_count": int(r["trade_count"])} for r in rows],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
