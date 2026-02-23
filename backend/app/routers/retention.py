from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from app.auth_deps import get_current_user
from app.replica_database import _replica_engine

router = APIRouter()

# One account can have many logins (rows in vtiger_trading_accounts).
# We join all logins to their trades, then GROUP BY accountid to sum across all logins.
_DATA_QUERY = text("""
    SELECT
        a.accountid,
        COUNT(t.ticket) AS trade_count
    FROM report.ant_acc a
    INNER JOIN report.vtiger_trading_accounts vta
        ON vta.vtigeraccountid = a.vtigeraccountid
    INNER JOIN report.dealio_mt4trades t
        ON t.login = vta.login
    WHERE a.client_qualification_time IS NOT NULL
      AND t.cmd IN (0, 1)
    GROUP BY a.accountid
    ORDER BY a.accountid
    LIMIT :limit OFFSET :offset
""")

_COUNT_QUERY = text("""
    SELECT COUNT(DISTINCT a.accountid)
    FROM report.ant_acc a
    INNER JOIN report.vtiger_trading_accounts vta
        ON vta.vtigeraccountid = a.vtigeraccountid
    INNER JOIN report.dealio_mt4trades t
        ON t.login = vta.login
    WHERE a.client_qualification_time IS NOT NULL
      AND t.cmd IN (0, 1)
""")


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: Any = Depends(get_current_user),
) -> dict:
    if _replica_engine is None:
        raise HTTPException(status_code=503, detail="Replica database is not configured")
    try:
        async with _replica_engine.connect() as conn:
            total_result = await conn.execute(_COUNT_QUERY)
            total = total_result.scalar() or 0

            data_result = await conn.execute(
                _DATA_QUERY,
                {"limit": page_size, "offset": (page - 1) * page_size},
            )
            rows = data_result.fetchall()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [{"accountid": str(r[0]), "trade_count": int(r[1])} for r in rows],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
