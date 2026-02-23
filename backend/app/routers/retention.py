from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.database import execute_query
from app.replica_database import get_replica_db

router = APIRouter()

# Step 1: Pull all qualified accounts + their logins from MSSQL
_MSSQL_ACCOUNTS_QUERY = """
    SELECT a.accountid, vta.login
    FROM report.ant_acc a
    INNER JOIN report.vtiger_trading_accounts vta
        ON a.accountid = vta.vtigeraccountid
    WHERE a.client_qualification_date IS NOT NULL
"""

# Step 2: Count trades per login from dealio replica
_REPLICA_TRADES_QUERY = """
    SELECT login, COUNT(*) AS trade_count
    FROM dealio.trades_mt4
    WHERE cmd IN (0, 1)
      AND login = ANY(:logins)
    GROUP BY login
"""


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: Any = Depends(get_current_user),
    replica_db: AsyncSession = Depends(get_replica_db),
) -> dict:
    try:
        # 1. Fetch qualified accounts + logins from MSSQL
        rows = await execute_query(_MSSQL_ACCOUNTS_QUERY)
        if not rows:
            return {"total": 0, "page": page, "page_size": page_size, "clients": []}

        # Build login → accountid map (account may have multiple logins)
        login_to_account: dict[int, str] = {}
        for r in rows:
            if r["login"] is not None:
                login_to_account[int(r["login"])] = str(r["accountid"])

        logins = list(login_to_account.keys())

        # 2. Count trades per login from dealio replica
        result = await replica_db.execute(
            text(_REPLICA_TRADES_QUERY),
            {"logins": logins},
        )
        trade_rows = result.mappings().all()

        # 3. Merge: sum trade counts per account across all logins
        account_trades: dict[str, int] = {}
        for tr in trade_rows:
            login = int(tr["login"])
            accountid = login_to_account.get(login)
            if accountid:
                account_trades[accountid] = account_trades.get(accountid, 0) + int(tr["trade_count"])

        # 4. Sort by accountid and paginate in Python
        sorted_clients = sorted(account_trades.items())
        total = len(sorted_clients)
        page_clients = sorted_clients[(page - 1) * page_size: page * page_size]

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [{"accountid": aid, "trade_count": count} for aid, count in page_clients],
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")
