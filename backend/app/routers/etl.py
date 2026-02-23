import logging

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.pg_database import get_db
from app.replica_database import get_replica_db

logger = logging.getLogger(__name__)
router = APIRouter()

_BATCH_SIZE = 10_000


@router.post("/etl/sync-trades")
async def sync_trades(
    db: AsyncSession = Depends(get_db),
    replica_db: AsyncSession = Depends(get_replica_db),
    _=Depends(require_admin),
) -> dict:
    """
    Full refresh of local trades_mt4 from the dealio replica.
    Truncates the local table, then copies all rows in batches.
    Admin only.
    """
    await db.execute(text("TRUNCATE TABLE trades_mt4"))
    await db.commit()

    total = 0
    offset = 0

    while True:
        result = await replica_db.execute(
            text(
                "SELECT ticket, login, cmd FROM dealio.trades_mt4"
                " ORDER BY ticket"
                " LIMIT :limit OFFSET :offset"
            ),
            {"limit": _BATCH_SIZE, "offset": offset},
        )
        rows = result.fetchall()
        if not rows:
            break

        await db.execute(
            text(
                "INSERT INTO trades_mt4 (ticket, login, cmd) VALUES (:ticket, :login, :cmd)"
                " ON CONFLICT (ticket) DO NOTHING"
            ),
            [{"ticket": r[0], "login": r[1], "cmd": r[2]} for r in rows],
        )
        await db.commit()

        total += len(rows)
        offset += _BATCH_SIZE
        logger.info("ETL trades_mt4: synced %d rows so far", total)

        if len(rows) < _BATCH_SIZE:
            break

    logger.info("ETL trades_mt4: completed — %d total rows", total)
    return {"synced": total}
