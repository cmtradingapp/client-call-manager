import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth_deps import get_current_user, require_admin
from app.models.etl_sync_log import EtlSyncLog
from app.pg_database import AsyncSessionLocal, get_db
from app.replica_database import get_replica_db

logger = logging.getLogger(__name__)
router = APIRouter()

_BATCH_SIZE = 10_000

_UPSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd) VALUES (:ticket, :login, :cmd)"
    " ON CONFLICT (ticket) DO UPDATE SET login = EXCLUDED.login, cmd = EXCLUDED.cmd"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run_full_sync(log_id: int) -> None:
    from app.replica_database import _ReplicaSession  # import at call-time

    if _ReplicaSession is None:
        async with AsyncSessionLocal() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "error"
                log.error_message = "Replica database not configured"
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()
        return

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE trades_mt4"))
            await db.commit()

        total = 0
        offset = 0

        while True:
            async with _ReplicaSession() as replica_db:
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

            async with AsyncSessionLocal() as db:
                await db.execute(text(_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2]} for r in rows])
                await db.commit()

            total += len(rows)
            offset += _BATCH_SIZE
            logger.info("ETL full sync: %d rows so far", total)

            if len(rows) < _BATCH_SIZE:
                break

        async with AsyncSessionLocal() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        logger.info("ETL full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL full sync failed: %s", e)
        async with AsyncSessionLocal() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "error"
                log.error_message = str(e)
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()


async def incremental_sync_trades(
    session_factory: async_sessionmaker,
    replica_session_factory: async_sessionmaker,
) -> None:
    """Incremental sync: insert/update trades with ticket > local max. Called by scheduler."""
    log_id: int | None = None
    try:
        async with session_factory() as db:
            result = await db.execute(text("SELECT COALESCE(MAX(ticket), 0) FROM trades_mt4"))
            last_ticket = result.scalar()

            log = EtlSyncLog(sync_type="incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        total = 0
        offset = 0

        while True:
            async with replica_session_factory() as replica_db:
                result = await replica_db.execute(
                    text(
                        "SELECT ticket, login, cmd FROM dealio.trades_mt4"
                        " WHERE ticket > :last_ticket"
                        " ORDER BY ticket"
                        " LIMIT :limit OFFSET :offset"
                    ),
                    {"last_ticket": last_ticket, "limit": _BATCH_SIZE, "offset": offset},
                )
                rows = result.fetchall()

            if not rows:
                break

            async with session_factory() as db:
                await db.execute(text(_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2]} for r in rows])
                await db.commit()

            total += len(rows)
            offset += _BATCH_SIZE

            if len(rows) < _BATCH_SIZE:
                break

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if total:
            logger.info("ETL incremental: %d new/updated trades", total)

    except Exception as e:
        logger.error("ETL incremental sync failed: %s", e)
        if log_id:
            try:
                async with session_factory() as db:
                    log = await db.get(EtlSyncLog, log_id)
                    if log:
                        log.status = "error"
                        log.error_message = str(e)
                        log.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.post("/etl/sync-trades")
async def sync_trades(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Trigger a full refresh in the background. Returns immediately."""
    log = EtlSyncLog(sync_type="full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync, log.id)
    return {"status": "started", "log_id": log.id}


@router.get("/etl/sync-status")
async def sync_status(
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> dict:
    """Return the last 20 sync runs."""
    result = await db.execute(
        text("SELECT * FROM etl_sync_log ORDER BY started_at DESC LIMIT 20")
    )
    rows = result.mappings().all()
    return {
        "logs": [
            {
                "id": r["id"],
                "sync_type": r["sync_type"],
                "status": r["status"],
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                "rows_synced": r["rows_synced"],
                "error_message": r["error_message"],
            }
            for r in rows
        ]
    }
