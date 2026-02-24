import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth_deps import get_current_user, require_admin
from app.database import execute_query
from app.models.etl_sync_log import EtlSyncLog
from app.pg_database import AsyncSessionLocal, get_db
from app.replica_database import get_replica_db

logger = logging.getLogger(__name__)
router = APIRouter()

_TRADES_BATCH_SIZE = 10_000
_ANT_ACC_BATCH_SIZE = 5_000

_TRADES_UPSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd) VALUES (:ticket, :login, :cmd)"
    " ON CONFLICT (ticket) DO UPDATE SET login = EXCLUDED.login, cmd = EXCLUDED.cmd"
)

_ANT_ACC_UPSERT = (
    "INSERT INTO ant_acc (accountid, client_qualification_date, modifiedtime)"
    " VALUES (:accountid, :client_qualification_date, :modifiedtime)"
    " ON CONFLICT (accountid) DO UPDATE SET"
    " client_qualification_date = EXCLUDED.client_qualification_date,"
    " modifiedtime = EXCLUDED.modifiedtime"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _update_log(log_id: int, status: str, rows_synced: int | None = None, error: str | None = None) -> None:
    async with AsyncSessionLocal() as db:
        log = await db.get(EtlSyncLog, log_id)
        if log:
            log.status = status
            log.rows_synced = rows_synced
            log.error_message = error
            log.completed_at = datetime.now(timezone.utc)
            await db.commit()


# ---------------------------------------------------------------------------
# Trades (dealio.trades_mt4) — full sync
# ---------------------------------------------------------------------------

async def _run_full_sync_trades(log_id: int) -> None:
    from app.replica_database import _ReplicaSession

    if _ReplicaSession is None:
        await _update_log(log_id, "error", error="Replica database not configured")
        return

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE trades_mt4"))
            await db.commit()

        total = 0
        cursor = 0

        while True:
            async with _ReplicaSession() as replica_db:
                result = await replica_db.execute(
                    text(
                        "SELECT ticket, login, cmd FROM dealio.trades_mt4"
                        " WHERE ticket > :cursor ORDER BY ticket LIMIT :limit"
                    ),
                    {"cursor": cursor, "limit": _TRADES_BATCH_SIZE},
                )
                rows = result.fetchall()

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2]} for r in rows])
                await db.commit()

            total += len(rows)
            cursor = rows[-1][0]
            logger.info("ETL trades full: %d rows (cursor=%d)", total, cursor)

            if len(rows) < _TRADES_BATCH_SIZE:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL trades full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL trades full sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# Trades — incremental sync (called by scheduler)
# ---------------------------------------------------------------------------

async def incremental_sync_trades(
    session_factory: async_sessionmaker,
    replica_session_factory: async_sessionmaker,
) -> None:
    log_id: int | None = None
    try:
        async with session_factory() as db:
            result = await db.execute(text("SELECT COALESCE(MAX(ticket), 0) FROM trades_mt4"))
            last_ticket = result.scalar()
            log = EtlSyncLog(sync_type="trades_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        total = 0
        cursor = last_ticket

        while True:
            # Retry up to 3 times on transient connection errors
            rows = None
            for attempt in range(3):
                try:
                    async with replica_session_factory() as replica_db:
                        result = await replica_db.execute(
                            text(
                                "SELECT ticket, login, cmd FROM dealio.trades_mt4"
                                " WHERE ticket > :cursor ORDER BY ticket LIMIT :limit"
                            ),
                            {"cursor": cursor, "limit": _TRADES_BATCH_SIZE},
                        )
                        rows = result.fetchall()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logger.warning("ETL trades: connection error on attempt %d, retrying: %s", attempt + 1, e)
                    await asyncio.sleep(2)

            if not rows:
                break

            async with session_factory() as db:
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2]} for r in rows])
                await db.commit()

            total += len(rows)
            cursor = rows[-1][0]

            if len(rows) < _TRADES_BATCH_SIZE:
                break

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if total:
            logger.info("ETL trades incremental: %d new/updated rows", total)

    except Exception as e:
        logger.error("ETL trades incremental failed: %s", e)
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
# Ant Acc (report.ant_acc) — full sync
# ---------------------------------------------------------------------------

async def _run_full_sync_ant_acc(log_id: int) -> None:
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE ant_acc"))
            await db.commit()

        total = 0
        offset = 0

        while True:
            rows = await execute_query(
                "SELECT accountid, client_qualification_date, modifiedtime"
                " FROM report.ant_acc"
                " ORDER BY accountid"
                " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                (offset, _ANT_ACC_BATCH_SIZE),
            )
            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(
                    text(_ANT_ACC_UPSERT),
                    [{"accountid": str(r["accountid"]), "client_qualification_date": r["client_qualification_date"], "modifiedtime": r["modifiedtime"]} for r in rows],
                )
                await db.commit()

            total += len(rows)
            offset += _ANT_ACC_BATCH_SIZE
            logger.info("ETL ant_acc full: %d rows so far", total)

            if len(rows) < _ANT_ACC_BATCH_SIZE:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL ant_acc full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL ant_acc full sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# Ant Acc — incremental sync (called by scheduler)
# ---------------------------------------------------------------------------

async def incremental_sync_ant_acc(session_factory: async_sessionmaker) -> None:
    log_id: int | None = None
    try:
        async with session_factory() as db:
            result = await db.execute(text("SELECT MAX(modifiedtime) FROM ant_acc"))
            last_modifiedtime = result.scalar()
            log = EtlSyncLog(sync_type="ant_acc_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        if last_modifiedtime is None:
            async with session_factory() as db:
                log = await db.get(EtlSyncLog, log_id)
                if log:
                    log.status = "completed"
                    log.rows_synced = 0
                    log.completed_at = datetime.now(timezone.utc)
                    await db.commit()
            return

        rows = await execute_query(
            "SELECT accountid, client_qualification_date, modifiedtime"
            " FROM report.ant_acc"
            " WHERE modifiedtime > ?"
            " ORDER BY modifiedtime",
            (last_modifiedtime,),
        )

        if rows:
            async with session_factory() as db:
                await db.execute(
                    text(_ANT_ACC_UPSERT),
                    [{"accountid": str(r["accountid"]), "client_qualification_date": r["client_qualification_date"], "modifiedtime": r["modifiedtime"]} for r in rows],
                )
                await db.commit()

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = len(rows)
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if rows:
            logger.info("ETL ant_acc incremental: %d rows updated", len(rows))

    except Exception as e:
        logger.error("ETL ant_acc incremental failed: %s", e)
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
    log = EtlSyncLog(sync_type="trades_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_trades, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-ant-acc")
async def sync_ant_acc(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    log = EtlSyncLog(sync_type="ant_acc_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_ant_acc, log.id)
    return {"status": "started", "log_id": log.id}


@router.get("/etl/sync-status")
async def sync_status(
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> dict:
    logs_result = await db.execute(
        text("SELECT * FROM etl_sync_log ORDER BY started_at DESC LIMIT 40")
    )
    rows = logs_result.mappings().all()

    trades_count = (await db.execute(text("SELECT COUNT(*) FROM trades_mt4"))).scalar() or 0
    ant_acc_count = (await db.execute(text("SELECT COUNT(*) FROM ant_acc"))).scalar() or 0

    return {
        "trades_row_count": trades_count,
        "ant_acc_row_count": ant_acc_count,
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
        ],
    }
