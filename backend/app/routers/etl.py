import asyncio
import logging
from datetime import datetime, timedelta, timezone

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
    "INSERT INTO trades_mt4 (ticket, login, cmd, profit, close_time) VALUES (:ticket, :login, :cmd, :profit, :close_time)"
    " ON CONFLICT (ticket) DO UPDATE SET login = EXCLUDED.login, cmd = EXCLUDED.cmd, profit = EXCLUDED.profit, close_time = EXCLUDED.close_time"
)

_ANT_ACC_SELECT = "SELECT accountid, client_qualification_date, modifiedtime FROM report.ant_acc"

_ANT_ACC_UPSERT = (
    "INSERT INTO ant_acc (accountid, client_qualification_date, modifiedtime)"
    " VALUES (:accountid, :client_qualification_date, :modifiedtime)"
    " ON CONFLICT (accountid) DO UPDATE SET"
    " client_qualification_date = EXCLUDED.client_qualification_date,"
    " modifiedtime = EXCLUDED.modifiedtime"
)

_ant_acc_map = lambda r: {"accountid": str(r["accountid"]), "client_qualification_date": r["client_qualification_date"], "modifiedtime": r["modifiedtime"]}  # noqa: E731


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


async def _is_running(prefix: str) -> bool:
    """Return True if any sync for this table prefix is already running."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT 1 FROM etl_sync_log WHERE sync_type LIKE :prefix AND status = 'running' LIMIT 1"),
            {"prefix": f"{prefix}%"},
        )
        return result.first() is not None


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
                        "SELECT ticket, login, cmd, profit, close_time FROM dealio.trades_mt4"
                        " WHERE ticket > :cursor ORDER BY ticket LIMIT :limit"
                    ),
                    {"cursor": cursor, "limit": _TRADES_BATCH_SIZE},
                )
                rows = result.fetchall()

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "close_time": r[4]} for r in rows])
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
    if await _is_running("trades"):
        logger.info("ETL trades: skipping scheduled run — sync already in progress")
        return
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
                                "SELECT ticket, login, cmd, profit, close_time FROM dealio.trades_mt4"
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
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "close_time": r[4]} for r in rows])
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
    if await _is_running("ant_acc"):
        logger.info("ETL ant_acc: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "ant_acc_incremental", "ant_acc", _ANT_ACC_SELECT, _ANT_ACC_UPSERT, _ant_acc_map)


# ---------------------------------------------------------------------------
# Shared helper for MSSQL-sourced full + incremental syncs
# ---------------------------------------------------------------------------

async def _mssql_full_sync(
    log_id: int,
    sync_type: str,
    select_sql: str,
    local_table: str,
    upsert_sql: str,
    row_mapper,
    batch_size: int = 5_000,
) -> None:
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text(f"TRUNCATE TABLE {local_table}"))
            await db.commit()

        total = 0
        offset = 0
        while True:
            rows = await execute_query(
                f"{select_sql} ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                (offset, batch_size),
            )
            if not rows:
                break
            async with AsyncSessionLocal() as db:
                await db.execute(text(upsert_sql), [row_mapper(r) for r in rows])
                await db.commit()
            total += len(rows)
            offset += batch_size
            logger.info("ETL %s full: %d rows so far", local_table, total)
            if len(rows) < batch_size:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL %s full sync complete: %d rows", local_table, total)
    except Exception as e:
        logger.error("ETL %s full sync failed: %s", local_table, e)
        await _update_log(log_id, "error", error=str(e))


async def _mssql_incremental_sync(
    session_factory: async_sessionmaker,
    sync_type: str,
    local_table: str,
    mssql_select: str,
    upsert_sql: str,
    row_mapper,
    timestamp_col: str = "modifiedtime",
    lookback_hours: int = 2,
) -> None:
    log_id: int | None = None
    try:
        async with session_factory() as db:
            result = await db.execute(text(f"SELECT MAX(modifiedtime) FROM {local_table}"))
            last_modifiedtime = result.scalar()
            log = EtlSyncLog(sync_type=sync_type, status="running")
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

        # Subtract lookback buffer to compensate for MSSQL/local timezone offset
        cutoff = last_modifiedtime - timedelta(hours=lookback_hours)

        rows = await execute_query(
            f"{mssql_select} WHERE {timestamp_col} > ? ORDER BY {timestamp_col}",
            (cutoff,),
        )
        if rows:
            async with session_factory() as db:
                await db.execute(text(upsert_sql), [row_mapper(r) for r in rows])
                await db.commit()

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = len(rows)
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()
        if rows:
            logger.info("ETL %s incremental: %d rows updated", local_table, len(rows))

    except Exception as e:
        logger.error("ETL %s incremental failed: %s", local_table, e)
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
# vtiger_trading_accounts
# ---------------------------------------------------------------------------

_VTA_SELECT = "SELECT login, vtigeraccountid, last_update AS modifiedtime FROM report.vtiger_trading_accounts"
_VTA_UPSERT = (
    "INSERT INTO vtiger_trading_accounts (login, vtigeraccountid, modifiedtime)"
    " VALUES (:login, :vtigeraccountid, :modifiedtime)"
    " ON CONFLICT (login) DO UPDATE SET"
    " vtigeraccountid = EXCLUDED.vtigeraccountid, modifiedtime = EXCLUDED.modifiedtime"
)
_vta_map = lambda r: {"login": r["login"], "vtigeraccountid": str(r["vtigeraccountid"]) if r["vtigeraccountid"] else None, "modifiedtime": r["modifiedtime"]}  # noqa: E731


async def _run_full_sync_vta(log_id: int) -> None:
    await _mssql_full_sync(log_id, "vta_full", _VTA_SELECT, "vtiger_trading_accounts", _VTA_UPSERT, _vta_map)


async def incremental_sync_vta(session_factory: async_sessionmaker) -> None:
    if await _is_running("vta"):
        logger.info("ETL vta: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "vta_incremental", "vtiger_trading_accounts", _VTA_SELECT, _VTA_UPSERT, _vta_map, timestamp_col="last_update")


# ---------------------------------------------------------------------------
# vtiger_mttransactions
# ---------------------------------------------------------------------------

_MTT_SELECT = "SELECT mttransactionsid, login, amount, transactiontype, modifiedtime FROM report.vtiger_mttransactions"
_MTT_UPSERT = (
    "INSERT INTO vtiger_mttransactions (mttransactionsid, login, amount, transactiontype, modifiedtime)"
    " VALUES (:mttransactionsid, :login, :amount, :transactiontype, :modifiedtime)"
    " ON CONFLICT (mttransactionsid) DO UPDATE SET"
    " login = EXCLUDED.login, amount = EXCLUDED.amount,"
    " transactiontype = EXCLUDED.transactiontype, modifiedtime = EXCLUDED.modifiedtime"
)
_mtt_map = lambda r: {"mttransactionsid": r["mttransactionsid"], "login": r["login"], "amount": r["amount"], "transactiontype": r["transactiontype"], "modifiedtime": r["modifiedtime"]}  # noqa: E731


async def _run_full_sync_mtt(log_id: int) -> None:
    await _mssql_full_sync(log_id, "mtt_full", _MTT_SELECT, "vtiger_mttransactions", _MTT_UPSERT, _mtt_map)


async def incremental_sync_mtt(session_factory: async_sessionmaker) -> None:
    if await _is_running("mtt"):
        logger.info("ETL mtt: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "mtt_incremental", "vtiger_mttransactions", _MTT_SELECT, _MTT_UPSERT, _mtt_map)


# ---------------------------------------------------------------------------
# Daily midnight full sync — all tables
# ---------------------------------------------------------------------------

async def _create_log(sync_type: str) -> int:
    async with AsyncSessionLocal() as db:
        log = EtlSyncLog(sync_type=sync_type, status="running")
        db.add(log)
        await db.commit()
        await db.refresh(log)
        return log.id


async def daily_full_sync_all() -> None:
    logger.info("Daily full sync starting")
    from app.replica_database import _ReplicaSession

    # trades (only if replica is available)
    if _ReplicaSession is not None:
        if not await _is_running("trades"):
            log_id = await _create_log("trades_full")
            await _run_full_sync_trades(log_id)
        else:
            logger.info("Daily sync: trades already running, skipped")

    # ant_acc
    if not await _is_running("ant_acc"):
        log_id = await _create_log("ant_acc_full")
        await _run_full_sync_ant_acc(log_id)
    else:
        logger.info("Daily sync: ant_acc already running, skipped")

    # vtiger_trading_accounts
    if not await _is_running("vta"):
        log_id = await _create_log("vta_full")
        await _run_full_sync_vta(log_id)
    else:
        logger.info("Daily sync: vta already running, skipped")

    # vtiger_mttransactions
    if not await _is_running("mtt"):
        log_id = await _create_log("mtt_full")
        await _run_full_sync_mtt(log_id)
    else:
        logger.info("Daily sync: mtt already running, skipped")

    logger.info("Daily full sync complete")


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


@router.post("/etl/sync-vta")
async def sync_vta(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    log = EtlSyncLog(sync_type="vta_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vta, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-mtt")
async def sync_mtt(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    log = EtlSyncLog(sync_type="mtt_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_mtt, log.id)
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
    vta_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_trading_accounts"))).scalar() or 0
    mtt_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_mttransactions"))).scalar() or 0

    return {
        "trades_row_count": trades_count,
        "ant_acc_row_count": ant_acc_count,
        "vta_row_count": vta_count,
        "mtt_row_count": mtt_count,
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
