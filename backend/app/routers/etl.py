import asyncio
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth_deps import get_current_user, require_admin
from app.database import execute_query
from app.models.etl_sync_log import EtlSyncLog
from app.pg_database import AsyncSessionLocal, engine, get_db
from app.replica_database import get_replica_db

logger = logging.getLogger(__name__)
router = APIRouter()

_TRADES_BATCH_SIZE = 100_000
_ANT_ACC_BATCH_SIZE = 100_000

_TRADES_INSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified)"
    " VALUES (:ticket, :login, :cmd, :profit, :computed_profit, :notional_value, :close_time, :open_time, :symbol, :last_modified)"
)
_TRADES_UPSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified)"
    " VALUES (:ticket, :login, :cmd, :profit, :computed_profit, :notional_value, :close_time, :open_time, :symbol, :last_modified)"
    " ON CONFLICT (ticket) DO UPDATE SET login = EXCLUDED.login, cmd = EXCLUDED.cmd,"
    " profit = EXCLUDED.profit, computed_profit = EXCLUDED.computed_profit, notional_value = EXCLUDED.notional_value,"
    " close_time = EXCLUDED.close_time, open_time = EXCLUDED.open_time, symbol = EXCLUDED.symbol,"
    " last_modified = EXCLUDED.last_modified"
)

_ANT_ACC_SELECT = "SELECT accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential, birth_date FROM report.ant_acc"

_ANT_ACC_UPSERT = (
    "INSERT INTO ant_acc (accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential, birth_date)"
    " VALUES (:accountid, :client_qualification_date, :modifiedtime, :is_test_account, :sales_client_potential, :birth_date)"
    " ON CONFLICT (accountid) DO UPDATE SET"
    " client_qualification_date = EXCLUDED.client_qualification_date,"
    " modifiedtime = EXCLUDED.modifiedtime,"
    " is_test_account = EXCLUDED.is_test_account,"
    " sales_client_potential = EXCLUDED.sales_client_potential,"
    " birth_date = EXCLUDED.birth_date"
)

_ant_acc_map = lambda r: {  # noqa: E731
    "accountid": str(r["accountid"]),
    "client_qualification_date": r["client_qualification_date"],
    "modifiedtime": r["modifiedtime"],
    "is_test_account": r["is_test_account"],
    "sales_client_potential": str(r["sales_client_potential"]) if r["sales_client_potential"] is not None else None,
    "birth_date": r["birth_date"].date() if hasattr(r["birth_date"], "date") else r["birth_date"],
}


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
            rows = None
            for attempt in range(5):
                try:
                    async with _ReplicaSession() as replica_db:
                        result = await replica_db.execute(
                            text(
                                "SELECT ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified FROM dealio.trades_mt4"
                                " WHERE ticket > :cursor ORDER BY ticket LIMIT :limit"
                            ),
                            {"cursor": cursor, "limit": _TRADES_BATCH_SIZE},
                        )
                        rows = result.fetchall()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = 2 ** attempt
                    logger.warning("ETL trades full: attempt %d failed (%s), retrying in %ds", attempt + 1, e, wait)
                    await asyncio.sleep(wait)

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_TRADES_INSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "computed_profit": r[4], "notional_value": r[5], "close_time": r[6], "open_time": r[7], "symbol": r[8], "last_modified": r[9]} for r in rows])
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
            log = EtlSyncLog(sync_type="trades_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        # Replica stores last_modified as timestamp without time zone — strip tz
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=3)).replace(tzinfo=None)
        total = 0
        offset = 0

        while True:
            rows = None
            for attempt in range(3):
                try:
                    async with replica_session_factory() as replica_db:
                        result = await replica_db.execute(
                            text(
                                "SELECT ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified FROM dealio.trades_mt4"
                                " WHERE last_modified > :cutoff ORDER BY last_modified, ticket LIMIT :limit OFFSET :offset"
                            ),
                            {"cutoff": cutoff, "limit": _TRADES_BATCH_SIZE, "offset": offset},
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
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "computed_profit": r[4], "notional_value": r[5], "close_time": r[6], "open_time": r[7], "symbol": r[8], "last_modified": r[9]} for r in rows])
                await db.commit()

            total += len(rows)
            offset += len(rows)

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

_ANT_ACC_TEST_FILTER = "ISNULL(is_test_account, 0) = 0"


async def _run_full_sync_ant_acc(log_id: int) -> None:
    await _mssql_full_sync(
        log_id, "ant_acc_full",
        f"{_ANT_ACC_SELECT} WHERE {_ANT_ACC_TEST_FILTER}",
        "ant_acc", _ANT_ACC_UPSERT, _ant_acc_map,
    )


# ---------------------------------------------------------------------------
# Ant Acc — incremental sync (called by scheduler)
# ---------------------------------------------------------------------------

async def incremental_sync_ant_acc(session_factory: async_sessionmaker) -> None:
    if await _is_running("ant_acc"):
        logger.info("ETL ant_acc: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "ant_acc_incremental", "ant_acc", _ANT_ACC_SELECT, _ANT_ACC_UPSERT, _ant_acc_map, extra_where=_ANT_ACC_TEST_FILTER)


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
    batch_size: int = 100_000,
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
    lookback_hours: int = 3,
    window_minutes: int | None = None,
    extra_where: str = "",
) -> None:
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type=sync_type, status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        extra = f" AND {extra_where}" if extra_where else ""

        rows = await execute_query(
            f"{mssql_select} WHERE {timestamp_col} > ?{extra} ORDER BY {timestamp_col}",
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

_VTA_SELECT = "SELECT login, vtigeraccountid, balance, credit, last_update AS modifiedtime FROM report.vtiger_trading_accounts"
_VTA_UPSERT = (
    "INSERT INTO vtiger_trading_accounts (login, vtigeraccountid, balance, credit, modifiedtime)"
    " VALUES (:login, :vtigeraccountid, :balance, :credit, :modifiedtime)"
    " ON CONFLICT (login) DO UPDATE SET"
    " vtigeraccountid = EXCLUDED.vtigeraccountid, balance = EXCLUDED.balance,"
    " credit = EXCLUDED.credit, modifiedtime = EXCLUDED.modifiedtime"
)
_vta_map = lambda r: {"login": r["login"], "vtigeraccountid": str(r["vtigeraccountid"]) if r["vtigeraccountid"] else None, "balance": r["balance"], "credit": r["credit"], "modifiedtime": r["modifiedtime"]}  # noqa: E731


async def _run_full_sync_vta(log_id: int) -> None:
    await _mssql_full_sync(log_id, "vta_full", _VTA_SELECT, "vtiger_trading_accounts", _VTA_UPSERT, _vta_map)


async def incremental_sync_vta(session_factory: async_sessionmaker) -> None:
    if await _is_running("vta"):
        logger.info("ETL vta: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "vta_incremental", "vtiger_trading_accounts", _VTA_SELECT, _VTA_UPSERT, _vta_map, timestamp_col="last_update", lookback_hours=3)


# ---------------------------------------------------------------------------
# vtiger_mttransactions
# ---------------------------------------------------------------------------

_MTT_SELECT = (
    "SELECT mttransactionsid, login, amount, transactiontype,"
    " transactionapproval, confirmation_time, payment_method, usdamount, modifiedtime"
    " FROM report.vtiger_mttransactions"
)
_MTT_UPSERT = (
    "INSERT INTO vtiger_mttransactions"
    " (mttransactionsid, login, amount, transactiontype, transactionapproval, confirmation_time, payment_method, usdamount, modifiedtime)"
    " VALUES (:mttransactionsid, :login, :amount, :transactiontype, :transactionapproval, :confirmation_time, :payment_method, :usdamount, :modifiedtime)"
    " ON CONFLICT (mttransactionsid) DO UPDATE SET"
    " login = EXCLUDED.login, amount = EXCLUDED.amount, transactiontype = EXCLUDED.transactiontype,"
    " transactionapproval = EXCLUDED.transactionapproval, confirmation_time = EXCLUDED.confirmation_time,"
    " payment_method = EXCLUDED.payment_method, usdamount = EXCLUDED.usdamount, modifiedtime = EXCLUDED.modifiedtime"
)
_mtt_map = lambda r: {  # noqa: E731
    "mttransactionsid": r["mttransactionsid"], "login": r["login"], "amount": r["amount"],
    "transactiontype": r["transactiontype"], "transactionapproval": r["transactionapproval"],
    "confirmation_time": r["confirmation_time"], "payment_method": r["payment_method"],
    "usdamount": r["usdamount"], "modifiedtime": r["modifiedtime"],
}


async def _run_full_sync_mtt(log_id: int) -> None:
    await _mssql_full_sync(log_id, "mtt_full", _MTT_SELECT, "vtiger_mttransactions", _MTT_UPSERT, _mtt_map)


async def incremental_sync_mtt(session_factory: async_sessionmaker) -> None:
    if await _is_running("mtt"):
        logger.info("ETL mtt: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "mtt_incremental", "vtiger_mttransactions", _MTT_SELECT, _MTT_UPSERT, _mtt_map, lookback_hours=3)


# ---------------------------------------------------------------------------
# dealio.users — full + incremental sync
# ---------------------------------------------------------------------------

_DEALIO_USERS_SELECT = (
    "SELECT login, lastupdate, sourceid, sourcename, sourcetype, groupname, groupcurrency,"
    " userid, actualuserid, regdate, lastdate, agentaccount, lastip::text AS lastip,"
    " balance, prevmonthbalance, prevbalance, prevequity, credit, name, country, city,"
    " state, zipcode, address, phone, email, compbalance, compprevbalance,"
    " compprevmonthbalance, compprevequity, compcredit, conversionratio, book,"
    " isenabled, status, prevmonthequity, compprevmonthequity, comment, color,"
    " leverage, condition, calculationcurrency, calculationcurrencydigits"
    " FROM dealio.users"
)

_DEALIO_USERS_UPSERT = (
    "INSERT INTO dealio_users"
    " (login, lastupdate, sourceid, sourcename, sourcetype, groupname, groupcurrency,"
    " userid, actualuserid, regdate, lastdate, agentaccount, lastip,"
    " balance, prevmonthbalance, prevbalance, prevequity, credit, name, country, city,"
    " state, zipcode, address, phone, email, compbalance, compprevbalance,"
    " compprevmonthbalance, compprevequity, compcredit, conversionratio, book,"
    " isenabled, status, prevmonthequity, compprevmonthequity, comment, color,"
    " leverage, condition, calculationcurrency, calculationcurrencydigits)"
    " VALUES"
    " (:login, :lastupdate, :sourceid, :sourcename, :sourcetype, :groupname, :groupcurrency,"
    " :userid, :actualuserid, :regdate, :lastdate, :agentaccount, :lastip,"
    " :balance, :prevmonthbalance, :prevbalance, :prevequity, :credit, :name, :country, :city,"
    " :state, :zipcode, :address, :phone, :email, :compbalance, :compprevbalance,"
    " :compprevmonthbalance, :compprevequity, :compcredit, :conversionratio, :book,"
    " :isenabled, :status, :prevmonthequity, :compprevmonthequity, :comment, :color,"
    " :leverage, :condition, :calculationcurrency, :calculationcurrencydigits)"
    " ON CONFLICT (login) DO UPDATE SET"
    " lastupdate = EXCLUDED.lastupdate, sourceid = EXCLUDED.sourceid, sourcename = EXCLUDED.sourcename,"
    " sourcetype = EXCLUDED.sourcetype, groupname = EXCLUDED.groupname, groupcurrency = EXCLUDED.groupcurrency,"
    " userid = EXCLUDED.userid, actualuserid = EXCLUDED.actualuserid, regdate = EXCLUDED.regdate,"
    " lastdate = EXCLUDED.lastdate, agentaccount = EXCLUDED.agentaccount, lastip = EXCLUDED.lastip,"
    " balance = EXCLUDED.balance, prevmonthbalance = EXCLUDED.prevmonthbalance, prevbalance = EXCLUDED.prevbalance,"
    " prevequity = EXCLUDED.prevequity, credit = EXCLUDED.credit, name = EXCLUDED.name,"
    " country = EXCLUDED.country, city = EXCLUDED.city, state = EXCLUDED.state,"
    " zipcode = EXCLUDED.zipcode, address = EXCLUDED.address, phone = EXCLUDED.phone,"
    " email = EXCLUDED.email, compbalance = EXCLUDED.compbalance, compprevbalance = EXCLUDED.compprevbalance,"
    " compprevmonthbalance = EXCLUDED.compprevmonthbalance, compprevequity = EXCLUDED.compprevequity,"
    " compcredit = EXCLUDED.compcredit, conversionratio = EXCLUDED.conversionratio, book = EXCLUDED.book,"
    " isenabled = EXCLUDED.isenabled, status = EXCLUDED.status, prevmonthequity = EXCLUDED.prevmonthequity,"
    " compprevmonthequity = EXCLUDED.compprevmonthequity, comment = EXCLUDED.comment, color = EXCLUDED.color,"
    " leverage = EXCLUDED.leverage, condition = EXCLUDED.condition, calculationcurrency = EXCLUDED.calculationcurrency,"
    " calculationcurrencydigits = EXCLUDED.calculationcurrencydigits"
)

_dealio_users_map = lambda r: {  # noqa: E731
    "login": r["login"], "lastupdate": r["lastupdate"], "sourceid": r["sourceid"],
    "sourcename": r["sourcename"], "sourcetype": r["sourcetype"], "groupname": r["groupname"],
    "groupcurrency": r["groupcurrency"], "userid": r["userid"], "actualuserid": r["actualuserid"],
    "regdate": r["regdate"], "lastdate": r["lastdate"], "agentaccount": r["agentaccount"],
    "lastip": r["lastip"], "balance": r["balance"], "prevmonthbalance": r["prevmonthbalance"],
    "prevbalance": r["prevbalance"], "prevequity": r["prevequity"], "credit": r["credit"],
    "name": r["name"], "country": r["country"], "city": r["city"], "state": r["state"],
    "zipcode": r["zipcode"], "address": r["address"], "phone": r["phone"], "email": r["email"],
    "compbalance": r["compbalance"], "compprevbalance": r["compprevbalance"],
    "compprevmonthbalance": r["compprevmonthbalance"], "compprevequity": r["compprevequity"],
    "compcredit": r["compcredit"], "conversionratio": r["conversionratio"], "book": r["book"],
    "isenabled": r["isenabled"], "status": r["status"], "prevmonthequity": r["prevmonthequity"],
    "compprevmonthequity": r["compprevmonthequity"], "comment": r["comment"], "color": r["color"],
    "leverage": r["leverage"], "condition": r["condition"], "calculationcurrency": r["calculationcurrency"],
    "calculationcurrencydigits": r["calculationcurrencydigits"],
}

_DEALIO_USERS_BATCH = 50_000


async def _run_full_sync_dealio_users(log_id: int) -> None:
    from app.replica_database import _ReplicaSession

    if _ReplicaSession is None:
        await _update_log(log_id, "error", error="Replica database not configured")
        return

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE dealio_users"))
            await db.commit()

        total = 0
        cursor = 0

        while True:
            rows = None
            for attempt in range(5):
                try:
                    async with _ReplicaSession() as replica_db:
                        result = await replica_db.execute(
                            text(f"{_DEALIO_USERS_SELECT} WHERE login > :cursor ORDER BY login LIMIT :limit"),
                            {"cursor": cursor, "limit": _DEALIO_USERS_BATCH},
                        )
                        rows = result.mappings().fetchall()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = 2 ** attempt
                    logger.warning("ETL dealio_users full: attempt %d failed (%s), retrying in %ds", attempt + 1, e, wait)
                    await asyncio.sleep(wait)

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_DEALIO_USERS_UPSERT), [_dealio_users_map(r) for r in rows])
                await db.commit()

            total += len(rows)
            cursor = rows[-1]["login"]
            logger.info("ETL dealio_users full: %d rows (cursor=%d)", total, cursor)

            if len(rows) < _DEALIO_USERS_BATCH:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL dealio_users full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL dealio_users full sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


async def incremental_sync_dealio_users(
    session_factory: async_sessionmaker,
    replica_session_factory: async_sessionmaker,
) -> None:
    if await _is_running("dealio_users"):
        logger.info("ETL dealio_users: skipping scheduled run — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="dealio_users_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        # lastupdate is timestamp with time zone — timezone-aware cutoff works directly
        cutoff = datetime.now(timezone.utc) - timedelta(hours=3)
        total = 0
        offset = 0

        while True:
            rows = None
            for attempt in range(3):
                try:
                    async with replica_session_factory() as replica_db:
                        result = await replica_db.execute(
                            text(
                                f"{_DEALIO_USERS_SELECT}"
                                " WHERE lastupdate > :cutoff ORDER BY lastupdate, login LIMIT :limit OFFSET :offset"
                            ),
                            {"cutoff": cutoff, "limit": _DEALIO_USERS_BATCH, "offset": offset},
                        )
                        rows = result.mappings().fetchall()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logger.warning("ETL dealio_users: connection error on attempt %d, retrying: %s", attempt + 1, e)
                    await asyncio.sleep(2)

            if not rows:
                break

            async with session_factory() as db:
                await db.execute(text(_DEALIO_USERS_UPSERT), [_dealio_users_map(r) for r in rows])
                await db.commit()

            total += len(rows)
            offset += len(rows)

            if len(rows) < _DEALIO_USERS_BATCH:
                break

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if total:
            logger.info("ETL dealio_users incremental: %d new/updated rows", total)

    except Exception as e:
        logger.error("ETL dealio_users incremental failed: %s", e)
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

    # dealio_users (only if replica is available)
    if _ReplicaSession is not None:
        if not await _is_running("dealio_users"):
            log_id = await _create_log("dealio_users_full")
            await _run_full_sync_dealio_users(log_id)
        else:
            logger.info("Daily sync: dealio_users already running, skipped")

    logger.info("Daily full sync complete")
    await refresh_retention_mv()


# ---------------------------------------------------------------------------
# Retention materialized view refresh
# ---------------------------------------------------------------------------

async def refresh_retention_mv() -> None:
    """Refresh retention_mv. Skips if a full ETL sync is running to avoid
    conflicting with TRUNCATE. Uses CONCURRENTLY when populated so reads
    never block; falls back to regular REFRESH on first population."""
    try:
        async with AsyncSessionLocal() as db:
            # Skip if any full sync is running — REFRESH reads conflict with TRUNCATE
            running = (await db.execute(text(
                "SELECT 1 FROM etl_sync_log WHERE status = 'running' AND sync_type LIKE '%_full' LIMIT 1"
            ))).first()
            if running:
                logger.info("retention_mv refresh skipped — full ETL sync in progress")
                return

            result = await db.execute(text("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'retention_mv'"))
            row = result.first()
            ispopulated = bool(row[0]) if row else False

        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(text("SET work_mem = '256MB'"))
            if ispopulated:
                await conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY retention_mv"))
            else:
                logger.info("retention_mv not yet populated — running initial population...")
                await conn.execute(text("REFRESH MATERIALIZED VIEW retention_mv"))
        logger.info("retention_mv refreshed (concurrent=%s)", ispopulated)
    except Exception as e:
        logger.error("retention_mv refresh failed: %s", e)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.post("/etl/sync-trades")
async def sync_trades(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("trades"):
        return {"status": "already_running"}
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
    if await _is_running("ant_acc"):
        return {"status": "already_running"}
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
    if await _is_running("vta"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vta_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vta, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-dealio-users")
async def sync_dealio_users(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("dealio_users"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="dealio_users_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_dealio_users, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-mtt")
async def sync_mtt(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("mtt"):
        return {"status": "already_running"}
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
        text("SELECT * FROM etl_sync_log ORDER BY started_at DESC LIMIT 100")
    )
    rows = logs_result.mappings().all()

    trades_count = (await db.execute(text("SELECT COUNT(*) FROM trades_mt4"))).scalar() or 0
    ant_acc_count = (await db.execute(text("SELECT COUNT(*) FROM ant_acc"))).scalar() or 0
    vta_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_trading_accounts"))).scalar() or 0
    mtt_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_mttransactions"))).scalar() or 0
    dealio_users_count = (await db.execute(text("SELECT COUNT(*) FROM dealio_users"))).scalar() or 0

    def _last_row(row) -> dict | None:
        if row is None:
            return None
        return {"id": str(row[0]), "modified": row[1].isoformat() if row[1] else None}

    trades_last = _last_row((await db.execute(text("SELECT ticket, last_modified FROM trades_mt4 WHERE last_modified <= NOW() ORDER BY last_modified DESC NULLS LAST LIMIT 1"))).first())
    ant_acc_last = _last_row((await db.execute(text("SELECT accountid, modifiedtime FROM ant_acc WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    vta_last = _last_row((await db.execute(text("SELECT login, modifiedtime FROM vtiger_trading_accounts WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    mtt_last = _last_row((await db.execute(text("SELECT mttransactionsid, modifiedtime FROM vtiger_mttransactions WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    dealio_users_last = _last_row((await db.execute(text("SELECT login, lastupdate FROM dealio_users WHERE lastupdate <= NOW() ORDER BY lastupdate DESC NULLS LAST LIMIT 1"))).first())

    return {
        "trades_row_count": trades_count,
        "ant_acc_row_count": ant_acc_count,
        "vta_row_count": vta_count,
        "mtt_row_count": mtt_count,
        "dealio_users_row_count": dealio_users_count,
        "trades_last": trades_last,
        "ant_acc_last": ant_acc_last,
        "vta_last": vta_last,
        "mtt_last": mtt_last,
        "dealio_users_last": dealio_users_last,
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
