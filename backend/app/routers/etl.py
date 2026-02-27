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

_ANT_ACC_SELECT = "SELECT accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential, birth_date, assigned_to, full_name FROM report.ant_acc"

_ANT_ACC_UPSERT = (
    "INSERT INTO ant_acc (accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential, birth_date, assigned_to, full_name)"
    " VALUES (:accountid, :client_qualification_date, :modifiedtime, :is_test_account, :sales_client_potential, :birth_date, :assigned_to, :full_name)"
    " ON CONFLICT (accountid) DO UPDATE SET"
    " client_qualification_date = EXCLUDED.client_qualification_date,"
    " modifiedtime = EXCLUDED.modifiedtime,"
    " is_test_account = EXCLUDED.is_test_account,"
    " sales_client_potential = EXCLUDED.sales_client_potential,"
    " birth_date = EXCLUDED.birth_date,"
    " assigned_to = EXCLUDED.assigned_to,"
    " full_name = EXCLUDED.full_name"
)

_ant_acc_map = lambda r: {  # noqa: E731
    "accountid": str(r["accountid"]),
    "client_qualification_date": r["client_qualification_date"],
    "modifiedtime": r["modifiedtime"],
    "is_test_account": r["is_test_account"],
    "sales_client_potential": str(r["sales_client_potential"]) if r["sales_client_potential"] is not None else None,
    "birth_date": r["birth_date"].date() if hasattr(r["birth_date"], "date") else r["birth_date"],
    "assigned_to": str(r["assigned_to"]) if r["assigned_to"] is not None else None,
    "full_name": str(r["full_name"]).replace("\x00", "").strip() if r["full_name"] is not None else None,
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
    lazy_truncate: bool = False,
) -> None:
    """Full sync from MSSQL to a local table.

    When lazy_truncate=True the TRUNCATE is deferred until the first batch
    is successfully fetched from MSSQL.  This ensures the local table stays
    populated if MSSQL is temporarily unavailable (no truncate-then-fail).
    Use this for small lookup tables like vtiger_users where an empty table
    is worse than stale data.
    """
    try:
        if not lazy_truncate:
            async with AsyncSessionLocal() as db:
                await db.execute(text(f"TRUNCATE TABLE {local_table}"))
                await db.commit()

        total = 0
        offset = 0
        truncated = lazy_truncate is False  # already done above when not lazy
        while True:
            rows = await execute_query(
                f"{select_sql} ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                (offset, batch_size),
            )
            if not rows:
                break
            # Lazy truncate: only wipe existing data once we know MSSQL responded
            if not truncated:
                async with AsyncSessionLocal() as db:
                    await db.execute(text(f"TRUNCATE TABLE {local_table}"))
                    await db.commit()
                truncated = True
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
    logger.info("ETL dealio_users: incremental_sync_dealio_users called")
    is_running = await _is_running("dealio_users")
    logger.info("ETL dealio_users: _is_running=%s", is_running)
    if is_running:
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

        # Strip tzinfo so the cutoff matches replica's timestamp without time zone,
        # same approach as trades incremental (avoids type mismatch on some replicas)
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

    # extensions
    if not await _is_running("extensions"):
        log_id = await _create_log("extensions_full")
        await _run_full_sync_extensions(log_id)
    else:
        logger.info("Daily sync: extensions already running, skipped")

    logger.info("Daily full sync complete")
    await refresh_retention_mv()


# ---------------------------------------------------------------------------
# vtiger_users / vtiger_campaigns — hourly full-refresh jobs
# ---------------------------------------------------------------------------

async def hourly_sync_vtiger_users(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of vtiger_users."""
    if await _is_running("vtiger_users"):
        logger.info("ETL vtiger_users: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="vtiger_users_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_vtiger_users(log_id)
    except Exception as e:
        logger.error("Hourly vtiger_users sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


async def hourly_sync_vtiger_campaigns(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of vtiger_campaigns."""
    if await _is_running("vtiger_campaigns"):
        logger.info("ETL vtiger_campaigns: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="vtiger_campaigns_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_vtiger_campaigns(log_id)
    except Exception as e:
        logger.error("Hourly vtiger_campaigns sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# vtiger_users (report.vtiger_users)
# ---------------------------------------------------------------------------

_VTIGER_USERS_SELECT = (
    "SELECT id, user_name, first_name, last_name, email, phone, department, status, office, position, fax"
    " FROM report.vtiger_users"
)

_VTIGER_USERS_UPSERT = (
    "INSERT INTO vtiger_users"
    " (id, user_name, first_name, last_name, email, phone, department, status, office, position, fax)"
    " VALUES"
    " (:id, :user_name, :first_name, :last_name, :email, :phone, :department, :status, :office, :position, :fax)"
    " ON CONFLICT (id) DO UPDATE SET"
    " user_name = EXCLUDED.user_name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,"
    " email = EXCLUDED.email, phone = EXCLUDED.phone, department = EXCLUDED.department,"
    " status = EXCLUDED.status, office = EXCLUDED.office, position = EXCLUDED.position, fax = EXCLUDED.fax"
)

_vtiger_users_map = lambda r: {  # noqa: E731
    "id": str(r["id"]) if r["id"] is not None else None,
    "user_name": r["user_name"], "first_name": r["first_name"], "last_name": r["last_name"],
    "email": r["email"], "phone": r["phone"], "department": r["department"],
    "status": r["status"], "office": r["office"], "position": r["position"], "fax": r["fax"],
}


async def _run_full_sync_vtiger_users(log_id: int) -> None:
    # lazy_truncate=True: preserve existing agent data if MSSQL is temporarily down
    await _mssql_full_sync(log_id, "vtiger_users_full", _VTIGER_USERS_SELECT, "vtiger_users", _VTIGER_USERS_UPSERT, _vtiger_users_map, lazy_truncate=True)




# ---------------------------------------------------------------------------
# vtiger_campaigns (report.vtiger_campaigns)
# ---------------------------------------------------------------------------

_VTIGER_CAMPAIGNS_SELECT = (
    "SELECT crmid, campaign_id, campaign_name, campaign_legacy_id, campaign_channel, campaign_sub_channel"
    " FROM report.vtiger_campaigns"
)

_VTIGER_CAMPAIGNS_UPSERT = (
    "INSERT INTO vtiger_campaigns"
    " (crmid, campaign_id, campaign_name, campaign_legacy_id, campaign_channel, campaign_sub_channel)"
    " VALUES"
    " (:crmid, :campaign_id, :campaign_name, :campaign_legacy_id, :campaign_channel, :campaign_sub_channel)"
    " ON CONFLICT (crmid) DO UPDATE SET"
    " campaign_id = EXCLUDED.campaign_id, campaign_name = EXCLUDED.campaign_name,"
    " campaign_legacy_id = EXCLUDED.campaign_legacy_id, campaign_channel = EXCLUDED.campaign_channel,"
    " campaign_sub_channel = EXCLUDED.campaign_sub_channel"
)

_vtiger_campaigns_map = lambda r: {  # noqa: E731
    "crmid": str(r["crmid"]) if r["crmid"] is not None else None,
    "campaign_id": str(r["campaign_id"]) if r["campaign_id"] is not None else None,
    "campaign_name": r["campaign_name"],
    "campaign_legacy_id": str(r["campaign_legacy_id"]) if r["campaign_legacy_id"] is not None else None,
    "campaign_channel": r["campaign_channel"],
    "campaign_sub_channel": r["campaign_sub_channel"],
}


async def _run_full_sync_vtiger_campaigns(log_id: int) -> None:
    await _mssql_full_sync(log_id, "vtiger_campaigns_full", _VTIGER_CAMPAIGNS_SELECT, "vtiger_campaigns", _VTIGER_CAMPAIGNS_UPSERT, _vtiger_campaigns_map)


# ---------------------------------------------------------------------------
# extensions (report.Extension_new) — hourly full-refresh
# ---------------------------------------------------------------------------

_EXTENSIONS_SELECT = (
    "SELECT [Name], [Extension], [User_name], [Agent_name], [Manager], [Position], [Office], [Email], [ManagerEmail]"
    " FROM [report].[Extension_new]"
)

_EXTENSIONS_UPSERT = (
    "INSERT INTO extensions"
    " (name, extension, user_name, agent_name, manager, position, office, email, manager_email, synced_at)"
    " VALUES"
    " (:name, :extension, :user_name, :agent_name, :manager, :position, :office, :email, :manager_email, NOW())"
    " ON CONFLICT (extension) DO UPDATE SET"
    " name = EXCLUDED.name, user_name = EXCLUDED.user_name, agent_name = EXCLUDED.agent_name,"
    " manager = EXCLUDED.manager, position = EXCLUDED.position, office = EXCLUDED.office,"
    " email = EXCLUDED.email, manager_email = EXCLUDED.manager_email, synced_at = NOW()"
)

_extensions_map = lambda r: {  # noqa: E731
    "name": r["Name"],
    "extension": str(r["Extension"]) if r["Extension"] is not None else None,
    "user_name": r["User_name"],
    "agent_name": r["Agent_name"],
    "manager": r["Manager"],
    "position": r["Position"],
    "office": r["Office"],
    "email": r["Email"],
    "manager_email": r["ManagerEmail"],
}


async def _run_full_sync_extensions(log_id: int) -> None:
    await _mssql_full_sync(log_id, "extensions_full", _EXTENSIONS_SELECT, "extensions", _EXTENSIONS_UPSERT, _extensions_map)


async def hourly_sync_extensions(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of extensions."""
    if await _is_running("extensions"):
        logger.info("ETL extensions: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="extensions_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_extensions(log_id)
    except Exception as e:
        logger.error("Hourly extensions sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# Dynamic retention MV builder
# ---------------------------------------------------------------------------

def _build_mv_sql(extra_cols: list) -> str:
    """Build the CREATE MATERIALIZED VIEW retention_mv SQL dynamically."""

    dealio_extras = [c for c in extra_cols if c["source_table"] == "dealio_users"]
    ant_acc_extras = [c for c in extra_cols if c["source_table"] == "ant_acc"]
    trades_extras = [c for c in extra_cols if c["source_table"] == "trades_mt4"]
    mtt_extras = [c for c in extra_cols if c["source_table"] == "vtiger_mttransactions"]
    vta_extras = [c for c in extra_cols if c["source_table"] == "vtiger_trading_accounts"]

    trades_agg_extras = ""
    for c in trades_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        trades_agg_extras += ",\n                    COALESCE(" + agg + "(t." + col + "), 0) AS " + col

    deposits_agg_extras = ""
    for c in mtt_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        deposits_agg_extras += ",\n                    COALESCE(" + agg + "(mtt." + col + "), 0) AS " + col

    balance_agg_extras = ""
    for c in dealio_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        balance_agg_extras += ",\n                    COALESCE(" + agg + "(du." + col + "), 0) AS " + col

    vta_extras_cte = ""
    vta_extras_join = ""
    if vta_extras:
        vta_select_parts = ["ql.accountid"]
        for c in vta_extras:
            agg = c["agg_fn"]
            col = c["source_column"]
            vta_select_parts.append("COALESCE(" + agg + "(vta." + col + "), 0) AS " + col)
        vta_select_str = ",\n                    ".join(vta_select_parts)
        vta_extras_cte = (
            ",\nvta_extras_agg AS (\n"
            "    SELECT\n"
            "                    " + vta_select_str + "\n"
            "    FROM qualifying_logins ql\n"
            "    LEFT JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = ql.accountid\n"
            "    GROUP BY ql.accountid\n"
            ")"
        )
        vta_extras_join = "\n            INNER JOIN vta_extras_agg vea ON vea.accountid = a.accountid"

    final_select_extras = ""
    for c in trades_extras:
        col = c["source_column"]
        final_select_extras += ",\n                ta." + col
    for c in mtt_extras:
        col = c["source_column"]
        final_select_extras += ",\n                da." + col
    for c in dealio_extras:
        col = c["source_column"]
        final_select_extras += ",\n                ab." + col
    for c in ant_acc_extras:
        col = c["source_column"]
        final_select_extras += ",\n                a." + col + " AS " + col
    for c in vta_extras:
        col = c["source_column"]
        final_select_extras += ",\n                vea." + col

    # Use chr(39) to embed SQL single quotes in the generated SQL
    sq = chr(39)
    sql = (
        "CREATE MATERIALIZED VIEW retention_mv AS\n"
        "            WITH qualifying_logins AS (\n"
        "                SELECT vta.login, a.accountid\n"
        "                FROM ant_acc a\n"
        "                INNER JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = a.accountid\n"
        "                WHERE a.client_qualification_date IS NOT NULL\n"
        "                  AND a.client_qualification_date >= " + sq + "2024-01-01" + sq + "\n"
        "                  AND (a.is_test_account IS NULL OR a.is_test_account = 0)\n"
        "            ),\n"
        "            trades_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COUNT(t.ticket) AS trade_count,\n"
        "                    COALESCE(SUM(t.computed_profit), 0) AS total_profit,\n"
        "                    MAX(t.open_time) AS last_trade_date,\n"
        "                    MAX(CASE WHEN t.close_time > '1971-01-01' THEN t.close_time END) AS last_close_time,\n"
        "                    ROUND(COUNT(CASE WHEN t.computed_profit > 0 THEN 1 END)::numeric / NULLIF(COUNT(t.ticket), 0) * 100, 1) AS win_rate,\n"
        "                    ROUND(COALESCE(AVG(t.notional_value), 0)::numeric, 2) AS avg_trade_size" + trades_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)\n"
        "                    AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN (" + sq + "inactivity" + sq + ", " + sq + "zeroingusd" + sq + ", " + sq + "spread" + sq + "))\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            deposits_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COUNT(mtt.mttransactionsid) AS deposit_count,\n"
        "                    COALESCE(SUM(mtt.usdamount), 0) AS total_deposit,\n"
        "                    MAX(mtt.confirmation_time) AS last_deposit_time" + deposits_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN vtiger_mttransactions mtt ON mtt.login = ql.login\n"
        "                    AND mtt.transactionapproval = " + sq + "Approved" + sq + "\n"
        "                    AND mtt.transactiontype = " + sq + "Deposit" + sq + "\n"
        "                    AND (mtt.payment_method IS NULL OR mtt.payment_method != " + sq + "BonusProtectedPositionCashback" + sq + ")\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            balance_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COALESCE(SUM(du.compbalance), 0) AS total_balance,\n"
        "                    COALESCE(SUM(du.compcredit), 0) AS total_credit,\n"
        "                    COALESCE(SUM(du.compprevequity), 0) AS total_equity" + balance_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN dealio_users du ON du.login = ql.login\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            trading_activity_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COALESCE(GREATEST(\n"
        "                        COUNT(CASE WHEN t.open_time >= CURRENT_DATE - 30 THEN 1 END)::numeric / 21,\n"
        "                        COUNT(CASE WHEN t.open_time >= CURRENT_DATE - 7 THEN 1 END)::numeric / 5\n"
        "                    ), 0) AS max_open_trade,\n"
        "                    COALESCE(GREATEST(\n"
        "                        SUM(CASE WHEN t.open_time >= CURRENT_DATE - 30 THEN COALESCE(t.notional_value, 0) ELSE 0 END)::numeric / 21,\n"
        "                        SUM(CASE WHEN t.open_time >= CURRENT_DATE - 7 THEN COALESCE(t.notional_value, 0) ELSE 0 END)::numeric / 5\n"
        "                    ), 0) AS max_volume\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)\n"
        "                    AND t.open_time >= CURRENT_DATE - 30\n"
        "                    AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN (" + sq + "inactivity" + sq + ", " + sq + "zeroingusd" + sq + ", " + sq + "spread" + sq + "))\n"
        "                GROUP BY ql.accountid\n"
        "            )" + vta_extras_cte + "\n"
        "            SELECT\n"
        "                a.accountid,\n"
        "                TRIM(COALESCE(a.full_name, '')) AS full_name,\n"
        "                a.client_qualification_date,\n"
        "                a.sales_client_potential,\n"
        "                a.birth_date,\n"
        "                a.assigned_to,\n"
        "                ta.trade_count,\n"
        "                ta.total_profit,\n"
        "                ta.last_trade_date,\n"
        "                ta.last_close_time,\n"
        "                da.deposit_count,\n"
        "                da.total_deposit,\n"
        "                da.last_deposit_time,\n"
        "                ab.total_balance,\n"
        "                ab.total_credit,\n"
        "                ab.total_equity,\n"
        "                ROUND(taa.max_open_trade::numeric, 1) AS max_open_trade,\n"
        "                ROUND(taa.max_volume::numeric, 1) AS max_volume,\n"
        "                ta.win_rate,\n"
        "                ta.avg_trade_size,\n"
        "                TRIM(COALESCE(vu.first_name, '') || ' ' || COALESCE(vu.last_name, '')) AS agent_name" + final_select_extras + "\n"
        "            FROM ant_acc a\n"
        "            INNER JOIN trades_agg ta ON ta.accountid = a.accountid\n"
        "            INNER JOIN deposits_agg da ON da.accountid = a.accountid\n"
        "            INNER JOIN balance_agg ab ON ab.accountid = a.accountid\n"
        "            INNER JOIN trading_activity_agg taa ON taa.accountid = a.accountid" + vta_extras_join + "\n"
        "            LEFT JOIN vtiger_users vu ON vu.id = a.assigned_to\n"
        "            WHERE a.client_qualification_date IS NOT NULL\n"
        "              AND (a.is_test_account IS NULL OR a.is_test_account = 0)\n"
        "            WITH NO DATA"
    )

    return sql


async def rebuild_retention_mv() -> None:
    """Rebuild retention_mv from scratch using current extra columns config."""
    logger.info("rebuild_retention_mv: starting")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT source_table, source_column, agg_fn, display_name FROM retention_extra_columns ORDER BY id")
        )
        extra_cols = [
            {"source_table": r[0], "source_column": r[1], "agg_fn": r[2], "display_name": r[3]}
            for r in result.fetchall()
        ]

    mv_sql = _build_mv_sql(extra_cols)

    async with AsyncSessionLocal() as db:
        await db.execute(text("DROP MATERIALIZED VIEW IF EXISTS retention_mv CASCADE"))
        await db.commit()

    logger.info("rebuild_retention_mv: dropped existing MV")

    async with AsyncSessionLocal() as db:
        await db.execute(text(mv_sql))
        await db.commit()

    logger.info("rebuild_retention_mv: created new MV definition")

    async with AsyncSessionLocal() as db:
        await db.execute(text("CREATE UNIQUE INDEX retention_mv_accountid ON retention_mv (accountid)"))
        await db.commit()

    logger.info("rebuild_retention_mv: unique index created")

    # Refresh (non-concurrent since freshly created)
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("SET work_mem = '256MB'"))
        await conn.execute(text("REFRESH MATERIALIZED VIEW retention_mv"))

    logger.info("rebuild_retention_mv: MV refreshed with data")

    # Compute scores for ALL clients and store in client_scores
    try:
        from sqlalchemy import select
        from app.models.scoring_rule import ScoringRule
        from app.routers.client_scoring import SCORING_COL_SQL, SCORING_OP_MAP
        async with AsyncSessionLocal() as db:
            rules_result = await db.execute(select(ScoringRule).order_by(ScoringRule.id))
            rules = rules_result.scalars().all()

            all_accts_result = await db.execute(text("SELECT accountid FROM retention_mv"))
            all_accountids = [r[0] for r in all_accts_result.fetchall()]

            if rules and all_accountids:
                score_map = {aid: 0 for aid in all_accountids}

                for rule in rules:
                    sql_expr = SCORING_COL_SQL.get(rule.field)
                    sql_op = SCORING_OP_MAP.get(rule.operator)
                    if not sql_expr or not sql_op:
                        continue
                    try:
                        cast_value = float(rule.value)
                    except (ValueError, TypeError):
                        cast_value = rule.value

                    cond_sql = f"{sql_expr} {sql_op} :val"
                    q = text(f"SELECT m.accountid FROM retention_mv m WHERE {cond_sql}")
                    matched = await db.execute(q, {"val": cast_value})
                    for row in matched.fetchall():
                        if row[0] in score_map:
                            score_map[row[0]] += rule.score

                if score_map:
                    upsert_sql = text("""
                        INSERT INTO client_scores (accountid, score, computed_at)
                        VALUES (:accountid, :score, NOW())
                        ON CONFLICT (accountid) DO UPDATE SET score = EXCLUDED.score, computed_at = NOW()
                    """)
                    await db.execute(upsert_sql, [{"accountid": k, "score": v} for k, v in score_map.items()])
                    await db.commit()
                    logger.info("rebuild_retention_mv: computed and stored scores for %d clients", len(score_map))
            else:
                logger.info("rebuild_retention_mv: no rules or no accounts — skipping score computation")
    except Exception as score_err:
        logger.warning("rebuild_retention_mv: score computation failed: %s", score_err)

    # Pre-compute task assignments for all clients so per-page lookup is O(1)
    await rebuild_task_assignments()


async def rebuild_task_assignments() -> None:
    """Recompute client_task_assignments for all accounts in retention_mv.

    Called after every MV rebuild and whenever tasks are created/updated/deleted.
    Replaces the previous approach of running N per-page queries (one per task).
    """
    import json as _json
    from sqlalchemy import select as _select
    from app.models.retention_task import RetentionTask
    from app.routers.retention_tasks import _build_task_where

    try:
        async with AsyncSessionLocal() as db:
            tasks_result = await db.execute(_select(RetentionTask).order_by(RetentionTask.id))
            tasks = tasks_result.scalars().all()

            # Always truncate so stale assignments are removed even when no tasks exist
            await db.execute(text("TRUNCATE TABLE client_task_assignments"))

            if tasks:
                assignments: list[dict] = []
                for task in tasks:
                    try:
                        conditions = _json.loads(task.conditions)
                    except Exception:
                        continue
                    t_where, t_params = _build_task_where(conditions)
                    # _MV_ACTIVE uses :activity_days — supply default
                    t_params.setdefault("activity_days", 35)
                    t_where_clause = " AND ".join(t_where)
                    q = text(f"SELECT m.accountid FROM retention_mv m WHERE {t_where_clause}")
                    matched = await db.execute(q, t_params)
                    for row in matched.fetchall():
                        assignments.append({"accountid": str(row[0]), "task_id": task.id})

                if assignments:
                    await db.execute(
                        text(
                            "INSERT INTO client_task_assignments (accountid, task_id) "
                            "VALUES (:accountid, :task_id) ON CONFLICT DO NOTHING"
                        ),
                        assignments,
                    )

            await db.commit()
            logger.info("rebuild_task_assignments: stored %d assignments", len(assignments) if tasks else 0)
    except Exception as ta_err:
        logger.warning("rebuild_task_assignments failed: %s", ta_err)


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


@router.post("/etl/sync-vtiger-users")
async def sync_vtiger_users(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("vtiger_users"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vtiger_users_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vtiger_users, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-vtiger-campaigns")
async def sync_vtiger_campaigns(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("vtiger_campaigns"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vtiger_campaigns_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vtiger_campaigns, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-extensions")
async def sync_extensions(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("extensions"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="extensions_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_extensions, log.id)
    return {"status": "started", "log_id": log.id}


async def _run_full_sync_open_pnl(log_id: int) -> None:
    """Sync aggregated open PNL per login from dealio.positions into local open_pnl_cache."""
    from app.replica_database import _ReplicaSession
    if _ReplicaSession is None:
        await _update_log(log_id, "error", error="Replica database not configured")
        return
    try:
        async with _ReplicaSession() as replica:
            result = await replica.execute(
                text("SELECT login, SUM(computedprofit) AS pnl FROM dealio.positions GROUP BY login")
            )
            rows = result.fetchall()

        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE open_pnl_cache"))
            if rows:
                await db.execute(
                    text(
                        "INSERT INTO open_pnl_cache (login, pnl, updated_at) "
                        "VALUES (:login, :pnl, NOW())"
                    ),
                    [{"login": str(r[0]), "pnl": float(r[1] or 0)} for r in rows],
                )
            await db.commit()

        await _update_log(log_id, "completed", rows_synced=len(rows))
        logger.info("sync_open_pnl: synced %d logins", len(rows))
    except Exception as e:
        await _update_log(log_id, "error", error=str(e))
        logger.error("sync_open_pnl failed: %s", e)


async def sync_open_pnl_background() -> None:
    """Scheduler-triggered silent sync of open_pnl_cache — no ETL log entry."""
    from app.replica_database import _ReplicaSession
    if _ReplicaSession is None:
        return
    try:
        async with _ReplicaSession() as replica:
            result = await replica.execute(
                text("SELECT login, SUM(computedprofit) AS pnl FROM dealio.positions GROUP BY login")
            )
            rows = result.fetchall()

        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE open_pnl_cache"))
            if rows:
                await db.execute(
                    text(
                        "INSERT INTO open_pnl_cache (login, pnl, updated_at) "
                        "VALUES (:login, :pnl, NOW())"
                    ),
                    [{"login": str(r[0]), "pnl": float(r[1] or 0)} for r in rows],
                )
            await db.commit()
        logger.info("sync_open_pnl_background: synced %d logins", len(rows))
    except Exception as e:
        logger.warning("sync_open_pnl_background failed: %s", e)


@router.post("/etl/sync-open-pnl")
async def sync_open_pnl(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("open_pnl"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="open_pnl_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_open_pnl, log.id)
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
    vtiger_users_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_users"))).scalar() or 0
    vtiger_campaigns_count = (await db.execute(text("SELECT COUNT(*) FROM vtiger_campaigns"))).scalar() or 0
    extensions_count = (await db.execute(text("SELECT COUNT(*) FROM extensions"))).scalar() or 0
    open_pnl_count = (await db.execute(text("SELECT COUNT(*) FROM open_pnl_cache"))).scalar() or 0

    def _last_row(row) -> dict | None:
        if row is None:
            return None
        return {"id": str(row[0]), "modified": row[1].isoformat() if row[1] else None}

    trades_last = _last_row((await db.execute(text("SELECT ticket, last_modified FROM trades_mt4 WHERE last_modified <= NOW() ORDER BY last_modified DESC NULLS LAST LIMIT 1"))).first())
    ant_acc_last = _last_row((await db.execute(text("SELECT accountid, modifiedtime FROM ant_acc WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    vta_last = _last_row((await db.execute(text("SELECT login, modifiedtime FROM vtiger_trading_accounts WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    mtt_last = _last_row((await db.execute(text("SELECT mttransactionsid, modifiedtime FROM vtiger_mttransactions WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))).first())
    dealio_users_last = _last_row((await db.execute(text("SELECT login, lastupdate FROM dealio_users WHERE lastupdate <= NOW() ORDER BY lastupdate DESC NULLS LAST LIMIT 1"))).first())
    vtiger_users_last = _last_row((await db.execute(text("SELECT id, NULL FROM vtiger_users LIMIT 1"))).first())
    vtiger_campaigns_last = _last_row((await db.execute(text("SELECT crmid, NULL FROM vtiger_campaigns LIMIT 1"))).first())
    extensions_last = _last_row((await db.execute(text("SELECT extension, synced_at FROM extensions ORDER BY synced_at DESC NULLS LAST LIMIT 1"))).first())
    open_pnl_last = _last_row((await db.execute(text("SELECT login, updated_at FROM open_pnl_cache ORDER BY updated_at DESC NULLS LAST LIMIT 1"))).first())

    return {
        "trades_row_count": trades_count,
        "ant_acc_row_count": ant_acc_count,
        "vta_row_count": vta_count,
        "mtt_row_count": mtt_count,
        "dealio_users_row_count": dealio_users_count,
        "vtiger_users_row_count": vtiger_users_count,
        "vtiger_campaigns_row_count": vtiger_campaigns_count,
        "extensions_row_count": extensions_count,
        "open_pnl_row_count": open_pnl_count,
        "trades_last": trades_last,
        "ant_acc_last": ant_acc_last,
        "vta_last": vta_last,
        "mtt_last": mtt_last,
        "dealio_users_last": dealio_users_last,
        "vtiger_users_last": vtiger_users_last,
        "vtiger_campaigns_last": vtiger_campaigns_last,
        "extensions_last": extensions_last,
        "open_pnl_last": open_pnl_last,
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
