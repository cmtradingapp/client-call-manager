import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.history_db import init_history_db
from app.pg_database import AsyncSessionLocal, init_pg
from app.replica_database import init_replica
from app.routers import calls, clients, filters
from app.routers.call_mappings import router as call_mappings_router
from app.routers.etl import daily_full_sync_all, incremental_sync_ant_acc, incremental_sync_dealio_users, incremental_sync_mtt, incremental_sync_trades, incremental_sync_vta, refresh_retention_mv, router as etl_router
from app.routers.retention import router as retention_router
from app.routers.retention_fields import router as retention_fields_router
from app.routers.auth import router as auth_router
from app.routers.roles_admin import router as roles_router
from app.routers.users_admin import router as users_router
from app.seed import seed_admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_history_db()
    await init_pg()
    init_replica()
    async with AsyncSessionLocal() as session:
        await seed_admin(session)
    # Mark any stale "running" jobs left over from a previous crash/restart
    from sqlalchemy import text as _text
    async with AsyncSessionLocal() as session:
        await session.execute(
            _text("UPDATE etl_sync_log SET status='error', error_message='Interrupted by restart' WHERE status='running'")
        )
        await session.commit()
    # Add new ant_acc columns if not yet present (safe ADD COLUMN IF NOT EXISTS)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS is_test_account SMALLINT"))
        await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS sales_client_potential VARCHAR(100)"))
        await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS birth_date DATE"))
        await session.commit()
    logger.info("ant_acc column migrations applied")
    # Create performance indexes if missing (covers existing deployments)
    async with AsyncSessionLocal() as session:
        # Covering index for retention query: filters on (login, cmd, symbol), reads notional_value, open_time, close_time
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_trades_mt4_login_cmd_cov ON trades_mt4 (login, cmd) INCLUDE (symbol, notional_value, close_time, open_time)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_trades_mt4_close_time ON trades_mt4 (close_time)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_ant_acc_qual_date ON ant_acc (client_qualification_date)"
        ))
        # Covering index for deposits_agg join in retention query
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_mtt_login_approval_type ON vtiger_mttransactions (login, transactionapproval, transactiontype) INCLUDE (usdamount, confirmation_time, payment_method)"
        ))
        # Index for qualifying_logins join (vtigeraccountid lookup)
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_vta_vtigeraccountid ON vtiger_trading_accounts (vtigeraccountid)"
        ))
        await session.commit()
    logger.info("Performance indexes created/verified")
    # Always recreate retention_mv with latest definition (WITH NO DATA = instant DDL, no lock contention)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("DROP MATERIALIZED VIEW IF EXISTS retention_mv CASCADE"))
        await session.commit()
    logger.info("retention_mv dropped for recreation (if existed)")
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE MATERIALIZED VIEW retention_mv AS
            WITH qualifying_logins AS (
                SELECT vta.login, a.accountid
                FROM ant_acc a
                INNER JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = a.accountid
                WHERE a.client_qualification_date IS NOT NULL
                  AND a.client_qualification_date >= '2024-01-01'
                  AND (a.is_test_account IS NULL OR a.is_test_account = 0)
            ),
            trades_agg AS (
                SELECT
                    ql.accountid,
                    COUNT(t.ticket) AS trade_count,
                    COALESCE(SUM(t.computed_profit), 0) AS total_profit,
                    MAX(t.open_time) AS last_trade_date,
                    MAX(t.open_time) AS last_close_time
                FROM qualifying_logins ql
                LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)
                    AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN ('inactivity', 'zeroingusd', 'spread'))
                GROUP BY ql.accountid
            ),
            deposits_agg AS (
                SELECT
                    ql.accountid,
                    COUNT(mtt.mttransactionsid) AS deposit_count,
                    COALESCE(SUM(mtt.usdamount), 0) AS total_deposit,
                    MAX(mtt.confirmation_time) AS last_deposit_time
                FROM qualifying_logins ql
                LEFT JOIN vtiger_mttransactions mtt ON mtt.login = ql.login
                    AND mtt.transactionapproval = 'Approved'
                    AND mtt.transactiontype = 'Deposit'
                    AND (mtt.payment_method IS NULL OR mtt.payment_method != 'BonusProtectedPositionCashback')
                GROUP BY ql.accountid
            ),
            balance_agg AS (
                SELECT
                    ql.accountid,
                    COALESCE(SUM(du.balance), 0) AS total_balance,
                    COALESCE(SUM(du.credit), 0) AS total_credit
                FROM qualifying_logins ql
                LEFT JOIN dealio_users du ON du.login = ql.login
                GROUP BY ql.accountid
            )
            SELECT
                a.accountid,
                a.client_qualification_date,
                a.sales_client_potential,
                a.birth_date,
                ta.trade_count,
                ta.total_profit,
                ta.last_trade_date,
                ta.last_close_time,
                da.deposit_count,
                da.total_deposit,
                da.last_deposit_time,
                ab.total_balance,
                ab.total_credit
            FROM ant_acc a
            INNER JOIN trades_agg ta ON ta.accountid = a.accountid
            INNER JOIN deposits_agg da ON da.accountid = a.accountid
            INNER JOIN balance_agg ab ON ab.accountid = a.accountid
            WHERE a.client_qualification_date IS NOT NULL
              AND (a.is_test_account IS NULL OR a.is_test_account = 0)
            WITH NO DATA
        """))
        await session.commit()
    # Unique index is always created fresh (MV was just recreated)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE UNIQUE INDEX retention_mv_accountid ON retention_mv (accountid)"
        ))
        await session.commit()
    logger.info("retention_mv and unique index created/verified")
    # Tune PostgreSQL — must run outside a transaction (AUTOCOMMIT)
    try:
        from app.pg_database import engine as _pg_engine
        async with _pg_engine.connect() as _conn:
            await _conn.execution_options(isolation_level="AUTOCOMMIT")
            await _conn.execute(_text("ALTER SYSTEM SET work_mem = '256MB'"))
            await _conn.execute(_text("ALTER SYSTEM SET effective_cache_size = '9GB'"))
            await _conn.execute(_text("ALTER SYSTEM SET shared_buffers = '3GB'"))
            await _conn.execute(_text("ALTER SYSTEM SET maintenance_work_mem = '512MB'"))
            await _conn.execute(_text("SELECT pg_reload_conf()"))
        logger.info("PostgreSQL system settings tuned")
    except Exception as pg_tune_err:
        logger.warning("Could not apply PostgreSQL system settings (need superuser): %s", pg_tune_err)
    app.state.http_client = httpx.AsyncClient(timeout=30.0)
    logger.info("Shared HTTP client initialised")

    from app.replica_database import _ReplicaSession
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        incremental_sync_ant_acc,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    scheduler.add_job(
        incremental_sync_vta,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    scheduler.add_job(
        incremental_sync_mtt,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    if _ReplicaSession is not None:
        scheduler.add_job(
            incremental_sync_trades,
            "interval",
            minutes=30,
            args=[AsyncSessionLocal, _ReplicaSession],
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        scheduler.add_job(
            incremental_sync_dealio_users,
            "interval",
            minutes=30,
            args=[AsyncSessionLocal, _ReplicaSession],
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
    scheduler.add_job(
        daily_full_sync_all,
        "cron",
        hour=0,
        minute=0,
    )
    scheduler.add_job(
        refresh_retention_mv,
        "interval",
        minutes=3,
    )
    scheduler.start()
    logger.info("ETL scheduler started — incremental sync every 30 minutes, daily full sync at midnight")

    yield

    scheduler.shutdown(wait=False)
    await app.state.http_client.aclose()
    logger.info("Shared HTTP client closed")


app = FastAPI(title="Client Call Manager API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api")
app.include_router(clients.router, prefix="/api")
app.include_router(calls.router, prefix="/api")
app.include_router(filters.router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.include_router(roles_router, prefix="/api")
app.include_router(call_mappings_router, prefix="/api")
app.include_router(etl_router, prefix="/api")
app.include_router(retention_router, prefix="/api")
app.include_router(retention_fields_router, prefix="/api")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/api/health/time")
async def health_time() -> dict:
    from datetime import datetime, timedelta, timezone, timezone
    from app.database import execute_query
    result = {"server_utc": datetime.now(timezone.utc).isoformat()}
    try:
        rows = await execute_query("SELECT GETUTCDATE() AS mssql_utc, GETDATE() AS mssql_local", ())
        if rows:
            result["mssql_utc"] = rows[0]["mssql_utc"].isoformat() if rows[0]["mssql_utc"] else None
            result["mssql_local"] = rows[0]["mssql_local"].isoformat() if rows[0]["mssql_local"] else None
            if rows[0]["mssql_utc"]:
                diff = datetime.now(timezone.utc).replace(tzinfo=None) - rows[0]["mssql_utc"]
                result["server_ahead_of_mssql_utc_seconds"] = round(diff.total_seconds())
    except Exception as e:
        result["mssql_error"] = str(e)
    return result


@app.get("/api/health/replica")
async def health_replica() -> dict:
    import asyncio
    from app.replica_database import get_replica_engine
    engine = get_replica_engine()
    if engine is None:
        return {"status": "not_configured", "detail": "REPLICA_DB_HOST is not set"}
    try:
        from sqlalchemy import text
        result = {}
        async def _check():
            async with engine.connect() as conn:
                row = (await conn.execute(text(
                    "SELECT NOW() AS server_now, NOW() AT TIME ZONE 'UTC' AS server_utc, current_setting('TimeZone') AS tz"
                ))).first()
                result["server_now"] = row[0].isoformat() if row[0] else None
                result["server_utc"] = row[1].isoformat() if row[1] else None
                result["server_timezone"] = row[2]
        await asyncio.wait_for(_check(), timeout=5.0)
        return {"status": "ok", "host": settings.replica_db_host, "port": settings.replica_db_port, "db": settings.replica_db_name, **result}
    except asyncio.TimeoutError:
        return {"status": "error", "detail": "Connection timed out — IP may not be whitelisted"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
