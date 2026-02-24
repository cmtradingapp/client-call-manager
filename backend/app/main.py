import logging
from contextlib import asynccontextmanager

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
from app.routers.etl import daily_full_sync_all, incremental_sync_ant_acc, incremental_sync_mtt, incremental_sync_trades, incremental_sync_vta, router as etl_router
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
    # Add new columns to trades_mt4 if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS profit NUMERIC(18,2)"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS notional_value NUMERIC(18,2)"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS close_time TIMESTAMP"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS open_time TIMESTAMP"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS symbol VARCHAR(50)"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS computed_profit NUMERIC(18,2)"))
        await session.execute(_text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS last_modified TIMESTAMP"))
        await session.commit()
    # Add new columns to vtiger_mttransactions if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE vtiger_mttransactions ADD COLUMN IF NOT EXISTS transactionapproval VARCHAR(100)"))
        await session.execute(_text("ALTER TABLE vtiger_mttransactions ADD COLUMN IF NOT EXISTS confirmation_time TIMESTAMP"))
        await session.execute(_text("ALTER TABLE vtiger_mttransactions ADD COLUMN IF NOT EXISTS payment_method VARCHAR(200)"))
        await session.execute(_text("ALTER TABLE vtiger_mttransactions ADD COLUMN IF NOT EXISTS usdamount NUMERIC(18,2)"))
        await session.commit()
    # Add new columns to vtiger_trading_accounts if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE vtiger_trading_accounts ADD COLUMN IF NOT EXISTS balance NUMERIC(18,2)"))
        await session.execute(_text("ALTER TABLE vtiger_trading_accounts ADD COLUMN IF NOT EXISTS credit NUMERIC(18,2)"))
        await session.commit()
    # Create performance indexes if missing (covers existing deployments)
    async with AsyncSessionLocal() as session:
        # Drop old trades indexes and rebuild with correct covering columns (symbol added)
        await session.execute(_text("DROP INDEX IF EXISTS ix_trades_mt4_login_cmd"))
        await session.execute(_text("DROP INDEX IF EXISTS ix_trades_mt4_login_cmd_cov"))
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
    # Tune PostgreSQL for this workload (requires superuser; silently skipped if not available)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("ALTER SYSTEM SET work_mem = '256MB'"))
            await session.execute(_text("ALTER SYSTEM SET effective_cache_size = '9GB'"))
            await session.execute(_text("ALTER SYSTEM SET shared_buffers = '3GB'"))
            await session.execute(_text("ALTER SYSTEM SET maintenance_work_mem = '512MB'"))
            await session.execute(_text("SELECT pg_reload_conf()"))
            await session.commit()
        logger.info("PostgreSQL system settings tuned")
    except Exception as pg_tune_err:
        logger.warning("Could not apply PostgreSQL system settings (need superuser): %s", pg_tune_err)
    # Migrate vtiger_mttransactions column names if table exists with old schema
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='vtiger_mttransactions' AND column_name='crmid') THEN
                    ALTER TABLE vtiger_mttransactions RENAME COLUMN crmid TO mttransactionsid;
                END IF;
                IF EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='vtiger_mttransactions' AND column_name='transaction_type') THEN
                    ALTER TABLE vtiger_mttransactions RENAME COLUMN transaction_type TO transactiontype;
                END IF;
            END$$;
        """))
        await session.commit()
    logger.info("vtiger_mttransactions schema migration checked")
    app.state.http_client = httpx.AsyncClient(timeout=30.0)
    logger.info("Shared HTTP client initialised")

    from app.replica_database import _ReplicaSession
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        incremental_sync_ant_acc,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
    )
    scheduler.add_job(
        incremental_sync_vta,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
    )
    scheduler.add_job(
        incremental_sync_mtt,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
    )
    if _ReplicaSession is not None:
        scheduler.add_job(
            incremental_sync_trades,
            "interval",
            minutes=30,
            args=[AsyncSessionLocal, _ReplicaSession],
        )
    scheduler.add_job(
        daily_full_sync_all,
        "cron",
        hour=0,
        minute=0,
    )
    scheduler.start()
    logger.info("ETL scheduler started — incremental sync every 5 minutes, daily full sync at midnight")

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
    from datetime import datetime, timezone
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
        async def _check():
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        await asyncio.wait_for(_check(), timeout=5.0)
        return {"status": "ok", "host": settings.replica_db_host, "port": settings.replica_db_port, "db": settings.replica_db_name}
    except asyncio.TimeoutError:
        return {"status": "error", "detail": "Connection timed out — IP may not be whitelisted"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
