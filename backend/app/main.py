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
from app.routers.etl import daily_full_sync_all, incremental_sync_ant_acc, incremental_sync_dealio_users, incremental_sync_mtt, incremental_sync_trades, incremental_sync_vta, hourly_sync_vtiger_users, hourly_sync_vtiger_campaigns, hourly_sync_extensions, refresh_retention_mv, rebuild_retention_mv, router as etl_router
from app.routers.retention import router as retention_router
from app.routers.retention_tasks import router as retention_tasks_router
from app.routers.client_scoring import router as client_scoring_router
from app.routers.crm import router as crm_router
from app.routers.auth import router as auth_router
from app.routers.roles_admin import router as roles_router
from app.routers.users_admin import router as users_router
from app.routers.integrations_admin import router as integrations_router
from app.routers.audit_log_admin import router as audit_log_router
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
    # Migrate: ensure retention_extra_columns table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS retention_extra_columns ("
            "id SERIAL PRIMARY KEY, "
            "display_name VARCHAR(128) NOT NULL, "
            "source_table VARCHAR(64) NOT NULL, "
            "source_column VARCHAR(128) NOT NULL, "
            "agg_fn VARCHAR(16) NOT NULL DEFAULT 'SUM', "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("retention_extra_columns migration applied")
    # Migrate: ensure retention_tasks table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS retention_tasks ("
            "id SERIAL PRIMARY KEY, "
            "name VARCHAR(255) NOT NULL, "
            "conditions TEXT NOT NULL DEFAULT '[]', "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("retention_tasks table migration applied")
    # Migrate: add color column to retention_tasks if not present
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "ALTER TABLE retention_tasks ADD COLUMN IF NOT EXISTS color VARCHAR(20) NOT NULL DEFAULT 'grey'"
        ))
        await session.commit()
    logger.info("retention_tasks.color column migration applied")
    # Migrate: ensure scoring_rules table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS scoring_rules ("
            "id SERIAL PRIMARY KEY, "
            "field VARCHAR(64) NOT NULL, "
            "operator VARCHAR(8) NOT NULL, "
            "value VARCHAR(64) NOT NULL, "
            "score INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("scoring_rules table migration applied")
    # Widen sync_type column if still VARCHAR(20) — dealio_users_incremental is 24 chars
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE etl_sync_log ALTER COLUMN sync_type TYPE VARCHAR(50)"))
        await session.commit()
    logger.info("etl_sync_log.sync_type column widened to VARCHAR(50)")
    # Add equity column to dealio_users if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE dealio_users ADD COLUMN IF NOT EXISTS equity FLOAT"))
        await session.commit()
    logger.info("dealio_users.equity column migration applied")
    # Add assigned_to column to ant_acc if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS assigned_to VARCHAR(50)"))
        await session.commit()
    logger.info("ant_acc.assigned_to column migration applied")
    # Recreate vtiger_users with correct schema (drop old schema if columns changed)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("DROP TABLE IF EXISTS vtiger_users"))
        await session.execute(_text(
            "CREATE TABLE vtiger_users ("
            "id VARCHAR(50) PRIMARY KEY, "
            "user_name TEXT, first_name TEXT, last_name TEXT, "
            "email TEXT, phone TEXT, department TEXT, status TEXT, "
            "office TEXT, position TEXT, fax TEXT)"
        ))
        await session.commit()
    logger.info("vtiger_users table recreated with correct schema")
    # Recreate vtiger_campaigns with correct schema (drop old schema if columns changed)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("DROP TABLE IF EXISTS vtiger_campaigns"))
        await session.execute(_text(
            "CREATE TABLE vtiger_campaigns ("
            "crmid VARCHAR(50) PRIMARY KEY, "
            "campaign_id TEXT, campaign_name TEXT, "
            "campaign_legacy_id TEXT, campaign_channel TEXT, campaign_sub_channel TEXT)"
        ))
        await session.commit()
    logger.info("vtiger_campaigns table recreated with correct schema")
    # Migrate: ensure extensions table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS extensions ("
            "id SERIAL PRIMARY KEY, "
            "name TEXT, extension VARCHAR(50) UNIQUE, "
            "user_name TEXT, agent_name TEXT, manager TEXT, "
            "position TEXT, office TEXT, email TEXT, manager_email TEXT, "
            "synced_at TIMESTAMPTZ DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("extensions table migration applied")
    # Migrate: ensure integrations table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS integrations ("
            "id SERIAL PRIMARY KEY, "
            "name VARCHAR(255) NOT NULL, "
            "base_url VARCHAR(500) NOT NULL, "
            "auth_key VARCHAR(500), "
            "description TEXT, "
            "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("integrations table migration applied")
    # Migrate: ensure audit_log table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS audit_log ("
            "id SERIAL PRIMARY KEY, "
            "agent_id INTEGER NOT NULL, "
            "agent_username VARCHAR(64) NOT NULL, "
            "client_account_id VARCHAR(64) NOT NULL, "
            "action_type VARCHAR(32) NOT NULL, "
            "action_value TEXT, "
            "timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_agent_username ON audit_log (agent_username)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_client_account_id ON audit_log (client_account_id)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_action_type ON audit_log (action_type)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_timestamp ON audit_log (timestamp)"
        ))
        await session.commit()
    logger.info("audit_log table migration applied")
    # Rebuild retention_mv using current extra columns config (must run after all table migrations)
    await rebuild_retention_mv()
    logger.info("retention_mv rebuilt with dynamic columns")
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
        hourly_sync_vtiger_users,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    scheduler.add_job(
        hourly_sync_vtiger_campaigns,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    scheduler.add_job(
        hourly_sync_extensions,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
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
    logger.info("ETL scheduler started — incremental sync every 30 min, vtiger/extensions hourly full refresh, daily full sync at midnight")

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
app.include_router(retention_tasks_router, prefix="/api")
app.include_router(client_scoring_router, prefix="/api")
app.include_router(crm_router, prefix="/api")
app.include_router(integrations_router, prefix="/api")
app.include_router(audit_log_router, prefix="/api")


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
