import logging
import ssl

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

logger = logging.getLogger(__name__)

_replica_engine = None
_ReplicaSession = None


def _build_replica_url() -> str:
    return (
        f"postgresql+asyncpg://{settings.replica_db_user}:{settings.replica_db_password}"
        f"@{settings.replica_db_host}:{settings.replica_db_port}/{settings.replica_db_name}"
    )


def init_replica() -> None:
    global _replica_engine, _ReplicaSession
    if not settings.replica_db_host:
        logger.warning("Replica DB not configured — skipping")
        return
    try:
        if settings.replica_db_ssl:
            ssl_ctx = ssl.create_default_context(cafile="/app/certs/ca.crt")
            ssl_ctx.load_cert_chain("/app/certs/client.crt", "/app/certs/client.key")
            connect_args = {"ssl": ssl_ctx}
        else:
            connect_args = {}
        _replica_engine = create_async_engine(_build_replica_url(), echo=False, pool_pre_ping=True, connect_args=connect_args)
        _ReplicaSession = async_sessionmaker(_replica_engine, expire_on_commit=False)
        logger.info("Replica DB engine initialised (%s:%s/%s)", settings.replica_db_host, settings.replica_db_port, settings.replica_db_name)
    except Exception as e:
        logger.error("Failed to initialise replica DB engine: %s", e)


def get_replica_engine():
    return _replica_engine


async def get_replica_db() -> AsyncSession:
    if _ReplicaSession is None:
        raise RuntimeError("Replica database is not configured")
    async with _ReplicaSession() as session:
        yield session
