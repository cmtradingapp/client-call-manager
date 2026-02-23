import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.history_db import init_history_db
from app.pg_database import AsyncSessionLocal, init_pg
from app.replica_database import init_replica
from app.routers import calls, clients, filters
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
    app.state.http_client = httpx.AsyncClient(timeout=30.0)
    logger.info("Shared HTTP client initialised")
    yield
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


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
