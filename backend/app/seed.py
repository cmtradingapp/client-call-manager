import logging

from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def seed_admin(session: AsyncSession) -> None:
    result = await session.execute(select(User).where(User.username == "admin"))
    existing = result.scalar_one_or_none()
    if existing:
        logger.info("Admin user already exists, skipping seed")
        return

    admin = User(
        username="admin",
        email="admin@backoffice.local",
        hashed_password=hash_password("Hdtkfvi12345"),
        role="admin",
        is_active=True,
    )
    session.add(admin)
    await session.commit()
    logger.info("Admin user created successfully")
