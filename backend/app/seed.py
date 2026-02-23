import logging

from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.role import ALL_PAGES, Role
from app.models.user import User

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def seed_admin(session: AsyncSession) -> None:
    # Seed admin role
    result = await session.execute(select(Role).where(Role.name == "admin"))
    admin_role = result.scalar_one_or_none()
    if not admin_role:
        admin_role = Role(name="admin", permissions=ALL_PAGES)
        session.add(admin_role)
        await session.flush()
        logger.info("Admin role created")

    # Seed admin user
    result = await session.execute(select(User).where(User.username == "admin"))
    if not result.scalar_one_or_none():
        admin = User(
            username="admin",
            email="admin@backoffice.local",
            hashed_password=hash_password("Hdtkfvi12345"),
            role="admin",
            is_active=True,
        )
        session.add(admin)
        logger.info("Admin user created")

    await session.commit()
