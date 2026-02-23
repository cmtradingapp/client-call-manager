from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.pg_database import Base


class RetentionField(Base):
    __tablename__ = "retention_fields"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_name: Mapped[str] = mapped_column(String(128), nullable=False)
    table_a: Mapped[str] = mapped_column(String(128), nullable=False)
    column_a: Mapped[str] = mapped_column(String(128), nullable=False)
    operator: Mapped[str] = mapped_column(String(4), nullable=False)
    table_b: Mapped[str] = mapped_column(String(128), nullable=False)
    column_b: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
