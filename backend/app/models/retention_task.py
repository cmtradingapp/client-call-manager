from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func
from app.pg_database import Base


class RetentionTask(Base):
    __tablename__ = "retention_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    conditions = Column(Text, nullable=False, default="[]")  # JSON array stored as text
    created_at = Column(DateTime(timezone=True), server_default=func.now())
