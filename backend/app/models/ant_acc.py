from sqlalchemy import Column, Date, DateTime, Index, String

from app.pg_database import Base


class AntAcc(Base):
    __tablename__ = "ant_acc"

    accountid = Column(String(50), primary_key=True)
    client_qualification_date = Column(Date, nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)

    __table_args__ = (Index("ix_ant_acc_modifiedtime", "modifiedtime"),)
