from sqlalchemy import Column, Date, DateTime, Index, SmallInteger, String

from app.pg_database import Base


class AntAcc(Base):
    __tablename__ = "ant_acc"

    accountid = Column(String(50), primary_key=True)
    client_qualification_date = Column(Date, nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)
    is_test_account = Column(SmallInteger, nullable=True)
    sales_client_potential = Column(String(100), nullable=True)
    birth_date = Column(Date, nullable=True)
    full_name = Column(String(400), nullable=True)

    __table_args__ = (
        Index("ix_ant_acc_modifiedtime", "modifiedtime"),
        Index("ix_ant_acc_qual_date", "client_qualification_date"),
    )
