from sqlalchemy import BigInteger, Column, DateTime, Index, Numeric, String

from app.pg_database import Base


class VtigerMtTransaction(Base):
    __tablename__ = "vtiger_mttransactions"

    mttransactionsid = Column(BigInteger, primary_key=True)
    login = Column(BigInteger, nullable=True)
    amount = Column(Numeric(18, 2), nullable=True)
    transactiontype = Column(String(100), nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        Index("ix_vtiger_mttransactions_login", "login"),
        Index("ix_vtiger_mttransactions_modifiedtime", "modifiedtime"),
    )
