from sqlalchemy import BigInteger, Column, DateTime, Index, String

from app.pg_database import Base


class VtigerTradingAccount(Base):
    __tablename__ = "vtiger_trading_accounts"

    login = Column(BigInteger, primary_key=True)
    vtigeraccountid = Column(String(50), nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        Index("ix_vtiger_trading_accounts_vtigeraccountid", "vtigeraccountid"),
        Index("ix_vtiger_trading_accounts_modifiedtime", "modifiedtime"),
    )
