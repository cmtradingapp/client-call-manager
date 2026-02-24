from sqlalchemy import BigInteger, Column, DateTime, Index, Numeric, SmallInteger

from app.pg_database import Base


class TradesMt4(Base):
    __tablename__ = "trades_mt4"

    ticket = Column(BigInteger, primary_key=True)
    login = Column(BigInteger, nullable=False)
    cmd = Column(SmallInteger, nullable=False)
    profit = Column(Numeric(18, 2), nullable=True)
    close_time = Column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        Index("ix_trades_mt4_login", "login"),
        Index("ix_trades_mt4_login_cmd", "login", "cmd"),
        Index("ix_trades_mt4_close_time", "close_time"),
    )
