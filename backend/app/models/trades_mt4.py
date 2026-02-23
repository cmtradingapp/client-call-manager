from sqlalchemy import BigInteger, Column, Index, SmallInteger

from app.pg_database import Base


class TradesMt4(Base):
    __tablename__ = "trades_mt4"

    ticket = Column(BigInteger, primary_key=True)
    login = Column(BigInteger, nullable=False)
    cmd = Column(SmallInteger, nullable=False)

    __table_args__ = (Index("ix_trades_mt4_login", "login"),)
