import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.user import User
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/dashboard/trading")
async def get_trading_dashboard(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    """Return company-level trading metrics for the management dashboard.

    All data sourced from local PostgreSQL. Approval value for successful
    transactions is 'Approved' (not 'Completed').
    """

    # ------------------------------------------------------------------
    # Today's deposits (Approved only)
    # ------------------------------------------------------------------
    row = (await db.execute(text("""
        SELECT COUNT(*), COALESCE(SUM(usdamount), 0)
        FROM vtiger_mttransactions
        WHERE transactiontype = 'Deposit'
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE
    """))).fetchone()
    today_deposits_count = int(row[0]) if row else 0
    today_deposits_amount = round(float(row[1]), 2) if row else 0.0

    # ------------------------------------------------------------------
    # Today's FTDs — logins whose very first Approved deposit is today
    # ------------------------------------------------------------------
    row = (await db.execute(text("""
        WITH first_dep AS (
            SELECT login, MIN(DATE(confirmation_time)) AS first_date
            FROM vtiger_mttransactions
            WHERE transactiontype = 'Deposit' AND transactionapproval = 'Approved'
            GROUP BY login
        )
        SELECT COUNT(*), COALESCE(SUM(t.usdamount), 0)
        FROM vtiger_mttransactions t
        JOIN first_dep fd ON t.login = fd.login AND fd.first_date = CURRENT_DATE
        WHERE t.transactiontype = 'Deposit'
          AND t.transactionapproval = 'Approved'
          AND t.confirmation_time >= CURRENT_DATE
    """))).fetchone()
    today_ftds_count = int(row[0]) if row else 0
    today_ftds_amount = round(float(row[1]), 2) if row else 0.0

    # ------------------------------------------------------------------
    # Today's withdrawals (Approved)
    # ------------------------------------------------------------------
    row = (await db.execute(text("""
        SELECT COUNT(*), COALESCE(SUM(usdamount), 0)
        FROM vtiger_mttransactions
        WHERE transactiontype IN ('Withdrawal', 'Withdraw')
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE
    """))).fetchone()
    today_withdrawals_count = int(row[0]) if row else 0
    today_withdrawals_amount = round(float(row[1]), 2) if row else 0.0

    # ------------------------------------------------------------------
    # Net deposits today
    # ------------------------------------------------------------------
    net_deposits_today = round(today_deposits_amount - today_withdrawals_amount, 2)

    # ------------------------------------------------------------------
    # Total client balance + credit — only accounts with positive balance
    # (summing all 1M accounts including dormant gives unrealistic totals)
    # ------------------------------------------------------------------
    row = (await db.execute(text("""
        SELECT COALESCE(SUM(balance), 0), COALESCE(SUM(credit), 0),
               COUNT(*)
        FROM vtiger_trading_accounts
        WHERE balance > 0 AND balance < 10000000
    """))).fetchone()
    total_client_balance = round(float(row[0]), 2) if row else 0.0
    total_client_credit = round(float(row[1]), 2) if row else 0.0
    total_accounts = int(row[2]) if row else 0

    # ------------------------------------------------------------------
    # Active traders today — accounts whose balance changed today
    # (vtiger_trading_accounts.modifiedtime updated = trade activity)
    # ------------------------------------------------------------------
    row = (await db.execute(text("""
        SELECT COUNT(DISTINCT login)
        FROM vtiger_trading_accounts
        WHERE modifiedtime >= CURRENT_DATE
    """))).fetchone()
    active_traders_today = int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Retention client counts from retention_mv (guarded — may still build)
    # ------------------------------------------------------------------
    retention_mv_ready = False
    retention_clients_total = 0
    retention_clients_active = 0
    try:
        row = (await db.execute(text("""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE active = true)
            FROM retention_mv
        """))).fetchone()
        if row:
            retention_clients_total = int(row[0])
            retention_clients_active = int(row[1])
            retention_mv_ready = True
    except Exception as exc:
        logger.warning("retention_mv not available: %s", exc)

    # ------------------------------------------------------------------
    # Deposits last 7 days (for the chart)
    # ------------------------------------------------------------------
    deposits_last_7_days: List[Dict[str, Any]] = []
    try:
        rows = (await db.execute(text("""
            SELECT DATE(confirmation_time) AS date,
                   COUNT(*)              AS count,
                   COALESCE(SUM(usdamount), 0) AS amount_usd
            FROM vtiger_mttransactions
            WHERE transactiontype = 'Deposit'
              AND transactionapproval = 'Approved'
              AND confirmation_time >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(confirmation_time)
            ORDER BY date ASC
        """))).fetchall()
        for r in rows:
            deposits_last_7_days.append({
                "date": str(r[0]) if r[0] is not None else "",
                "count": int(r[1]) if r[1] is not None else 0,
                "amount_usd": round(float(r[2]), 2) if r[2] is not None else 0.0,
            })
    except Exception as exc:
        logger.warning("Could not fetch deposits last 7 days: %s", exc)

    # ------------------------------------------------------------------
    # Top depositors today (top 10 by USD amount)
    # ant_acc has no login column — bridge via vtiger_trading_accounts
    # ------------------------------------------------------------------
    top_depositors_today: List[Dict[str, Any]] = []
    try:
        rows = (await db.execute(text("""
            SELECT t.login,
                   a.full_name,
                   SUM(t.usdamount)        AS amount_usd,
                   MAX(t.payment_method)   AS payment_method
            FROM vtiger_mttransactions t
            LEFT JOIN vtiger_trading_accounts vta ON vta.login::text = t.login::text
            LEFT JOIN ant_acc a ON a.accountid::text = vta.vtigeraccountid::text
            WHERE t.transactiontype = 'Deposit'
              AND t.transactionapproval = 'Approved'
              AND t.confirmation_time >= CURRENT_DATE
            GROUP BY t.login, a.full_name
            ORDER BY amount_usd DESC
            LIMIT 10
        """))).fetchall()
        for r in rows:
            top_depositors_today.append({
                "login": str(r[0]) if r[0] is not None else "",
                "name": str(r[1]) if r[1] is not None else "",
                "amount_usd": round(float(r[2]), 2) if r[2] is not None else 0.0,
                "payment_method": str(r[3]) if r[3] is not None else "",
            })
    except Exception as exc:
        logger.warning("Could not fetch top depositors: %s", exc)

    return {
        "today_deposits": {
            "count": today_deposits_count,
            "amount_usd": today_deposits_amount,
        },
        "today_ftds": {
            "count": today_ftds_count,
            "amount_usd": today_ftds_amount,
        },
        "today_withdrawals": {
            "count": today_withdrawals_count,
            "amount_usd": today_withdrawals_amount,
        },
        "net_deposits_today": net_deposits_today,
        "total_client_balance": total_client_balance,
        "total_client_credit": total_client_credit,
        "total_accounts": total_accounts,
        "active_traders_today": active_traders_today,
        "retention_clients_total": retention_clients_total,
        "retention_clients_active": retention_clients_active,
        "retention_mv_ready": retention_mv_ready,
        "top_depositors_today": top_depositors_today,
        "deposits_last_7_days": deposits_last_7_days,
    }
