from datetime import date
from typing import Any

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

from app.auth_deps import get_current_user
from app.pg_database import get_db

router = APIRouter()

# active = had a trade (open_time) OR deposit in the last N days
_MV_ACTIVE = (
    "COALESCE("
    "m.last_trade_date > CURRENT_DATE - make_interval(days => :activity_days)"
    " OR m.last_deposit_time > CURRENT_DATE - make_interval(days => :activity_days)"
    ", false)"
)
_MV_ACTIVE_FTD = (
    f"(m.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_MV_ACTIVE})"
)

# Each value is a SQL expression used in ORDER BY.
# The query builder appends "NULLS LAST" for all columns so NULLs always
# sort to the bottom regardless of direction.
#
# Numeric columns: expressions that must compare as numbers are kept as their
# native numeric MV column/expression — PostgreSQL sorts these correctly when
# the column type is numeric/float.  The one exception is sales_client_potential
# which is stored as TEXT and must be cast explicitly.
#
# "score" is computed per-page in Python (not stored in retention_mv), so
# server-side sorting by score is not available; it falls back to accountid.
_SORT_COLS = {
    # --- text columns ---
    "accountid":            "m.accountid",
    "full_name":            "m.full_name",
    "assigned_to":          "m.assigned_to",
    "agent_name":           "m.agent_name",

    # --- date / timestamp columns (already correct types in MV) ---
    "client_qualification_date": "m.client_qualification_date",
    "last_trade_date":      "m.last_trade_date",
    "days_from_last_trade": "m.last_close_time",   # timestamp; NULLS LAST handles missing

    # --- integer/numeric columns (native numeric type in MV) ---
    "days_in_retention":    "(CURRENT_DATE - m.client_qualification_date)",
    "trade_count":          "m.trade_count",
    "total_profit":         "m.total_profit",
    "deposit_count":        "m.deposit_count",
    "total_deposit":        "m.total_deposit",
    "balance":              "m.total_balance",
    "credit":               "m.total_credit",
    "equity":               "m.total_equity",
    "max_open_trade":       "m.max_open_trade",
    "max_volume":           "m.max_volume",
    "age":                  "EXTRACT(year FROM AGE(m.birth_date))",

    # --- computed numeric expressions ---
    "live_equity":          "(m.total_balance + m.total_credit)",  # MV proxy (excludes live open_pnl)
    "open_pnl":             "m.total_equity",  # proxy; open_pnl is fetched live
    "turnover":             (
        "CASE WHEN (m.total_balance + m.total_credit) != 0"
        " THEN m.max_volume / (m.total_balance + m.total_credit)"
        " ELSE NULL END"
    ),

    # --- boolean expressions ---
    "active":               _MV_ACTIVE,
    "active_ftd":           _MV_ACTIVE_FTD,

    # --- text-stored numeric column — explicit cast required ---
    "sales_client_potential": "NULLIF(TRIM(m.sales_client_potential), '')::NUMERIC",

    # --- score: pre-computed in client_scores table, joined at query time ---
    "score":                "COALESCE(cs.score, 0)",
}

_OP_MAP = {"eq": "=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

# Valid operators including "between" (requires two values)
_VALID_OPS = set(_OP_MAP.keys()) | {"between"}


def _num_cond(op: str, expr: str, param: str, param2: str | None = None) -> str | None:
    """Build a numeric WHERE condition.

    For op='between', param2 must be provided; returns BETWEEN clause.
    For all other ops, uses _OP_MAP for the SQL operator.
    Returns None if the operator is unrecognised.
    """
    if op == "between":
        if param2 is None:
            return None
        return f"{expr} BETWEEN :{param} AND :{param2}"
    sql_op = _OP_MAP.get(op)
    return f"{expr} {sql_op} :{param}" if sql_op else None


def _date_preset_cond(expr: str, preset: str) -> str | None:
    """Return a SQL fragment for a named date preset (today / this_week / this_month).

    expr must be a date-typed SQL expression (cast if needed).
    Returns None for unknown preset values.
    """
    if preset == "today":
        return f"{expr} = CURRENT_DATE"
    if preset == "this_week":
        return f"{expr} >= date_trunc('week', CURRENT_DATE) AND {expr} < date_trunc('week', CURRENT_DATE) + INTERVAL '7 days'"
    if preset == "this_month":
        return f"{expr} >= date_trunc('month', CURRENT_DATE) AND {expr} < date_trunc('month', CURRENT_DATE) + INTERVAL '1 month'"
    return None


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort_by: str = Query("accountid"),
    sort_dir: str = Query("asc"),
    accountid: str = Query(""),
    # numeric filters
    trade_count_op: str = Query(""),
    trade_count_val: float | None = Query(None),
    days_op: str = Query(""),
    days_val: float | None = Query(None),
    profit_op: str = Query(""),
    profit_val: float | None = Query(None),
    days_from_last_trade_op: str = Query(""),
    days_from_last_trade_val: float | None = Query(None),
    deposit_count_op: str = Query(""),
    deposit_count_val: float | None = Query(None),
    total_deposit_op: str = Query(""),
    total_deposit_val: float | None = Query(None),
    balance_op: str = Query(""),
    balance_val: float | None = Query(None),
    credit_op: str = Query(""),
    credit_val: float | None = Query(None),
    equity_op: str = Query(""),
    equity_val: float | None = Query(None),
    live_equity_op: str = Query(""),
    live_equity_val: float | None = Query(None),
    max_open_trade_op: str = Query(""),
    max_open_trade_val: float | None = Query(None),
    max_volume_op: str = Query(""),
    max_volume_val: float | None = Query(None),
    turnover_op: str = Query(""),
    turnover_val: float | None = Query(None),
    # date range filters
    qual_date_from: str = Query(""),
    qual_date_to: str = Query(""),
    last_trade_from: str = Query(""),
    last_trade_to: str = Query(""),
    # agent filter
    assigned_to: str = Query(""),
    # task filter
    task_id: int | None = Query(None),
    # boolean filters
    active: str = Query(""),
    active_ftd: str = Query(""),
    # activity window
    activity_days: int = Query(35, ge=1, le=365),
    # -----------------------------------------------------------------------
    # Per-column text filters (ILIKE contains)
    # -----------------------------------------------------------------------
    filter_full_name: str = Query(""),
    filter_status: str = Query(""),       # maps to m.sales_client_potential (no retention_status column in MV)
    filter_agent: str = Query(""),        # ILIKE match on resolved agent name via vtiger_users subquery
    # -----------------------------------------------------------------------
    # Per-column numeric filters with operator + optional second value (between)
    # -----------------------------------------------------------------------
    filter_balance_op: str = Query(""),
    filter_balance_val: float | None = Query(None),
    filter_balance_val2: float | None = Query(None),
    filter_credit_op: str = Query(""),
    filter_credit_val: float | None = Query(None),
    filter_credit_val2: float | None = Query(None),
    filter_equity_op: str = Query(""),
    filter_equity_val: float | None = Query(None),
    filter_equity_val2: float | None = Query(None),
    filter_live_equity_op: str = Query(""),
    filter_live_equity_val: float | None = Query(None),
    filter_live_equity_val2: float | None = Query(None),
    filter_max_open_trade_op: str = Query(""),
    filter_max_open_trade_val: float | None = Query(None),
    filter_max_open_trade_val2: float | None = Query(None),
    filter_max_volume_op: str = Query(""),
    filter_max_volume_val: float | None = Query(None),
    filter_max_volume_val2: float | None = Query(None),
    filter_turnover_op: str = Query(""),
    filter_turnover_val: float | None = Query(None),
    filter_turnover_val2: float | None = Query(None),
    filter_score_op: str = Query(""),
    filter_score_val: float | None = Query(None),
    filter_score_val2: float | None = Query(None),
    # -----------------------------------------------------------------------
    # Per-column date filters: preset (today/this_week/this_month) OR from/to range
    # last_call  → m.last_trade_date (most recent trade / contact date in MV)
    # last_note  → m.last_deposit_time (most recent deposit, used as last note proxy in MV)
    # reg_date   → m.client_qualification_date
    # -----------------------------------------------------------------------
    filter_last_call_preset: str = Query(""),
    filter_last_call_from: str = Query(""),
    filter_last_call_to: str = Query(""),
    filter_last_note_preset: str = Query(""),
    filter_last_note_from: str = Query(""),
    filter_last_note_to: str = Query(""),
    filter_reg_date_preset: str = Query(""),
    filter_reg_date_from: str = Query(""),
    filter_reg_date_to: str = Query(""),
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        # Fetch configured extra columns
        _ec_result = await db.execute(
            text("SELECT source_column FROM retention_extra_columns ORDER BY id")
        )
        _extra_col_names = [r[0] for r in _ec_result.fetchall()]
        _sort_cols_ext = dict(_SORT_COLS)
        for _ecn in _extra_col_names:
            _sort_cols_ext[_ecn] = "m." + _ecn
        sort_col = _sort_cols_ext.get(sort_by, "m.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

        where: list[str] = ["m.client_qualification_date IS NOT NULL"]
        params: dict = {"activity_days": activity_days}

        if accountid:
            where.append("(m.accountid ILIKE :accountid_pattern OR m.full_name ILIKE :accountid_pattern)")
            params["accountid_pattern"] = f"%{accountid}%"

        if qual_date_from:
            where.append("m.client_qualification_date >= :qual_date_from")
            params["qual_date_from"] = date.fromisoformat(qual_date_from)
        if qual_date_to:
            where.append("m.client_qualification_date <= :qual_date_to")
            params["qual_date_to"] = date.fromisoformat(qual_date_to)

        if days_op and days_val is not None:
            cond = _num_cond(days_op, "(CURRENT_DATE - m.client_qualification_date)", "days_val")
            if cond:
                where.append(cond)
                params["days_val"] = int(days_val)

        if trade_count_op and trade_count_val is not None:
            cond = _num_cond(trade_count_op, "m.trade_count", "trade_count_val")
            if cond:
                where.append(cond)
                params["trade_count_val"] = int(trade_count_val)

        if profit_op and profit_val is not None:
            cond = _num_cond(profit_op, "m.total_profit", "profit_val")
            if cond:
                where.append(cond)
                params["profit_val"] = profit_val

        if last_trade_from:
            where.append("m.last_trade_date::date >= :last_trade_from")
            params["last_trade_from"] = date.fromisoformat(last_trade_from)
        if last_trade_to:
            where.append("m.last_trade_date::date <= :last_trade_to")
            params["last_trade_to"] = date.fromisoformat(last_trade_to)

        if days_from_last_trade_op and days_from_last_trade_val is not None:
            cond = _num_cond(days_from_last_trade_op, "(CURRENT_DATE - m.last_close_time::date)", "days_from_last_trade_val")
            if cond:
                where.append(f"m.last_close_time IS NOT NULL AND {cond}")
                params["days_from_last_trade_val"] = int(days_from_last_trade_val)

        if deposit_count_op and deposit_count_val is not None:
            cond = _num_cond(deposit_count_op, "m.deposit_count", "deposit_count_val")
            if cond:
                where.append(cond)
                params["deposit_count_val"] = int(deposit_count_val)

        if total_deposit_op and total_deposit_val is not None:
            cond = _num_cond(total_deposit_op, "m.total_deposit", "total_deposit_val")
            if cond:
                where.append(cond)
                params["total_deposit_val"] = total_deposit_val

        if balance_op and balance_val is not None:
            cond = _num_cond(balance_op, "m.total_balance", "balance_val")
            if cond:
                where.append(cond)
                params["balance_val"] = balance_val

        if credit_op and credit_val is not None:
            cond = _num_cond(credit_op, "m.total_credit", "credit_val")
            if cond:
                where.append(cond)
                params["credit_val"] = credit_val

        if equity_op and equity_val is not None:
            cond = _num_cond(equity_op, "m.total_equity", "equity_val")
            if cond:
                where.append(cond)
                params["equity_val"] = equity_val

        if live_equity_op and live_equity_val is not None:
            cond = _num_cond(live_equity_op, "(m.total_balance + m.total_credit)", "live_equity_val")
            if cond:
                where.append(cond)
                params["live_equity_val"] = live_equity_val

        if max_open_trade_op and max_open_trade_val is not None:
            cond = _num_cond(max_open_trade_op, "m.max_open_trade", "max_open_trade_val")
            if cond:
                where.append(cond)
                params["max_open_trade_val"] = max_open_trade_val

        if max_volume_op and max_volume_val is not None:
            cond = _num_cond(max_volume_op, "m.max_volume", "max_volume_val")
            if cond:
                where.append(cond)
                params["max_volume_val"] = max_volume_val

        if turnover_op and turnover_val is not None:
            cond = _num_cond(turnover_op, "CASE WHEN (m.total_balance + m.total_credit) != 0 THEN m.max_volume / (m.total_balance + m.total_credit) ELSE 0 END", "turnover_val")
            if cond:
                where.append(cond)
                params["turnover_val"] = turnover_val

        # -----------------------------------------------------------------------
        # Per-column text filters (ILIKE contains, case-insensitive)
        # -----------------------------------------------------------------------
        if filter_full_name:
            where.append("m.full_name ILIKE :filter_full_name_pattern")
            params["filter_full_name_pattern"] = f"%{filter_full_name}%"

        if filter_status:
            # retention_status is not a column in retention_mv;
            # sales_client_potential is the closest text-status field available.
            where.append("m.sales_client_potential ILIKE :filter_status_pattern")
            params["filter_status_pattern"] = f"%{filter_status}%"

        if filter_agent:
            # agent_name is now pre-computed in retention_mv — simple ILIKE on the column.
            where.append("m.agent_name ILIKE :filter_agent_pattern")
            params["filter_agent_pattern"] = f"%{filter_agent}%"

        # -----------------------------------------------------------------------
        # Per-column numeric filters (op + val + optional val2 for between)
        # -----------------------------------------------------------------------
        _numeric_filter_defs = [
            # (op_param_value, val_param_value, val2_param_value, sql_expr, param_prefix)
            (filter_balance_op,        filter_balance_val,        filter_balance_val2,        "m.total_balance",                                                                                                                       "filter_balance"),
            (filter_credit_op,         filter_credit_val,         filter_credit_val2,         "m.total_credit",                                                                                                                        "filter_credit"),
            (filter_equity_op,         filter_equity_val,         filter_equity_val2,         "m.total_equity",                                                                                                                        "filter_equity"),
            (filter_live_equity_op,    filter_live_equity_val,    filter_live_equity_val2,    "(m.total_balance + m.total_credit)",                                                                                                    "filter_live_equity"),
            (filter_max_open_trade_op, filter_max_open_trade_val, filter_max_open_trade_val2, "m.max_open_trade",                                                                                                                      "filter_max_open_trade"),
            (filter_max_volume_op,     filter_max_volume_val,     filter_max_volume_val2,     "m.max_volume",                                                                                                                          "filter_max_volume"),
            (filter_turnover_op,       filter_turnover_val,       filter_turnover_val2,       "CASE WHEN (m.total_balance + m.total_credit) != 0 THEN m.max_volume / (m.total_balance + m.total_credit) ELSE NULL END",               "filter_turnover"),
        ]
        for _op, _val, _val2, _expr, _prefix in _numeric_filter_defs:
            if not _op or _op not in _VALID_OPS or _val is None:
                continue
            _p1 = f"{_prefix}_val"
            _p2 = f"{_prefix}_val2"
            # For "between", both values must be present; skip if val2 is missing.
            if _op == "between" and _val2 is None:
                continue
            _cond = _num_cond(_op, _expr, _p1, _p2 if _op == "between" else None)
            if _cond:
                # Exclude NULLs so the filter doesn't silently skip rows with a NULL column.
                where.append(f"{_expr} IS NOT NULL AND {_cond}")
                params[_p1] = _val
                if _op == "between":
                    params[_p2] = _val2

        # score filter: score is now stored in client_scores (joined as cs) — apply server-side.
        if filter_score_op and filter_score_op in _VALID_OPS and filter_score_val is not None:
            if filter_score_op == "between" and filter_score_val2 is None:
                pass  # skip incomplete between filter
            else:
                _score_cond = _num_cond(
                    filter_score_op,
                    "COALESCE(cs.score, 0)",
                    "filter_score_val",
                    "filter_score_val2" if filter_score_op == "between" else None,
                )
                if _score_cond:
                    where.append(_score_cond)
                    params["filter_score_val"] = filter_score_val
                    if filter_score_op == "between":
                        params["filter_score_val2"] = filter_score_val2

        # -----------------------------------------------------------------------
        # Per-column date filters
        # last_call preset/range  → m.last_trade_date (::date cast for comparisons)
        # last_note preset/range  → m.last_deposit_time (::date cast)
        # reg_date  preset/range  → m.client_qualification_date
        # -----------------------------------------------------------------------
        _date_filter_defs = [
            # (preset_val, from_val, to_val, sql_date_expr, param_prefix, null_guard_col)
            (filter_last_call_preset,  filter_last_call_from,  filter_last_call_to,  "m.last_trade_date::date",   "filter_last_call",  "m.last_trade_date"),
            (filter_last_note_preset,  filter_last_note_from,  filter_last_note_to,  "m.last_deposit_time::date", "filter_last_note",  "m.last_deposit_time"),
            (filter_reg_date_preset,   filter_reg_date_from,   filter_reg_date_to,   "m.client_qualification_date", "filter_reg_date", None),
        ]
        for _preset, _from, _to, _date_expr, _dp, _null_col in _date_filter_defs:
            _date_conds: list[str] = []
            if _null_col:
                _null_guard = f"{_null_col} IS NOT NULL"
            else:
                _null_guard = None

            if _preset:
                _pc = _date_preset_cond(_date_expr, _preset)
                if _pc:
                    _date_conds.append(_pc)
            else:
                if _from:
                    _date_conds.append(f"{_date_expr} >= :{_dp}_from")
                    params[f"{_dp}_from"] = date.fromisoformat(_from)
                if _to:
                    _date_conds.append(f"{_date_expr} <= :{_dp}_to")
                    params[f"{_dp}_to"] = date.fromisoformat(_to)

            if _date_conds:
                combined = " AND ".join(_date_conds)
                if _null_guard:
                    where.append(f"{_null_guard} AND {combined}")
                else:
                    where.append(combined)

        if assigned_to:
            where.append("m.assigned_to = :assigned_to")
            params["assigned_to"] = assigned_to

        if active == "true":
            where.append(f"({_MV_ACTIVE})")
        elif active == "false":
            where.append(f"NOT ({_MV_ACTIVE})")

        if active_ftd == "true":
            where.append(f"({_MV_ACTIVE_FTD})")
        elif active_ftd == "false":
            where.append(f"NOT ({_MV_ACTIVE_FTD})")

        # Task filter — inject task conditions into the main WHERE clause
        if task_id is not None:
            import json as _json
            from sqlalchemy import select as _select
            from app.models.retention_task import RetentionTask
            from app.routers.retention_tasks import _build_task_where
            _task = await db.get(RetentionTask, task_id)
            if _task is None:
                raise HTTPException(status_code=404, detail="Task not found")
            _t_where, _t_params = _build_task_where(_json.loads(_task.conditions))
            where.extend(_t_where[1:])  # skip the first clause (client_qualification_date IS NOT NULL) — already in main where
            params.update(_t_params)

        where_clause = " AND ".join(where)

        count_result = await db.execute(
            text(f"SELECT COUNT(*) FROM retention_mv m LEFT JOIN client_scores cs ON cs.accountid = m.accountid WHERE {where_clause}"),
            params,
        )
        total = count_result.scalar() or 0

        _extra_sel = ""
        if _extra_col_names:
            _extra_sel = ",\n                    " + ",\n                    ".join("m." + c for c in _extra_col_names)
        rows_result = await db.execute(
            text(f"""
                SELECT
                    m.accountid,
                    m.full_name,
                    m.client_qualification_date,
                    (CURRENT_DATE - m.client_qualification_date) AS days_in_retention,
                    m.trade_count,
                    m.total_profit,
                    m.last_trade_date,
                    CASE WHEN m.last_close_time IS NOT NULL
                         THEN (CURRENT_DATE - m.last_close_time::date) END AS days_from_last_trade,
                    {_MV_ACTIVE} AS active,
                    {_MV_ACTIVE_FTD} AS active_ftd,
                    m.deposit_count,
                    m.total_deposit,
                    m.total_balance AS balance,
                    m.total_credit AS credit,
                    m.total_equity AS equity,
                    m.max_open_trade,
                    m.max_volume{_extra_sel},
                    m.assigned_to,
                    m.agent_name,
                    m.sales_client_potential,
                    CASE WHEN m.birth_date IS NOT NULL
                         THEN EXTRACT(year FROM AGE(m.birth_date))::int END AS age,
                    COALESCE(cs.score, 0) AS score
                FROM retention_mv m
                LEFT JOIN client_scores cs ON cs.accountid = m.accountid
                WHERE {where_clause}
                ORDER BY {sort_col} {direction} NULLS LAST
                LIMIT :limit OFFSET :offset
            """),
            {**params, "limit": page_size, "offset": (page - 1) * page_size},
        )
        rows = rows_result.mappings().all()

        # Fetch Open PNL live from the replica for this page's accounts
        open_pnl_map: dict = {}
        try:
            from app.replica_database import _ReplicaSession
            if _ReplicaSession is not None and rows:
                account_ids = [str(r["accountid"]) for r in rows]
                # Map accounts -> logins via local vtiger_trading_accounts
                login_result = await db.execute(
                    text("SELECT login, vtigeraccountid FROM vtiger_trading_accounts WHERE vtigeraccountid = ANY(:ids)"),
                    {"ids": account_ids},
                )
                login_rows = login_result.fetchall()
                logins = [lr[0] for lr in login_rows]
                login_to_account = {lr[0]: str(lr[1]) for lr in login_rows}
                if logins:
                    async with _ReplicaSession() as replica:
                        pnl_result = await replica.execute(
                            text("SELECT login, SUM(computedprofit) FROM dealio.positions WHERE login = ANY(:logins) GROUP BY login"),
                            {"logins": logins},
                        )
                        for pnl_row in pnl_result.fetchall():
                            acct = login_to_account.get(pnl_row[0])
                            if acct:
                                open_pnl_map[acct] = open_pnl_map.get(acct, 0.0) + (pnl_row[1] or 0.0)
        except Exception as pnl_err:
            logger.warning("Could not fetch open PNL from replica: %s", pnl_err)

        # Look up pre-computed task assignments for this page (single indexed query)
        from sqlalchemy import select as _select
        from app.models.retention_task import RetentionTask
        tasks_map: dict = {str(r["accountid"]): [] for r in rows}
        try:
            page_aids = [str(r["accountid"]) for r in rows]
            if page_aids:
                all_tasks_result = await db.execute(
                    _select(RetentionTask).order_by(RetentionTask.id)
                )
                tasks_by_id = {t.id: t for t in all_tasks_result.scalars().all()}
                if tasks_by_id:
                    assign_result = await db.execute(
                        text(
                            "SELECT accountid, task_id FROM client_task_assignments "
                            "WHERE accountid = ANY(:ids)"
                        ),
                        {"ids": page_aids},
                    )
                    for row in assign_result.fetchall():
                        aid = str(row[0])
                        task = tasks_by_id.get(row[1])
                        if aid in tasks_map and task:
                            tasks_map[aid].append({"name": task.name, "color": task.color or "grey"})
        except Exception as tasks_err:
            logger.warning("Could not load task assignments for page: %s", tasks_err)

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": [
                {
                    "accountid": str(r["accountid"]),
                    "full_name": r["full_name"] or "",
                    "client_qualification_date": r["client_qualification_date"].isoformat() if r["client_qualification_date"] else None,
                    "days_in_retention": int(r["days_in_retention"]) if r["days_in_retention"] is not None else None,
                    "trade_count": int(r["trade_count"]),
                    "total_profit": float(r["total_profit"]),
                    "last_trade_date": r["last_trade_date"].isoformat() if r["last_trade_date"] else None,
                    "days_from_last_trade": int(r["days_from_last_trade"]) if r["days_from_last_trade"] is not None else None,
                    "active": bool(r["active"]),
                    "active_ftd": bool(r["active_ftd"]),
                    "deposit_count": int(r["deposit_count"]),
                    "total_deposit": float(r["total_deposit"]),
                    "balance": float(r["balance"]),
                    "credit": float(r["credit"]),
                    "equity": float(r["equity"]),
                    "open_pnl": open_pnl_map.get(str(r["accountid"]), 0.0),
                    "max_open_trade": round(float(r["max_open_trade"]), 1) if r["max_open_trade"] is not None else None,
                    "max_volume": round(float(r["max_volume"]), 1) if r["max_volume"] is not None else None,
                    "live_equity": round(float(r["balance"]) + float(r["credit"]) + open_pnl_map.get(str(r["accountid"]), 0.0), 2),
                    "turnover": round(
                        float(r["max_volume"]) / (float(r["balance"]) + float(r["credit"]) + open_pnl_map.get(str(r["accountid"]), 0.0)), 1
                    ) if r["max_volume"] is not None and (float(r["balance"]) + float(r["credit"]) + open_pnl_map.get(str(r["accountid"]), 0.0)) != 0 else 0.0,
                    "assigned_to": r["assigned_to"],
                    "agent_name": r["agent_name"] or None,
                    "tasks": tasks_map.get(str(r["accountid"]), []),
                    "score": int(r["score"]),
                    "sales_client_potential": r["sales_client_potential"],
                    "age": int(r["age"]) if r["age"] is not None else None,
                    **{col: r[col] for col in _extra_col_names},
                }
                for r in rows
            ],
        }
    except Exception as e:
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")


@router.get("/retention/agents")
async def get_retention_agents(
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list:
    try:
        result = await db.execute(
            text("SELECT id, first_name, last_name FROM vtiger_users ORDER BY first_name, last_name")
        )
        rows = result.fetchall()
        return [
            {"id": str(r[0]), "name": f"{r[1] or ''} {r[2] or ''}".strip()}
            for r in rows
            if r[0]
        ]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch agents: {e}")
