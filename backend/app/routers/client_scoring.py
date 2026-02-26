import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.scoring_rule import ScoringRule
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Column map — same fields available in retention_mv as used by retention_tasks
# ---------------------------------------------------------------------------

SCORING_COL_SQL: Dict[str, str] = {
    "balance":              "m.total_balance",
    "credit":               "m.total_credit",
    "equity":               "m.total_equity",
    "trade_count":          "m.trade_count",
    "total_profit":         "m.total_profit",
    "days_in_retention":    "(CURRENT_DATE - m.client_qualification_date)",
    "deposit_count":        "m.deposit_count",
    "total_deposit":        "m.total_deposit",
    "days_from_last_trade": "(CURRENT_DATE - m.last_close_time::date)",
    "sales_potential":      "NULLIF(TRIM(m.sales_client_potential), '')::numeric",
    "age":                  "EXTRACT(year FROM AGE(m.birth_date))::numeric",
}

SCORING_OP_MAP: Dict[str, str] = {
    "eq":  "=",
    "gt":  ">",
    "lt":  "<",
    "gte": ">=",
    "lte": "<=",
}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ScoringRuleCreate(BaseModel):
    field: str
    operator: str
    value: str
    score: int


class ScoringRuleUpdate(BaseModel):
    field: Optional[str] = None
    operator: Optional[str] = None
    value: Optional[str] = None
    score: Optional[int] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rule_out(rule: ScoringRule) -> Dict[str, Any]:
    return {
        "id": rule.id,
        "field": rule.field,
        "operator": rule.operator,
        "value": rule.value,
        "score": rule.score,
        "created_at": rule.created_at.isoformat() if rule.created_at else None,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/retention/scoring-rules")
async def list_scoring_rules(
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    result = await db.execute(
        select(ScoringRule).order_by(ScoringRule.id)
    )
    rules = result.scalars().all()
    return [_rule_out(r) for r in rules]


@router.post("/retention/scoring-rules", status_code=201)
async def create_scoring_rule(
    body: ScoringRuleCreate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    if body.field not in SCORING_COL_SQL:
        raise HTTPException(status_code=400, detail=f"Invalid field: {body.field}")
    if body.operator not in SCORING_OP_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid operator: {body.operator}")
    rule = ScoringRule(
        field=body.field,
        operator=body.operator,
        value=body.value,
        score=body.score,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return _rule_out(rule)


@router.put("/retention/scoring-rules/{rule_id}")
async def update_scoring_rule(
    rule_id: int,
    body: ScoringRuleUpdate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    rule = await db.get(ScoringRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Scoring rule not found")
    if body.field is not None:
        if body.field not in SCORING_COL_SQL:
            raise HTTPException(status_code=400, detail=f"Invalid field: {body.field}")
        rule.field = body.field
    if body.operator is not None:
        if body.operator not in SCORING_OP_MAP:
            raise HTTPException(status_code=400, detail=f"Invalid operator: {body.operator}")
        rule.operator = body.operator
    if body.value is not None:
        rule.value = body.value
    if body.score is not None:
        rule.score = body.score
    await db.commit()
    await db.refresh(rule)
    return _rule_out(rule)


@router.delete("/retention/scoring-rules/{rule_id}", status_code=204)
async def delete_scoring_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> None:
    rule = await db.get(ScoringRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Scoring rule not found")
    await db.delete(rule)
    await db.commit()
