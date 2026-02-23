from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.database import execute_query
from app.models.retention_field import RetentionField
from app.pg_database import get_db

router = APIRouter()

AVAILABLE_TABLES = [
    "report.ant_acc",
    "report.vtiger_trading_accounts",
    "report.dealio_mt4trades",
]

OPERATORS = ["+", "-", "*", "/"]


class RetentionFieldCreate(BaseModel):
    field_name: str
    table_a: str
    column_a: str
    operator: str
    table_b: str
    column_b: str


class RetentionFieldOut(BaseModel):
    id: int
    field_name: str
    table_a: str
    column_a: str
    operator: str
    table_b: str
    column_b: str
    created_at: datetime

    model_config = {"from_attributes": True}


@router.get("/retention-fields/tables")
async def get_tables(_: str = Depends(require_admin)) -> List[str]:
    return AVAILABLE_TABLES


@router.get("/retention-fields/columns")
async def get_columns(
    table: str = Query(...),
    _: str = Depends(require_admin),
) -> List[str]:
    if table not in AVAILABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"Invalid table: {table}")
    parts = table.split(".", 1)
    schema, table_name = (parts[0], parts[1]) if len(parts) == 2 else ("dbo", parts[0])
    try:
        rows = await execute_query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            (schema, table_name),
        )
        return [r["COLUMN_NAME"] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch columns: {e}")


@router.get("/retention-fields", response_model=List[RetentionFieldOut])
async def list_retention_fields(db: AsyncSession = Depends(get_db)) -> List[RetentionFieldOut]:
    result = await db.execute(select(RetentionField).order_by(RetentionField.created_at))
    return result.scalars().all()


@router.post("/retention-fields", response_model=RetentionFieldOut)
async def create_retention_field(
    body: RetentionFieldCreate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_admin),
) -> RetentionFieldOut:
    if body.table_a not in AVAILABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"Invalid table: {body.table_a}")
    if body.table_b not in AVAILABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"Invalid table: {body.table_b}")
    if body.operator not in OPERATORS:
        raise HTTPException(status_code=400, detail=f"Invalid operator: {body.operator}")
    field = RetentionField(**body.model_dump())
    db.add(field)
    await db.commit()
    await db.refresh(field)
    return field


@router.delete("/retention-fields/{field_id}", status_code=204)
async def delete_retention_field(
    field_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_admin),
) -> None:
    field = await db.get(RetentionField, field_id)
    if not field:
        raise HTTPException(status_code=404, detail="Field not found")
    await db.delete(field)
    await db.commit()
