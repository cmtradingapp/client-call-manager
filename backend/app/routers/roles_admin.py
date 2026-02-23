from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.models.role import ALL_PAGES, Role
from app.pg_database import get_db

router = APIRouter()


class RoleRequest(BaseModel):
    name: str
    permissions: list[str]


@router.get("/admin/roles")
async def list_roles(db: AsyncSession = Depends(get_db), _=Depends(require_admin)):
    result = await db.execute(select(Role).order_by(Role.created_at))
    roles = result.scalars().all()
    return [
        {
            "id": r.id,
            "name": r.name,
            "permissions": r.permissions,
            "created_at": r.created_at.isoformat(),
        }
        for r in roles
    ]


@router.get("/admin/pages")
async def list_pages(_=Depends(require_admin)):
    return ALL_PAGES


@router.post("/admin/roles", status_code=201)
async def create_role(body: RoleRequest, db: AsyncSession = Depends(get_db), _=Depends(require_admin)):
    result = await db.execute(select(Role).where(Role.name == body.name))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Role already exists")
    role = Role(name=body.name, permissions=body.permissions)
    db.add(role)
    await db.commit()
    return {"id": role.id, "name": role.name}


@router.patch("/admin/roles/{role_id}")
async def update_role(role_id: int, body: RoleRequest, db: AsyncSession = Depends(get_db), _=Depends(require_admin)):
    result = await db.execute(select(Role).where(Role.id == role_id))
    role = result.scalar_one_or_none()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    if role.name == "admin":
        raise HTTPException(status_code=400, detail="Cannot modify the admin role")
    role.name = body.name
    role.permissions = body.permissions
    await db.commit()
    return {"ok": True}


@router.delete("/admin/roles/{role_id}", status_code=204)
async def delete_role(role_id: int, db: AsyncSession = Depends(get_db), _=Depends(require_admin)):
    result = await db.execute(select(Role).where(Role.id == role_id))
    role = result.scalar_one_or_none()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    if role.name == "admin":
        raise HTTPException(status_code=400, detail="Cannot delete the admin role")
    await db.delete(role)
    await db.commit()
