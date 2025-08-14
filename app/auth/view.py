
from fastapi import APIRouter, Depends, Query, HTTPException, status
from app.auth.helpers import create_jwt, ACCESS_TOKEN_TOKEN_TYPE, token_repo, UserRole
from app.auth.models import TokenFilter
from app.core.session_manager import SessionDep, TransactionSessionDep
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.validation import validate_token, require_admin
from typing import Optional


router = APIRouter(prefix="/tokens", tags=["Auth"])


@router.get("/generate-access", dependencies=[Depends(require_admin)])
async def generate_access_token(
    role: UserRole,
    description: Optional[str],
    session: AsyncSession = TransactionSessionDep,
):
    """Генерирует токен доступа с указанной ролью"""
    if role not in [r for r in UserRole]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role. Available roles: {[r.value for r in UserRole]}",
        )

    token = create_jwt(token_type=ACCESS_TOKEN_TOKEN_TYPE, role=role)

    await token_repo.add(
        session=session,
        values=TokenFilter(
            token=token,
            is_active=True,
            role=role,
            description=description,
        ),
    )

    return {
        "token": token,
        "role": role,
        "description": description,
    }


@router.get("/generate-admin", dependencies=[Depends(require_admin)])
async def generate_admin_token(
    description: Optional[str],
    session: AsyncSession = TransactionSessionDep,
):
    """Генерирует токен администратора (только для внутреннего использования)"""
    token = create_jwt(
        token_type=ACCESS_TOKEN_TOKEN_TYPE, role=UserRole.admin
    )

    await token_repo.add(
        session=session,
        values=TokenFilter(
            token=token,
            is_active=True,
            role=UserRole.admin,
            description=description,
        ),
    )

    return {
        "token": token,
        "role": UserRole.admin,
        "description": description,
    }


@router.get("/list", dependencies=[Depends(require_admin)])
async def list_tokens(session: AsyncSession = SessionDep):
    """Список всех токенов (только для администраторов)"""
    tokens = await token_repo.find_all(session=session)
    return [
        {
            "pid": token.pid,
            "is_active": token.is_active,
            "role": token.role,
            "description": token.description
        }
        for token in tokens
    ]


@router.post("/deactivate/{token_pid}", dependencies=[Depends(require_admin)])
async def deactivate_token(
    token_pid: int, session: AsyncSession = TransactionSessionDep
):
    """Деактивирует токен (только для администраторов)"""


    res = await token_repo.update(
        session=session, filters=TokenFilter(pid=token_pid), values=TokenFilter(is_active=False)
    )
    if not res:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Token not found"
        )

    return {"message": "Token deactivated successfully"}


@router.get("/my-info", dependencies=[Depends(validate_token)])
async def get_my_token_info(
    token_data: dict = Depends(validate_token), session: AsyncSession = SessionDep
):
    """Получить информацию о текущем токене"""
    token = await token_repo.find_one_or_none(
        session=session,
        filters=TokenFilter(token=token_data["token"]),
    )

    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Token not found in database"
        )

    return {
        "role": token.role,
        "description": token.description,
        "is_active": token.is_active
    }
