
from app.core.session_manager import SessionDep
from fastapi import HTTPException, status, Request, Depends
import app.auth.utils as auth_utils
from app.utils.setup_logging import setup_logging
from app.auth.helpers import (
    TOKEN_TYPE_FIELD,
    ACCESS_TOKEN_TOKEN_TYPE,
    token_repo,
    ROLE_FIELD,
    USER_ID_FIELD,
)
from app.auth.models import TokenFilter, UserRole
import logging
from app.core.session_manager import SessionDep
from typing import List, Optional


logger = setup_logging(__name__)
logger.setLevel(logging.DEBUG)


async def validate_token_type(payload: dict, token_type: str) -> bool:
    current_token_type = payload.get(TOKEN_TYPE_FIELD)
    if current_token_type == token_type:
        return True
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"invalid token type {current_token_type!r} expected {token_type!r}",
    )


async def validate_token(
    token: str, token_type=ACCESS_TOKEN_TOKEN_TYPE, session=SessionDep
):
    try:
        if not token:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid token")
        payload = auth_utils.decode_jwt(token=token)

        await validate_token_type(payload=payload, token_type=token_type)

        db_token = await token_repo.find_one_or_none(
            session=session, filters=TokenFilter(token=token)
        )
        if not db_token or not db_token.is_active:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid token")

        return {
            "success": True,
            "error": None,
            "role": payload.get(ROLE_FIELD, UserRole.keywords),
            "payload": payload,
            "token": token
        }
    except Exception as e:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid token")


def require_roles(allowed_roles: List[str]):
    """Декоратор для проверки ролей пользователя"""

    def role_checker(token_data: dict = Depends(validate_token)):
        user_role = token_data.get("role", UserRole.keywords)
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required roles: {allowed_roles}, your role: {user_role}",
            )
        return token_data

    return role_checker


# Предопределенные проверки ролей для удобства
require_admin = require_roles([UserRole.admin])
require_operator = require_roles([UserRole.admin, UserRole.operator])
require_viewer = require_roles([UserRole.admin, UserRole.operator, UserRole.stats])
require_guest = require_roles([UserRole.admin, UserRole.operator, UserRole.stats, UserRole.keywords])


async def get_current_user_role(token_data: dict = Depends(validate_token)) -> str:
    """Получить роль текущего пользователя"""
    return token_data.get("role", UserRole.keywords)

