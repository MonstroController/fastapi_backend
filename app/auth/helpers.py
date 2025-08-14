import app.auth.utils as auth_utils
from app.core.config import settings
from app.core.base.base_repository import BaseRepository
from app.auth.models import TokenOrm, UserRole
from datetime import timedelta
import logging
from app.utils.setup_logging import setup_logging
from fastapi import Depends
from redis.asyncio import Redis

logger = setup_logging(__name__)
TOKEN_TYPE_FIELD = "type"
ACCESS_TOKEN_TOKEN_TYPE = "access"
REFRESH_TOKEN_TOKEN_TYPE = "refresh"
ROLE_FIELD = "role"
USER_ID_FIELD = "user_id"
from uuid import UUID, uuid4


def create_jwt(
    token_type: str,
    token_data: dict | None = None,
    role: str = UserRole.keywords,
) -> dict:
    jwt_payload = {TOKEN_TYPE_FIELD: token_type}
    jwt_payload[ROLE_FIELD] = role
    jwt_payload.update(token_data) if token_data else None
    return auth_utils.encode_jwt(payload=jwt_payload)


class TokenRepository(BaseRepository):
    model = TokenOrm


token_repo = TokenRepository()
