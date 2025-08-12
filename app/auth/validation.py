
from app.core.config import settings
from app.core.session_manager import SessionDep
from fastapi import HTTPException, status, Request
import app.auth.utils as auth_utils
from datetime import datetime, timezone
from app.utils.setup_logging import setup_logging
from app.auth.helpers import TOKEN_TYPE_FIELD, ACCESS_TOKEN_TOKEN_TYPE, token_repo
from app.auth.models import TokenFilter
import logging
from app.core.session_manager import SessionDep


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


async def validate_token(token: str, token_type=ACCESS_TOKEN_TOKEN_TYPE, session=SessionDep):
    try:
        if not token:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid token")
        payload = auth_utils.decode_jwt(token=token)
 
        await validate_token_type(payload=payload, token_type=token_type)
        if token_repo.find_one_or_none(session=session, filters=TokenFilter(token=token)):
            return {"success": True, "error": None}
        raise HTTPException(status_code=403, detail="Forbidden: Invalid token")
    except Exception as e:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid token")