from fastapi import APIRouter, Depends, Query
from app.auth.helpers import create_jwt, ACCESS_TOKEN_TOKEN_TYPE, token_repo
from app.auth.models import TokenFilter
from app.core.session_manager import SessionDep, TransactionSessionDep
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.validation import validate_token


router = APIRouter(prefix="/tokens", tags=["Auth"], dependencies=[Depends(validate_token)])


@router.get("/generate-access")
async def generate_access_token(token: str = Query(), session: AsyncSession = TransactionSessionDep):
    token = create_jwt(token_type=ACCESS_TOKEN_TOKEN_TYPE)
    await token_repo.add(session=session, values=TokenFilter(token=token, is_active=True))
    return token




