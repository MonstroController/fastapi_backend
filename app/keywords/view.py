from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.validation import validate_token
from .crud import *
from .service import keywords_service
from app.core.session_manager import SessionDep
import random

router = APIRouter(prefix="/keywords", tags=["Keywords"], dependencies=[Depends(validate_token)])


@router.get("/video")
async def rand_video_keyword(session: AsyncSession = SessionDep):
    res = await keywords_service.get_random_video_keyword(session=session)
    return HTMLResponse(res)


@router.get("/default")
async def rand_default_keyword(
    min: int = 4, max: int = 7, session: AsyncSession = SessionDep
):
    res = await keywords_service.get_random_default_keyword(
        min=min, max=max, session=session
    )
    return HTMLResponse(res)

@router.get("/mail")
async def rand_mail_keyword(session: AsyncSession = SessionDep):
    res = await keywords_service.get_random_mail_keyword(session=session)
    return HTMLResponse(res)

@router.get("/google")
async def rand_google_keyword(session: AsyncSession = SessionDep):
    res = await keywords_service.get_random_google_keyword(session=session)
    return HTMLResponse(res)