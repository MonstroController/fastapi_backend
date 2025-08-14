from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.validation import validate_token, require_admin, require_guest
from .crud import *
from .service import keywords_service
from app.core.session_manager import SessionDep
import random

router = APIRouter(
    prefix="/keywords", tags=["Keywords"], dependencies=[Depends(require_guest)]
)


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


@router.get("/mixed")
async def get_mixed_keywords(
    include_default: bool = Query(True, description="Включить default ключевые слова"),
    include_video: bool = Query(True, description="Включить video ключевые слова"),
    include_google: bool = Query(True, description="Включить google ключевые слова"),
    include_mail: bool = Query(True, description="Включить mail ключевые слова"),
    session: AsyncSession = SessionDep,
):
    """
    Получает 1 ключевое слово из выбранных групп со случайным равномерным распределением

    Параметры:
    - include_default: включить default ключевые слова (случайная длина 4-7)
    - include_video: включить video ключевые слова
    - include_google: включить google ключевые слова
    - include_mail: включить mail ключевые слова

    Алгоритм: случайный выбор группы с равными шансами
    - 4 группы = 25% шанс для каждой
    - 2 группы = 50% шанс для каждой
    - 1 группа = 100% шанс

    Примеры:
    - /keywords/mixed?include_default=true&include_video=false
    - /keywords/mixed?include_google=true&include_mail=true
    - /keywords/mixed (все группы)
    """
    res = await keywords_service.get_mixed_keywords(
        session=session,
        include_default=include_default,
        include_video=include_video,
        include_google=include_google,
        include_mail=include_mail,
    )
    return HTMLResponse(res)
