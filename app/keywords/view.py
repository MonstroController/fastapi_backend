from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from .crud import *
from .service import keywords_service
from app.core.session_manager import SessionDep
import random

router = APIRouter(prefix="/keywords", tags=["Keywords"])


@router.get("/video")
async def rand_video_keyword(pid, session: AsyncSession = SessionDep):
    res = await keywords_service.get_random_video_keyword(pid=pid, session=session)
    return HTMLResponse(res)


@router.get("/default")
async def rand_default_keyword(
    pid, min: int = 4, max: int = 7, session: AsyncSession = SessionDep
):
    res = await keywords_service.get_random_default_keyword(
        pid=pid, min=min, max=max, session=session
    )
    return HTMLResponse(res)
