from fastapi import APIRouter, Depends, Path, Body, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Annotated
from .service import stats_service
from .schemas import Stats, StatsFilter

from app.core.session_manager import SessionDep, TransactionSessionDep
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/stats", tags=["Results"])


@router.get("/stats")
async def get_minutely_stats(session: AsyncSession = SessionDep, start_time: str = None, end_time: str = None, interval: str = "hour"):
    data = await stats_service.get_minutely_stats(session=session, start_time=start_time, end_time=end_time, interval=interval)
    if data.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Нет данных для отображения")
    
    graphics = await stats_service.create_graphics(df=data, interval=interval)
    return StreamingResponse(graphics, media_type="image/png")