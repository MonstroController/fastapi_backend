from fastapi import APIRouter, Depends, Path, Body, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Annotated
from .service import stats_service
from .schemas import Stats, StatsFilter

from app.core.session_manager import SessionDep, TransactionSessionDep
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/stats", tags=["Results"])


@router.get("/stats")
async def get_minutely_stats(
    session: AsyncSession = SessionDep,
    start_time: str = None,
    end_time: str = None,
    interval: str = "hour",
):
    data = await stats_service.get_minutely_stats(
        session=session, start_time=start_time, end_time=end_time, interval=interval
    )
    if data.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Нет данных для отображения"
        )

    graphics = await stats_service.create_graphics(df=data, interval=interval)
    return StreamingResponse(graphics, media_type="image/png")


@router.get("/stats/{action_type}")
async def get_stats(
    session: AsyncSession = SessionDep,
    action_type: str = None,
    period: str | None = Query(
        "24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"
    ),
):
    total_count = 0
    if action_type in [
        "trash_party_check",
        "overtime_party_check",
        "working_party_check",
    ]:
        data = await stats_service.get_stats(
            session=session,
            action_type=action_type,
            period=period,
            grouping=grouping,
            is_sum=False,
        )

    else:
        data = await stats_service.get_stats(
            session=session, action_type=action_type, period=period, grouping=grouping
        )
        if data is not None and not data.empty:
            total_count = data["count"].sum()
    if data is not None and not data.empty:
        graphics = await stats_service.new_create_graphics(
            df_grouped=data, total_count=total_count, period=period
        )
        return StreamingResponse(graphics, media_type="image/png")
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Нет данных для отображения"
        )
