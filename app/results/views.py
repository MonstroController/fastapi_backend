from fastapi import APIRouter, Depends, Path, Body, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Annotated
from .service import click_result_service
from .schemas import ClickResult, ClickResultFilter
from app.dependencies.results import generate_click_result

from app.core.session_manager import SessionDep, TransactionSessionDep
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/results", tags=["Results"])


@router.get("/upload")
async def get_info_from_monstro(
    click_result=Depends(generate_click_result),
    session: AsyncSession = TransactionSessionDep,
):
    return await click_result_service.add(session=session, values=click_result)


@router.get("/count")
async def get_results(session: AsyncSession = SessionDep):
    return await click_result_service.count(session=session)


@router.get("/keyword/{keyword}", response_model=list[ClickResult])
async def get_result_by_keyword(
    keyword: str, session: AsyncSession = SessionDep
) -> list[ClickResult]:
    return await click_result_service.find_all(
        session=session, filters=ClickResultFilter(keyword=keyword)
    )


@router.get("/{click_result_pid}", response_model=ClickResult)
async def get_result_by_pid(
    click_result_pid: int, session: AsyncSession = SessionDep
) -> ClickResult:
    return await click_result_service.find_one_or_none_by_pid(
        session=session, data_pid=click_result_pid
    )


@router.post("", response_model=list[ClickResult])
async def get_result_by_filter(
    filter: Annotated[ClickResultFilter, Body], session: AsyncSession = SessionDep
) -> list[ClickResult]:
    return await click_result_service.find_all(session=session, filters=filter)


@router.get("/domain/{domain}", response_model=list[ClickResult])
async def get_result_by_keyword(
    domain, session: AsyncSession = SessionDep
) -> list[ClickResult]:
    return await click_result_service.find_all(
        session=session, filters=ClickResultFilter(domain=domain)
    )


@router.get("/party/{party}", response_model=list[ClickResult])
async def get_result_by_party(
    party, session: AsyncSession = SessionDep
) -> list[ClickResult]:
    return click_result_service.find_all(
        session=session, filters=ClickResultFilter(party=party)
    )


@router.get("/profile/{profile_id}", response_model=list[ClickResult])
async def get_result_by_profile_id(
    profile_id, session: AsyncSession = SessionDep
) -> list[ClickResult]:
    return click_result_service.find_all(
        session=session, filters=ClickResultFilter(profile_id=profile_id)
    )   

@router.get("/stats/{copyname}")
async def get_position_stats(
    copyname: str,
    period: str | None = Query("24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"),
    grouping: str | None = Query("1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"),
    ask: str | None = Query(None, description="Filter by specific search query"),
    is_adding: bool = True,
    is_all: bool = False,
    session: AsyncSession = SessionDep
):
    result = await click_result_service.get_clicks_stats(
        session=session,
        copyname=copyname,
        period=period,
        grouping=grouping,
        ask=ask,
        is_adding=is_adding,
        is_all=is_all
    )
    df_grouped = result["df_grouped"]
    latest_pos = result["latest_pos"]
    addings_count = result["addings_count"]
    total_count = result["total_count"]

    if df_grouped.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Нет данных для отображения")
    
    main_df_grouped = result["main_df_grouped"]
    if is_all:
        graphics = await click_result_service.new_create_graphics(df_grouped=df_grouped, main_df_grouped=main_df_grouped, latest_pos=latest_pos, addings_count=addings_count, total_count=total_count, ask=ask)
    elif not is_adding:
        graphics = await click_result_service.new_create_graphics(main_df_grouped=main_df_grouped, latest_pos=latest_pos, addings_count=addings_count, total_count=total_count, ask=ask)
    else:
        graphics = await click_result_service.new_create_graphics(df_grouped=df_grouped, latest_pos=latest_pos, addings_count=addings_count, total_count=total_count, ask=ask)
    return StreamingResponse(graphics, media_type="image/png")