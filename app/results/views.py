from fastapi import APIRouter, Depends, Path, Body, Query
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
