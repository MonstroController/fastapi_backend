from fastapi import APIRouter, Depends
from .schemas import ProfileRead, ProfileFilters, SelectionParameters
from .service import profiles_service
from app.core.session_manager import SessionDep
from sqlalchemy.ext.asyncio import AsyncSession


router = APIRouter(prefix="/profiles", tags=["Profiles"])


@router.get("/{pid}", response_model=ProfileRead)
async def get_profile_by_pid(pid, session: AsyncSession = SessionDep) -> ProfileRead:
    return await profiles_service.find_one_or_none_by_id(session=session, pid=pid)


@router.get("/party/{party}", response_model=list[ProfileRead])
async def get_profiles_by_party(
    party, session: AsyncSession = SessionDep
) -> list[ProfileRead]:
    return await profiles_service.find_all(
        session=session, filters=ProfileFilters(party=party)
    )
