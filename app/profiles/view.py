from fastapi import APIRouter, Depends, HTTPException, status
from .schemas import ProfileRead, ProfileFilters, SelectionParameters
from .service import profiles_service
from app.core.session_manager import SessionDep
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.validation import validate_token


router = APIRouter(prefix="/profiles", tags=["Profiles"], dependencies=[Depends(validate_token)])


@router.get("/{pid}")
async def get_profile_by_pid(pid, session: AsyncSession = SessionDep) -> ProfileRead:
    profile = await profiles_service.find_one_or_none_by_pid(session=session, data_pid=int(pid))
    if not profile:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return profile


@router.get("/party/{party}", response_model=list[ProfileRead])
async def get_profiles_by_party(
    party, session: AsyncSession = SessionDep
) -> list[ProfileRead]:
    return await profiles_service.find_all(
        session=session, filters=ProfileFilters(party=party)
    )
