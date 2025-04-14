from app.core.session_manager import session_manager
from app.profiles.service import profiles_service


@session_manager.connection(commit=True)
async def update_working_party_schedule(session):
    await profiles_service.check_working_party_for_update(session=session)


@session_manager.connection(commit=True)
async def clean_working_party_schedule(session):
    await profiles_service.from_working_party_to_trash_party(session=session)


@session_manager.connection(commit=True)
async def clean_all_parties_overtime_schedule(session):
    await profiles_service.clean_to_overtime_party(session)


@session_manager.connection(commit=True)
async def delete_trash_and_overtime(session):
    await profiles_service.delete_trash_and_overtime(session)
