from app.core.session_manager import session_manager
from app.results.service import click_result_service


@session_manager.connection(commit=True)
async def delete_overtime_results(session):
    await click_result_service.delete_overtime(session=session)
