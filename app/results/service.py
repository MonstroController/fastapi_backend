from .crud import click_result_repository, ClickResultsRepository
from .schemas import ClickResult
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.base.base_service import BaseService
from app.profiles.utils import hours_to_dates


class ClickResultService(BaseService):
    def __init__(self, repository: ClickResultsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def delete_overtime(self, session: AsyncSession):
        min_date = hours_to_dates(max_hours_life=7 * 24)
        count = await self.repository.delete_overtime_results(
            session=session, min_date=min_date
        )
        return count


click_result_service: ClickResultService = ClickResultService(
    repository=click_result_repository
)
