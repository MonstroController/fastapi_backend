from app.core.base.base_repository import BaseRepository
from .model import ClickResultsOrm
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, delete


logger = logging.getLogger(__name__)


class ClickResultsRepository(BaseRepository):
    model = ClickResultsOrm

    async def delete_overtime_results(self, session: AsyncSession, min_date):
        """Удаляет результаты которые старше min_date"""
        print(min_date)
        query = delete(ClickResultsOrm.pid).where(
            and_(ClickResultsOrm.data_create <= min_date)
        )
        res = await session.execute(query)
        res = res.rowcount
        logger.info(f"Delete results: {res}")
        return res


click_result_repository: ClickResultsRepository = ClickResultsRepository()
