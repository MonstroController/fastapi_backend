from app.core.base.base_repository import BaseRepository
from .model import ClickResultsOrm
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, select


logger = logging.getLogger(__name__)

class ClickResultsRepository(BaseRepository):
    model = ClickResultsOrm



    async def get_overtime_results(self, session: AsyncSession, min_date):
        """Находит результаты которые старше min_date"""
        query = select(ClickResultsOrm.pid).where(and_(ClickResultsOrm.data_create <= min_date))
        res = await session.execute(query)
        res = res.scalars().all()
        logger.info(f"Get result capacity for cleaning to s_>72: {len(res)}")
        return res


click_result_repository: ClickResultsRepository = ClickResultsRepository()