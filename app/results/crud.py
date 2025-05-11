from app.core.base.base_repository import BaseRepository
from .model import ClickResultsOrm
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, delete, select
import pandas as pd
from datetime import datetime, timedelta



logger = logging.getLogger(__name__)


class ClickResultsRepository(BaseRepository):
    model = ClickResultsOrm

    async def delete_overtime_results(self, session: AsyncSession, min_date):
        """Удаляет результаты которые старше min_date"""

        query = delete(ClickResultsOrm).where(
            and_(ClickResultsOrm.data_create <= min_date)
        )
        res = await session.execute(query)
        res = res.rowcount
        logger.info(f"Delete results: {res}")
        return res
        

    async def get_clicks_data(self, session: AsyncSession, copyname: str, period=None, ask: str | None = None):
        """Получает статистику с возможностью фильтрации по периоду и группировки."""
        
        query = (
            select(
                ClickResultsOrm.pos,
                ClickResultsOrm.data_create,
                ClickResultsOrm.keyword,
                ClickResultsOrm.fullask
            ).where(ClickResultsOrm.copyname == copyname)
        )
        logger.info(f"Get clicks data: {ask}")
        # Фильтрация по запросу (ask), если указан
        if ask:
            query = query.where(ClickResultsOrm.keyword == ask)
        
        # Фильтрация по периоду
        if period and period != "all":
            end_date = datetime.now()
            if period == "1h":
                start_date = end_date - timedelta(hours=1)
            elif period == "12h":
                start_date = end_date - timedelta(hours=12)
            elif period == "24h":
                start_date = end_date - timedelta(hours=24)
            elif period == "3d":
                start_date = end_date - timedelta(days=3)
            elif period == "7d":
                start_date = end_date - timedelta(days=7)
            elif period == "30d":
                start_date = end_date - timedelta(days=30)
            else:
                start_date = end_date - timedelta(hours=24)  # По умолчанию 24 часа
    
            query = query.where(ClickResultsOrm.data_create >= start_date)
        
        query = query.order_by(ClickResultsOrm.data_create)
        result = await session.execute(query)
        data = result.fetchall()
        logger.info(f"Get clicks data: {len(data)}")
        return data
        



click_result_repository: ClickResultsRepository = ClickResultsRepository()
