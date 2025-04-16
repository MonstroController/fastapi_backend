from app.core.base.base_repository import BaseRepository
from fastapi import HTTPException
from .model import StatsOrm
import logging
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, delete, select, func
import datetime


logger = logging.getLogger(__name__)


class StatsRepository(BaseRepository):
    model = StatsOrm

    async def get_minutely_stats(self, session: AsyncSession, start_time, end_time, interval: str = "hour"):
        """Получает почасовую статистику из таблицы stats."""
        if not start_time or not end_time:
            end_time_dt = datetime.datetime.now()
            start_time_dt = end_time_dt - datetime.timedelta(hours=3)

        else:
            try:
                start_time_dt = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                end_time_dt = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise HTTPException(status_code=400, detail="Неверный формат времени. Используйте YYYY-MM-DD HH:MM:SS")
            
        query = (
            select(
                func.date_trunc(interval, StatsOrm.operation_timestamp).label(interval),
                StatsOrm.action_type,
                func.sum(StatsOrm.affected_rows).label("total_rows"),
            ).where(
                StatsOrm.operation_timestamp >= start_time_dt,
                StatsOrm.operation_timestamp <= end_time_dt
            )
            .group_by(
                interval,
                StatsOrm.action_type,
            )
            .order_by(interval)
        )
        result = await session.execute(query)
        data = result.fetchall()
      
        df = pd.DataFrame(data, columns=[interval, "action_type", "total_rows"])
        return df


stats_repository: StatsRepository = StatsRepository()
