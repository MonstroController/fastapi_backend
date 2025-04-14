from app.core.base.base_repository import BaseRepository
from .model import StatsOrm
import logging
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, delete, select, func


logger = logging.getLogger(__name__)


class StatsRepository(BaseRepository):
    model = StatsOrm

    async def get_minutely_stats(self, session: AsyncSession):
        """Получает почасовую статистику из таблицы stats."""
        query = (
            select(
                func.date_trunc("minute", StatsOrm.operation_timestamp).label("minute"),
                StatsOrm.action_type,
                func.sum(StatsOrm.affected_rows).label("total_rows"),
            )
            .group_by(
                "minute",
                StatsOrm.action_type,
            )
            .order_by("minute")
        )
        result = await session.execute(query)
        data = result.fetchall()
      
        df = pd.DataFrame(data, columns=["minute", "action_type", "total_rows"])
        return df


stats_repository: StatsRepository = StatsRepository()
