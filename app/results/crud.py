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
    
    async def get_clicks_stats(self, session: AsyncSession, copyname: str, period=None, grouping='1h', ask: str | None = None):
        """Получает статистику с возможностью фильтрации по периоду и группировки."""
        
        query = (
            select(
                ClickResultsOrm.pos,
                ClickResultsOrm.data_create,
                ClickResultsOrm.fullask
            ).where(ClickResultsOrm.copyname == copyname)
        )
        
        # Фильтрация по запросу (ask), если указан
        if ask:
            query = query.where(ClickResultsOrm.fullask == ask)
        
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
        
        df = pd.DataFrame(data, columns=["pos", "time", "ask"])
        print(df)
        if not df.empty:
            # Вычисляем последнюю индивидуальную позицию    
            latest_row = df.loc[df["time"].idxmax()]
            latest_pos = latest_row["pos"] + 1  # Корректируем с 0 на 1
            
            # Корректируем все позиции для группировки
            df["pos"] = df["pos"] + 1
            
            # Группировка данных
            df_numeric = df[["time", "pos"]].copy()
            if grouping == '10m':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='10Min')).mean().reset_index()
            elif grouping == '30m':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='30Min')).mean().reset_index()
            elif grouping == '1h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='1H')).mean().reset_index()
            elif grouping == '2h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='2H')).mean().reset_index()
            elif grouping == '6h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='6H')).mean().reset_index()
            elif grouping == '12h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='12H')).mean().reset_index()
            elif grouping == '24h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='24H')).mean().reset_index()
            else:
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='1H')).mean().reset_index()
            
    
            df_grouped = df_grouped.dropna(subset=['pos'])
            
            if "ask" in df.columns and not df["ask"].isna().all():
                df_grouped["ask"] = df["ask"].iloc[0]
            
            return {"df_grouped": df_grouped, "latest_pos": latest_pos}
        else:
            return {"df_grouped": df, "latest_pos": None}



click_result_repository: ClickResultsRepository = ClickResultsRepository()
