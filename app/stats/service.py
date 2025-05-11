from .crud import stats_repository, StatsRepository
from .schemas import Stats
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.base.base_service import BaseService
from app.profiles.utils import hours_to_dates
import io
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates
import numpy as np

class StatsService(BaseService):
    def __init__(self, repository: StatsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)


    async def group(self, df: pd.DataFrame, grouping, is_sum=True):
        if is_sum:
            df_grouped = df.set_index('time').groupby(pd.Grouper(freq=grouping)).sum().reset_index()
        else:
            df_grouped = df.set_index('time').groupby(pd.Grouper(freq=grouping)).mean().reset_index()
        return df_grouped
    
    async def grouping_stats_data(self, df: pd.DataFrame, grouping, is_sum=True):
        if not df.empty:
            # Вычисляем последнюю индивидуальную позицию    
            latest_row = df.loc[df["time"].idxmax()]
          
            if grouping == '10m':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '30m':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '1h':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '2h':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '6h':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '12h':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            elif grouping == '24h':
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            else:
                df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)
            
 
            df_grouped = df_grouped.dropna(subset=['count'])
            
            return df_grouped
        else:
            return None
    
    async def get_stats(self, session: AsyncSession, action_type: str, period, grouping, is_sum=True):
        data =  await self.repository.get_stats_data(session=session, action_type=action_type, period=period)
        df = pd.DataFrame(data, columns=["count", "time"])
        df_grouped = await self.grouping_stats_data(df, grouping=grouping, is_sum=is_sum)
        return df_grouped
    
    async def new_create_graphics(self, df_grouped: pd.DataFrame = pd.DataFrame(), period: str = "24h",  total_count: int = 0):
        """Создает улучшенную визуализацию статистики количества с усреднением."""
        
        plt.figure(figsize=(14, 8))
        max_count = max(np.ceil(df_grouped["count"].max()), 10) if not df_grouped.empty else 10
    
        if max_count <= 100:
            num_ticks = 10  
        elif max_count <= 1000:
            num_ticks = 8   
        elif max_count <= 10000:
            num_ticks = 6 
        else:
            num_ticks = 5 

        y_ticks = np.linspace(0, max_count, num_ticks)
        plt.yticks(y_ticks, [f'{int(x):,}' for x in y_ticks])  
   
        plt.ylim(0, max_count * 1.1)  

        plt.gcf().autofmt_xdate()
        date_range = (df_grouped["time"].max() - df_grouped["time"].min()).days if not df_grouped.empty else 0
        if date_range > 60:
            date_format = mdates.DateFormatter('%m.%Y')
        elif date_range > 5:
            date_format = mdates.DateFormatter('%d.%m')
        else:
            date_format = mdates.DateFormatter('%d.%m %H:%M')
        plt.gca().xaxis.set_major_formatter(date_format)
        
        # Рисуем график, если данные есть
        if df_grouped is not None and not df_grouped.empty:
            plt.plot(df_grouped["time"], df_grouped["count"], 'o-', color='#3a7ced', linewidth=1.5, markersize=5)
        
     
        
        plt.xlabel('Время', fontsize=12)
        plt.ylabel('Количество', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.title(f'Статистика количества за {period}', fontsize=14)
        if total_count:
            plt.text(0.5, 0.95, f'Всего: {total_count:,}', ha='center', va='center', transform=plt.gca().transAxes, fontsize=12)
    
        plt.subplots_adjust(bottom=0.3)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        return buf


stats_service: StatsService = StatsService(repository=stats_repository)
