from .crud import stats_repository, StatsRepository
from .schemas import Stats
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.base.base_service import BaseService
from app.profiles.utils import hours_to_dates
import io
import matplotlib.pyplot as plt


class StatsService(BaseService):
    def __init__(self, repository: StatsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def get_minutely_stats(self, session: AsyncSession, start_time, end_time, interval):
        return await self.repository.get_minutely_stats(session=session, start_time=start_time, end_time=end_time, interval=interval)
    
    async def create_graphics(self, df, interval):
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 8))
        axes = axes.flatten()
        for idx, action in enumerate(df["action_type"].unique()):
            subset = df[df['action_type'] == action]
            if not subset.empty:
                axes[idx].plot(
                    subset[interval], 
                    subset['total_rows'], 
                    label=action, 
                    color='blue'
                )
                axes[idx].set_title(f'{action}')
                axes[idx].set_xlabel('Время')
                axes[idx].set_ylabel('Затронутые строки')
                axes[idx].legend()
                axes[idx].grid(True)
                axes[idx].tick_params(axis='x', rotation=45)
            else:
                axes[idx].set_title(f'{action} (нет данных)')
                axes[idx].set_xlabel('Время')
                axes[idx].set_ylabel('Затронутые строки')
                axes[idx].grid(True)

     
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        return buf


stats_service: StatsService = StatsService(repository=stats_repository)
