from .crud import click_result_repository, ClickResultsRepository
from .schemas import ClickResult
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.base.base_service import BaseService
from app.profiles.utils import hours_to_dates
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import io



class ClickResultService(BaseService):
    def __init__(self, repository: ClickResultsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def delete_overtime(self, session: AsyncSession):
        min_date = hours_to_dates(max_hours_life=7 * 24)
        await self.repository.delete_overtime_results(
            session=session, min_date=min_date
        )

    async def get_clicks_stats(self, session: AsyncSession, copyname: str, period, grouping, ask):
        return await self.repository.get_clicks_stats(session=session, copyname=copyname, period=period, grouping=grouping, ask=ask)
    
    
    async def create_graphics(self, df_grouped, latest_pos, ask=None):
        """Создает улучшенную визуализацию статистики позиций с усреднением."""
        
        plt.figure(figsize=(14, 8))
        
        # Устанавливаем пределы Y-оси: меньшие значения (1) наверху, большие (max_pos) внизу
        max_pos = max(np.ceil(df_grouped["pos"].max()), 10) if not df_grouped.empty else 10
        plt.ylim(max_pos, 0.5)  # Устанавливаем диапазон от max_pos (снизу) до 0.5 (сверху)
        
        # Устанавливаем метки Y-оси от 1 до max_pos
        plt.yticks(range(1, int(max_pos) + 1))
        
        # Настраиваем форматирование дат на X-оси
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
        if not df_grouped.empty:
            plt.plot(df_grouped["time"], df_grouped["pos"], 'o-', color='#3a7ced', linewidth=1.5, markersize=5)
        
        # Устанавливаем заголовок
        title = 'График по позициям запроса'
        if ask:
            title += f' - {ask}'
        elif not df_grouped.empty and "ask" in df_grouped.columns and not df_grouped["ask"].isna().all():
            title += f' - {df_grouped["ask"].iloc[0]}'
        plt.title(title, fontsize=14)
        
        # Добавляем подписи осей и сетку
        plt.xlabel('Время', fontsize=12)
        plt.ylabel('Позиция', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Добавляем аннотацию текущей позиции
        if latest_pos is not None:
            plt.figtext(0.02, 0.02, f'Текущая позиция: {int(latest_pos)}',
                        fontsize=12,
                        bbox=dict(facecolor='#e8f7e8', edgecolor='#7ac47a', boxstyle='round,pad=0.5', alpha=0.7))
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        return buf
     


click_result_service: ClickResultService = ClickResultService(
    repository=click_result_repository
)
