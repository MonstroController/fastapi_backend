from .crud import click_result_repository, ClickResultsRepository
from .schemas import ClickResult
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.base.base_service import BaseService
from app.profiles.utils import hours_to_dates
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
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

    async def grouping_stats_data(self, df: pd.DataFrame, grouping):
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
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='1h')).mean().reset_index()
            elif grouping == '2h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='2h')).mean().reset_index()
            elif grouping == '6h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='6h')).mean().reset_index()
            elif grouping == '12h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='12h')).mean().reset_index()
            elif grouping == '24h':
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='24h')).mean().reset_index()
            else:
                df_grouped = df_numeric.set_index('time').groupby(pd.Grouper(freq='1h')).mean().reset_index()
            
    
            df_grouped = df_grouped.dropna(subset=['pos'])
            
            if "ask" in df.columns and not df["ask"].isna().all():
                df_grouped["ask"] = df["ask"].iloc[0]
            return df_grouped, latest_pos
        else:
            return None, None
        

    async def get_clicks_stats(self, session: AsyncSession, copyname: str, period, grouping, ask, is_adding, is_all):
        data =  await self.repository.get_clicks_data(session=session, copyname=copyname, period=period, ask=ask)
        main_data = []
        total_count = len(data)
        addings_count = 0
        for el in data:
            add = el[3].split("|")[1]
            if add in el[2]:
                addings_count += 1
            elif not is_adding or is_all:
                main_data.append(el)
        
      
        df = pd.DataFrame(data, columns=["pos", "time", "ask", "fullask"])
        main_df = pd.DataFrame(main_data, columns=["pos", "time", "ask", "fullask"])
        df_grouped, latest_pos  = await self.grouping_stats_data(df, grouping=grouping)

        main_df_grouped, main_latest_pos = await self.grouping_stats_data(main_df, grouping=grouping)
        
        if (df_grouped is not None and not df_grouped.empty) or (main_df_grouped is not None and not main_df_grouped.empty):
            return {"df_grouped": df_grouped, "main_df_grouped": main_df_grouped, "latest_pos": latest_pos, "main_latest_pos": main_latest_pos, "addings_count": addings_count, "total_count": total_count}

        return {"df_grouped": df, "latest_pos": None, "addings_count": addings_count, "total_count": total_count}
    
    
    async def new_create_graphics(self, latest_pos, addings_count, total_count, df_grouped: pd.DataFrame = pd.DataFrame(), main_df_grouped: pd.DataFrame =  pd.DataFrame(),ask=None):
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
        
        if ask:
            plt.plot(df_grouped["time"], df_grouped["pos"], 'o-', color='#3a7ced', linewidth=1.5, markersize=5)
        # Рисуем график, если данные есть
        if df_grouped is not None and not df_grouped.empty:
            plt.plot(df_grouped["time"], df_grouped["pos"], 'o-', color='#3a7ced', linewidth=1.5, markersize=5, label="С брендовыми")
          
            
        if main_df_grouped is not None and not main_df_grouped.empty:
            plt.plot(main_df_grouped["time"], main_df_grouped["pos"], 'o-', color='#ff1414', linewidth=1.5, markersize=5, label="Без брендовыx")
        
        # Устанавливаем заголовок
        title = 'График по позициям запроса'
        if ask:
            title += f' - {ask}'
        else:
            title = 'График по позициям всех запросов'
        plt.title(title, fontsize=14)
        plt.legend()
        # Добавляем подписи осей и сетку
        plt.xlabel('Время', fontsize=12)
        plt.ylabel('Позиция', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Добавляем аннотацию с текущей позицией и дополнительной информацией
        if latest_pos is not None and (not df_grouped.empty or not main_df_grouped.empty):
            if df_grouped.empty:
                df_grouped = main_df_grouped
            # Вычисляем дополнительную информацию
            avg_pos = df_grouped["pos"].mean()

            
            # Формируем текст аннотации для позиций
            position_text = (
                f'Текущая позиция: {int(latest_pos)}\n'
                f'Средняя позиция: {avg_pos:.1f}\n'
            )
            
            # Добавляем аннотацию с позициями в левый нижний угол
            plt.figtext(0.02, 0.02, position_text,
                        fontsize=12,
                        bbox=dict(facecolor='#e8f7e8', edgecolor='#7ac47a', boxstyle='round,pad=0.5', alpha=0.7))
            
            # Формируем текст аннотации для количества запросов
            counts_text = (
                f'Брендовые запросы: {addings_count}\n'
                f'Всего запросов: {total_count}'
            )
            
            # Добавляем аннотацию с количеством запросов чуть правее
            plt.figtext(0.3, 0.02, counts_text,
                        fontsize=12,
                        bbox=dict(facecolor='#e8f7e8', edgecolor='#7ac47a', boxstyle='round,pad=0.5', alpha=0.7))
        
        # Увеличиваем пространство под графиком
        plt.subplots_adjust(bottom=0.3)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        return buf

click_result_service: ClickResultService = ClickResultService(
    repository=click_result_repository
)
