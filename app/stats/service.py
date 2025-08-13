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
from typing import List, Dict, Optional, Tuple


class StatsService(BaseService):
    def __init__(self, repository: StatsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def group(self, df: pd.DataFrame, grouping, is_sum=True):
        if is_sum:
            df_grouped = (
                df.set_index("time")
                .groupby(pd.Grouper(freq=grouping))
                .sum()
                .reset_index()
            )
        else:
            df_grouped = (
                df.set_index("time")
                .groupby(pd.Grouper(freq=grouping))
                .mean()
                .reset_index()
            )
        return df_grouped

    async def grouping_stats_data(self, df: pd.DataFrame, grouping, is_sum=True):
        if not df.empty:
            # Вычисляем последнюю индивидуальную позицию
            latest_row = df.loc[df["time"].idxmax()]

            df_grouped = await self.group(df, grouping=grouping, is_sum=is_sum)

            df_grouped = df_grouped.dropna(subset=["count"])

            return df_grouped
        else:
            return None

    async def get_stats(
        self, session: AsyncSession, action_type: str, period, grouping, is_sum=True
    ):
        data = await self.repository.get_stats_data(
            session=session, action_type=action_type, period=period
        )
        df = pd.DataFrame(data, columns=["count", "time"])
        df_grouped = await self.grouping_stats_data(
            df, grouping=grouping, is_sum=is_sum
        )
        return df_grouped

    async def get_multiple_stats(
        self,
        session: AsyncSession,
        action_types: List[str],
        period: str,
        grouping: str,
        is_sum: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        Получает статистику для нескольких типов действий одновременно.

        Args:
            session: Сессия БД
            action_types: Список типов действий
            period: Период (1h, 12h, 24h, 3d, 7d, 30d, all)
            grouping: Группировка (10m, 30m, 1h, 2h, 6h, 12h, 24h)
            is_sum: Использовать сумму или среднее значение

        Returns:
            Словарь с данными для каждого типа действия
        """
        result = {}
        for action_type in action_types:
            df_grouped = await self.get_stats(
                session=session,
                action_type=action_type,
                period=period,
                grouping=grouping,
                is_sum=is_sum,
            )
            result[action_type] = df_grouped
        return result

    async def create_overlay_graphics(
        self,
        stats_data: Dict[str, pd.DataFrame],
        period: str = "24h",
        title: str = "Сравнительная статистика",
        colors: Optional[List[str]] = None,
        show_legend: bool = True,
        y_label: str = "Количество",
        normalize: bool = False,
    ):
        """
        Создает график с наложением нескольких типов статистики.

        Args:
            stats_data: Словарь с данными статистики {action_type: DataFrame}
            period: Период для заголовка
            title: Заголовок графика
            colors: Список цветов для линий
            show_legend: Показывать легенду
            y_label: Подпись оси Y
            normalize: Нормализовать данные (0-100%)
        """
        plt.figure(figsize=(14, 8))
        if colors is None:
            colors = [
                "#3a7ced",
                "#ff6b6b",
                "#4ecdc4",
                "#45b7d1",
                "#96ceb4",
                "#feca57",
                "#ff9ff3",
                "#54a0ff",
            ]

        all_times = []
        for df in stats_data.values():
            if df is not None and not df.empty:
                all_times.extend(df["time"].tolist())

        if not all_times:
            raise ValueError("Нет данных для отображения")

        min_time = min(all_times)
        max_time = max(all_times)

        max_count = 0
        for df in stats_data.values():
            if df is not None and not df.empty:
                if normalize:
                    df["count_normalized"] = (df["count"] / df["count"].max()) * 100
                    max_count = max(max_count, 100)
                else:
                    max_count = max(max_count, df["count"].max())

        # Настройка осей
        if max_count <= 100:
            num_ticks = 10
        elif max_count <= 1000:
            num_ticks = 8
        elif max_count <= 10000:
            num_ticks = 6
        else:
            num_ticks = 5

        y_ticks = np.linspace(0, max_count, num_ticks)
        plt.yticks(y_ticks, [f"{int(x):,}" for x in y_ticks])
        plt.ylim(0, max_count * 1.1)

        # Настройка оси X
        plt.xlim(min_time, max_time)
        plt.gcf().autofmt_xdate()

        date_range = (max_time - min_time).days
        if date_range > 60:
            date_format = mdates.DateFormatter("%m.%Y")
        elif date_range > 5:
            date_format = mdates.DateFormatter("%d.%m")
        else:
            date_format = mdates.DateFormatter("%d.%m %H:%M")
        plt.gca().xaxis.set_major_formatter(date_format)

        legend_labels = []
        for i, (action_type, df) in enumerate(stats_data.items()):
            if df is not None and not df.empty:
                color = colors[i % len(colors)]
                y_data = df["count_normalized"] if normalize else df["count"]
                plt.plot(
                    df["time"],
                    y_data,
                    "o-",
                    color=color,
                    linewidth=2,
                    markersize=4,
                    label=action_type,
                )
                legend_labels.append(action_type)

        # Настройка графика
        plt.xlabel("Время", fontsize=12)
        plt.ylabel(y_label, fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.title(title, fontsize=14)

        if show_legend and legend_labels:
            plt.legend(legend_labels, loc="upper right", fontsize=10)

        plt.tight_layout()

        # Сохранение
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100, bbox_inches="tight")
        buf.seek(0)
        plt.close()
        return buf

    async def create_comparison_graphics(
        self,
        session: AsyncSession,
        action_types: List[str],
        period: str = "24h",
        grouping: str = "1h",
        title: str = "Сравнительная статистика",
        colors: Optional[List[str]] = None,
        normalize: bool = False,
        is_sum: bool = True,
    ):
        """
        Универсальная функция для создания сравнительных графиков.

        Args:
            session: Сессия БД
            action_types: Список типов действий для сравнения
            period: Период данных
            grouping: Группировка данных
            title: Заголовок графика
            colors: Цвета для линий
            normalize: Нормализовать данные
            is_sum: Использовать сумму или среднее
        """
        # Получаем данные для всех типов
        stats_data = await self.get_multiple_stats(
            session=session,
            action_types=action_types,
            period=period,
            grouping=grouping,
            is_sum=is_sum,
        )

        # Создаем график
        return await self.create_overlay_graphics(
            stats_data=stats_data,
            period=period,
            title=title,
            colors=colors,
            normalize=normalize,
        )

    async def create_graphics(
        self,
        df_grouped: pd.DataFrame = pd.DataFrame(),
        period: str = "24h",
        total_count: int = 0,
    ):
        """Создает улучшенную визуализацию статистики количества с усреднением."""

        plt.figure(figsize=(14, 8))
        max_count = (
            max(np.ceil(df_grouped["count"].max()), 10) if not df_grouped.empty else 10
        )

        if max_count <= 100:
            num_ticks = 10
        elif max_count <= 1000:
            num_ticks = 8
        elif max_count <= 10000:
            num_ticks = 6
        else:
            num_ticks = 5

        y_ticks = np.linspace(0, max_count, num_ticks)
        plt.yticks(y_ticks, [f"{int(x):,}" for x in y_ticks])

        plt.ylim(0, max_count * 1.1)

        plt.gcf().autofmt_xdate()
        date_range = (
            (df_grouped["time"].max() - df_grouped["time"].min()).days
            if not df_grouped.empty
            else 0
        )
        if date_range > 60:
            date_format = mdates.DateFormatter("%m.%Y")
        elif date_range > 5:
            date_format = mdates.DateFormatter("%d.%m")
        else:
            date_format = mdates.DateFormatter("%d.%m %H:%M")
        plt.gca().xaxis.set_major_formatter(date_format)

        # Рисуем график, если данные есть
        if df_grouped is not None and not df_grouped.empty:
            plt.plot(
                df_grouped["time"],
                df_grouped["count"],
                "o-",
                color="#3a7ced",
                linewidth=1.5,
                markersize=5,
            )

        plt.xlabel("Время", fontsize=12)
        plt.ylabel("Количество", fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.title(f"Статистика количества за {period}", fontsize=14)
        if total_count:
            plt.text(
                0.5,
                0.95,
                f"Всего: {total_count:,}",
                ha="center",
                va="center",
                transform=plt.gca().transAxes,
                fontsize=12,
            )

        plt.subplots_adjust(bottom=0.3)

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100)
        buf.seek(0)
        plt.close()
        return buf


stats_service: StatsService = StatsService(repository=stats_repository)
