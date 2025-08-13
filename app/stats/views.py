from fastapi import APIRouter, Depends, Path, Body, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Annotated, List
from .service import stats_service
from .schemas import Stats, StatsFilter

from app.core.session_manager import SessionDep, TransactionSessionDep
from app.auth.validation import validate_token
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get("/stats/{action_type}")
async def get_stats(
    session: AsyncSession = SessionDep,
    action_type: str = None,
    period: str | None = Query(
        "24h", description="Filter period for example: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data for example: 1min, 5min, 10min, 30min, 1h, 2h, 6h, 12h, 24h"
    ),
):
    total_count = 0
    if action_type in [
        "trash_party_check",
        "overtime_party_check",
        "working_party_check",
    ]:
        data = await stats_service.get_stats(
            session=session,
            action_type=action_type,
            period=period,
            grouping=grouping,
            is_sum=False,
        )

    else:
        data = await stats_service.get_stats(
            session=session, action_type=action_type, period=period, grouping=grouping
        )
        if data is not None and not data.empty:
            total_count = data["count"].sum()
    if data is not None and not data.empty:
        graphics = await stats_service.create_graphics(
            df_grouped=data, total_count=total_count, period=period
        )
        return StreamingResponse(graphics, media_type="image/png")
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Нет данных для отображения"
        )


@router.get("/stats/compare/overlay")
async def get_overlay_stats(
    session: AsyncSession = SessionDep,
    action_types: str = Query(..., description="Comma-separated action types"),
    period: str | None = Query(
        "24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"
    ),
    title: str | None = Query("Сравнительная статистика", description="Graph title"),
    normalize: bool = Query(False, description="Normalize data to 0-100%"),
    is_sum: bool = Query(True, description="Use sum or average for grouping"),
):
    """
    Создает график с наложением нескольких типов статистики.

    Примеры использования:
    - /stats/compare/overlay?action_types=working_party_check,trash_party_check,overtime_party_check
    - /stats/compare/overlay?action_types=to_mail,to_google&period=7d&normalize=true
    - /stats/compare/overlay?action_types=to_working,to_trash,to_overtime&grouping=2h
    """
    try:
        # Парсим типы действий
        action_types_list = [at.strip() for at in action_types.split(",")]

        # Проверяем, что все типы валидны
        valid_types = [
            "working_party_check",
            "trash_party_check",
            "overtime_party_check",
            "to_working",
            "to_trash",
            "to_overtime",
            "deleted",
            "to_mail",
            "to_google",
            "from_google",
            "from_mail",
        ]

        for action_type in action_types_list:
            if action_type not in valid_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Неизвестный тип действия: {action_type}. Допустимые типы: {', '.join(valid_types)}",
                )

        # Создаем график
        graphics = await stats_service.create_comparison_graphics(
            session=session,
            action_types=action_types_list,
            period=period,
            grouping=grouping,
            title=title,
            normalize=normalize,
            is_sum=is_sum,
        )

        return StreamingResponse(graphics, media_type="image/png")

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Нет данных для отображения: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при создании графика: {str(e)}",
        )


@router.get("/stats/compare/profiles")
async def get_profiles_comparison(
    session: AsyncSession = SessionDep,
    period: str | None = Query(
        "24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"
    ),
    normalize: bool = Query(False, description="Normalize data to 0-100%"),
):
    """
    Специализированный эндпоинт для сравнения статистики профилей.
    Показывает количество профилей в разных группах.
    """
    action_types = ["working_party_check", "trash_party_check", "overtime_party_check"]

    try:
        graphics = await stats_service.create_comparison_graphics(
            session=session,
            action_types=action_types,
            period=period,
            grouping=grouping,
            title="Статистика профилей по группам",
            normalize=normalize,
            is_sum=False,  # Для количества профилей используем среднее
        )

        return StreamingResponse(graphics, media_type="image/png")

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Нет данных для отображения: {str(e)}",
        )


@router.get("/stats/compare/operations")
async def get_operations_comparison(
    session: AsyncSession = SessionDep,
    period: str | None = Query(
        "24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"
    ),
    normalize: bool = Query(False, description="Normalize data to 0-100%"),
):
    """
    Специализированный эндпоинт для сравнения операций с профилями.
    Показывает количество перемещений и удалений профилей.
    """
    action_types = ["to_working", "to_trash", "to_overtime", "deleted"]

    try:
        graphics = await stats_service.create_comparison_graphics(
            session=session,
            action_types=action_types,
            period=period,
            grouping=grouping,
            title="Операции с профилями",
            normalize=normalize,
            is_sum=True,  # Для операций используем сумму
        )

        return StreamingResponse(graphics, media_type="image/png")

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Нет данных для отображения: {str(e)}",
        )


@router.get("/stats/compare/parsing")
async def get_parsing_comparison(
    session: AsyncSession = SessionDep,
    period: str | None = Query(
        "24h", description="Filter period: 1h, 12h, 24h, 3d, 7d, 30d, all"
    ),
    grouping: str | None = Query(
        "1h", description="Group data: 10m, 30m, 1h, 2h, 6h, 12h, 24h"
    ),
    normalize: bool = Query(False, description="Normalize data to 0-100%"),
):
    """
    Специализированный эндпоинт для сравнения статистики парсинга.
    Показывает количество найденных данных из разных источников.
    """
    action_types = ["to_mail", "to_google", "from_google", "from_mail"]

    try:
        graphics = await stats_service.create_comparison_graphics(
            session=session,
            action_types=action_types,
            period=period,
            grouping=grouping,
            title="Статистика парсинга",
            normalize=normalize,
            is_sum=True,  # Для парсинга используем сумму
        )

        return StreamingResponse(graphics, media_type="image/png")

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Нет данных для отображения: {str(e)}",
        )
