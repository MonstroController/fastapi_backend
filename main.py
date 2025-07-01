from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from app import monstro as monstro_router
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from app.utils.setup_logging import setup_logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.profiles.sheduler import (
    update_working_party_schedule,
    clean_working_party_schedule,
    clean_all_parties_overtime_schedule,
    delete_trash_and_overtime,
)
from app.core.config import settings
from app.results.sheduler import delete_overtime_results

logger = setup_logging()
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[dict, None]:
    global logger
    """
    Управляет жизненным циклом планировщика приложения.

    Args:
        app (FastAPI): Экземпляр приложения FastAPI.
    """

    logger.info("Инициализация приложения...")
    try:
        scheduler.add_job(
            update_working_party_schedule,
            trigger=IntervalTrigger(minutes=1),
            id="currency_update_working_party",
            replace_existing=True,
        )

        scheduler.add_job(
            clean_working_party_schedule,
            trigger=IntervalTrigger(minutes=1),
            id="currency_clear_working_party",
            replace_existing=True,
        )
        scheduler.add_job(
            clean_all_parties_overtime_schedule,
            trigger=IntervalTrigger(minutes=1),
            id="currency_clear_parties",
            replace_existing=True,
        )

        scheduler.add_job(
            delete_trash_and_overtime,
            trigger=IntervalTrigger(minutes=2), max_instances=5,
            id="currency_delete_parties",
        )

        scheduler.add_job(
            delete_overtime_results,
            trigger=IntervalTrigger(minutes=5),
            id="currency_delete_results",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Планировщик обновления ")
        yield
    except Exception as e:
        logger.error(f"Ошибка инициализации планировщика: {e}")
    finally:
        # Завершение работы планировщика
        scheduler.shutdown()
        logger.info("Планировщик обновления курсов валют остановлен")

    logger.info("Завершение работы приложения...")


def create_app() -> FastAPI:
    """
    Создание и конфигурация FastAPI приложения.

    Returns:
        Сконфигурированное приложение FastAPI
    """
    app = FastAPI(
        title="Стартовая сборка FastAPI",
        description=(
            "Стартовая сборка с интегрированной SQLAlchemy 2 для разработки FastAPI приложений с продвинутой "
            "архитектурой\n\n"
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    # Настройка CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Регистрация роутеров
    register_routers(app)

    return app


def register_routers(app: FastAPI) -> None:
    """Регистрация роутеров приложения."""
    # Корневой роутер
    root_router = APIRouter()

    @root_router.get("/", tags=["root"])
    def home_page():
        return {
            "message": "Hello",
        }

    # Подключение роутеров
    app.include_router(root_router, tags=["root"])
    # app.include_router(router_auth, prefix='/auth', tags=['Auth'])
    app.include_router(router=monstro_router)


# Создание экземпляра приложения
app = create_app()


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Start the application")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=4)
