from celery import Celery
from app.core.config import settings


celery = Celery(
    "worker",
    broker=settings.redis.CELERY_BROKER_URL,
    backend=settings.redis.CELERY_RESULT_BACKEND,
)

celery.conf.beat_schedule = {
    "transfer-every-5-minutes": {
        "task": "app.tasks.transfer_profiles",  # ← точно такое имя!
        "schedule": settings.redis.TRANSFER_PERIOD,  # каждые 5 минут
        "args": (),
    },
}

celery.conf.timezone = "UTC"
celery.autodiscover_tasks(["app.tasks"])
