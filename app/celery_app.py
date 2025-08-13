from celery import Celery
from app.core.config import settings
from celery.schedules import timedelta


celery = Celery(
    "worker",
    broker=settings.redis.CELERY_BROKER_URL,
    backend=settings.redis.CELERY_RESULT_BACKEND,
)

celery.conf.beat_schedule = {
    "transfer-every-5-minutes": {
        "task": "app.tasks.transfer_profiles",
        "schedule": settings.redis.TRANSFER_PERIOD,
        "args": (),
    },
    "mail-parser-every-5-minutes": {
        "task": "app.tasks.mail_parser",
        "schedule": settings.redis.MAIL_PARSER_PERIOD,
        "args": (),
    },
    "google-parser-every-5-minutes": {
        "task": "app.tasks.google_parser",
        "schedule": settings.redis.GOOGLE_PARSER_PERIOD,
        "args": (),
    },
    "cleanup-old-data-google-and-mail": {
        "task": "app.tasks.google_and_mail_cleaner",
        "schedule": settings.redis.GOOGLE_AND_MAIL_CLEAN_PERIOD,
        "args": (),
    }
}

celery.conf.timezone = "UTC"
celery.autodiscover_tasks(["app.tasks"])
