import os
import datetime
from app.core.config import settings
from app.core.session_manager import session_manager
from celery.utils.log import get_task_logger
from app.celery_app import celery
from app.keywords.crud import mail_repo
from app.keywords.schemas import MailKeywordFilter
from app.parser.google import (
    add_new_questions_google,
    delete_old_google_and_mail_questions,
    get_last_questions_google,
    get_seen_questions_google,
    save_seen_questions_google,
)
from app.parser.mail import (
    get_last_questions_mail,
    get_seen_questions_mail,
    save_seen_questions_mail,
)
from app.profiles.sheduler import delete_trash_and_overtime
from app.stats.service import stats_service
from app.transfer.utils import (
    get_daily_transfer_count,
    count_client_profiles,
    insert_profiles,
    delete_profiles_by_pid,
    increment_daily_transfer_count,
    fetch_profiles,
)
from app.profiles.service import profiles_service
from app.core.session_manager import session_manager
import asyncio


# Подключения можно выносить в функции или использовать пул SQLAlchemy
# SRC_DSN = settings.db.DATABASE_URL_psycopg2
DST_DSN = os.environ.get("DSN_1")
DAILY_LIMIT = settings.redis.DAY_LIMIT

logger = get_task_logger(__name__)


@celery.task
def transfer_profiles():
    """Основная функция перемещения профилей с учетом дневного лимита."""
    current_count = get_daily_transfer_count()
    logger.info(f"Использовано: {current_count}")
    if current_count >= DAILY_LIMIT:
        logger.info("Дневной лимит перемещения достигнут. Пропускаем операцию.")
        return "Daily limit reached"

    existing = count_client_profiles()
    profiles, conn = fetch_profiles(existing)
    if not profiles:
        return "No profiles to transfer"

    inserted_pids = insert_profiles(profiles)
    if inserted_pids:
        delete_profiles_by_pid(inserted_pids, conn)
        increment_daily_transfer_count(len(inserted_pids))
        logger.info(
            f"Перемещено {len(inserted_pids)} профилей. Всего за день: {current_count + len(inserted_pids)}"
        )

    return f"Transferred {len(inserted_pids)} profiles"


async def add_new_questions(questions):
    async with session_manager.get_session() as session:
        async with session_manager.managed_transaction(session):
            await mail_repo.add_many(
                session=session,
                instances=[
                    MailKeywordFilter(text=question["title"]) for question in questions
                ],
            )


@celery.task
def mail_parser():
    last = get_seen_questions_mail()
    logger.info(f"Last seen mail: {last}")
    new_loop = asyncio.new_event_loop()
    new_questions, last_question = new_loop.run_until_complete(
        get_last_questions_mail(last)
    )
    new_loop.close()
    if last_question:
        save_seen_questions_mail(last_question)
    return f"Found {len(new_questions)} new question(s)"


@celery.task
def google_parser():
    last = get_seen_questions_google()
    logger.info(f"Last seen Google Trends: {last}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    new_questions, new_last = loop.run_until_complete(get_last_questions_google(last))
    loop.close()
    if new_questions:
        add_new_questions_google(new_questions)
        logger.info(f"Добавлено {len(new_questions)} новых тем")

    else:
        logger.info("Новых тем не найдено")
    save_seen_questions_google(new_last)
    return f"Found {len(new_questions)} new topic(s)"


@celery.task
def google_and_mail_cleaner():
    """
    Задача для очистки старых записей из таблиц google_keys и mail_keys.
    Использует настройку DATA_LIFETIME_DAYS для расчета даты удаления.
    """

    google_last_date = datetime.datetime.now() - datetime.timedelta(
        days=settings.redis.GOOGLE_DATA_LIFETIME_DAYS
    )

    mail_last_date = datetime.datetime.now() - datetime.timedelta(
        days=settings.redis.MAIL_DATA_LIFETIME_DAYS
    )

    logger.info(
        f"Очистка записей старше {google_last_date} - google, {mail_last_date} - mail, (время жизни: {settings.redis.GOOGLE_DATA_LIFETIME_DAYS} - google, {settings.redis.MAIL_DATA_LIFETIME_DAYS} - mail, дней)"
    )

    google_deleted, mail_deleted = delete_old_google_and_mail_questions(
        google_last_date, mail_last_date
    )

    total_deleted = google_deleted + mail_deleted
    logger.info(f"Очистка завершена. Удалено записей: {total_deleted}")

    return f"Cleaned {total_deleted} old records (Google: {google_deleted}, Mail: {mail_deleted})"