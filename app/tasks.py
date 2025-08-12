import os
from app.core.config import settings
from app.core.session_manager import session_manager
from celery.utils.log import get_task_logger
from app.celery_app import celery
from app.keywords.crud import mail_repo
from app.keywords.schemas import MailKeywordFilter
from app.parser.mail import get_last_questions, get_seen_questions, save_seen_questions
from app.transfer.utils import get_daily_transfer_count, count_client_profiles, insert_profiles, delete_profiles_by_pid, increment_daily_transfer_count, fetch_profiles
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
            await mail_repo.add_many(session=session, instances=[MailKeywordFilter(text=question["title"]) for question in questions])

@celery.task
def mail_parser():
    last = get_seen_questions()
    logger.info(f"Last seen mail: {last}")
    new_loop = asyncio.new_event_loop()
    new_questions, last_question = new_loop.run_until_complete(get_last_questions(last))
    new_loop.close()
    save_seen_questions(last_question)
    return f"Found {len(new_questions)} new question(s)"