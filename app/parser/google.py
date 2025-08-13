import asyncio
import re
import datetime
from celery import Celery, result
from celery.utils.log import get_task_logger
from playwright.async_api import async_playwright
import psycopg2

from app.core.config import settings
from app.core.session_manager import session_manager
from app.core.redis_conf import get_redis_client
from app.stats.schemas import StatsFilter
from app.stats.service import stats_service

app = Celery("tasks")
logger = get_task_logger(__name__)

SEEN_KEY = "last_question_google"


def get_seen_questions_google(redis_client=get_redis_client()) -> str:
    raw = redis_client.get(SEEN_KEY)
    return raw.decode("utf-8") if raw else ""


def save_seen_questions_google(data: str, redis_client=get_redis_client()):
    redis_client.set(SEEN_KEY, data)


def add_new_questions_google(questions: list[dict[str, str]]):
    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)
    cur = conn.cursor()
    query = """INSERT INTO google_keys VALUES (DEFAULT, DEFAULT, %s)"""
    stats_query = """INSERT INTO stats VALUES (DEFAULT, %s, %s, DEFAULT)"""
    records = [(q["title"],) for q in questions]
    cur.executemany(query, records)
    cur.execute(stats_query, ("to_google", str(len(questions))))
    conn.commit()
    cur.close()
    conn.close()


def delete_old_google_and_mail_questions(google_last_date, mail_last_date):
    """
    Удаляет старые записи из таблиц google_keys и mail_keys.

    Args:
        google_last_date: datetime объект или строка в формате 'YYYY-MM-DD HH:MM:SS'
        mail_last_date: datetime объект или строка в формате 'YYYY-MM-DD HH:MM:SS'

    Returns:
        tuple: (количество удаленных записей из google_keys, количество удаленных записей из mail_keys)
    """

    # Преобразуем даты в правильный формат, если они переданы как строки
    if isinstance(google_last_date, str):
        try:
            google_last_date = datetime.datetime.fromisoformat(
                google_last_date.replace("Z", "+00:00")
            )
        except ValueError:
            logger.error(
                f"Неверный формат даты для google_last_date: {google_last_date}"
            )
            return 0, 0

    if isinstance(mail_last_date, str):
        try:
            mail_last_date = datetime.datetime.fromisoformat(
                mail_last_date.replace("Z", "+00:00")
            )
        except ValueError:
            logger.error(f"Неверный формат даты для mail_last_date: {mail_last_date}")
            return 0, 0

    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)
    cur = conn.cursor()

    try:
        
        google_query = """DELETE FROM google_keys WHERE created_at < %s"""
        cur.execute(google_query, (google_last_date,))
        google_deleted = cur.rowcount
        mail_query = """DELETE FROM mail_keys WHERE created_at < %s"""
        cur.execute(mail_query, (mail_last_date,))
        mail_deleted = cur.rowcount

        cur.execute(
            """INSERT INTO stats VALUES (DEFAULT, %s, %s, DEFAULT)""",
            ("from_google", str(google_deleted)),
        )
        cur.execute(
            """INSERT INTO stats VALUES (DEFAULT, %s, %s, DEFAULT)""",
            ("from_mail", str(mail_deleted)),
        )

        conn.commit()

        logger.info(
            f"Удалено {google_deleted} записей из google_keys и {mail_deleted} записей из mail_keys"
        )

        return google_deleted, mail_deleted

    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при удалении старых записей: {e}")
        return 0, 0
    finally:
        cur.close()
        conn.close()


async def get_last_questions_google(
    last_seen_text: str | None = None, last_seen_theme: str | None = None
):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://trends.google.com/trending?geo=RU", timeout=60000)
        await page.wait_for_selector('div[class="mZ3RIc"]', timeout=10000)
        await page.wait_for_selector('span[class="mUIrbf-vQzf8d"]', timeout=10000)

        cards = await page.query_selector_all('div[class="mZ3RIc"]')
        cards_themes = await page.query_selector_all('span[class="mUIrbf-vQzf8d"]')
        results = []
        results_theme = []
        found_last = False

        for card in cards:
            title = (await card.inner_text()).strip()
            if title == last_seen_text:
                logger.debug(f"Уже видел: {title}")
                found_last = True
                break
            results.append({"title": title})

        for card_theme in cards_themes:
            title = (await card_theme.inner_text()).strip()
            if title == last_seen_theme:
                logger.debug(f"Уже видел: {title}")
                found_last = True
                break

            results_theme.append({"title": title})

        await browser.close()

    new_last = results[0]["title"] if results else last_seen_text
    logger.info(f"New last: {new_last}")
    if not found_last and results:
        logger.info(
            "⚠ Последняя сохранённая тема не найдена. Возможно, она вытеснена новыми."
        )
    return results, new_last
