from app.core.redis_conf import get_redis_client
from app.core.config import settings
from app.core.session_manager import session_manager
from app.keywords.crud import mail_repo
from app.keywords.schemas import MailKeywordFilter
from playwright.async_api import async_playwright
from celery.utils.log import get_task_logger
import asyncio
import datetime
import re
import json
import psycopg2
import string

from app.stats.schemas import StatsFilter
from app.stats.service import stats_service

SEEN_KEY = "last_question_mail"

logger = get_task_logger(__name__)


def clean_text_from_punctuation(text: str) -> str:
    """
    Удаляет все знаки пунктуации из текста, оставляя только буквы, цифры и пробелы
    """
    cleaned = re.sub(r"[^\w\s]", "", text)
    return cleaned


def get_seen_questions_mail(redis_client=get_redis_client()) -> str:
    raw = redis_client.get(SEEN_KEY)
    return raw.decode("utf-8") if raw else ""


def save_seen_questions_mail(data: str, redis_client=get_redis_client()):
    redis_client.set(SEEN_KEY, data)


def extract_question_id(href: str) -> int:
    match = re.search(r"/question/(\d+)", href)
    return int(match.group(1)) if match else -1


async def scroll_to_bottom(page, max_scrolls=10):
    """Прокручивает страницу вниз до конца или до max_scrolls попыток."""
    previous_height = None
    for _ in range(max_scrolls):
        current_height = await page.evaluate("document.body.scrollHeight")
        if current_height == previous_height:
            break
        previous_height = current_height
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)


def add_new_questions_mail(questions):
    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)
    cur = conn.cursor()
    stats_query = """INSERT INTO stats VALUES (DEFAULT, %s, %s, DEFAULT)"""
    query = """INSERT INTO mail_keys VALUES (DEFAULT, DEFAULT, %s)"""
    records = [(question,) for question in questions]

    cur.executemany(query, records)
    cur.execute(stats_query, ("to_mail", str(len(questions))))
    conn.commit()
    cur.close()
    conn.close()


async def get_last_questions_mail(last):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://otvet.mail.ru/")
        await page.wait_for_selector('div[class^="_Card_"]')

        await scroll_to_bottom(page)

        question_cards = await page.query_selector_all('div[class^="_Card_"]')

        results = []

        for card in question_cards:
            link = await card.query_selector('a[href^="/question/"]')
            if not link:
                continue

            href = await link.get_attribute("href")
            if not href:
                continue

            span = await link.query_selector("span")
            if not span:
                continue

            title = await span.inner_text()
            if last == title:
                logger.info(f"Вопрос: '{title}' уже был, останавливаемся")
                break

            cleaned_title = clean_text_from_punctuation(title.strip())

            if cleaned_title:
                results.append(cleaned_title)

        await browser.close()
        last = None
        if results:
            last = results[0]
            logger.info(f"\n✅ Найдено {len(results)} новых вопрос(ов):")
            add_new_questions_mail(results)
            logger.info(f"Последний: {last}")
        return results, last
