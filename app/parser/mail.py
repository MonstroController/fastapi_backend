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

SEEN_KEY = "last_question_mail"

logger = get_task_logger(__name__)

def get_seen_questions(redis_client=get_redis_client()) -> dict[int, str]:
    raw = redis_client.get(SEEN_KEY)
    if not raw:
        return {}
    return raw

def save_seen_questions(data: str, redis_client=get_redis_client()):
    redis_client.set(SEEN_KEY, data)

def extract_question_id(href: str) -> int:
    match = re.search(r'/question/(\d+)', href)
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

def add_new_questions(questions):
    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)  # Основная база данных
    cur = conn.cursor()
    query = """INSERT INTO mail_keys VALUES (DEFAULT, DEFAULT, %s)"""
    records = [(question["title"], ) for question in questions]
    logger.info(f"Records: {records}")
    cur.executemany(query, records)
    conn.commit()
    cur.close()
    conn.close()


async def get_last_questions(last):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://otvet.mail.ru/')
        await page.wait_for_selector('div[class^="_Card_"]')

        await scroll_to_bottom(page)

        question_cards = await page.query_selector_all('div[class^="_Card_"]')

        results = []
        new_seen_questions: dict[int, str] = {}

        for card in question_cards:
            link = await card.query_selector('a[href^="/question/"]')
            if not link:
                continue

            href = await link.get_attribute('href')
            if not href:
                continue

            qid = extract_question_id(href)
            if qid == last:
                logger.info(f"Вопрос {qid} уже был. Останавливаемся.")
                break

            span = await link.query_selector('span')
            if not span:
                continue

            title = await span.inner_text()

            results.append({
                "qid": qid,
                "title": title.strip(),
            })

            new_seen_questions[qid] = title.strip()

        await browser.close()
        seen_questions = new_seen_questions
        last = None
        for key in seen_questions.keys():
            last = key
            break
        logger.info(f"\n✅ Найдено {len(results)} новых вопрос(ов):")
        add_new_questions(results)
        logger.info("Вопросы добавлены в базу")
        return results, last
