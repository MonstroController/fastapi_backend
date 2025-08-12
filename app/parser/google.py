import asyncio
import re
from celery import Celery
from celery.utils.log import get_task_logger
from playwright.async_api import async_playwright
import psycopg2

from your_project.config import settings
from your_project.redis_client import get_redis_client

app = Celery("tasks")
logger = get_task_logger(__name__)

SEEN_KEY = "last_question_google"

def get_seen_questions(redis_client=get_redis_client()) -> str:
    raw = redis_client.get(SEEN_KEY)
    return raw.decode("utf-8") if raw else ""

def save_seen_questions(data: str, redis_client=get_redis_client()):
    redis_client.set(SEEN_KEY, data)

def add_new_questions(questions: list[dict[str, str]]):
    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)
    cur = conn.cursor()
    query = """INSERT INTO mail_keys VALUES (DEFAULT, DEFAULT, %s)"""
    records = [(q["title"],) for q in questions]
    logger.info(f"Records to insert: {records}")
    cur.executemany(query, records)
    conn.commit()
    cur.close()
    conn.close()

async def get_last_questions(last_seen_text: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://trends.google.com/trending?geo=RU', timeout=60000)
        await page.wait_for_selector('div[class="mZ3RIc"]', timeout=10000)

        cards = await page.query_selector_all('div[class="mZ3RIc"]')
        print(cards)
        results = []
        found_last = False

        # for card in cards:
        #     title_element = await card.query_selector('div > div')
        #     if not title_element:
        #         continue
        #     title = (await title_element.inner_text()).strip()

        #     if title == last_seen_text:
        #         logger.info(f"Уже видел: {title}")
        #         found_last = True
        #         break

        #     logger.info(f"Новая тема: {title}")
        #     results.append({"title": title})

        await browser.close()

        # new_last = results[0]["title"] if results else last_seen_text
        # if not found_last and results:
        #     logger.info("⚠ Последняя сохранённая тема не найдена. Возможно, она вытеснена новыми.")

        # return results, new_last

@app.task
def google_trends_parser():
    last = get_seen_questions()
    logger.info(f"Last seen Google Trends: {last}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    new_questions, new_last = loop.run_until_complete(get_last_questions(last))
    loop.close()
    if new_questions:
        add_new_questions(new_questions)
        logger.info(f"Добавлено {len(new_questions)} новых тем")
    else:
        logger.info("Новых тем не найдено")
    save_seen_questions(new_last)
    return f"Found {len(new_questions)} new topic(s)"


if __name__ == "__main__":
    asyncio.run(get_last_questions())