import asyncio
import re
from celery import Celery
from celery.utils.log import get_task_logger
from playwright.async_api import async_playwright
import psycopg2



app = Celery("tasks")
logger = get_task_logger(__name__)

SEEN_KEY = "last_question_google"


async def get_last_questions(last_seen_text: str | None = None, last_seen_theme: str | None = None):
    print("start")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://trends.google.com/trending?geo=RU', timeout=60000)
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

            print(f"Новая тема: {title}")
            results.append({"title": title})
    
        for card_theme in cards_themes:
            title = (await card_theme.inner_text()).strip()
            if title == last_seen_theme:
                logger.debug(f"Уже видел: {title}")
                found_last = True
                break

            logger.debug(f"Новая тема: {title}")
            results_theme.append({"title": title})

        await browser.close()
    
    new_last = results[0]["title"] if results else last_seen_text
    logger.info(f"New last: {new_last}")
    if not found_last and results:
        logger.info("⚠ Последняя сохранённая тема не найдена. Возможно, она вытеснена новыми.")

    # return results, new_last



if __name__ == "__main__":
    asyncio.run(get_last_questions())