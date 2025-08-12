from playwright.async_api import async_playwright
import asyncio
import datetime
import re

# Храним уже виденные вопросы (ID -> URL)
seen_questions: dict[int, str] = {}

def extract_question_id(href: str) -> int:
    match = re.search(r'/question/(\d+)', href)
    return int(match.group(1)) if match else -1

async def scroll_to_bottom(page, max_scrolls=5):
    """Прокручивает страницу вниз до конца или до max_scrolls попыток."""
    previous_height = None
    for _ in range(max_scrolls):
        current_height = await page.evaluate("document.body.scrollHeight")
        if current_height == previous_height:
            break
        previous_height = current_height
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)  # Ждём подгрузку

async def main():
    global seen_questions

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://otvet.mail.ru/')
        await page.wait_for_selector('div[class^="_Card_"]')

        # Прокручиваем вниз до конца
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
            if qid in seen_questions:
                print(f"Вопрос {qid} уже был. Останавливаемся.")
                break

            span = await link.query_selector('span')
            if not span:
                continue

            title = await span.inner_text()

            results.append({
                "qid": qid,
                "title": title.strip(),
                "created_at": datetime.datetime.now()
            })

            new_seen_questions[qid] = title.strip()

        await browser.close()
        print(f"Было: {seen_questions}")
        seen_questions = new_seen_questions
        print(f"Стало: {seen_questions}")
        for key in seen_questions.keys():
            print(f"Последний: {key}")
            break

        print(f"\n✅ Найдено {len(results)} новых вопрос(ов):")
        return results


# 🔁 Запуск (для демонстрации — один раз)
if __name__ == "__main__":
    asyncio.run(main())


