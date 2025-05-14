import aiohttp
from app.core.config import settings

async def notify_admins(text):
    async with aiohttp.ClientSession() as session:
        async with session.post(settings.tg_bot.TELEGRAM_BOT_URL, json={"text": text}) as response:
            res = await response.json(content_type='text/html') 
    return res