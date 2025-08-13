from app.core.base.base_repository import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from .model import GoogleKeywordsOrm, VideoKeywordsOrm, keywords_models, MailKeywordsOrm
import logging
import random

logger = logging.getLogger(__name__)


class KeywordsRepository(BaseRepository):
    model = VideoKeywordsOrm

    async def get_random_keyword(self, max_count, session: AsyncSession):
        pid = random.randint(1, max_count)
        res = await self.find_one_or_none_by_pid(session=session, data_pid=pid)
        return res.text
    
class MailKeywordsRepository(BaseRepository):
    model = MailKeywordsOrm

    async def get_random_keyword(self, session: AsyncSession):
        query = """SELECT text FROM mail_keys ORDER BY RANDOM() LIMIT 1"""
        keyword = await session.execute(text(query))
        keyword = keyword.fetchone()
        logger.info(f"Keyword: {keyword}")
        return keyword[0]

class GoogleKeywordsRepository(BaseRepository):
    model = GoogleKeywordsOrm

    async def get_random_keyword(self, session: AsyncSession):
        query = """SELECT text FROM google_keys ORDER BY RANDOM() LIMIT 1"""
        keyword = await session.execute(text(query))
        keyword = keyword.fetchone()
        logger.info(f"Keyword: {keyword}")
        return keyword[0]
    
keywords_repository: KeywordsRepository = KeywordsRepository()
mail_repo: MailKeywordsRepository = MailKeywordsRepository()
google_repo: GoogleKeywordsRepository = GoogleKeywordsRepository()