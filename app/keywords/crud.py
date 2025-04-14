from app.core.base.base_repository import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .model import VideoKeywordsOrm, keywords_models
import logging
import random

logger = logging.getLogger(__name__)


class KeywordsRepository(BaseRepository):
    model = VideoKeywordsOrm

    async def get_random_keyword(self, pid, max_count, session: AsyncSession):
        pid = random.randint(1, max_count)
        res = await self.find_one_or_none_by_pid(session=session, data_pid=pid)
        return res.text


keywords_repository: KeywordsRepository = KeywordsRepository()
