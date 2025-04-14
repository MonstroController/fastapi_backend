from .crud import KeywordsRepository, keywords_repository
from app.core.base.base_service import BaseService
from sqlalchemy.ext.asyncio import AsyncSession
from .model import keywords_models, VideoKeywordsOrm
from sqlalchemy import func, select
from app.core.config import settings
import random


import logging

logger = logging.getLogger(__name__)


class KeywordsService(BaseService):
    def __init__(self, repository: KeywordsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def get_random_default_keyword(self, pid, min, max, session: AsyncSession):
        db_num = random.randint(min, max)
        self.repository.model = keywords_models[db_num]
        return await self.repository.get_random_keyword(
            pid, session=session, max_count=100000
        )

    async def get_random_video_keyword(self, pid, session: AsyncSession):
        self.repository.model = VideoKeywordsOrm
        return await self.repository.get_random_keyword(
            pid, session=session, max_count=19000
        )


keywords_service: KeywordsService = KeywordsService(repository=keywords_repository)
