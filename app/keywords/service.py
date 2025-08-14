from .crud import KeywordsRepository, google_repo, keywords_repository, mail_repo
from app.core.base.base_service import BaseService
from sqlalchemy.ext.asyncio import AsyncSession
from .model import keywords_models, VideoKeywordsOrm, MailKeywordsOrm
from sqlalchemy import func, select
from app.core.config import settings
import random


import logging

logger = logging.getLogger(__name__)


class KeywordsService(BaseService):
    def __init__(self, repository: KeywordsRepository):
        self.repository = repository
        super().__init__(repository=self.repository)
        self.keyword_group_mapping = {
            "default": self.get_random_default_keyword,
            "video": self.get_random_video_keyword,
            "google": self.get_random_google_keyword,
            "mail": self.get_random_mail_keyword
        }

    async def get_random_default_keyword(self,session: AsyncSession, min = 4, max = 7):
        db_num = random.randint(min, max)
        self.repository.model = keywords_models[db_num]
        return await self.repository.get_random_keyword(
            session=session, max_count=100000
        )

    async def get_random_video_keyword(self, session: AsyncSession):
        self.repository.model = VideoKeywordsOrm
        return await self.repository.get_random_keyword(
            session=session, max_count=19000
        )

    async def get_random_mail_keyword(self, session: AsyncSession):
        return await mail_repo.get_random_keyword(session=session)

    async def get_random_google_keyword(self, session: AsyncSession):
        return await google_repo.get_random_keyword(session=session)

    async def get_mixed_keywords(
        self,
        session: AsyncSession,
        include_default: bool = True,
        include_video: bool = True,
        include_google: bool = True,
        include_mail: bool = True,
        total_count: int = 1,
    ):
        """
        Получает 1 ключевое слово из групп со случайным равномерным распределением

        Args:
            session: Сессия базы данных
            include_default: Включить default ключевые слова
            include_video: Включить video ключевые слова
            include_google: Включить google ключевые слова
            include_mail: Включить mail ключевые слова
            total_count: Игнорируется, всегда возвращает 1 слово

        Returns:
            Одно ключевое слово из случайно выбранной группы
        """
        # Определяем активные группы
        active_groups = []
        if include_default:
            active_groups.append("default")
        if include_video:
            active_groups.append("video")
        if include_google:
            active_groups.append("google")
        if include_mail:
            active_groups.append("mail")

        if not active_groups:
            return "Нет активных групп для получения ключевых слов"

        selected_group = random.choice(active_groups)

        try:
            keyword = await self.keyword_group_mapping[selected_group](session=session)
            return keyword

        except Exception as e:
            logger.error(
                f"Ошибка при получении ключевого слова из группы {selected_group}: {e}"
            )
            


keywords_service: KeywordsService = KeywordsService(repository=keywords_repository)
