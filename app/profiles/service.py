from .crud import ProfilesRepository, profiles_repository
from .model import ProfilesOrm
from .schemas import ProfileRead, ProfileFilters
from .utils import hours_to_dates
from app.core.base.base_service import BaseService
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
from app.core.config import settings
from app.stats.service import stats_service
from app.stats.schemas import StatsFilter
from app.utils.telegram_bot import notify_admins
import aiohttp

import logging

logger = logging.getLogger(__name__)


class ProfilesService(BaseService):
    def __init__(self, repository: ProfilesRepository):
        self.repository = repository
        super().__init__(repository=self.repository)

    async def from_work_parties_to_party(self, party, count, session):
        min_date, max_date = hours_to_dates(
            settings.profiles.MIN_LIFE_HOURS_TO_WORKING_PARTY,
            settings.profiles.MAX_LIFE_HOURS_TO_WORKING_PARTY,
        )
        parties = await self.repository.get_parties_for_working_party(
            session=session, min_date=min_date, max_date=max_date
        )
        for party in parties:
            local_count = await self.repository.count(
                session=session, filters=ProfileFilters(party=party)
            )
            if local_count < settings.profiles.MINIMUM_WALKING_PARTY_CAPACITY:
                await notify_admins(
                    f"!!WARNING!!\nВ группe {party} меньше {settings.profiles.MINIMUM_WALKING_PARTY_CAPACITY} профилей: {local_count}"
                )
            else:
                logger.info(f"Count: {local_count}")

        if len(parties) != 0 and (party_fraction := count // len(parties)) != 0:
            total = 0
            for party in parties:
                res_count = await self.repository.update_profiles_to_working_party(
                    session=session,
                    party_fraction=party_fraction,
                    party=party,
                    min_date=min_date,
                    max_date=max_date,
                    working_party=settings.profiles.WORKING_PARTY,
                )
                total += res_count
                if res_count < party_fraction:
                    party_fraction += party_fraction - res_count
            await stats_service.add(
                session=session,
                values=StatsFilter(action_type="to_working", affected_rows=total),
            )

    async def check_working_party_for_update(self, session: AsyncSession):
        profiles_count = await self.repository.count(
            session=session,
            filters=ProfileFilters(party=settings.profiles.WORKING_PARTY),
        )
        logger.info(f"Pofiles count before alarm: {profiles_count}")
        # if profiles_count < settings.profiles.MINIMUM_WORKING_PARTY_CAPACITY:
        #     res = await notify_admins(
        #         f"!!WARNING!!\nВ группе s_mix меньше {settings.profiles.MINIMUM_WORKING_PARTY_CAPACITY} профилей: {profiles_count}"
        #     )

        await stats_service.add(
            session=session,
            values=StatsFilter(
                action_type="working_party_check", affected_rows=profiles_count
            ),
        )
        logger.info(f"Profiles count: {profiles_count}")
        logger.info(f"needed count: {settings.profiles.NORMAL_WORKING_PARTY_CAPACITY}")
        if profiles_count < settings.profiles.NORMAL_WORKING_PARTY_CAPACITY:

            shortage = settings.profiles.NORMAL_WORKING_PARTY_CAPACITY - profiles_count

            await self.from_work_parties_to_party(
                settings.profiles.WORKING_PARTY, shortage, session
            )

    async def from_working_party_to_trash_party(
        self,
        session: AsyncSession,
        trash_party: str = settings.profiles.TRASH_PARTY,
        big_age_party="s_>72",
    ):

        total = await self.repository.update_spent_profiles_in_working_party(
            session=session
        )
        count_profiles = await self.repository.count(
            session=session,
            filters=ProfileFilters(party=settings.profiles.TRASH_PARTY),
        )
        await stats_service.add(
            session=session,
            values=StatsFilter(
                action_type="trash_party_check", affected_rows=count_profiles
            ),
        )
        await stats_service.add(
            session=session,
            values=StatsFilter(action_type="to_trash", affected_rows=total),
        )

    async def clean_to_overtime_party(
        self,
        session: AsyncSession,
        max_hours_life: int = settings.profiles.MAX_LIFE_HOURS_TO_WORKING_PARTY,
        overtime_party: str = "s>72",
    ):
        min_date = hours_to_dates(max_hours_life=max_hours_life)
        total = await self.repository.update_overtime_profiles(
            session=session, min_date=min_date
        )
        count_profiles = await self.repository.count(
            session=session,
            filters=ProfileFilters(party=settings.profiles.OVERTIME_PARTY),
        )
        await stats_service.add(
            session=session,
            values=StatsFilter(
                action_type="overtime_party_check", affected_rows=count_profiles
            ),
        )
        await stats_service.add(
            session=session,
            values=StatsFilter(action_type="to_overtime", affected_rows=total),
        )

    async def delete_trash_and_overtime(
        self, session: AsyncSession, days_limit: int = 5
    ):
        min_date = hours_to_dates(max_hours_life=days_limit * 24)
        total = await self.repository.delete_from_trash_and_overtime(
            session=session,
            trash_party=settings.profiles.TRASH_PARTY,
            min_date=min_date,
        )
        await stats_service.add(
            session=session,
            values=StatsFilter(action_type="deleted", affected_rows=total),
        )


profiles_service: ProfilesService = ProfilesService(repository=profiles_repository)
