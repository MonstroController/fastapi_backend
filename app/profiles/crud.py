from app.core.base.base_repository import BaseRepository
from .model import ProfilesOrm
from .schemas import ProfileFilters
from sqlalchemy import select, func, update, not_, and_, text, delete, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
import logging
from app.stats.service import stats_service
from app.stats.schemas import StatsFilter
from .utils import hours_to_dates


logger = logging.getLogger(__name__)


class ProfilesRepository(BaseRepository):
    model = ProfilesOrm

    async def update_profiles_to_working_party(
        self,
        session: AsyncSession,
        party_fraction,
        party,
        min_date,
        max_date,
        working_party,
    ):
        """Меняет группу профилей на рабочую с настройкой фильтра даты\n
        session: Текущая сессия\n
        party_fraction: Сколько профилей обновить (лимит)\n
        party: Из какой группы брать\n
        min_date max_date: интервал, допустимый для даты создания профиля
        working_party: Новая группа"""
        selected_ids = (
            select(ProfilesOrm)
            .where(
                and_(
                    ProfilesOrm.party == party,
                    ProfilesOrm.data_create.between(min_date, max_date),
                    ProfilesOrm.date_block
                    <= func.now()
                    + text(
                        f"interval '{settings.profiles.TIME_BEFORE_DATE_BLOCK} hours'"
                    ),
                    ProfilesOrm.folder.op("~")("^([1,]+)$"),
                    not_(ProfilesOrm.folder.op("~")("[^1,]")),
                )
            )
            .limit(party_fraction)
            .cte("selected_ids")
        )

        update_stmt = (
            update(ProfilesOrm)
            .where(ProfilesOrm.pid.in_(select(selected_ids.c.pid)))
            .values(
                party=working_party,
                last_date_work=ProfilesOrm.last_date_work,
                date_block=func.now(),
                warm=ProfilesOrm.warm,
            )
        )

        res = await session.execute(update_stmt)
        await session.commit()
        logger.info(
            f"For party {party}: update {res.rowcount} profiles to new party: {working_party}"
        )
        return res.rowcount

    async def get_parties_for_working_party(
        self, session: AsyncSession, min_date, max_date
    ):
        """Находит подходящие группы, где есть нужные по фильтру даты профиля"""
        query = (
            select(ProfilesOrm.party, func.count())
            .select_from(ProfilesOrm)
            .where(
                and_(
                    ProfilesOrm.party.like("s_%"),
                    ProfilesOrm.party != settings.profiles.WORKING_PARTY,
                    ProfilesOrm.data_create.between(min_date, max_date),
                    ProfilesOrm.date_block
                    <= (
                        func.now()
                        + text(
                            f"interval '{settings.profiles.TIME_BEFORE_DATE_BLOCK} hours'"
                        )
                    ),
                )
            )
            .group_by(ProfilesOrm.party)
        )
        res = await session.execute(query)
        await session.commit()
        return res.scalars().all()

    async def update_spent_profiles_in_working_party(self, session: AsyncSession):
        """Находит профиля из рабочей группы, которые уже отработали"""
        query = (
            update(ProfilesOrm)
            .where(
                and_(
                    ProfilesOrm.party == settings.profiles.WORKING_PARTY,
                    not_(ProfilesOrm.folder.op("~")("^([1,]+)$")),
                    ProfilesOrm.folder.op("~")("[^1,]"),
                )
            )
            .values(party=settings.profiles.TRASH_PARTY)
        )
        res = await session.execute(query)
        logger.info(
            f"Set {res.rowcount} profiles to {settings.profiles.TRASH_PARTY} party from {settings.profiles.WORKING_PARTY}"
        )
        await session.commit()
        return res.rowcount

    async def update_overtime_profiles(self, session: AsyncSession, min_date) -> int:
        """Находит профиля из все групп s_ которые старше min_date и обновляет поле party"""

        query = (
            update(ProfilesOrm)
            .where(
                and_(
                    ProfilesOrm.party.like("s_%"),
                    ProfilesOrm.data_create <= min_date,
                    ProfilesOrm.party != "s>72",
                    not_(ProfilesOrm.party == "s_mix")
                )
            )
            .values(party="s>72")
        )
        res = await session.execute(query)
        logger.info(f"Set {res.rowcount} profiles to s>72")
        await session.commit()
        return res.rowcount

    async def delete_from_trash_and_overtime(
        self, session: AsyncSession, trash_party, min_date
    ):
        query = delete(ProfilesOrm).where(
            and_(
                ProfilesOrm.data_create <= min_date, not_(ProfilesOrm.party.like("f_"), not_(ProfilesOrm.party.like("t_")))
            )
        )
        res = await session.execute(query)
        logger.info(f"{res.rowcount} profiles was deleted")
        await session.commit()
        return res.rowcount


profiles_repository: ProfilesRepository = ProfilesRepository()
