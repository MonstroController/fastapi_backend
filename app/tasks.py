import os
import asyncpg
from app.core.config import settings
import asyncio
from app.core.session_manager import session_manager
from app.profiles.service import profiles_service
from sqlalchemy import select, and_, func, text, not_
from app.profiles.model import ProfilesOrm
from celery.utils.log import get_task_logger
from app.celery_app import celery
from datetime import timedelta, datetime, timezone
import psycopg2
import redis

# Подключения можно выносить в функции или использовать пул SQLAlchemy
# SRC_DSN = settings.db.DATABASE_URL_psycopg2
DST_DSN = os.environ.get("DSN_1")
DAILY_LIMIT = settings.redis.DAY_LIMIT

redis_client = redis.Redis(host="broker_redis", port=6379, db=3)
logger = get_task_logger(__name__)


def hours_to_dates(min_hours_life=0, max_hours_life=0):
    """Get dates interval (min and max dates) from min and max hours of life"""
    now = datetime.now(timezone.utc)
    if min_hours_life == 0:
        return now - timedelta(hours=max_hours_life)
    elif max_hours_life == 0:
        return now - timedelta(hours=min_hours_life)
    min_date = now - timedelta(hours=min_hours_life)
    max_date = now - timedelta(hours=max_hours_life)
    return min_date, max_date


def count_client_profiles():
    conn = psycopg2.connect(DST_DSN)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM profiles WHERE party = 'rus';")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0]


def get_profiles(conn, party, min_date, max_date, party_fraction):
    cur = conn.cursor()
    query = """
    SELECT * FROM profiles
    WHERE party = %s
    AND data_create BETWEEN %s AND %s
    AND date_block <= NOW() + INTERVAL '%s hours'
    AND folder ~ '^([1,]+)$'
    AND NOT folder ~ '[^1,]'
    LIMIT %s;
    """
    interval_hours = settings.profiles.TIME_BEFORE_DATE_BLOCK
    cur.execute(query, (party, max_date, min_date, interval_hours, party_fraction))
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    profiles = [dict(zip(columns, row)) for row in rows]
    cur.close()
    return profiles


def fetch_profiles(existing):
    profiles = []
    count = settings.redis.CAPACITY_LIMIT - existing
    conn = psycopg2.connect(settings.db.DATABASE_URL_psycopg2)  # Основная база данных
    cur = conn.cursor()

    min_date, max_date = hours_to_dates(
        settings.profiles.MIN_LIFE_HOURS_TO_WORKING_PARTY,
        settings.profiles.MAX_LIFE_HOURS_TO_WORKING_PARTY,
    )
    logger.info(f"min: {min_date}")
    logger.info(f"max: {max_date}")

    query = """
    SELECT party, COUNT(*) as count
    FROM profiles
    WHERE party LIKE 's_%%'
    AND party != %s
    AND data_create BETWEEN %s AND %s
    AND date_block <= NOW() + INTERVAL '%s hours'
    GROUP BY party;
    """
    interval_hours = settings.profiles.TIME_BEFORE_DATE_BLOCK
    cur.execute(
        query, (settings.profiles.WORKING_PARTY, max_date, min_date, interval_hours)
    )
    parties = cur.fetchall()

    total = 0
    if len(parties) != 0:
        party_fraction = count // len(parties)
        if party_fraction > 0:
            for party, _ in parties:
                res = get_profiles(conn, party, min_date, max_date, party_fraction)
                logger.info(f"From {party} take {len(res)} profiles. Total: {total}")
                total += len(res)
                profiles.extend(res)
        logger.info(
            f"party_fraction: {party_fraction}, total: {total}, count: {count}, parties: {len(parties)}"
        )
        if total < count and parties:
            logger.info(f"parties: {parties}")
            res = get_profiles(conn, parties[0][0], min_date, max_date, count - total)
            total += len(res)
            profiles.extend(res)

    cur.close()
    return profiles, conn


def insert_profiles(profiles):
    if not profiles:
        return []

    conn = psycopg2.connect(DST_DSN)
    cur = conn.cursor()

    records = [
        (
            p["pid"],
            p["data_create"] - timedelta(days=15),
            "rus",
            0,
            p["accounts"],
            p["is_google"],
            p["is_yandex"],
            p["is_mail"],
            p["is_youtube"],
            p["ismobiledevice"],
            p["platform"],
            p["platform_version"],
            p["browser"],
            p["browser_version"],
            p["folder"],
            p["fingerprints"],
            p["cookies"],
            "",
            p["last_date_work"] - timedelta(days=15),
            p["date_block"] - timedelta(days=15),
            p["last_visit_sites"],
            p["last_task"],
            p["geo"],
            p["tel"],
            p["email"],
            p["name"],
            p["mouse_config"],
            0,
            0,
            p["localstorage"],
            0,
            p["warm"] - timedelta(days=15),
        )
        for p in profiles
    ]

    query = """
    INSERT INTO profiles (pid, data_create, party, cookies_len, accounts, is_google, is_yandex, is_mail, is_youtube, 
    ismobiledevice, platform, platform_version, browser, browser_version, folder, fingerprints, cookies, proxy, last_date_work, 
    date_block, last_visit_sites, last_task, geo, tel, email, name, mouse_config, domaincount, metrikacount, localstorage, yacount, warm)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (pid) DO NOTHING
    """

    cur.executemany(query, records)
    conn.commit()
    cur.close()
    conn.close()

    return [p["pid"] for p in profiles]


def delete_profiles_by_pid(pids, conn):
    """Удаляет профили из вашей базы данных по списку PID."""
    if not pids:
        return

    cur = conn.cursor()
    stats_query = "INSERT INTO stats (action_type, affected_rows) VALUES (%s, %s)"
    query = "DELETE FROM profiles WHERE pid IN %s"
    cur.execute(query, (tuple(pids),))
    cur.execute(
        stats_query,
        (
            "transport_1",
            len(pids),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_daily_transfer_count():
    """Получает текущее количество перемещенных профилей за день из Redis."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"today key: {today}")
    key = f"transfer_count:{today}"
    count = redis_client.get(key)
    return int(count) if count else 0


def increment_daily_transfer_count(count):
    """Увеличивает счетчик перемещенных профилей в Redis."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"transfer_count:{today}"
    redis_client.incrby(key, count)
    redis_client.expire(key, 86400)  # Ключ истекает через 24 часа


@celery.task
def transfer_profiles():
    """Основная функция перемещения профилей с учетом дневного лимита."""
    current_count = get_daily_transfer_count()
    logger.info(f"Использовано: {current_count}")
    if current_count >= DAILY_LIMIT:
        logger.info("Дневной лимит перемещения достигнут. Пропускаем операцию.")
        return "Daily limit reached"

    existing = count_client_profiles()
    profiles, conn = fetch_profiles(existing)
    if not profiles:
        return "No profiles to transfer"

    inserted_pids = insert_profiles(profiles)
    if inserted_pids:
        delete_profiles_by_pid(inserted_pids, conn)
        increment_daily_transfer_count(len(inserted_pids))
        logger.info(
            f"Перемещено {len(inserted_pids)} профилей. Всего за день: {current_count + len(inserted_pids)}"
        )

    return f"Transferred {len(inserted_pids)} profiles"
