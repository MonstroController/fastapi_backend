from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn, BaseModel

BASE_DIR = Path(__file__).parent.parent


class EnvBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

class DBSettings(EnvBaseSettings):
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    DSN_1: str

    @property
    def DATABASE_URL_asyncpg(self):
        # DSN
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    @property
    def DATABASE_URL_psycopg2(self):
        # DSN
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
class RedisSettings(EnvBaseSettings):
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str
    TRANSFER_PERIOD: int
    DAY_LIMIT: int
    CAPACITY_LIMIT: int

class ProfilesController(EnvBaseSettings):
    NORMAL_WORKING_PARTY_CAPACITY: int = 1000  # size of s_mix
    MINIMUM_WORKING_PARTY_CAPACITY: int = 500
    MINIMUM_WALKING_PARTY_CAPACITY: int = 2000
    MIN_LIFE_HOURS_TO_WORKING_PARTY: int = 19  # hours
    MAX_LIFE_HOURS_TO_WORKING_PARTY: int = 72  # hours
    TIME_BEFORE_DATE_BLOCK: int = 1  # hours
    CHECK_TO_APPEND_TIME: int = 2  # minutes
    CHECK_TO_TRASH_TIME: int = 2  # minutes
    TRASH_PARTY: str = "A"  # from s_mix to this party
    WORKING_PARTY: str = "s_mix"  # from s_... to this party
    OVERTIME_PARTY: str = "s>72"  # from s_... to this party

class TelegramBotSettings(EnvBaseSettings):
    TELEGRAM_BOT_URL: str


class Settings(BaseSettings):
    api_v1_prefix: str = "/v1"

    db: DBSettings = DBSettings()

    profiles: ProfilesController = ProfilesController()

    redis: RedisSettings = RedisSettings()

    tg_bot: TelegramBotSettings = TelegramBotSettings()


settings = Settings()
