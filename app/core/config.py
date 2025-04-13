from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn, BaseModel

BASE_DIR = Path(__file__).parent.parent


class DBSettings(BaseSettings):
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str

    @property
    def DATABASE_URL_asyncpg(self):
        # DSN
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    model_config = SettingsConfigDict(env_file=".env.prod")


class ProfilesController(BaseSettings):
    NORMAL_WORKING_PARTY_CAPACITY: int = 1000 #size of s_mix
    MIN_LIFE_HOURS_TO_WORKING_PARTY: int = 19 #hours
    MAX_LIFE_HOURS_TO_WORKING_PARTY: int = 72 #hours
    TIME_BEFORE_DATE_BLOCK: int = 1 #hours
    CHECK_TO_APPEND_TIME: int = 2 # minutes
    CHECK_TO_TRASH_TIME: int = 2 #minutes
    TRASH_PARTY: str = "A"   #from s_mix to this party
    WORKING_PARTY: str = "s_mix" #from s_... to this party

    
    
class Settings(BaseSettings):
    api_v1_prefix: str = "/v1"

    db: DBSettings = DBSettings()

    profiles: ProfilesController = ProfilesController()


settings = Settings()