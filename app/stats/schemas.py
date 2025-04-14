from pydantic import BaseModel, ConfigDict
import datetime


class Stats(BaseModel):
    action_type: str
    affected_rows: int
    operation_timestamp: datetime.datetime


class StatsRead(Stats):
    model_config = ConfigDict(from_attributes=True)

    pid: int


class StatsCreate(Stats):
    pass


class StatsFilter(BaseModel):
    pid: int | None = None
    action_type: str | None = None
    affected_rows: int | None = None
    operation_timestamp: datetime.datetime | None = None
