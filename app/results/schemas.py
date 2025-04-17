from pydantic import BaseModel, ConfigDict
import datetime


class ClickResult(BaseModel):
    find: bool
    clickurl: bool
    error: bool
    pos: int
    copyname: str
    profile_id: int | None
    data_create: datetime.datetime | None
    yacount: int
    metrikacount: int
    search_type: str
    search_domain: str
    domain: str
    party: str
    keyword: str
    fullask: str
    url: str | None = None
    lendomain: int
    lencookies: int


class ClickResultRead(ClickResult):
    model_config = ConfigDict(from_attributes=True)

    pid: int


class ClickResultCreate(ClickResult):
    pass


class ClickResultFilter(BaseModel):
    find: bool | None = None
    clickurl: bool | None = None
    error: bool | None = None
    pos: int | None = None
    copyname: str | None = None
    profile_id: int | None = None
    data_create: datetime.datetime | None = None
    yacount: int | None = None
    metrikacount: int | None = None
    search_type: str | None = None
    search_domain: str | None = None
    domain: str | None = None
    party: str | None = None
    keyword: str | None = None
    lendomain: int | None = None
    lencookies: int | None = None
