from pydantic import BaseModel
import datetime


class ProfileRead(BaseModel):
    pid: int
    data_create: datetime.datetime
    party: str | None
    cookies_len: int | None
    accounts: str | None
    is_google: bool
    is_yandex: bool
    is_mail: bool
    is_youtube: bool
    ismobiledevice: bool
    platform: str | None
    platform_version: str | None
    browser: str | None
    browser_version: str | None
    folder: str | None
    fingerprints: str | None
    cookies: str | None
    localstorage: str | None
    proxy: str | None
    last_date_work: datetime.datetime | None
    date_block: datetime.datetime | None
    last_visit_sites: str | None
    last_task: str | None
    geo: str | None
    tel: str | None
    email: str | None
    name: str | None
    mouse_config: str | None
    domaincount: int
    metrikacount: int | None
    yacount: int | None
    warm: datetime.datetime | None


class ProfileFilters(BaseModel):
    pid: int | None = None
    data_create: datetime.datetime | None = None
    party: str | None = None
    cookies_len: int | None = None
    accounts: str | None = None
    is_google: bool | None = None
    is_yandex: bool | None = None
    is_mail: bool | None = None
    is_youtube: bool | None = None
    ismobiledevice: bool | None = None
    platform: str | None = None
    platform_version: str | None = None
    browser: str | None = None
    browser_version: str | None = None
    folder: str | None = None
    fingerprints: str | None = None
    cookies: str | None = None
    localstorage: str | None = None
    proxy: str | None = None
    last_date_work: datetime.datetime | None = None
    date_block: datetime.datetime | None = None
    last_visit_sites: str | None = None
    last_task: str | None = None
    geo: str | None = None
    tel: str | None = None
    email: str | None = None
    name: str | None = None
    mouse_config: str | None = None
    domaincount: int | None = None
    metrikacount: int | None = None
    yacount: int | None = None
    warm: datetime.datetime | None = None


class ProfileUpdatePartial(BaseModel):
    party: str | None = None
    folder: str | None = None
    last_visit_sites: str
    last_task: str
    geo: str
    tel: str
    email: str
    name: str
    mouse_config: str


class SelectionParameters(BaseModel):
    parties: list[str]
    new_party: str
    profiles_count: int
    min_hours_life: int
    max_hours_life: int
