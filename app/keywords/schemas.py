from pydantic import BaseModel
import datetime


class Keyword(BaseModel):
    pid: int
    text: str
    frequecy: int


class MailKeywordFilter(BaseModel):
    pid: int | None = None
    created_at: datetime.datetime | None = None
    text: str | None = None

