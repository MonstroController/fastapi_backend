from pydantic import BaseModel
import datetime


class Keyword(BaseModel):
    pid: int
    text: str
    frequecy: int
