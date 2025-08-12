from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy import func, TIMESTAMP
from app.core.base.base_model import Base, idpk
from datetime import datetime
from pydantic import BaseModel


class TokenOrm(Base):
    __tablename__ = "tokens"
    pid: Mapped[idpk]
    token: Mapped[str] = mapped_column(nullable=False)
    is_active: Mapped[bool] = mapped_column(default="True", server_default="True")
    description: Mapped[str] = mapped_column(nullable=True)


class TokenFilter(BaseModel):
    pid: int | None = None
    token: str | None = None
    is_active: bool | None = None