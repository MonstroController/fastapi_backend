from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, declared_attr
from sqlalchemy import text, func
from app.core.base.base_model import Base

import datetime


class StatsOrm(Base):

    __tablename__ = "stats"

    action_type: Mapped[str]
    affected_rows: Mapped[int]
    operation_timestamp: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now()
    )
