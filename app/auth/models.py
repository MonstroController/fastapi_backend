import operator
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy import func, TIMESTAMP, Enum
from app.core.base.base_model import Base, idpk
from datetime import datetime
from pydantic import BaseModel
from enum import Enum as PyEnum
import enum


class UserRole(str, PyEnum):
    """Роли пользователей в системе"""

    admin = "admin"  # Полный доступ ко всем функциям
    operator = "operator"  # Доступ к получению статистики всех операций + VIEWER
    stats = "stats"  # Доступ к получению статистики проектов + GUEST
    keywords = "keywords"  # Доступ только к получению ключей


class TokenOrm(Base):
    __tablename__ = "tokens"
    pid: Mapped[idpk]
    token: Mapped[str] = mapped_column(nullable=False)
    is_active: Mapped[bool] = mapped_column(default="True", server_default="True")
    description: Mapped[str] = mapped_column(nullable=True)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, name="user_role", create_type=False),
        default=UserRole.keywords,
        server_default=UserRole.keywords,
    ) 


class TokenFilter(BaseModel):
    pid: int | None = None
    token: str | None = None
    is_active: bool | None = None
    role: UserRole | None = None
