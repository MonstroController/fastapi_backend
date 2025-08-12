from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy import func, TIMESTAMP
from app.core.base.base_model import Base, idpk
from datetime import datetime



class VideoKeywordsOrm(Base):
    __tablename__ = "video_keys"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]


class Keywords_3_Orm(Base):
    __tablename__ = "keys_3"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]


class Keywords_4_Orm(Base):
    __tablename__ = "keys_4"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]


class Keywords_5_Orm(Base):
    __tablename__ = "keys_5"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]


class Keywords_6_Orm(Base):
    __tablename__ = "keys_6"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]


class Keywords_7_Orm(Base):
    __tablename__ = "keys_7"
    pid: Mapped[idpk]
    text: Mapped[str] = mapped_column(nullable=True)
    frequency: Mapped[int]

class MailKeywordsOrm(Base):
    __tablename__ = "mail_keys"
    pid: Mapped[idpk]
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), default=func.now())
    text: Mapped[str] = mapped_column(nullable=True)

class GoogleKeywordsOrm(Base):
    __tablename__ = "google_keys"
    pid: Mapped[idpk]
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), default=func.now())
    text: Mapped[str] = mapped_column(nullable=True)



keywords_models = {
    3: Keywords_3_Orm,
    4: Keywords_4_Orm,
    5: Keywords_5_Orm,
    6: Keywords_6_Orm,
    7: Keywords_7_Orm,
}
