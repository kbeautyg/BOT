# models/rss_feed.py

import datetime
from typing import Optional, List
from sqlalchemy import (
    ForeignKey,
    String,
    Text,
    Integer,
    Boolean, # Import Boolean
    DateTime,
    UniqueConstraint,
    JSON # Import JSON for channels and filter_keywords
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

# Импортируем централизованный базовый класс моделей
from services.db import Base

class RssFeed(Base):
    """
    SQLAlchemy ORM модель для таблицы 'rss_feeds'.
    """
    __tablename__ = "rss_feeds"

    # Гарантирует, что каждый пользователь может добавить одну и ту же ленту (по URL) только один раз.
    __table_args__ = (UniqueConstraint('user_id', 'feed_url', name='uq_user_feed_url'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id")) # Внешний ключ на таблицу users

    feed_url: Mapped[str] = mapped_column(String(2048), unique=False) # unique=False т.к. уникальность гарантируется по user_id+feed_url

    # Список chat_id (string) каналов/групп для публикации
    channels: Mapped[List[str]] = mapped_column(JSON)

    frequency_minutes: Mapped[int] = mapped_column(Integer) # Частота проверки ленты в минутах

    # Список ключевых слов для фильтрации записей (опционально)
    filter_keywords: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)

    last_checked_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, nullable=True) # Время последней успешной проверки

    is_active: Mapped[bool] = mapped_column(Boolean, default=True) # Флаг активности ленты

    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        """
        Предоставляет краткое строковое представление объекта RssFeed.
        """
        # Обрезаем URL для более чистого представления, если он слишком длинный
        url_repr = self.feed_url[:50] + '...' if self.feed_url and len(self.feed_url) > 50 else self.feed_url
        return (
            f"<RssFeed(id={self.id}, user_id={self.user_id}, url='{url_repr}', "
            f"frequency={self.frequency_minutes}min, is_active={self.is_active})>"
        )


