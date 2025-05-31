# models/post.py

import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey, JSON, Text
from sqlalchemy.sql import func

# Удаляем локальное определение Base и импорт DeclarativeBase
# Вместо этого, импортируем центральный Base из сервисного модуля
from services.db import Base

class Post(Base):
    """
    SQLAlchemy ORM модель для таблицы 'posts'.
    """
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id")) # Внешний ключ на таблицу users

    # Предполагаем, что храним строковые идентификаторы чатов (например, username или chat_id как str)
    chat_ids: Mapped[list[str]] = mapped_column(JSON)

    text: Mapped[str] = mapped_column(Text, nullable=True) # Текст поста, может отсутствовать
    media_paths: Mapped[list[str]] = mapped_column(JSON, nullable=True) # Пути или ID медиафайлов, могут отсутствовать

    # Тип расписания: 'one_time', 'recurring'
    schedule_type: Mapped[str]

    # Параметры расписания (cron, дни недели и т.п.)
    schedule_params: Mapped[dict] = mapped_column(JSON, nullable=True)

    # Дата запуска для 'one_time' расписания
    run_date: Mapped[datetime.datetime] = mapped_column(nullable=True)

    # Время в секундах, через которое пост должен быть удален
    delete_after_seconds: Mapped[int] = mapped_column(nullable=True)

    # Словарь, хранящий chat_id: message_id для отправленных сообщений, для последующего удаления.
    sent_message_data: Mapped[dict] = mapped_column(JSON, nullable=True)

    # Статус поста: 'scheduled', 'sent', 'deleted', 'error', 'sending_failed', 'deletion_failed'
    status: Mapped[str] = mapped_column(default="scheduled")

    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        """
        Строковое представление объекта Post.
        """
        return (
            f"Post(id={self.id!r}, user_id={self.user_id!r}, chat_ids={self.chat_ids!r}, "
            f"schedule_type={self.schedule_type!r}, status={self.status!r}, "
            f"run_date={self.run_date!r})" # В __repr__ включаем основные поля
        )

