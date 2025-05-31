
# services/scheduler.py

# Основные компоненты планировщика задач с использованием APScheduler и SQLAlchemyJobStore.
# Управляет расписанием публикаций постов и проверок RSS-лент, а также удалением постов.

import datetime
import logging
import os
import json
from typing import TYPE_CHECKING, Any, Dict, Optional, List, Callable

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger # Импорт для планирования RSS-проверок
from apscheduler.jobstores.base import JobLookupError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

# Импорты зависимостей из проекта:
# async_engine и AsyncSessionLocal (фабрика сессий) для доступа к БД
from services.db import async_engine, AsyncSessionLocal
# Функции сервисов, которые будут выполняться в задачах планировщика или нужны для восстановления
from services.db import (
    get_post_by_id,
    update_post_status,
    get_all_posts_for_scheduling,
    get_all_active_rss_feeds, # Используется в restore_scheduled_jobs
    get_rss_feed_by_id # Используется в _task_check_rss_feed
)
# Импорт Telegram API сервисов
from services.telegram_api import send_post_content, delete_telegram_messages
# Импорт RSS сервиса
import services.rss_service # Импорт сервиса для проверки RSS (вызывается из задачи)

# Для аннотации типа Aiogram Bot без циклической зависимости
if TYPE_CHECKING:
    from aiogram import Bot
    # Импорт моделей для аннотаций (если нужны в сигнатурах задач, restore и т.п.)
    from models.post import Post
    from models.rss_feed import RssFeed


# Настройка логирования
logger = logging.getLogger(__name__)

# 2. Константы и конфигурация
# Часовой пояс для планировщика. Берется из переменной окружения или по умолчанию 'Europe/Berlin'.
TIME_ZONE_STR = os.getenv('TIME_ZONE', 'Europe/Berlin')
# Название таблицы в БД для хранения задач APScheduler.
APS_JOBS_TABLE_NAME = 'apscheduler_jobs'

# Вспомогательная фабрика сессий для использования внутри задач.
# Передача фабрики позволяет задачам создавать свои собственные сессии.
# session_factory: Callable[..., AsyncSession] = AsyncSessionLocal # This can be passed directly

# 7. Вспомогательная функция _parse_cron_params
def _parse_cron_params(cron_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует пользовательский формат cron_params в аргументы для триггера 'cron' APScheduler.

    Пользовательский формат: {'type': 'daily|weekly|monthly|yearly', 'time': 'HH:MM', ...}
    APScheduler cron формат: {'hour': ..., 'minute': ..., 'day_of_week': ..., 'month': ..., 'day': ...}

    Args:
        cron_params: Словарь с параметрами расписания в пользовательском формате.
                     Предполагается, что time_str уже валидирован по формату 'HH:MM'.
                     Предполагается, что days_of_week (если есть) - список строк ['mon', ...].
                     Предполагается, что day (если есть) - int 1-31.
                     Предполагается, что month_day (если есть) - строка 'DD.MM'.

    Returns:
        Словарь с аргументами для CronTrigger APScheduler.

    Raises:
        ValueError: Если параметры некорректны или отсутствуют обязательные поля для типа.
    """
    trigger_args: Dict[str, Any] = {}
    cron_type = cron_params.get('type')
    time_str = cron_params.get('time')  # 'HH:MM'

    if not time_str:
        raise ValueError("Cron parameters must include 'time' in HH:MM format (e.g., '14:30').")

    try:
        hour, minute = map(int, time_str.split(':'))
        # APScheduler CronTrigger expects strings for numerical fields in **kwargs for older versions,
        # and can accept ints for newer versions, but string is safer for compatibility.
        trigger_args['hour'] = str(hour)
        trigger_args['minute'] = str(minute)
    except ValueError:
         # This should ideally be caught by validate_cron_params in utils, but handle defensively.
        raise ValueError(f"Invalid time format: {time_str}. Must be HH:MM.")

    if cron_type == 'daily':
        pass # Only time needed
    elif cron_type == 'weekly':
        days_of_week = cron_params.get('days_of_week')  # ['mon', 'fri', ...]
        if not isinstance(days_of_week, list) or not days_of_week:
            raise ValueError("For 'weekly' cron, 'days_of_week' (list of str, e.g., ['mon', 'fri']) is required.")
        # APScheduler CronTrigger expects day_of_week as comma-separated string (0-6 or mon-sun)
        trigger_args['day_of_week'] = ','.join(days_of_week)
    elif cron_type == 'monthly':
        day = cron_params.get('day_of_month')  # 1-31 (int)
        if not isinstance(day, int) or not (1 <= day <= 31):
            raise ValueError("For 'monthly' cron, 'day_of_month' (int 1-31) is required.")
        trigger_args['day'] = str(day)
    elif cron_type == 'yearly':
        month_day_str = cron_params.get('month_day') # 'DD.MM' (string)
        if not isinstance(month_day_str, str):
             raise ValueError("For 'yearly' cron, 'month_day' (string 'DD.MM') is required.")
        try:
            day, month = map(int, month_day_str.split('.'))
            if not (1 <= month <= 12) or not (1 <= day <= 31): # Basic range check, full validation in utils
                 raise ValueError("Invalid day.month format or range.")
            trigger_args['month'] = str(month)
            trigger_args['day'] = str(day)
        except ValueError:
            raise ValueError(f"Invalid month_day format: {month_day_str}. Must be DD.MM.")
    else:
        # Unknown or missing type
        raise ValueError(f"Unsupported cron type: {cron_type}. Supported types: daily, weekly, monthly, yearly.")

    # CronTrigger timezone defaults to scheduler's timezone if not specified.
    # Using scheduler's timezone is generally correct if all times are entered in that zone.
    # trigger_args['timezone'] = APScheduler timezone object (set on scheduler) or TIME_ZONE_STR

    return trigger_args


# 4. Функции задач (вызываются планировщиком)
async def _task_publish_post(
    bot: 'Bot',
    session_factory: Callable[..., AsyncSession],
    post_id: int,
    scheduler_instance: AsyncIOScheduler # Экземпляр планировщика нужен для планирования задачи удаления изнутри
):
    """
    Задача планировщика для публикации поста.
    Загружает пост из БД, вызывает логику публикации и обновляет статус поста.

    Args:
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        post_id: ID поста для публикации.
        scheduler_instance: Экземпляр планировщика для добавления последующих задач (например, удаления).
    """
    logger.info(f"Задача публикации поста {post_id} запущена.")
    async with session_factory() as session:
        try:
            post: Optional['Post'] = await get_post_by_id(session, post_id)
            if not post:
                logger.error(f"Задача публикации поста {post_id} не выполнена: Пост с ID {post_id} не найден в БД.")
                # Пост удален из БД до выполнения задачи. Задача завершается.
                # No need to update status as post doesn't exist
                return

            if post.status not in ['scheduled', 'pending_reschedule']:
                 logger.warning(f"Задача публикации поста {post_id} пропущена: Пост уже имеет статус '{post.status}'.")
                 # If it's a recurring task, it will continue, but won't publish if status is not 'scheduled'.
                 # If it's a one-time task, it should ideally not be in the scheduler if status changed.
                 return # Task completes without publishing


            # --- Логика публикации поста в Telegram ---
            logger.info(f"Публикация поста {post.id} в чаты: {post.chat_ids}...")

            if not post.chat_ids:
                 logger.warning(f"Пост {post.id} не имеет указанных чатов для отправки.")
                 # Обновляем статус поста на 'no_chats_specified'
                 await update_post_status(session, post_id, 'no_chats_specified')
                 await session.commit() # Commit the status update
                 return

            # Prepare media items if media_paths exist
            # This requires services.content_manager.prepare_input_media_list
            # Need to import services.content_manager and potentially FSInputFile
            # from services.content_manager import prepare_input_media_list # Need this import
            # from aiogram.types import FSInputFile # Need this import if prepare uses FSInputFile

            # Assuming media_paths are local file paths
            from services.content_manager import prepare_input_media_list # Assuming this import is available

            input_media_items = []
            if post.media_paths:
                 try:
                     input_media_items = prepare_input_media_list(post.media_paths)
                     if post.media_paths and not input_media_items:
                          # Failed to prepare media files (e.g., not found, invalid format)
                          logger.error(f"Пост {post.id}: Не удалось подготовить медиафайлы из путей: {post.media_paths}. Отправка отменена.")
                          await update_post_status(session, post_id, 'media_error')
                          await session.commit()
                          # TODO: Cleanup temp files associated with this post (complex, maybe outside task)
                          return # Exit task on media preparation failure

                 except Exception as media_prep_e:
                      logger.exception(f"Пост {post.id}: Ошибка при подготовке медиафайлов: {media_prep_e}. Отправка отменена.")
                      await update_post_status(session, post_id, 'media_error')
                      await session.commit()
                      return # Exit task on media preparation failure


            sent_message_data: Dict[str, int] = {} # Dictionary to store chat_id_str: message_id_int
            successfully_sent_chats = []
            # Iterate through configured channels and send the post
            for chat_id_str in post.chat_ids:
                try:
                    # Call send_post_content for EACH chat_id if needed, or if send_post_content
                    # handles multiple chat_ids internally (which it currently doesn't based on signature).
                    # send_post_content sends to *one* chat_id. Need to loop here.

                    # Pass text and media items to the sender.
                    # send_post_content returns a list of sent Message objects.
                    # For single message/media, list contains 1 item. For media group, list contains N items.
                    # We need the message_id of the *first* or *main* message in the group for potential deletion.
                    # Telegram's deleteMessage API takes message_id. For media groups, deleting the first message
                    # does NOT delete the whole group. You must delete *all* messages in the group.
                    # send_media_group returns a list of all sent messages in the group.

                    sent_messages_list = await send_post_content(
                        bot=bot,
                        chat_id=chat_id_str,
                        text=post.text,
                        media_items=input_media_items, # Pass the list of InputMedia objects
                        parse_mode='HTML' # Or get from user settings/post config
                        # reply_markup=... # Add markup if needed for post content (e.g., inline buttons)
                    )

                    if sent_messages_list:
                         # Store chat_id and message_ids of ALL messages sent to this chat
                         # For deletion, we need ALL message IDs for a chat ID.
                         # Let's store as {chat_id_str: [message_id1, message_id2, ...]}
                         sent_message_data[chat_id_str] = [m.message_id for m in sent_messages_list]
                         logger.info(f"Пост {post.id} отправлен в чат {chat_id_str}. IDs: {sent_message_data[chat_id_str]}")
                         successfully_sent_chats.append(chat_id_str)

                    else:
                         # send_post_content returns empty list on failure
                         logger.error(f"Не удалось отправить пост {post.id} в чат {chat_id_str}. send_post_content вернул пустой список.")
                         # Continue to next chat

                except Exception as send_error:
                    logger.exception(f"Ошибка при отправке поста {post.id} в чат {chat_id_str}: {send_error}")
                    # Continue to next chat


            # Close file handles opened by prepare_input_media_list AFTER sending attempt to all chats
            if input_media_items:
                 from aiogram.types import FSInputFile # Assuming this is used
                 for media_item in input_media_items:
                      # Check if the media is an FSInputFile instance with a file handle to close
                      if isinstance(media_item, (InputMediaPhoto, InputMediaVideo, InputMediaDocument)) and isinstance(media_item.media, FSInputFile):
                           if hasattr(media_item.media, 'file') and hasattr(media_item.media.file, 'close'):
                                try:
                                     media_item.media.file.close()
                                     logger.debug(f"Closed file handle for {media_item.media.path}")
                                except Exception as e:
                                     logger.warning(f"Error closing file handle {media_item.media.path}: {e}")


            # --- Обновление статуса поста в БД и сохранение данных об отправке ---
            if not successfully_sent_chats:
                 logger.error(f"Не удалось отправить пост {post.id} ни в один из указанных чатов.")
                 await update_post_status(session, post_id, 'sending_failed')
                 await session.commit() # Commit status update
                 # TODO: Cleanup temp files if any were created (complex, needs external process)
                 return # Exit task

            # Update status to 'sent' and save the sent message data
            # Update post object directly and commit
            post.status = 'sent'
            post.sent_message_data = sent_message_data # Save the dict {chat_id: [msg_ids]}
            # post.sent_at = datetime.datetime.now(scheduler_instance.timezone) # Optional: Add sent_at field to Post model
            await session.commit() # Commit status and sent_message_data

            logger.info(f"Статус поста {post.id} обновлен на 'sent'. Данные об отправке сохранены.")


            # --- Планирование задачи удаления, если delete_after_seconds задан ---
            # Task needs original chat IDs and message IDs for deletion.
            # These are stored in post.sent_message_data.
            if post.delete_after_seconds is not None and post.delete_after_seconds > 0:
                # Deletion time is calculated from the time the post was successfully SENT.
                # Use the current time of the task execution as the "sent time" baseline.
                # If sent_at field was added to Post model and updated above, use that instead.
                # sent_time = post.sent_at if hasattr(post, 'sent_at') and post.sent_at else datetime.datetime.now(scheduler_instance.timezone) # Use scheduler's timezone
                sent_time = datetime.datetime.now(scheduler_instance.timezone)

                # Calculate deletion time
                deletion_time = sent_time + datetime.timedelta(seconds=post.delete_after_seconds)

                # Data for deletion task: needs to be JSON serializable
                target_chat_ids = list(post.sent_message_data.keys()) # List of chat_id strings
                original_message_ids_flat = [] # Flatten list of lists of message IDs
                for msg_id_list in post.sent_message_data.values():
                     original_message_ids_flat.extend(msg_id_list)

                # Pass data to the deletion task. The task signature expects JSON strings.
                # This requires the deletion task to re-associate chat IDs and message IDs correctly.
                # A simpler approach for the deletion task might be to pass `post_id` and let the task
                # fetch `sent_message_data` from the DB again. Let's refactor `_task_delete_post`.

                logger.info(f"Пост {post_id} имеет delete_after_seconds={post.delete_after_seconds}. Планирование задачи удаления на {deletion_time}...")

                # Call schedule_post_deletion with post_id and deletion_time
                await schedule_post_deletion(
                    scheduler=scheduler_instance,
                    bot=bot, # Pass bot
                    session_factory=session_factory, # Pass factory
                    post_id=post_id,
                    deletion_time=deletion_time,
                    # No need to pass json strings if deletion task fetches from DB
                    # original_message_ids_json=json.dumps(original_message_ids_flat), # Might need flat list or dict structure
                    # target_chat_ids_json=json.dumps(target_chat_ids)
                )
                logger.info(f"Задача удаления поста {post_id} успешно запланирована.")

            # TODO: Cleanup temp files associated with this post (complex, needs external process)


        except Exception as e:
            # Catch any exception during task execution (excluding send_error already handled)
            logger.exception(f"Критическая ошибка при выполнении задачи публикации поста {post_id}: {e}")
            # Attempt to update status to 'error' in a new session if current session might be invalid
            try:
                async with session_factory() as error_session:
                     # Check post status again to avoid overwriting 'sent' if error happened AFTER commit
                     post_check = await get_post_by_id(error_session, post_id)
                     if post_check and post_check.status not in ['sent', 'deleted', 'sending_failed', 'media_error']:
                          await update_post_status(error_session, post_id, 'error')
                          await error_session.commit()
                          logger.info(f"Статус поста {post_id} обновлен на 'error' из-за критической ошибки.")
                     elif not post_check:
                          logger.warning(f"Пост {post_id} не найден при попытке обновить статус на 'error'.")
                     # else: status already more specific or sent/deleted, don't overwrite

            except Exception as rollback_e:
                 logger.error(f"Критическая ошибка: Не удалось обновить статус поста {post_id} на 'error' после исключения: {rollback_e}")

            # TODO: Cleanup temp files associated with this post (complex, needs external process)


async def _task_delete_post(
    bot: 'Bot',
    session_factory: Callable[..., AsyncSession],
    post_id: int
    # No need for json strings here if task fetches data from DB
    # original_message_ids_json: str, # JSON string of message IDs
    # target_chat_ids_json: str # JSON string of chat IDs
):
    """
    Задача планировщика для удаления поста из Telegram.
    Удаляет сообщения в указанных чатах и обновляет статус поста в БД.

    Args:
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        post_id: ID поста для удаления.
    """
    logger.info(f"Задача удаления поста {post_id} запущена.")
    async with session_factory() as session:
        try:
            post: Optional['Post'] = await get_post_by_id(session, post_id)
            if not post:
                logger.error(f"Задача удаления поста {post_id} не выполнена: Пост с ID {post_id} не найден в БД.")
                # If post not found, assume it was already deleted from DB. Task finishes.
                return

            if post.status == 'deleted':
                 logger.warning(f"Задача удаления поста {post_id} пропущена: Пост уже имеет статус '{post.status}'.")
                 return # Task finishes

            # Fetch the sent message data from the post object
            sent_message_data: Dict[str, List[int]] = post.sent_message_data or {}

            if not sent_message_data:
                 logger.warning(f"Пост {post.id} не имеет сохраненных данных об отправленных сообщениях. Удаление в Telegram невозможно.")
                 # Update status to 'deletion_skipped_no_data' or similar
                 await update_post_status(session, post_id, 'deletion_skipped') # Assuming 'deletion_skipped' status exists
                 await session.commit()
                 return # Exit task

            logger.info(f"Пост {post.id} имеет данные об отправке. Попытка удаления сообщений...")

            all_chats_successfully_processed = True # Track if we attempted delete for all chats

            # Iterate through chat_id: message_ids pairs and attempt deletion
            for chat_id_str, message_ids_list in sent_message_data.items():
                 try:
                     chat_id = int(chat_id_str) # Ensure chat_id is int for delete_telegram_messages if needed (it handles string too)
                     # Use delete_telegram_messages service
                     successfully_deleted_in_chat = await delete_telegram_messages(
                         bot=bot,
                         chat_id=chat_id, # Pass chat_id (can be str)
                         message_ids=message_ids_list # Pass list of message IDs for this chat
                     )
                     if not successfully_deleted_in_chat:
                          logger.warning(f"Не удалось удалить ВСЕ сообщения для поста {post.id} в чате {chat_id_str}.")
                          all_chats_successfully_processed = False
                     else:
                          logger.info(f"Сообщения для поста {post.id} в чате {chat_id_str} обработаны (удалены или не найдены).")

                 except ValueError:
                    logger.error(f"Некорректный chat_id '{chat_id_str}' в sent_message_data для поста {post.id}. Пропускаю удаление в этом чате.")
                    all_chats_successfully_processed = False # Treat as partial failure
                 except Exception as delete_chat_error:
                    logger.exception(f"Ошибка при удалении сообщений для поста {post.id} в чате {chat_id_str}: {delete_chat_error}")
                    all_chats_successfully_processed = False # Treat as partial failure


            # --- Обновление статуса поста в БД ---
            if all_chats_successfully_processed:
                 # If deletion in Telegram was successful for all specified chats (or messages not found)
                 await update_post_status(session, post_id, 'deleted')
                 logger.info(f"Статус поста {post_id} обновлен на 'deleted'.")
            else:
                 # If deletion failed for one or more chats
                 await update_post_status(session, post_id, 'deletion_failed')
                 logger.warning(f"Не удалось удалить сообщения для поста {post.id} во всех чатах. Статус обновлен на 'deletion_failed'.")

            await session.commit() # Commit status update


        except Exception as e:
            # Catch any critical errors during task execution (excluding chat-specific errors)
            logger.exception(f"Критическая ошибка при выполнении задачи удаления поста {post_id}: {e}")
            # Attempt to update status to 'deletion_error' in a new session
            try:
                async with session_factory() as error_session:
                     # Check post status again
                     post_check = await get_post_by_id(error_session, post_id)
                     if post_check and post_check.status not in ['deleted', 'deletion_failed', 'deletion_skipped']: # Don't overwrite these statuses
                          await update_post_status(error_session, post_id, 'deletion_error') # Assuming 'deletion_error' status exists
                          await error_session.commit()
                          logger.info(f"Статус поста {post_id} обновлен на 'deletion_error' из-за ошибки.")
                     elif not post_check:
                           logger.warning(f"Пост {post_id} не найден при попытке обновить статус на 'deletion_error'.")

            except Exception as rollback_e:
                 logger.error(f"Критическая ошибка: Не удалось обновить статус поста {post_id} на 'deletion_error' после исключения: {rollback_e}")


# --- НОВАЯ ФУНКЦИЯ ЗАДАЧИ ДЛЯ RSS ---
async def _task_check_rss_feed(
    bot: 'Bot', # Add bot instance
    session_factory: Callable[..., AsyncSession],
    rss_feed_id: int
):
    """
    Задача планировщика для проверки одной RSS-ленты.
    Загружает ленту из БД, вызывает логику проверки и публикации новых элементов.

    Args:
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        rss_feed_id: ID RSS-ленты для проверки.
    """
    logger.info(f"Задача проверки RSS-ленты {rss_feed_id} запущена.")
    # check_and_publish_single_rss_feed manages its own session inside.
    # It needs the session_factory to create sessions.
    try:
        # Call the core RSS processing logic from rss_service
        # This function manages DB interaction (fetching feed, items, updating last_checked)
        # It also calls telegram_api.send_post_content which needs the bot instance.
        await services.rss_service.check_and_publish_single_rss_feed(
            bot=bot, # Pass bot
            session_factory=session_factory,
            feed_id=rss_feed_id
        )
        logger.info(f"Задача проверки RSS-ленты {rss_feed_id} завершена.")

    except Exception as e:
        logger.exception(f"Ошибка при выполнении задачи проверки RSS-ленты {rss_feed_id}: {e}")
        # The check_and_publish_single_rss_feed should handle logging specific errors
        # (like parsing errors, DB errors within its session context).
        # This catch-all here is for unexpected errors that escape the inner function.
        # In case of error, no update to last_checked_at will occur within the inner function
        # if the error happened before that step.

# 3. Функция init_scheduler
def init_scheduler(engine: AsyncEngine, bot: 'Bot') -> AsyncIOScheduler:
    """
    Инициализирует и запускает APScheduler с SQLAlchemyJobStore.

    Args:
        engine: Асинхронный движок SQLAlchemy.
        bot: Экземпляр Aiogram Bot (нужен для передачи в задачи).

    Returns:
        Инициализированный и запущенный экземпляр AsyncIOScheduler.
    """
    logger.info("Инициализация планировщика задач...")
    # Настройка хранилища задач
    jobstores = {
        'default': SQLAlchemyJobStore(url=str(engine.url), tablename=APS_JOBS_TABLE_NAME)
    }
    # Настройка параметров задач по умолчанию
    job_defaults = {
        'coalesce': True, # Пропускать пропущенные запуски повторяющихся задач, кроме самого последнего
        'max_instances': 5, # Максимальное количество одновременно запущенных экземпляров задачи
        'misfire_grace_time': 300 # В секундах. Задачи, пропущенные более чем на это время, будут отменены.
    }
    # Создание экземпляра планировщика
    # Set the timezone on the scheduler itself.
    scheduler = AsyncIOScheduler(
        jobstores=jobstores,
        job_defaults=job_defaults,
        timezone=pytz.timezone(TIME_ZONE_STR) # Установка часового пояса планировщика
    )

    # Start the scheduler. It will load existing jobs from the store.
    scheduler.start()
    logger.info(" APScheduler запущен.")

    # Add a fixed interval job to process *all* active RSS feeds that are due.
    # This is an alternative model to scheduling a job per feed.
    # With this model, you would have ONE scheduler job that wakes up every X minutes
    # and looks in the DB for all RSS feeds whose `last_checked_at` + `frequency_minutes`
    # is in the past, and checks them.
    # This avoids potentially thousands of individual jobs if the user adds many feeds.
    # Let's adjust to this model, as it scales better.
    # The task function would be process_all_active_rss_feeds from rss_service.py
    # This task needs bot and session_factory.

    # --- Using a single periodic job for all RSS feeds ---
    # job_id = 'master_rss_checker'
    # # Check every 15 minutes (adjust as needed)
    # check_interval_minutes = int(os.getenv('RSS_MASTER_CHECK_INTERVAL', '15'))
    # logger.info(f"Планирование мастер-задачи проверки RSS каждые {check_interval_minutes} минут.")
    # scheduler.add_job(
    #     services.rss_service.process_all_active_rss_feeds, # The task function
    #     'interval',
    #     minutes=check_interval_minutes,
    #     args=[bot, AsyncSessionLocal], # Pass dependencies to the task
    #     id=job_id,
    #     replace_existing=True,
    #     # next_run_time=datetime.datetime.now(scheduler.timezone) # Optional: run immediately on start
    # )
    # logger.info(f"Мастер-задача проверки RSS запланирована с ID: {job_id}.")
    # --- End of single periodic job model ---

    # --- Reverting to per-feed jobs as implemented in provided code, but fixing args ---
    # The provided code schedules _task_check_rss_feed per feed, this requires
    # scheduler.add_job(..., args=[bot, session_factory, rss_feed_id])
    # Ensure schedule_rss_check (and restore logic) passes bot correctly.

    # Возвращаем экземпляр планировщика
    return scheduler


# 5. Функции управления задачами (Посты)
async def schedule_post_publication(
    scheduler: AsyncIOScheduler,
    bot: 'Bot', # Aiogram Bot instance
    session_factory: Callable[..., AsyncSession], # services.db.AsyncSessionLocal
    post_id: int,
    run_date: Optional[datetime.datetime] = None,
    cron_params: Optional[Dict[str, Any]] = None
):
    """
    Планирует или обновляет задачу публикации поста.

    Args:
        scheduler: Экземпляр APScheduler.
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        post_id: ID поста для публикации.
        run_date: Дата и время для одноразового запуска. Используется для DateTrigger.
                  Должен быть с учетом таймзоны (preferably timezone-aware).
                  Если наивный, будет локализован с использованием таймзоны планировщика.
        cron_params: Словарь параметров для запуска по расписанию (cron).
                     Используется для CronTrigger.

    Raises:
        ValueError: Если не указаны ни run_date, ни cron_params, или cron_params некорректны.
        Exception: В случае ошибок при добавлении задачи в планировщик.
    """
    job_id = f'post_publish_{post_id}'
    # Аргументы, которые будут переданы в функцию _task_publish_post при ее выполнении.
    # Pass bot and session_factory to the task.
    args = [bot, session_factory, post_id]
    # scheduler_instance is needed *inside* _task_publish_post to schedule deletion task
    kwargs = {'scheduler_instance': scheduler}

    trigger = None
    if run_date:
        # Одноразовый запуск
        # Ensure run_date is timezone-aware. If not, localize using scheduler's timezone.
        if run_date.tzinfo is None:
            logger.warning(f"run_date для поста {post_id} не содержит таймзону. Локализую с использованием таймзоны планировщика ({scheduler.timezone}).")
            # Make naive datetime aware in the scheduler's timezone
            run_date = scheduler.timezone.localize(run_date)
        else:
             # Convert to scheduler's timezone if it's already aware but different
             try:
                  run_date = run_date.astimezone(scheduler.timezone)
             except Exception as tz_conv_e:
                  logger.warning(f"Ошибка при конвертации run_date поста {post_id} в таймзону планировщика ({scheduler.timezone}): {tz_conv_e}. Использую исходную таймзону.")
                  # Keep the original timezone-aware datetime


        trigger = DateTrigger(run_date)
        logger.info(f"Планирование одноразовой публикации поста {post_id} на {run_date.isoformat()} с job_id: {job_id}")
    elif cron_params:
        # Запуск по расписанию (cron)
        try:
            # _parse_cron_params maps user format to APScheduler format
            cron_args = _parse_cron_params(cron_params)
            # CronTrigger uses the scheduler's timezone by default if not specified explicitly in args
            trigger = CronTrigger(**cron_args, timezone=scheduler.timezone) # Explicitly set timezone
            logger.info(f"Планирование публикации поста {post_id} по CRON расписанию {cron_params} (args: {cron_args}) с job_id: {job_id}")
        except ValueError as e:
            logger.error(f"Ошибка при парсинге CRON параметров для поста {post_id}: {e}")
            raise # Пробрасываем ошибку, если параметры CRON некорректны
    else:
        raise ValueError("Необходимо указать run_date или cron_params для планирования публикации.")

    try:
        # Добавляем или заменяем задачу. replace_existing=True удобен для перепланирования.
        scheduler.add_job(
            _task_publish_post, # The function to run
            trigger=trigger,
            args=args, # Positional arguments for the function
            kwargs=kwargs, # Keyword arguments for the function
            id=job_id, # Unique identifier for the job
            replace_existing=True # Replace job if ID already exists
        )
        logger.info(f"Задача публикации поста {post_id} успешно добавлена/обновлена.")
    except Exception as e:
        logger.exception(f"Ошибка при добавлении/обновлении задачи публикации поста {post_id}: {e}")
        # Важно: В реальном приложении здесь может потребоваться логика обработки ошибок,
        # например, обновление статуса поста на "scheduling_failed" в БД.


async def schedule_post_deletion(
    scheduler: AsyncIOScheduler,
    bot: 'Bot', # Aiogram Bot instance
    session_factory: Callable[..., AsyncSession], # services.db.AsyncSessionLocal
    post_id: int,
    deletion_time: datetime.datetime
    # No need for json strings here, task fetches from DB
    # original_message_ids_json: str, # JSON string of message IDs
    # target_chat_ids_json: str # JSON string of chat IDs
):
    """
    Планирует или обновляет задачу удаления поста.

    Args:
        scheduler: Экземпляр APScheduler.
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        post_id: ID поста для удаления.
        deletion_time: Дата и время для удаления. Должен быть с учетом таймзоны.
                       Если наивный, будет локализован с использованием таймзоны планировщика.

    Raises:
         Exception: В случае ошибок при добавлении задачи в планировщик.
    """
    job_id = f'post_delete_{post_id}'
    # Аргументы, которые будут переданы в функцию _task_delete_post при ее выполнении
    # Pass bot, session_factory, and post_id
    args = [bot, session_factory, post_id]
    # kwargs are empty for deletion task as it doesn't schedule further tasks

    # Одноразовый запуск на указанное время
    # Ensure deletion_time is timezone-aware. If not, localize using scheduler's timezone.
    if deletion_time.tzinfo is None:
        logger.warning(f"deletion_time для поста {post_id} не содержит таймзону. Локализую с использованием таймзоны планировщика ({scheduler.timezone}).")
        # Make naive datetime aware in the scheduler's timezone
        deletion_time = scheduler.timezone.localize(deletion_time)
    else:
         # Convert to scheduler's timezone if it's already aware but different
         try:
              deletion_time = deletion_time.astimezone(scheduler.timezone)
         except Exception as tz_conv_e:
              logger.warning(f"Ошибка при конвертации deletion_time поста {post_id} в таймзону планировщика ({scheduler.timezone}): {tz_conv_e}. Использую исходную таймзону.")
              # Keep the original timezone-aware datetime


    trigger = DateTrigger(deletion_time)
    logger.info(f"Планирование задачи удаления поста {post_id} на {deletion_time.isoformat()} с job_id: {job_id}")

    try:
        scheduler.add_job(
            _task_delete_post, # The function to run
            trigger=trigger,
            args=args, # Positional arguments
            id=job_id, # Unique ID
            replace_existing=True # Replace existing deletion job for this post
        )
        logger.info(f"Задача удаления поста {post_id} успешно добавлена/обновлена.")
    except Exception as e:
        logger.exception(f"Ошибка при добавлении/обновлении задачи удаления поста {post_id}: {e}")
        # Here also, consider updating post status to indicate scheduling failure.


# --- НОВЫЕ ФУНКЦИИ УПРАВЛЕНИЯ ЗАДАЧАМИ (RSS) ---
async def schedule_rss_check(
    scheduler: AsyncIOScheduler,
    bot: 'Bot', # Add bot instance
    session_factory: Callable[..., AsyncSession],
    rss_feed_id: int,
    frequency_minutes: int
):
    """
    Планирует или обновляет задачу проверки RSS-ленты.

    Args:
        scheduler: Экземпляр APScheduler.
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
        rss_feed_id: ID RSS-ленты для проверки.
        frequency_minutes: Частота проверки в минутах.

    Raises:
         ValueError: Если frequency_minutes некорректно (<= 0).
         Exception: В случае ошибок при добавлении задачи в планировщик.
    """
    # Minimum frequency should be enforced (e.g., 5 minutes)
    MIN_RSS_FREQUENCY_MINUTES = int(os.getenv('RSS_MIN_FREQ', '5'))
    if frequency_minutes < MIN_RSS_FREQUENCY_MINUTES:
        raise ValueError(f"Некорректная частота проверки для RSS-ленты {rss_feed_id}: {frequency_minutes} минут. Должно быть не менее {MIN_RSS_FREQUENCY_MINUTES}.")

    job_id = f'rss_check_{rss_feed_id}'
    # Аргументы, которые будут переданы в функцию _task_check_rss_feed
    # Pass bot, session_factory, and rss_feed_id
    args = [bot, session_factory, rss_feed_id]

    # Триггер для повторяющегося запуска через интервал времени
    trigger = IntervalTrigger(minutes=frequency_minutes, timezone=scheduler.timezone) # Set timezone on trigger

    logger.info(f"Планирование задачи проверки RSS-ленты {rss_feed_id} с частотой {frequency_minutes} мин. с job_id: {job_id}")

    try:
        # Add or replace the job for this specific RSS feed
        scheduler.add_job(
            _task_check_rss_feed, # The function to run
            trigger=trigger,
            args=args, # Positional arguments
            id=job_id, # Unique ID per feed
            replace_existing=True # Replace existing job for this feed
        )
        logger.info(f"Задача проверки RSS-ленты {rss_feed_id} успешно добавлена/обновлена.")
    except Exception as e:
        logger.exception(f"Ошибка при добавлении/обновлении задачи проверки RSS-ленты {rss_feed_id}: {e}")
        # In real app, consider updating RSS feed status in DB to indicate scheduling failure.


async def reschedule_rss_check(
    scheduler: AsyncIOScheduler,
    bot: 'Bot',
    session_factory: Callable[..., AsyncSession],
    rss_feed_id: int,
    new_frequency_minutes: int
):
    """
    Перепланирует задачу проверки RSS-ленты с новой частотой.
    По сути, просто вызывает schedule_rss_check с флагом replace_existing=True.
    """
    logger.info(f"Запрос на перепланирование проверки RSS-ленты {rss_feed_id} на частоту {new_frequency_minutes} мин.")
    # schedule_rss_check already uses replace_existing=True
    await schedule_rss_check(scheduler, bot, session_factory, rss_feed_id, new_frequency_minutes)
    logger.info(f"Проверка RSS-ленты {rss_feed_id} перепланирована.")


async def remove_scheduled_job(scheduler: AsyncIOScheduler, job_id: str):
    """
    Удаляет запланированную задачу по ее ID.

    Args:
        scheduler: Экземпляр APScheduler.
        job_id: ID задачи для удаления.
    """
    logger.info(f"Попытка удаления задачи с job_id: {job_id}")
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Задача с job_id: {job_id} успешно удалена.")
    except JobLookupError:
        # This is normal if the job already completed (one-time) or was removed earlier
        logger.debug(f"Задача с job_id: {job_id} не найдена в планировщике (возможно, уже выполнена или удалена).")
    except Exception as e:
        logger.exception(f"Ошибка при удалении задачи с job_id: {job_id}: {e}")

# Функции перепланирования постов используют replace_existing=True в schedule_post_publication/deletion.
# Явные функции reschedule_post_publication/deletion сохранены для единообразия и ясности API.

async def reschedule_post_publication(
    scheduler: AsyncIOScheduler,
    bot: 'Bot',
    session_factory: Callable[..., AsyncSession],
    post_id: int,
    run_date: Optional[datetime.datetime] = None,
    cron_params: Optional[Dict[str, Any]] = None
):
    """
    Перепланирует публикацию поста. По сути, просто вызывает schedule_post_publication
    с флагом replace_existing=True.
    """
    logger.info(f"Запрос на перепланирование публикации поста {post_id}.")
    # schedule_post_publication already uses replace_existing=True
    await schedule_post_publication(scheduler, bot, session_factory, post_id, run_date, cron_params)
    logger.info(f"Публикация поста {post_id} перепланирована.")


async def reschedule_post_deletion(
    scheduler: AsyncIOScheduler,
    bot: 'Bot',
    session_factory: Callable[..., AsyncSession],
    post_id: int,
    deletion_time: datetime.datetime
    # No need for json strings here
    # original_message_ids_json: str,
    # target_chat_ids_json: str
):
    """
    Перепланирует удаление поста. По сути, просто вызывает schedule_post_deletion
    с флагом replace_existing=True.
    """
    logger.info(f"Запрос на перепланирование удаления поста {post_id}.")
    # schedule_post_deletion already uses replace_existing=True
    await schedule_post_deletion(scheduler, bot, session_factory, post_id, deletion_time)
    logger.info(f"Удаление поста {post_id} перепланировано.")


# 6. Функция восстановления задач
async def restore_scheduled_jobs(
    scheduler: AsyncIOScheduler,
    bot: 'Bot', # Aiogram Bot instance - нужен для восстановления задач публикации/удаления постов и RSS
    session_factory: Callable[..., AsyncSession], # services.db.AsyncSessionLocal
):
    """
    Восстанавливает задачи планировщика на основе статуса постов и активных RSS-лент в БД при запуске приложения.
    Заново планирует задачи, если они отсутствуют в хранилище планировщика.

    Args:
        scheduler: Экземпляр APScheduler.
        bot: Экземпляр Aiogram Bot.
        session_factory: Фабрика асинхронных сессий SQLAlchemy.
    """
    logger.info("Начало восстановления запланированных задач из БД.")
    async with session_factory() as session:
        try:
            # 1. Восстановление задач публикации для постов со статусом 'scheduled'
            # Include 'pending_reschedule' status? Yes, in get_all_posts_for_scheduling default.
            scheduled_posts: List['Post'] = await get_all_posts_for_scheduling(session, statuses=["scheduled", "pending_reschedule"])
            logger.info(f"Найдено {len(scheduled_posts)} постов со статусом 'scheduled'/'pending_reschedule' для восстановления публикации.")
            for post in scheduled_posts:
                publish_job_id = f'post_publish_{post.id}'
                existing_job = scheduler.get_job(publish_job_id)

                if not existing_job:
                    logger.warning(f"Задача публикации для поста {post.id} (ID: {publish_job_id}) отсутствует в планировщике. Попытка восстановления.")
                    try:
                        # Check if post has necessary scheduling info
                        if post.schedule_type == 'one_time' and post.run_date:
                            await schedule_post_publication(
                                scheduler, bot, session_factory, post.id, run_date=post.run_date
                            )
                        elif post.schedule_type == 'recurring' and post.schedule_params:
                            # validate_cron_params check can be added here for robustness
                            await schedule_post_publication(
                                scheduler, bot, session_factory, post.id, cron_params=post.schedule_params
                            )
                        else:
                            logger.error(f"Не удалось восстановить задачу публикации для поста {post.id}: Отсутствуют необходимые параметры расписания (run_date или schedule_params/type) в БД. Статус: {post.status}.")
                            # Optionally: update post status to 'scheduling_error'
                            # post.status = 'scheduling_error'
                            # await session.commit()

                    except ValueError as e:
                        logger.error(f"Не удалось восстановить задачу публикации для поста {post.id} из-за некорректных CRON параметров в БД: {post.schedule_params}. Ошибка: {e}")
                        # Optionally: обновить статус поста на 'scheduling_error'
                        # post.status = 'scheduling_error'
                        # await session.commit()
                    except Exception as e:
                         logger.exception(f"Ошибка при планировании задачи публикации для поста {post.id} во время восстановления: {e}")
                         # Optionally: обновить статус поста на 'scheduling_error'
                         # post.status = 'scheduling_error'
                         # await session.commit()


            # 2. Восстановление задач удаления для постов со статусом 'sent' и заданным delete_after_seconds
            # These posts must have sent_message_data and delete_after_seconds > 0.
            sent_posts_needing_deletion_check: List['Post'] = await get_all_posts_for_scheduling(session, statuses=["sent", "deletion_failed", "deletion_error", "deletion_skipped"]) # Include failed deletion states too
            sent_posts_needing_deletion = [
                p for p in sent_posts_needing_deletion_check
                if p.delete_after_seconds is not None and p.delete_after_seconds > 0
                and p.sent_message_data # Ensure sent_message_data is not None/empty
            ]
            logger.info(f"Найдено {len(sent_posts_needing_deletion)} постов со статусом 'sent'/etc. и заданным временем удаления для проверки восстановления задачи удаления.")

            # Need to recalculate the deletion time based on the original sent time.
            # If sent_at field existed in Post, it would be used.
            # Since it doesn't, APScheduler can reschedule based on the time the *original* job was supposed to run.
            # However, if the scheduler crashed *after* the post was sent but *before* the deletion job was added,
            # we need to calculate deletion_time = <time_of_sending> + delete_after_seconds.
            # Using post.updated_at as a proxy for sent time (if updated on send) or current time is inaccurate.
            # The most robust way without a `sent_at` field is tricky.
            # A simplified approach for recovery is to schedule the deletion relative to NOW, IF the original scheduled time + deletion_seconds is in the future.
            # Or, just schedule relative to NOW + delete_after_seconds IF the original scheduled time was in the past.

            # Let's assume the original scheduled time (post.run_date for one_time, or next_run_time of the original job if recurring)
            # is the baseline for deletion_time calculation.
            # If original job ran, and deletion job wasn't scheduled, we use run_date + delete_after_seconds.
            # Need to find the NEXT_RUN_TIME of the *original publish job* if it was recurring and just fired.
            # This is getting complicated without `sent_at`.

            # Simpler approach for recovery: If status is 'sent' and deletion_seconds is set,
            # and NO deletion job exists for this post, check if deletion time (calculated from NOW + seconds)
            # is in the future. If so, schedule it. This might slightly shift the deletion time.
            # This isn't perfect but is a practical recovery strategy.

            now = datetime.datetime.now(scheduler.timezone) # Current time in scheduler's timezone

            for post in sent_posts_needing_deletion:
                 delete_job_id = f'post_delete_{post.id}'
                 existing_delete_job = scheduler.get_job(delete_job_id)

                 if not existing_delete_job:
                      # Attempt to schedule deletion ONLY IF the calculated time (relative to NOW) is in the future.
                      # This avoids scheduling deletion for posts whose deletion time already passed.
                      # If we had a sent_at field: deletion_time = post.sent_at + datetime.timedelta(seconds=post.delete_after_seconds)
                      # Using NOW: deletion_time = now + datetime.timedelta(seconds=post.delete_after_seconds)
                      # This calculation needs rethinking based on whether the original job already fired.

                      # A more robust recovery might check if the original publish job ran successfully
                      # and if a delete job was subsequently created. This is complex.

                      # Let's use a simple rule: If post is 'sent' and needs deletion, and no delete job exists,
                      # assume the original job ran (or was skipped/misfired), and schedule deletion relative to NOW
                      # IF the original intended deletion time was in the future.
                      # Original intended run time: post.run_date (for one_time). For recurring, it's the time of the specific run.
                      # This requires storing the *specific run time* for recurring posts if we want precise deletion.
                      # Without that, we have to approximate.

                      # Simplest pragmatic recovery: If 'sent', needs deletion, no delete job exists, schedule deletion relative to NOW.
                      # This means the deletion time will be NOW + delete_after_seconds, potentially later than originally intended.
                      # Let's refine this: Calculate deletion_time relative to NOW. If it's in the future, schedule it.
                      calculated_deletion_time_from_now = now + datetime.timedelta(seconds=post.delete_after_seconds)

                      if calculated_deletion_time_from_now > now:
                            logger.warning(f"Задача удаления для поста {post.id} отсутствует в планировщике. Попытка восстановления на {calculated_deletion_time_from_now.isoformat()} (расчет от текущего времени).")
                            # Pass post_id to deletion task. It will fetch sent_message_data from DB.
                            await schedule_post_deletion(
                                scheduler, bot, session_factory, post.id,
                                deletion_time=calculated_deletion_time_from_now
                            )
                      else:
                           logger.warning(f"Задача удаления для поста {post.id} отсутствует, но рассчитанное время удаления ({calculated_deletion_time_from_now.isoformat()} от NOW) уже в прошлом. Задача не будет восстановлена.")
                           # Optionally, update status to 'deletion_restore_failed'
                           # post.status = 'deletion_restore_failed'
                           # await session.commit()

                 # else:
                 #    logger.debug(f"Задача удаления для поста {post.id} (ID: {delete_job_id}) уже существует.")

            # 3. Восстановление задач проверки RSS-лент для активных лент
            # These are per-feed jobs calling _task_check_rss_feed
            active_rss_feeds: List['RssFeed'] = await get_all_active_rss_feeds(session)
            logger.info(f"Найдено {len(active_rss_feeds)} активных RSS-лент для восстановления проверки.")
            for feed in active_rss_feeds:
                 rss_check_job_id = f'rss_check_{feed.id}'
                 existing_rss_job = scheduler.get_job(rss_check_job_id)

                 # Check if job exists AND frequency is valid (non-positive frequency means no scheduling)
                 if not existing_rss_job:
                     MIN_RSS_FREQUENCY_MINUTES = int(os.getenv('RSS_MIN_FREQ', '5'))
                     if feed.frequency_minutes is not None and feed.frequency_minutes >= MIN_RSS_FREQUENCY_MINUTES:
                         logger.warning(f"Задача проверки RSS-ленты {feed.id} (URL: {feed.feed_url}, ID: {rss_check_job_id}) отсутствует в планировщике. Попытка восстановления.")
                         try:
                             # schedule_rss_check needs bot, session_factory, feed_id, frequency_minutes
                             await schedule_rss_check(
                                 scheduler, bot, session_factory, feed.id, feed.frequency_minutes
                             )
                         except ValueError as e:
                             logger.error(f"Не удалось восстановить задачу проверки RSS-ленты {feed.id} из-за некорректной частоты в БД ({feed.frequency_minutes} мин.): {e}")
                             # Optionally: обновить статус ленты на 'scheduling_error' if RssFeed model has status
                             # if hasattr(feed, 'status'): feed.status = 'scheduling_error'
                         except Exception as e:
                             logger.exception(f"Ошибка при планировании задачи проверки RSS-ленты {feed.id} во время восстановления: {e}")
                             # Optionally: обновить статус ленты на 'scheduling_error'
                             # if hasattr(feed, 'status'): feed.status = 'scheduling_error'

                     else:
                         logger.error(f"Не удалось восстановить задачу проверки RSS-ленты {feed.id}: Некорректная или отсутствующая частота проверки ({feed.frequency_minutes} мин.) в БД.")
                         # Optionally: обновить статус ленты на 'scheduling_error'
                         # if hasattr(feed, 'status'): feed.status = 'scheduling_error'

                 # else:
                 #     logger.debug(f"Задача проверки RSS-ленты {feed.id} (ID: {rss_check_job_id}) уже существует.")

            # Commit any status updates made during recovery (e.g., scheduling_error)
            await session.commit() # Commit any changes made in this session

        except Exception as e:
            logger.exception(f"Критическая ошибка при восстановлении задач планировщика из БД: {e}")
            # In case of critical failure, the session might be invalid.
            # Rely on the `async with session_factory()` context manager for rollback if needed.


    logger.info("Восстановление запланированных задач завершено.")

