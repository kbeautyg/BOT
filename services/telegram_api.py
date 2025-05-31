# services/telegram_api.py

import logging
from typing import List, Optional, Union, Dict, Any, Tuple

from aiogram import Bot
from aiogram.types import Message, InputMedia, InputMediaPhoto, InputMediaVideo, InputMediaDocument, Chat, ChatMember
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, AiogramError, MessageToDeleteNotFound, MessageCantBeDeleted
from aiogram.utils.markdown import escape_md # Импорт для экранирования MarkdownV2

# Настройка логирования
logger = logging.getLogger(__name__)

# Максимальная длина подписи для send_media_group
# Официальное ограничение Telegram для sendMediaGroup caption: 1024 символа
# https://core.telegram.org/bots/api#inputmediaphoto
# https://core.telegram.org/bots/api#inputmediavideo
# https://core.telegram.org/bots/api#inputmediadocument
MAX_MEDIA_GROUP_CAPTION_LENGTH = 1024

async def send_post_content(
    bot: Bot,
    chat_id: Union[int, str],
    text: Optional[str] = None,
    media_items: Optional[List[InputMedia]] = None,
    parse_mode: Optional[str] = ParseMode.HTML,
    reply_markup: Optional[Any] = None # aiogram InlineKeyboardMarkup, ReplyKeyboardMarkup, etc.
) -> List[Message]:
    """
    Отправляет контент поста (текст, медиа, медиагруппу) в указанный чат.

    Гибко обрабатывает наличие текста и списка медиафайлов.
    Если есть медиа:
        - Если один элемент: отправляет его с текстом как подписью.
        - Если более одного элемента: отправляет как медиагруппу. Если текст слишком длинный
          для подписи медиагруппы (более 1024 символов), отправляет его отдельным сообщением
          ПЕРЕД медиагруппой.
    Если медиа нет:
        - Отправляет только текст как обычное сообщение.

    Args:
        bot: Экземпляр Bot aiogram.
        chat_id: ID чата или его username (например, '@channelusername').
        text: Необязательный текст поста.
        media_items: Необязательный список объектов InputMedia (InputMediaPhoto, InputMediaVideo и т.д.).
        parse_mode: Режим парсинга текста (например, 'HTML', 'MarkdownV2'). По умолчанию 'HTML'.
        reply_markup: Необязательная разметка клавиатуры. Применяется к текстовому сообщению
                      или к первому элементу медиагруппы/одиночному медиа.

    Returns:
        Список объектов Message, которые были отправлены.
        Возвращает пустой список в случае ошибки или если ничего не было отправлено.
    """
    sent_messages: List[Message] = []
    log_prefix = f"send_post_content to chat {chat_id}:"

    # Ensure chat_id is string for sending to channels/groups by username or ID
    chat_id_str = str(chat_id)

    if not text and not media_items:
        logger.warning(f"{log_prefix} Ни текста, ни медиа не предоставлено. Ничего не отправлено.")
        return sent_messages # Возвращаем пустой список, так как ничего не отправлено

    try:
        if media_items:
            # Есть медиа
            if len(media_items) == 1:
                # Отправка одиночного медиа с подписью
                logger.info(f"{log_prefix} Отправка одиночного медиа (тип: {type(media_items[0]).__name__}).")
                # Назначаем текст как подпись
                media_items[0].caption = text
                media_items[0].parse_mode = parse_mode
                # Клавиатура только для одиночного медиа
                # aiogram>=3.0 send_photo/video/document methods accept reply_markup directly
                # The reply_markup in InputMediaPhoto/Video is specifically for `reply_markup` inside `send_media_group`
                # For single media, pass it to the send_* method.

                if isinstance(media_items[0], InputMediaPhoto):
                    message = await bot.send_photo(
                        chat_id=chat_id_str,
                        photo=media_items[0].media,
                        caption=media_items[0].caption,
                        parse_mode=media_items[0].parse_mode,
                        reply_markup=reply_markup # Apply markup here
                    )
                    sent_messages.append(message)

                elif isinstance(media_items[0], InputMediaVideo):
                     # aiogram send_video takes media as 'video' argument
                     message = await bot.send_video(
                        chat_id=chat_id_str,
                        video=media_items[0].media,
                        caption=media_items[0].caption,
                        parse_mode=media_items[0].parse_mode,
                        reply_markup=reply_markup, # Apply markup here
                        duration=getattr(media_items[0], 'duration', None),
                        width=getattr(media_items[0], 'width', None),
                        height=getattr(media_items[0], 'height', None),
                        thumbnail=getattr(media_items[0], 'thumbnail', None)
                    )
                     sent_messages.append(message)

                elif isinstance(media_items[0], InputMediaDocument):
                     # aiogram send_document takes media as 'document' argument
                     message = await bot.send_document(
                        chat_id=chat_id_str,
                        document=media_items[0].media,
                        caption=media_items[0].caption,
                        parse_mode=media_items[0].parse_mode,
                        reply_markup=reply_markup, # Apply markup here
                        thumbnail=getattr(media_items[0], 'thumbnail', None)
                    )
                     sent_messages.append(message)
                # Add other single media types (audio, voice, etc.) if supported/needed by the bot
                # elif isinstance(media_items[0], InputMediaAudio): ...

                else:
                    logger.error(f"{log_prefix} Неподдерживаемый тип InputMedia для одиночной отправки: {type(media_items[0]).__name__}")
                    # Close file handle if it was opened for this unsupported type
                    if hasattr(media_items[0], 'media') and hasattr(media_items[0].media, 'close'):
                         try: media_items[0].media.close()
                         except Exception as e: logger.warning(f"Error closing file handle for unsupported media: {e}")
                    return sent_messages # Возвращаем пустой список при ошибке

            else:
                # Отправка медиагруппы
                logger.info(f"{log_prefix} Отправка медиагруппы из {len(media_items)} элементов.")
                group_caption = text
                separate_text_message = None
                media_group_markup = None # Markup for the media group (usually attached to the first item)

                # Проверяем длину текста для подписи медиагруппы
                if text and len(text) > MAX_MEDIA_GROUP_CAPTION_LENGTH:
                    logger.warning(f"{log_prefix} Текст ({len(text)} символов) слишком длинный для подписи медиагруппы (макс {MAX_MEDIA_GROUP_CAPTION_LENGTH}). Отправляем текст отдельным сообщением ПЕРЕД медиагруппой.")
                    group_caption = None # Убираем текст из подписи медиагруппы
                    # Отправляем текст отдельным сообщением перед медиагруппой
                    try:
                        separate_text_message = await bot.send_message(
                            chat_id=chat_id_str,
                            text=text,
                            parse_mode=parse_mode,
                            reply_markup=reply_markup # Клавиатура для отдельного текстового сообщения
                        )
                        sent_messages.append(separate_text_message)
                    except TelegramAPIError as e:
                         logger.error(f"{log_prefix} Ошибка при отправке отдельного текстового сообщения перед медиагруппой: {e}")
                         # Continue sending media group without text if separate text fails
                    except Exception as e:
                         logger.exception(f"{log_prefix} Неизвестная ошибка при отправке отдельного текстового сообщения перед медиагруппой: {e}")
                         # Continue sending media group without text


                # Назначаем подпись первому элементу медиагруппы, если она есть и не была отправлена отдельно
                if group_caption:
                    # Note: Only the first element's caption is displayed for the whole group.
                    # https://core.telegram.org/bots/api#sendmediagroup
                    # Ensure text is escaped according to the parse_mode for the caption.
                    # If parse_mode is HTML, basic HTML can be in the caption. If MarkdownV2, need to escape.
                    # Let's assume parse_mode applies to the caption.
                    media_items[0].caption = group_caption
                    media_items[0].parse_mode = parse_mode
                    # Markup for media group is also attached to the first item.
                    # https://core.telegram.org/bots/api#sendmediagroup says reply_markup is ignored for sendMediaGroup itself,
                    # but InlineKeyboardMarkup can be attached to *individual* InputMedia elements.
                    # The common pattern is to attach the main inline keyboard to the *first* media item.
                    media_items[0].reply_markup = reply_markup if separate_text_message is None else None # Apply markup to first media if no separate text

                # Ensure other items don't have captions/markup if they shouldn't
                # According to Telegram API docs, only the first caption/markup matters for the group.
                # To be safe and avoid unexpected behavior with future API changes,
                # explicitly remove captions/markup from subsequent items unless needed.
                # for item in media_items[1:]:
                #      item.caption = None
                #      item.parse_mode = None
                #      item.reply_markup = None

                # Отправка медиагруппы
                # send_media_group returns a list of Message objects
                group_messages = await bot.send_media_group(
                    chat_id=chat_id_str,
                    media=media_items,
                )
                sent_messages.extend(group_messages)

        elif text:
            # Нет медиа, отправляем только текст
            logger.info(f"{log_prefix} Отправка текстового сообщения.")
            message = await bot.send_message(
                chat_id=chat_id_str,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
            sent_messages.append(message)

    except TelegramAPIError as e:
        logger.error(f"{log_prefix} Ошибка Telegram API: {e}")
        # In case of Telegram API error, we might get a partial list or an empty list.
        # The caller needs to check if the returned list is empty.
        # Depending on error type (e.g., BOT_BLOCKED, CHAT_NOT_FOUND, MESSAGE_TOO_LONG),
        # more specific handling might be needed at the calling site (e.g., update post status).
        sent_messages = [] # Ensure empty list on critical error
    except AiogramError as e:
        logger.error(f"{log_prefix} Ошибка Aiogram: {e}")
        sent_messages = [] # Ensure empty list on critical error
    except Exception as e:
        logger.exception(f"{log_prefix} Неожиданная ошибка: {e}")
        sent_messages = [] # Ensure empty list on critical error

    if sent_messages:
        logger.info(f"{log_prefix} Успешно отправлено {len(sent_messages)} сообщение(ий).")
    return sent_messages


async def delete_telegram_messages(
    bot: Bot,
    chat_id: Union[int, str],
    message_ids: List[int]
) -> bool:
    """
    Удаляет список сообщений в указанном чате по их ID.

    Обрабатывает исключения, связанные с Telegram API, особенно MessageToDeleteNotFound
    и MessageCantBeDeleted.

    Args:
        bot: Экземпляр Bot aiogram.
        chat_id: ID чата или его username.
        message_ids: Список ID сообщений для удаления.

    Returns:
        True, если все сообщения были успешно удалены или если сообщение не было найдено
        (что тоже считается успехом с точки зрения достижения конечного состояния - сообщения нет).
        False, если произошла другая ошибка Telegram API или AiogramError при попытке удаления
        хотя бы одного сообщения, которую не удалось обработать (например, нет прав).
    """
    if not message_ids:
        logger.info(f"delete_telegram_messages for chat {chat_id}: Список message_ids пуст. Ничего удалять.")
        return True # Ничего не нужно удалять, считаем успехом

    # Ensure chat_id is string for sending to channels/groups by username or ID
    chat_id_str = str(chat_id)

    logger.info(f"delete_telegram_messages for chat {chat_id_str}: Попытка удалить {len(message_ids)} сообщение(ий).")
    all_successful = True

    # Telegram API позволяет удалять сообщения по одному ID
    # Для пакетного удаления нужно вызывать deleteMessage для каждого ID.
    # Ограничение: не старше 48 часов в супергруппах/каналах, в личных чатах - без ограничений.
    # У бота должны быть права на удаление сообщений.
    # В супергруппах и каналах бот должен быть администратором с правом CanDeleteMessages.

    for message_id in message_ids:
        try:
            # bot.delete_message возвращает True в случае успеха
            await bot.delete_message(chat_id=chat_id_str, message_id=message_id)
            logger.debug(f"delete_telegram_messages for chat {chat_id_str}: Сообщение {message_id} успешно удалено.")
        except MessageToDeleteNotFound:
            # Это не ошибка, сообщение уже удалено или никогда не существовало.
            logger.warning(f"delete_telegram_messages for chat {chat_id_str}: Сообщение {message_id} не найдено или уже удалено.")
            # all_successful remains True for this specific message
        except MessageCantBeDeleted:
             # У бота нет прав или сообщение старше 48 часов в супергруппе/канале
             logger.error(f"delete_telegram_messages for chat {chat_id_str}: Не удалось удалить сообщение {message_id}. Возможно, нет прав или сообщение слишком старое.")
             all_successful = False # Отмечаем, что одно удаление не удалось
        except TelegramAPIError as e:
            # Другие ошибки Telegram API
            logger.error(f"delete_telegram_messages for chat {chat_id_str}: Ошибка Telegram API при удалении сообщения {message_id}: {e}")
            all_successful = False # Отмечаем, что одно удаление не удалось
        except AiogramError as e:
            # Ошибки Aiogram
            logger.error(f"delete_telegram_messages for chat {chat_id_str}: Ошибка Aiogram при удалении сообщения {message_id}: {e}")
            all_successful = False
        except Exception as e:
            # Неожиданные ошибки
            logger.exception(f"delete_telegram_messages for chat {chat_id_str}: Неожиданная ошибка при удалении сообщения {message_id}: {e}")
            all_successful = False

    logger.info(f"delete_telegram_messages for chat {chat_id_str}: Попытка удаления завершена. Все сообщения удалены/не найдены: {all_successful}.")
    return all_successful


async def get_chat_member(
    bot: Bot,
    chat_id: Union[int, str],
    user_id: int
) -> Optional[ChatMember]:
    """
    Получает информацию об участнике чата.

    Args:
        bot: Экземпляр Bot aiogram.
        chat_id: ID чата или его username.
        user_id: ID пользователя, информацию о котором нужно получить.

    Returns:
        Объект ChatMember, если пользователь найден в чате, иначе None.
        Возвращает None также в случае ошибок Telegram API (например, USER_NOT_IN_CHAT).
    """
    # Ensure chat_id is string
    chat_id_str = str(chat_id)
    log_prefix = f"get_chat_member for chat {chat_id_str}, user {user_id}:"
    logger.debug(f"{log_prefix} Запрос информации об участнике чата.")
    try:
        chat_member = await bot.get_chat_member(chat_id=chat_id_str, user_id=user_id)
        # logger.debug(f"{log_prefix} Информация получена успешно.") # Too noisy
        return chat_member
    except TelegramAPIError as e:
        # Examples: USER_NOT_IN_CHAT, CHAT_NOT_FOUND, BOT_ADMIN_REQUIRED, BAD_REQUEST
        # Some errors are expected (like USER_NOT_IN_CHAT if user is not in group/channel)
        # Handle USER_NOT_IN_CHAT specifically if needed, otherwise just log and return None
        logger.warning(f"{log_prefix} Ошибка Telegram API при получении участника: {e}")
        return None
    except AiogramError as e:
        logger.error(f"{log_prefix} Ошибка Aiogram при получении участника: {e}")
        return None
    except Exception as e:
        logger.exception(f"{log_prefix} Неожиданная ошибка при получении участника: {e}")
        return None


async def get_chat(
    bot: Bot,
    chat_id: Union[int, str]
) -> Optional[Chat]:
    """
    Получает информацию о чате (группе, канале, личном чате).

    Args:
        bot: Экземпляр Bot aiogram.
        chat_id: ID чата или его username.

    Returns:
        Объект Chat, если чат найден, иначе None.
        Возвращает None также в случае ошибок Telegram API.
    """
    # Ensure chat_id is string
    chat_id_str = str(chat_id)
    log_prefix = f"get_chat for chat {chat_id_str}:"
    logger.debug(f"{log_prefix} Запрос информации о чате.")
    try:
        chat = await bot.get_chat(chat_id=chat_id_str)
        # logger.debug(f"{log_prefix} Информация о чате получена успешно.") # Too noisy
        return chat
    except TelegramAPIError as e:
        # Examples: CHAT_NOT_FOUND, BOT_ADMIN_REQUIRED, BAD_REQUEST
        logger.warning(f"{log_prefix} Ошибка Telegram API при получении чата: {e}")
        return None
    except AiogramError as e:
        logger.error(f"{log_prefix} Ошибка Aiogram при получении чата: {e}")
        return None
    except Exception as e:
        logger.exception(f"{log_prefix} Неожиданная ошибка при получении чата: {e}")
        return None

async def get_bot_channels_for_user(bot: Bot, user_id: int) -> List[Dict[str, Union[int, str]]]:
    """
    (ЗАГЛУШКА) Получает список каналов/групп, где бот является администратором,
    и пользователь может в них публиковать (подразумевается, что пользователь владелец или админ бота).

    В реальной реализации может потребоваться:
    1. Получить список всех чатов, в которых бот является администратором.
    2. Для каждого чата проверить права бота (CanPostMessages, CanEditMessages, CanDeleteMessages).
    3. Опционально проверить, является ли текущий пользователь (user_id) создателем/владельцем этих чатов
       или является администратором бота (например, по ADMIN_USER_ID из .env).
       API Telegram не предоставляет простого способа узнать, является ли конкретный пользователь
       админом в каждом чате, или какие чаты принадлежат пользователю.
       Обычно, бот просто работает в чатах, куда его добавил пользователь, и пользователь
       предполагается имеющим право настраивать публикации через бота.
       Более надежный способ - хранить список разрешенных чатов для каждого пользователя в БД.

    Сейчас возвращает пустой список.

    Args:
        bot: Экземпляр Aiogram Bot.
        user_id: ID пользователя, для которого запрашивается список.

    Returns:
        Список словарей [{'id': chat_id, 'name': chat_name}] для чатов, куда бот может публиковать.
    """
    logger.warning(f"Using stub implementation for get_bot_channels_for_user for user {user_id}. No channels returned.")
    # TODO: Implement actual logic to retrieve channels/groups where bot is admin with required permissions.
    # This would likely involve:
    # 1. Storing a list of chats the user wants to use with the bot (e.g., user adds chat IDs via command/flow).
    # 2. Bot attempts to get chat member info for itself in those chats to check its admin status and permissions.
    # 3. Returning only chats where bot is admin and has necessary permissions (send messages, etc.).

    # Placeholder example returning a dummy channel (replace with actual logic)
    # return [{'id': -1001234567890, 'name': 'Мой тестовый канал'}]
    # return [{'id': 'MY_TEST_CHANNEL_USERNAME', 'name': 'Мой тестовый канал'}]
    return []



