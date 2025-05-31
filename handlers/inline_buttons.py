# handlers/inline_buttons.py

import logging

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext # Необходим для управления состоянием FSM при навигации
from sqlalchemy.ext.asyncio import AsyncSession # Необходим для работы с базой данных
from apscheduler.schedulers.asyncio import AsyncIOScheduler # Необходим для работы с планировщиком задач
from apscheduler.jobstores.base import JobLookupError # Для обработки случая, когда задача не найдена

# Импорт CallbackData и клавиатур из проекта
from keyboards.inline_keyboards import (
    DeleteCallbackData,
    NavigationCallbackData,
    get_post_management_keyboard, # Может потребоваться для отмены удаления
)
# Импорт функций работы с БД и планировщиком
from services.db import delete_post_by_id, get_post_by_id # get_post_by_id нужен для получения данных поста при отмене, если требуется
from services.scheduler import remove_scheduled_job

# Настройка логирования
logger = logging.getLogger(__name__)

# Инициализация роутера
inline_buttons_router = Router()

# --- Обработчики для PostCallbackData (через DeleteCallbackData для подтверждения) ---

# Обработчик подтверждения удаления поста
# Используем DeleteCallbackData, как определено в keyboards/inline_keyboards.py для подтверждения
@inline_buttons_router.callback_query(DeleteCallbackData.filter(F.action == "confirm" and F.item_type == "post"))
async def process_confirm_post_delete(
    callback: CallbackQuery,
    callback_data: DeleteCallbackData,
    session: AsyncSession, # Инъекция асинхронной сессии SQLAlchemy
    scheduler: AsyncIOScheduler # Инъекция экземпляра планировщика APScheduler
):
    """
    Обрабатывает подтверждение удаления поста из Telegram и БД.

    Args:
        callback: Объект CallbackQuery.
        callback_data: Распакованные данные DeleteCallbackData.
        session: Сессия БД, предоставленная через DI.
        scheduler: Планировщик задач, предоставленный через DI.
    """
    # item_id в CallbackData хранится как строка, преобразуем в int
    post_id = int(callback_data.item_id)
    logger.info(f"Получено подтверждение на удаление поста ID:{post_id} от пользователя {callback.from_user.id}.")

    try:
        # 1. Удалить пост из базы данных
        # delete_post_by_id возвращает True, если пост был найден и удален
        deleted_from_db = await delete_post_by_id(session, post_id)

        if deleted_from_db:
            logger.info(f"Пост ID:{post_id} успешно удален из БД.")

            # 2. Удалить связанные задачи из планировщика
            # Задачи публикации и удаления могут иметь ID в формате post_publish_<id> и post_delete_<id>
            publish_job_id = f'post_publish_{post_id}'
            delete_job_id = f'post_delete_{post_id}'

            await remove_scheduled_job(scheduler, publish_job_id)
            await remove_scheduled_job(scheduler, delete_job_id)
            logger.info(f"Связанные задачи планировщика для поста ID:{post_id} (publish:{publish_job_id}, delete:{delete_job_id}) удалены (если существовали).")

            # 3. Отправить подтверждение пользователю
            await callback.message.edit_text(f"✅ Пост ID:{post_id} и все связанные задачи успешно удалены.", reply_markup=None)

        else:
            logger.warning(f"Попытка удаления поста ID:{post_id} из БД, но он не был найден. Возможно, уже удален.")
            await callback.message.edit_text(f"ℹ️ Пост ID:{post_id} не найден в базе данных или уже был удален.", reply_markup=None)

    except Exception as e:
        logger.exception(f"Ошибка при обработке подтверждения удаления поста ID:{post_id}: {e}")
        # Информировать пользователя об ошибке
        await callback.message.edit_text(f"❌ Произошла ошибка при попытке удаления поста ID:{post_id}.", reply_markup=None)

    # Всегда отвечаем на callback query, чтобы убрать часы загрузки на кнопке
    await callback.answer("Обработано")


# Обработчик отмены удаления поста
# Используем DeleteCallbackData, как определено в keyboards/inline_keyboards.py для отмены
@inline_buttons_router.callback_query(DeleteCallbackData.filter(F.action == "cancel" and F.item_type == "post"))
async def process_cancel_post_delete(
    callback: CallbackQuery,
    callback_data: DeleteCallbackData,
    session: AsyncSession # Сессия БД нужна, если мы хотим показать актуальное состояние поста
):
    """
    Обрабатывает отмену удаления поста.

    Args:
        callback: Объект CallbackQuery.
        callback_data: Распакованные данные DeleteCallbackData.
        session: Сессия БД, предоставленная через DI.
    """
    # item_id в CallbackData хранится как строка, преобразуем в int
    post_id = int(callback_data.item_id)
    logger.info(f"Получена отмена удаления поста ID:{post_id} от пользователя {callback.from_user.id}.")

    try:
        # Получить актуальный пост, чтобы решить, какое сообщение или клавиатуру показать
        post = await get_post_by_id(session, post_id)

        if post:
            # Если пост существует, показываем его снова, возможно с клавиатурой управления
            # В зависимости от статуса поста, может быть разная клавиатура
            # Например, если статус 'scheduled' или 'sent', можно показать get_post_management_keyboard
            # Если статус 'deleted', нужно просто сообщить, что пост уже удален
            if post.status == 'deleted':
                 await callback.message.edit_text(f"ℹ️ Пост ID:{post_id} уже помечен как удаленный.", reply_markup=None)
            else:
                 # Показываем сообщение об отмене и, возможно, возвращаем клавиатуру управления
                 # Для простоты, вернемся к сообщению об отмене без перерисовки полного поста
                 # Если бы у нас был шаблон для отображения поста, мы бы вызвали его здесь
                 # For now, just edit the text and remove the confirmation keyboard
                 await callback.message.edit_text(
                     f"✅ Отмена удаления поста ID:{post_id}.",
                     # reply_markup=get_post_management_keyboard(post_id) # Опционально, вернуть клавиатуру управления
                     reply_markup=None # Убираем клавиатуру подтверждения
                 )
        else:
            # Если пост не найден (возможно, он был удален кем-то другим пока шло подтверждение)
            await callback.message.edit_text(f"ℹ️ Пост ID:{post_id} не найден в базе данных.", reply_markup=None)

    except Exception as e:
        logger.exception(f"Ошибка при обработке отмены удаления поста ID:{post_id}: {e}")
        # Информировать пользователя об ошибке
        await callback.message.edit_text(f"❌ Произошла ошибка при отмене удаления поста ID:{post_id}.", reply_markup=None)


    # Отвечаем на callback query
    await callback.answer("Удаление отменено.")


# --- Обработчик для NavigationCallbackData ---

# Обработчик навигации в главное меню
@inline_buttons_router.callback_query(NavigationCallbackData.filter(F.target == "main_menu"))
async def process_navigate_to_main_menu(
    callback: CallbackQuery,
    callback_data: NavigationCallbackData, # Используем данные навигации
    state: FSMContext # Необходим для сброса состояния FSM
):
    """
    Обрабатывает навигацию пользователя обратно в главное меню.

    Args:
        callback: Объект CallbackQuery.
        callback_data: Распакованные данные NavigationCallbackData.
        state: Контекст FSM для управления состоянием пользователя.
    """
    user_id = callback.from_user.id
    logger.info(f"Пользователь {user_id} запросил навигацию в главное меню.")

    try:
        # 1. Сбросить состояние FSM пользователя
        await state.clear()
        logger.info(f"Состояние FSM для пользователя {user_id} сброшено.")

        # 2. Отредактировать предыдущее сообщение или отправить новое
        # Цель - убрать текущую inline-клавиатуру и, возможно, показать главное меню
        # Если главное меню представлено ReplyKeyboard, можно просто отредактировать
        # текущее сообщение, убрав inline-клавиатуру, и Telegram покажет ReplyKeyboard.
        # Если главное меню представлено InlineKeyboard, нужно будет ее сгенерировать и
        # отредактировать сообщение с ней.
        # Исходя из reference, главное меню - это ReplyKeyboard.

        await callback.message.edit_text(
            "➡️ **Главное меню**\nВыберите действие на клавиатуре ниже:",
            reply_markup=None # Убираем текущую inline клавиатуру
        )
        # Note: Sending the ReplyKeyboardMarkup itself is typically done by the
        # state entry handler for the main menu state. Clearing the state
        # and removing the inline keyboard is sufficient here.

    except Exception as e:
        logger.exception(f"Ошибка при навигации пользователя {user_id} в главное меню: {e}")
        await callback.message.edit_text("❌ Произошла ошибка при переходе в главное меню.")

    # Отвечаем на callback query
    await callback.answer("Переход в главное меню")

# Примечание: Для корректной работы этих обработчиков, необходимо:
# 1. Зарегистрировать inline_buttons_router в главном диспетчере (dp).
# 2. Настроить инъекцию зависимостей (сессии БД и планировщика) в хэндлеры.
#    Это можно сделать через middlewares или путем добавления зависимостей в data объекта Bot
#    и последующего доступа в хэндлерах или через фабрики.
#    Типичный способ: передать AsyncSessionLocal и scheduler в dp.update.middleware или bot.data
#    и использовать фабрику сессий или экземпляр scheduler в хэндлерах.
#    Аннотации типов `session: AsyncSession` и `scheduler: AsyncIOScheduler`
#    предполагают, что настроена автоматическая инъекция по типам.

