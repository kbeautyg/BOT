import os
import json
import logging
import asyncio
from datetime import datetime
import pytz # Required for timezone handling
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ContentType
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import executor
from supabase import create_client, Client
from aiogram.utils.exceptions import ChatNotFound, ChatAdminRequired, BadRequest, TelegramAPIError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import dateparser # Helps parse date/time strings
import datetime as dt # Use dt for datetime module to avoid conflict with datetime object

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
API_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Check for required environment variables
if not API_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing BOT_TOKEN, SUPABASE_URL, or SUPABASE_KEY.")
    # This should likely be a more graceful exit in production
    raise Exception("Missing BOT_TOKEN or Supabase configuration.")

# Initialize bot, dispatcher, storage, and Supabase client
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
scheduler = AsyncIOScheduler()

# In-memory cache for user data (tg_id -> {id, name, lang, timezone})
user_cache = {}
# In-memory cache for channel data (db_id -> {channel_id, title, owner_id})
channel_cache = {}

# Menu button texts
MENU_BUTTONS = {
    "create_post": {"ru": "Создать пост", "en": "Create Post"},
    "scheduled_posts": {"ru": "Запланированные посты", "en": "Scheduled Posts"},
    "settings": {"ru": "Настройки", "en": "Settings"},
    "manage_channels": {"ru": "Управление каналами", "en": "Manage Channels"}
}

# Text prompts in both languages
TEXTS = {
    "welcome": {
        "ru": "Привет! Этот бот поможет вам управлять постами в ваших Telegram-каналах.",
        "en": "Hello! This bot will help you manage posts in your Telegram channels."
    },
    "menu_prompt": {
        "ru": "Выберите действие в меню ниже.",
        "en": "Please choose an action from the menu below."
    },
    "no_edit_channels": {
        "ru": "У вас нет каналов, в которые вы можете создавать посты. Сначала добавьте канал через /add_channel.",
        "en": "You have no channels where you can create posts. Please add a channel first using /add_channel."
    },
    "choose_channel_post": {
        "ru": "Выберите канал для публикации поста:",
        "en": "Choose a channel to create a post in:"
    },
    "enter_post_text": {
        "ru": "Отправьте текст для поста (или /skip, чтобы оставить пост без текста):",
        "en": "Send the text for the post (or /skip to leave the post text empty):"
    },
    "enter_post_media": {
        "ru": "Теперь отправьте изображение или другое медиа для поста, или /skip, чтобы пропустить добавление медиа.\n\n*(Отправьте /cancel для отмены.)*",
        "en": "Now send an image or other media for the post, or /skip to skip attaching media.\n\n*(Send /cancel to cancel.)*"
    },
    "enter_button_text": {
        "ru": "Отправьте текст для кнопки (или /skip, если не хотите добавлять кнопки):",
        "en": "Send the text for an inline button (or /skip if you don't want to add buttons):"
    },
    "enter_button_url": {
        "ru": "Отправьте URL для кнопки \"{btn_text}\":",
        "en": "Send the URL for the button \"{btn_text}\":"
    },
    "ask_add_another_button": {
        "ru": "Кнопка добавлена. Добавить ещё одну кнопку?",
        "en": "Button added. Do you want to add another button?"
    },
     "ask_schedule_options": {
        "ru": "Что делать с постом?",
        "en": "What do you want to do with the post?"
    },
    "prompt_schedule_datetime": {
        "ru": "Отправьте дату и время публикации в формате ДД.ММ.ГГГГ ЧЧ:ММ (например, 25.12.2023 18:30).\nВаш текущий часовой пояс: {timezone}.\n\n*(Отправьте /cancel для отмены.)*",
        "en": "Send the publication date and time in DD.MM.YYYY HH:MM format (e.g., 25.12.2023 18:30).\nYour current timezone: {timezone}.\n\n*(Send /cancel to cancel.)*"
    },
    "invalid_datetime_format": {
        "ru": "Неверный формат даты/времени или время в прошлом. Пожалуйста, используйте формат ДД.ММ.ГГГГ ЧЧ:ММ и укажите будущее время.",
        "en": "Invalid date/time format or time is in the past. Please use DD.MM.YYYY HH:MM format and specify a future time."
    },
     "confirm_post_preview_text": {
        "ru": "Предварительный просмотр поста:\n\n",
        "en": "Post preview:\n\n"
    },
    "post_scheduled_confirmation": {
        "ru": "Пост успешно запланирован на {scheduled_at}.",
        "en": "Post successfully scheduled for {scheduled_at}."
    },
     "post_published_confirmation": {
        "ru": "Пост успешно опубликован.",
        "en": "Post successfully published."
    },
    "draft_saved": {
        "ru": "Пост сохранён как черновик.", # This might be used if scheduling is skipped entirely
        "en": "The post has been saved as a draft."
    },
    "choose_channel_drafts": {
        "ru": "Выберите канал, черновики которого вы хотите просмотреть:",
        "en": "Choose a channel to view drafts:"
    },
    "no_drafts": {
        "ru": "Черновиков в этом канале нет.",
        "en": "There are no drafts in this channel."
    },
    "drafts_header": {
        "ru": "Черновики канала {channel}:",
        "en": "Drafts for channel {channel}:"
    },
    "post_published": { # Old, likely unused now
        "ru": "Пост опубликован в канале.",
        "en": "Post has been published to the channel."
    },
    "post_deleted": {
        "ru": "Черновик удалён.",
        "en": "Draft has been deleted."
    },
    "manage_intro_none": {
        "ru": "У вас ещё нет добавленных каналов.",
        "en": "You have not added any channels yet."
    },
    "manage_intro": {
        "ru": "Управление каналами:",
        "en": "Manage channels:"
    },
    "manage_channel_title": {
        "ru": "Управление каналом \"{title}\":",
        "en": "Managing channel \"{title}\":"
    },
    "prompt_add_channel": {
        "ru": "Отправьте @username или ID канала, который вы хотите добавить. Убедитесь, что бот является администратором в этом канале.",
        "en": "Please send the channel @username or ID that you want to add. Make sure the bot is an administrator in this channel."
    },
    "channel_added": {
        "ru": "Канал успешно добавлен!",
        "en": "Channel added successfully!"
    },
    "channel_exists": {
        "ru": "Этот канал уже зарегистрирован в системе.",
        "en": "This channel is already registered in the system."
    },
    "not_admin": {
        "ru": "Вы не администратор этого канала или бот не добавлен в администраторы и не имеет прав на отправку сообщений.",
        "en": "You are not an admin of this channel, or the bot is not added as an admin and does not have send message permissions."
    },
    "channel_not_found": {
        "ru": "Канал не найден или бот не имеет к нему доступа.",
        "en": "Channel not found or the bot has no access to it."
    },
    "prompt_add_editor": {
        "ru": "Отправьте имя пользователя (@username) или ID человека, которого нужно добавить:",
        "en": "Send the @username or ID of the person you want to add:"
    },
    "user_not_found": {
        "ru": "Пользователь не найден. Убедитесь, что он запустил бота хотя бы один раз.",
        "en": "User not found. Make sure they have started the bot at least once."
    },
    "user_already_editor": {
        "ru": "Этот пользователь уже имеет доступ к каналу с ролью {role}.",
        "en": "This user already has access to the channel with role {role}."
    },
    "choose_role": {
        "ru": "Выберите роль для пользователя:",
        "en": "Choose a role for the user:"
    },
    "role_editor": {
        "ru": "Редактор",
        "en": "Editor"
    },
    "role_viewer": {
        "ru": "Наблюдатель",
        "en": "Viewer"
    },
     "role_owner": { # Added for user_already_editor message
        "ru": "Владелец",
        "en": "Owner"
    },
    "editor_added": {
        "ru": "Пользователь добавлен в канал как {role_text}.",
        "en": "User has been added to the channel as {role_text}."
    },
    "remove_editor_prompt": {
        "ru": "Выберите пользователя для удаления:",
        "en": "Select a user to remove:"
    },
    "user_removed": {
        "ru": "Пользователь удалён из редакторов.",
        "en": "The user has been removed from editors."
    },
    "confirm_delete_channel": {
        "ru": "Вы уверены, что хотите удалить канал \"{title}\" из системы? Все связанные посты (черновики и запланированные) будут удалены.",
        "en": "Are you sure you want to remove channel \"{title}\" from the system? All associated posts (drafts and scheduled) will be deleted."
    },
    "channel_removed": {
        "ru": "Канал \"{title}\" удалён.",
        "en": "Channel \"{title}\" has been removed."
    },
    "language_prompt": {
        "ru": "Выберите язык:",
        "en": "Choose a language:"
    },
    "language_changed": {
        "ru": "Язык интерфейса изменён.",
        "en": "Bot language has been updated."
    },
     "timezone_prompt": {
        "ru": "Отправьте ваш часовой пояс, например, Europe/Moscow или UTC+3. Вы можете найти список поддерживаемых поясов [здесь](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).",
        "en": "Send your timezone, for example, Europe/Moscow or UTC+3. You can find a list of supported timezones [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)."
    },
    "timezone_updated": {
        "ru": "Ваш часовой пояс обновлён на {timezone}.",
        "en": "Your timezone has been updated to {timezone}."
    },
    "invalid_timezone": {
        "ru": "Неверный формат часового пояса. Пожалуйста, используйте стандартные названия (например, Europe/Moscow) или формат UTC±ЧЧ:ММ.",
        "en": "Invalid timezone format. Please use standard names (e.g., Europe/Moscow) or UTC±HH:MM format."
    },
    "no_permission": {
        "ru": "У вас нет прав для выполнения этого действия.",
        "en": "You do not have permission to perform this action."
    },
    "invalid_input": {
        "ru": "Неправильный формат. Пожалуйста, выберите действие из меню или отмените текущее действие с помощью /cancel.",
        "en": "Invalid input format. Please choose an action from the menu or cancel the current action using /cancel."
    },
    "post_content_empty": {
        "ru": "Пост не может быть пустым. Добавьте текст или медиа.",
        "en": "Post cannot be empty. Please add text or media."
    },
    # Scheduled Posts Texts
    "choose_channel_scheduled": {
        "ru": "Выберите канал, запланированные посты которого вы хотите просмотреть:",
        "en": "Choose a channel to view scheduled posts:"
    },
    "no_scheduled_posts": {
        "ru": "В этом канале нет запланированных постов.",
        "en": "There are no scheduled posts in this channel."
    },
    "scheduled_posts_header": {
        "ru": "Запланированные посты канала {channel}:",
        "en": "Scheduled posts for channel {channel}:"
    },
    "view_scheduled_post_prompt_text": {
        "ru": "Просмотр запланированного поста (ID: {post_id}, на {scheduled_at_local}):",
        "en": "Viewing scheduled post (ID: {post_id}, for {scheduled_at_local}):"
    },
     "scheduled_post_deleted": {
        "ru": "Запланированный пост удалён.",
        "en": "Scheduled post deleted."
    },
    "confirm_delete_scheduled": {
        "ru": "Вы уверены, что хотите удалить этот запланированный пост?",
        "en": "Are you sure you want to delete this scheduled post?"
    },
    "edit_scheduled_post_options": {
        "ru": "Что вы хотите отредактировать в этом запланированном посте?",
        "en": "What do you want to edit in this scheduled post?"
    }
}

# Keyboard builders
def main_menu_keyboard(lang: str) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(MENU_BUTTONS["create_post"][lang], MENU_BUTTONS["scheduled_posts"][lang])
    kb.row(MENU_BUTTONS["settings"][lang], MENU_BUTTONS["manage_channels"][lang])
    return kb

def yes_no_keyboard(lang: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Да" if lang == "ru" else "Yes", callback_data="add_btn_yes"),
           InlineKeyboardButton("Нет" if lang == "ru" else "No", callback_data="add_btn_no"))
    return kb

def schedule_options_keyboard(lang: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Опубликовать сейчас" if lang == "ru" else "Publish Now", callback_data="schedule_now"))
    kb.add(InlineKeyboardButton("Запланировать на время" if lang == "ru" else "Schedule for Later", callback_data="schedule_later"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data="edit_back_to_content")) # Back to editing content
    return kb

def post_preview_keyboard(lang: str, is_scheduled: bool, post_db_id: int = None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    if is_scheduled:
        kb.add(InlineKeyboardButton("Запланировать" if lang == "ru" else "Schedule", callback_data=f"confirm_schedule"))
    else:
         kb.add(InlineKeyboardButton("Опубликовать" if lang == "ru" else "Publish", callback_data=f"confirm_publish"))
    # Add edit options
    # Use dummy IDs like -1 for new posts that don't have a DB ID yet
    p_id = post_db_id if post_db_id else -1
    kb.add(InlineKeyboardButton("✏️ Изменить текст" if lang == "ru" else "✏️ Edit Text", callback_data=f"edit_post:text:{p_id}"))
    kb.add(InlineKeyboardButton("🖼️ Изменить медиа" if lang == "ru" else "🖼️ Edit Media", callback_data=f"edit_post:media:{p_id}"))
    kb.add(InlineKeyboardButton("🔘 Изменить кнопки" if lang == "ru" else "🔘 Edit Buttons", callback_data=f"edit_post:buttons:{p_id}"))
    if is_scheduled: # Option to change schedule time only applies if it's scheduled
         kb.add(InlineKeyboardButton("⏰ Изменить время" if lang == "ru" else "⏰ Edit Time", callback_data=f"edit_post:time:{p_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data="cancel_post_creation"))
    return kb

def scheduled_post_actions_keyboard(lang: str, post_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✏️ " + ("Редактировать" if lang == "ru" else "Edit"), callback_data=f"edit_scheduled:{post_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить" if lang == "ru" else "Delete"), callback_data=f"delete_scheduled:{post_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"back_to_scheduled_list"))
    return kb

def edit_scheduled_post_keyboard(lang: str, post_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✏️ Изменить текст" if lang == "ru" else "✏️ Edit Text", callback_data=f"edit_post:text:{post_id}"))
    kb.add(InlineKeyboardButton("🖼️ Изменить медиа" if lang == "ru" else "🖼️ Edit Media", callback_data=f"edit_post:media:{post_id}"))
    kb.add(InlineKeyboardButton("🔘 Изменить кнопки" if lang == "ru" else "🔘 Edit Buttons", callback_data=f"edit_post:buttons:{post_id}"))
    kb.add(InlineKeyboardButton("⏰ Изменить время" if lang == "ru" else "⏰ Edit Time", callback_data=f"edit_post:time:{post_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"view_scheduled:{post_id}"))
    return kb


def manage_channel_keyboard(lang: str, channel_db_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить редактора" if lang == "ru" else "Add Editor"), callback_data=f"addedit:{channel_db_id}"))
    kb.add(InlineKeyboardButton("➖ " + ("Удалить редактора" if lang == "ru" else "Remove Editor"), callback_data=f"remedit:{channel_db_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить канал" if lang == "ru" else "Delete Channel"), callback_data=f"delchan:{channel_db_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data="back_to_manage"))
    return kb


# Middleware to ensure user registration and cache data
class DBMiddleware(BaseMiddleware):
    async def on_pre_process_update(self, update: types.Update, data: dict):
        user = None
        if update.message:
            user = update.message.from_user
        elif update.callback_query:
            user = update.callback_query.from_user
        elif update.inline_query: # Consider other update types if your bot uses them
             user = update.inline_query.from_user
        # Add other update types (e.g., chosen_inline_result, shipping_query, pre_checkout_query)
        # if they need user registration logic.

        if user and user.id:
            tg_id = user.id

            # --- Измененный блок: Надежное извлечение имени ---
            name = user.full_name # Попробуем полное имя сначала
            if not name: # Если полное имя отсутствует или пустое
                name = user.first_name # Попробуем имя
                if user.last_name: # Если есть фамилия, добавим ее
                    if name:
                         name += " " + user.last_name
                    else:
                         name = user.last_name # Или только фамилия, если имени нет
            if not name: # Если полное имя и комбинация имени+фамилии не дали результата
                 name = user.username # Попробуем username как последний вариант
            
            # Если даже username отсутствует, присваиваем значение по умолчанию
            # ЭТО КРИТИЧНО, ЕСЛИ СТОЛБЕЦ name В БД - NOT NULL
            if not name:
                 name = f"Пользователь_{tg_id}"
            # --- Конец измененного блока ---


            user_record = user_cache.get(tg_id)

            if not user_record:
                # Try fetching from DB using tg_id
                res = supabase.table("users").select("*").eq("tg_id", tg_id).execute()
                if res.data:
                    user_record = res.data[0]
                    # Update name in DB if changed (and if new name is not empty/None, depending on DB constraint)
                    # Обновление имени пользователя по его Supabase ID
                    # --- Измененный блок: Обновление имени ---
                    # Сравниваем текущее имя в БД с новым из апдейта
                    # Обновляем только если новое имя отличается и оно не пустое
                    # Если столбец NOT NULL, name уже имеет значение по умолчанию, даже если API не вернул имя.
                    current_db_name = user_record.get("name")
                    if current_db_name != name:
                         # Убедимся, что новое имя не пустая строка перед обновлением, если это имеет смысл
                         # (для NOT NULL это всегда будет строка, но для NULL возможно)
                         # Если столбец NOT NULL, то `name` гарантированно строка здесь.
                         supabase.table("users").update({"name": name}).eq("id", user_record["id"]).execute()
                         user_record["name"] = name # Update cached version too
                    # --- Конец измененного блока ---

                    user_cache[tg_id] = user_record # Cache the full record
                else:
                    # Insert new user with tg_id and name
                    # Вставка нового пользователя, включая tg_id и name
                    # Переменная 'name' на этом этапе гарантированно содержит строку (для NOT NULL)
                    # или строку/None (для NULL)
                    # --- Измененный блок: Вставка нового пользователя ---
                    try:
                         res_insert = supabase.table("users").insert({"tg_id": tg_id, "name": name}).execute()
                         if res_insert.data:
                             user_record = res_insert.data[0]
                             user_cache[tg_id] = user_record # Cache new record
                             logger.info(f"New user registered: {tg_id} ({name})")
                         else:
                             # Это может произойти, если есть ошибка в запросе к Supabase,
                             # например, нарушение ограничений или проблема с кешем схемы PostgREST
                             logger.error(f"Failed to insert new user {tg_id} with name '{name}': {res_insert.error}")
                             # Cannot proceed without a user record
                             user_record = None # Ensure user_record is None to trigger error handling below
                    except Exception as e:
                         logger.error(f"Exception during new user insertion for {tg_id} with name '{name}': {e}")
                         # Handle potential exceptions during DB insertion
                         user_record = None

                    # --- Конец измененного блока ---

            if user_record:
                # Передача Supabase ID пользователя и других данных в data
                data["user_id"] = user_record["id"]
                # Используем .get() для безопасности доступа к ключам, которые могли быть не установлены при старой схеме
                data["lang"] = user_record.get("language", "ru")
                data["timezone"] = user_record.get("timezone", "UTC") # Default to UTC if not set
            else:
                 # Критическая ошибка: не удалось получить или создать запись пользователя в БД
                 logger.error(f"User record is None after DB check/insert for tg_id {tg_id}. Cannot proceed.")
                 # Handle critical error - cannot proceed for this user
                 # Attempt to notify the user and consume the update
                 try:
                      # Используем lang из кэша или дефолтное значение для сообщения об ошибке
                      error_lang = user_cache.get(tg_id, {}).get("language", "ru")
                      if update.message:
                          await bot.send_message(tg_id, "Произошла внутренняя ошибка при идентификации пользователя. Пожалуйста, попробуйте позже." if error_lang == "ru" else "An internal error occurred while identifying the user. Please try again later.")
                      elif update.callback_query:
                           await bot.answer_callback_query(update.callback_query.id, "Произошла внутренняя ошибка.", show_alert=True)
                      elif update.inline_query:
                           # Handle inline query error response if applicable
                           pass # Or answer inline query with an error message

                 except Exception as e:
                     logger.error(f"Failed to send error message to user {tg_id}: {e}")

                 update.consumed = True # Consume update if we can't get user info

dp.middleware.setup(DBMiddleware())

# FSM state groups
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_media = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    waiting_for_add_more_buttons = State()
    waiting_for_schedule_options = State()
    waiting_for_datetime = State() # This state name is used for *new* post scheduling
    waiting_for_preview_confirm = State()

class AddChannelState(StatesGroup):
    waiting_for_channel_info = State()

class AddEditorState(StatesGroup):
    waiting_for_username = State()
    waiting_for_role = State()

class SettingsState(StatesGroup):
    waiting_for_timezone = State()

class ScheduledPostsState(StatesGroup):
    waiting_for_channel_selection = State()
    viewing_scheduled_post = State() # State for when a specific scheduled post is being viewed/edited
    # State specifically for editing the datetime of an *existing* scheduled post
    waiting_for_datetime = State()


# Scheduler helper function
async def schedule_post_job(post_id: int):
    """Fetches post from DB, publishes it, updates status."""
    try:
        # --- Modified: Select job_id as well ---
        res = supabase.table("posts").select("id, channel_id, content, media_type, media_file_id, buttons_json, status, job_id").eq("id", post_id).execute()
        if not res.data:
            logger.warning(f"Scheduler job failed: Post {post_id} not found.")
            # Attempt to remove job if it exists by job_id
            # This part of cleanup is better handled in load_scheduled_posts or a dedicated cleanup task
            # As we don't have the job_id easily here unless fetched from DB, and the post is gone.
            # We trust APScheduler's persistence and the load function for cleanup.
            return

        post = res.data[0]
        channel_db_id = post["channel_id"]
        content = post["content"] or ""
        media_type = post["media_type"]
        media_file_id = post["media_file_id"]
        buttons_json = post["buttons_json"]
        current_job_id = post["job_id"] # Get job_id from DB record
        # Ensure post is still scheduled before sending
        if post["status"] != "scheduled":
             logger.warning(f"Post {post_id} status is not 'scheduled' ({post['status']}). Skipping publication.")
             # If status is wrong, it implies the job should no longer be here.
             # Try to remove the job based on the stored job_id.
             if current_job_id:
                  try:
                      scheduler.remove_job(current_job_id)
                      logger.info(f"Removed stale scheduler job {current_job_id} for post {post_id} with status {post['status']}")
                  except Exception as e:
                       logger.error(f"Failed to remove stale scheduler job {current_job_id} for post {post_id}: {e}")
             # Also update DB to remove job_id if status wasn't scheduled but job_id was present
             try:
                  supabase.table("posts").update({"job_id": None}).eq("id", post_id).execute()
             except Exception as e:
                  logger.error(f"Failed to clear job_id for post {post_id} with status {post['status']}: {e}")

             return # Exit if not scheduled

        # Get channel Telegram ID
        channel_res = supabase.table("channels").select("channel_id").eq("id", channel_db_id).execute()
        if not channel_res.data:
             logger.error(f"Scheduler job failed: Channel DB ID {channel_db_id} not found for post {post_id}. Cannot publish.")
             # Mark post status as failed publishing and clear job_id
             try:
                  supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
             except Exception as e:
                  logger.error(f"Failed to mark post {post_id} as publishing_failed and clear job_id: {e}")
             return

        tg_channel_id = channel_res.data[0]["channel_id"]

        reply_markup = None
        if buttons_json:
            try:
                btn_list = json.loads(buttons_json)
                if btn_list:
                    reply_markup = InlineKeyboardMarkup()
                    # Add buttons in rows of up to 2, or 1 if URL is long
                    row_buttons = []
                    for b in btn_list:
                        if len(row_buttons) < 2: # Try adding to current row
                             row_buttons.append(InlineKeyboardButton(b["text"], url=b["url"]))
                        else: # Row is full, add it and start a new row
                             reply_markup.row(*row_buttons)
                             row_buttons = [InlineKeyboardButton(b["text"], url=b["url"])]
                    if row_buttons: # Add the last row if it has buttons
                        reply_markup.row(*row_buttons)

            except json.JSONDecodeError:
                logger.error(f"Failed to decode buttons_json for post {post_id}")
            except Exception as e:
                logger.error(f"Error building keyboard for post {post_id}: {e}")


        try:
            logger.info(f"Attempting to send scheduled post {post_id} to channel {tg_channel_id}")
            # Use caption for media posts, text for text-only posts
            send_text = content if not (media_type and media_file_id) else None # Text only if NO media
            send_caption = content if media_type and media_file_id else None # Caption only if there's media

            # Truncate caption/text if too long
            if send_caption and len(send_caption) > 1024:
                 send_caption = send_caption[:1021] + "..."
                 logger.warning(f"Truncated publish caption for post {post_id}.")
            if send_text and len(send_text) > 4096:
                 send_text = send_text[:4093] + "..."
                 logger.warning(f"Truncated publish text for post {post_id}.")

            if media_type and media_file_id:
                if media_type == "photo":
                    await bot.send_photo(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
                elif media_type == "video":
                    await bot.send_video(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
                elif media_type == "document":
                    await bot.send_document(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
                elif media_type == "audio":
                    await bot.send_audio(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
                elif media_type == "animation":
                    await bot.send_animation(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
                else:
                    logger.warning(f"Unknown media type '{media_type}' for post {post_id}. Sending as text.")
                    await bot.send_message(tg_channel_id, send_text or " ", reply_markup=reply_markup, parse_mode="Markdown") # Send space if content is empty
            else:
                await bot.send_message(tg_channel_id, send_text or " ", reply_markup=reply_markup, parse_mode="Markdown") # Send space if content is empty


            # Update post status and remove job_id in DB
            # --- Modified: Set job_id to NULL on publish ---
            supabase.table("posts").update({"status": "published", "job_id": None}).eq("id", post_id).execute()
            logger.info(f"Post {post_id} successfully published to {tg_channel_id}.")

            # Remove the job from APScheduler explicitly after successful execution
            # Although APScheduler DateTrigger jobs run only once and are removed automatically,
            # explicitly removing here handles cases where the job might persist unexpectedly.
            if current_job_id:
                 try:
                     scheduler.remove_job(current_job_id)
                     logger.info(f"Removed job {current_job_id} from scheduler after successful publication of post {post_id}.")
                 except Exception as e:
                     logger.error(f"Failed to remove job {current_job_id} from scheduler after publication of post {post_id}: {e}")


        except (ChatNotFound, ChatAdminRequired, BadRequest) as e:
             logger.error(f"Telegram API permissions/chat error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
             # Mark post status as failed publishing and clear job_id
             # --- Modified: Set job_id to NULL on publishing_failed ---
             supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
             # Optionally notify owner?
             pass

        except TelegramAPIError as e:
            logger.error(f"Telegram API Generic Error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
            # Mark post status as failed publishing and clear job_id
            # --- Modified: Set job_id to NULL on publishing_failed ---
            supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
            pass # Or update status?

        except Exception as e:
            logger.error(f"Unexpected error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
            # --- Modified: Set job_id to NULL on publishing_failed ---
            supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()


    except Exception as e:
        logger.error(f"Error in schedule_post_job for post {post_id}: {e}")
        # If an error occurs before accessing the post or getting channel_id
        try:
             # --- Modified: Set job_id to NULL on publishing_failed ---
             supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
             logger.info(f"Marked post {post_id} as publishing_failed due to error before sending.")
        except Exception as db_err:
             logger.error(f"Failed to mark post {post_id} as publishing_failed after error: {db_err}")


async def load_scheduled_posts():
    """Loads scheduled posts from DB and adds them to the scheduler."""
    now_utc = datetime.now(pytz.utc)
    # Only load posts with status 'scheduled' and scheduled in the future
    # --- Modified: Select job_id here ---
    res = supabase.table("posts").select("id, scheduled_at, job_id").eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).execute()
    scheduled_posts = res.data or []
    logger.info(f"Found {len(scheduled_posts)} scheduled posts to load.")

    # Clean up any existing jobs that correspond to posts that are no longer scheduled or are in the past
    try:
        all_jobs = scheduler.get_jobs()
        active_post_ids = {p["id"] for p in scheduled_posts}
        # Collect job_ids from DB for validation
        db_job_ids = {p["job_id"] for p in scheduled_posts if p.get("job_id")}

        for job in all_jobs:
             # Check if the job is for our schedule_post_job function and has args
             if job.func == schedule_post_job and job.args and len(job.args) > 0:
                  job_post_id = job.args[0]
                  # Check if the job's associated post_id is in our list of active scheduled posts
                  # OR if the job's ID is not in the list of expected job_ids from the DB
                  # (This handles cases where a post was rescheduled and has a new job_id)
                  if job_post_id not in active_post_ids or job.id not in db_job_ids:
                       try:
                            scheduler.remove_job(job.id)
                            logger.info(f"Removed orphaned/stale scheduler job {job.id} for post {job_post_id}")
                       except Exception as e:
                            logger.error(f"Failed to remove stale scheduler job {job.id}: {e}")
             elif job.func == schedule_post_job and (not job.args or len(job.args) == 0):
                 # Remove jobs with missing args as they are invalid
                 try:
                      scheduler.remove_job(job.id)
                      logger.warning(f"Removed invalid scheduler job {job.id} with missing args.")
                 except Exception as e:
                      logger.error(f"Failed to remove invalid scheduler job {job.id}: {e}")
             # --- Added: Check for jobs in APScheduler that are NOT linked in DB ---
             elif job.id not in db_job_ids:
                 # This job exists in APScheduler but its ID isn't found in the job_id column
                 # of any 'scheduled' post in the DB. It might be an old job from a deleted post,
                 # or a job whose post status changed.
                 # If it's not linked to any 'scheduled' post via job_id, remove it.
                 # We already covered cases where post_id is known but post is not active.
                 # This catches jobs whose post_id might be unknown or that weren't created by this bot logic (less likely but safer).
                 # Need to be careful not to remove jobs not related to posts (if any are added later).
                 # For now, assuming all jobs added are post jobs.
                 # A safer check would be to see if *any* post has this job.id, regardless of status, and if not, remove it.
                 # Let's stick to checking against the job_ids from *active scheduled* posts for simplicity and relevance to this task.
                 pass # This case is largely covered by the first check (job_post_id not in active_post_ids)


    except Exception as e:
         logger.error(f"Error cleaning up old scheduler jobs: {e}")


    for post in scheduled_posts:
        post_id = post["id"]
        scheduled_time_utc = datetime.fromisoformat(post["scheduled_at"])
        db_job_id = post.get("job_id") # Get job_id from DB

        # Only add jobs for posts scheduled in the future (sanity check, although DB query should handle this)
        if scheduled_time_utc <= now_utc:
             logger.warning(f"Post {post_id} scheduled time {scheduled_time_utc} is in the past. Marking as draft.")
             try:
                  # --- Modified: Set job_id to NULL when marking as draft ---
                  supabase.table("posts").update({"status": "draft", "job_id": None}).eq("id", post_id).execute()
                  # If there was a job_id, also try to remove the job
                  if db_job_id:
                       try: scheduler.remove_job(db_job_id)
                       except Exception as e: logger.error(f"Failed to remove past-scheduled job {db_job_id}: {e}")

             except Exception as e:
                  logger.error(f"Failed to mark post {post_id} as draft after past time check: {e}")
             continue # Skip scheduling

        try:
            # Add new job
            # Use the job_id from DB if it exists, otherwise generate a new one
            # Using replace_existing=True with a potentially non-unique generated ID is risky.
            # It's better to always generate a new unique ID if the DB job_id is missing or stale.
            # Let's use the DB job_id if present, otherwise generate. `replace_existing=True` is needed if we restart and add jobs with the same ID.
            # APScheduler recommends using stable IDs for persistence. The job_id from DB *is* the stable ID.
            job_to_add_id = db_job_id if db_job_id else f"post_{post_id}_{scheduled_time_utc.timestamp()}_{os.urandom(4).hex()}" # Add random part for uniqueness if generating

            job = scheduler.add_job(
                schedule_post_job,
                trigger=DateTrigger(run_date=scheduled_time_utc),
                args=[post_id],
                id=job_to_add_id,
                replace_existing=True # Replace if ID is already there (important for stability during restarts)
            )
            # Ensure job_id in DB matches the one created/used by APScheduler
            # This step is crucial. If DB job_id was NULL or different, update it.
            if db_job_id != job.id:
                 # --- Modified: Update job_id in DB after adding/replacing job ---
                 supabase.table("posts").update({"job_id": job.id}).eq("id", post_id).execute()
                 logger.info(f"Updated job_id in DB for post {post_id} to {job.id}.")
            else:
                 logger.info(f"Verified job_id {job.id} in DB matches APScheduler job for post {post_id}.")


            logger.info(f"Loaded scheduled post {post_id} with job ID {job.id} for {scheduled_time_utc}.")

        except Exception as e:
            logger.error(f"Failed to load scheduled post {post_id} into scheduler: {e}")


async def on_startup(dp):
    await bot.delete_webhook(drop_pending_updates=True)
    scheduler.start()
    # --- Logic related to line 555 ---
    # load_scheduled_posts is called here, which now includes job_id handling and cleanup
    await load_scheduled_posts()
    logger.info("Bot started and scheduler loaded.")

# --- General Handlers ---
@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, lang: str): # lang is passed by middleware
    welcome_text = TEXTS["welcome"][lang] + "\n" + TEXTS["menu_prompt"][lang]
    await message.reply(welcome_text, reply_markup=main_menu_keyboard(lang))

@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext, lang: str):
    current_state = await state.get_state()
    if not current_state:
        await message.reply("Нет активного действия для отмены." if lang == "ru" else "No active action to cancel.")
        return

    # Try to clean up inline keyboards associated with the current state
    data = await state.get_data()
    msg_to_delete_id = data.get("select_msg_id") # Used in channel selection etc.
    preview_msg_id = data.get("preview_msg_id") # Used in preview state
    # Check if we were editing a scheduled post
    editing_scheduled_post_id = data.get("post_db_id") # If post_db_id exists, we were editing a scheduled post

    if msg_to_delete_id:
        try:
            await bot.delete_message(message.chat.id, msg_to_delete_id)
        except Exception:
            pass # Ignore errors if message already deleted or inaccessible

    if preview_msg_id:
        try:
             # Edit preview message to indicate cancellation instead of deleting it
             await bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=preview_msg_id, reply_markup=None)
             # Try editing caption, but handle if it's a media message without a caption
             try:
                  current_caption = (await bot.copy_message(message.chat.id, message.chat.id, preview_msg_id)).caption # Get current caption safely
                  # For a text message, need to get text instead of caption
                  if current_caption is None:
                      current_caption = (await bot.copy_message(message.chat.id, message.chat.id, preview_msg_id)).text
                  new_content = (current_caption or data.get("content") or "") + ("\n\n*Отменено*" if lang=="ru" else "\n\n*Cancelled*")

                  media_type = data.get("media_type")
                  media_file_id = data.get("media_file_id")

                  if media_type and media_file_id: # Edit caption
                       if len(new_content) > 1024: new_content = new_content[:1021] + "..."
                       await bot.edit_message_caption(chat_id=message.chat.id, message_id=preview_msg_id, caption=new_content, parse_mode="Markdown")
                  else: # Edit text
                       if len(new_content) > 4096: new_content = new_content[:4093] + "..."
                       await bot.edit_message_text(chat_id=message.chat.id, message_id=preview_msg_id, text=new_content, parse_mode="Markdown")

             except Exception:
                  # If caption/text editing fails (e.g., message type issue, or original message was empty)
                  pass # Ignore error
        except Exception:
            pass # Ignore errors (e.g. message deleted)

    await state.finish()

    # --- Added: If cancelling from scheduled post flow, return to list ---
    if editing_scheduled_post_id:
        # Try to get channel ID from the cancelled state data or DB if available
        channel_db_id = data.get("channel_id")
        if channel_db_id:
             # Instead of main menu, go back to scheduled list for that channel
             await bot.send_message(message.chat.id, "Действие отменено." if lang == "ru" else "Action cancelled.")
             await send_scheduled_posts_list(message.chat.id, channel_db_id, lang, data.get("user_id")) # user_id is in middleware data
             return

    # Default: return to main menu
    await message.reply("Действие отменено." if lang == "ru" else "Action cancelled.", reply_markup=main_menu_keyboard(lang))

@dp.callback_query_handler(lambda c: c.data == "cancel_post_creation", state=PostStates.waiting_for_preview_confirm)
async def cb_cancel_post_creation(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
     data = await state.get_data()
     preview_msg_id = data.get("preview_msg_id")
     # Check if we were editing a scheduled post (post_db_id will be present)
     editing_scheduled_post_id = data.get("post_db_id")


     if preview_msg_id:
        try:
             # Edit preview message to indicate cancellation instead of deleting it
             await call.message.edit_reply_markup(reply_markup=None)
             # Try editing caption, but handle if it's a media message without a caption
             try:
                  current_caption = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).caption # Get current caption safely
                  if current_caption is None: # If no caption, it was a text message
                      current_caption = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).text

                  new_content = (current_caption or data.get("content") or "") + ("\n\n*Отменено*" if lang=="ru" else "\n\n*Cancelled*")
                  media_type = data.get("media_type")
                  media_file_id = data.get("media_file_id")

                  if media_type and media_file_id: # Edit caption
                       if len(new_content) > 1024: new_content = new_content[:1021] + "..."
                       await call.message.edit_caption(caption=new_content, parse_mode="Markdown")
                  else: # Edit text
                       if len(new_content) > 4096: new_content = new_content[:4093] + "..."
                       await call.message.edit_text(text=new_content, parse_mode="Markdown")

             except Exception:
                  pass
        except Exception:
            pass # Ignore errors

     await call.answer("Отменено." if lang == "ru" else "Cancelled.")
     await state.finish()

     # --- Added: If cancelling from scheduled post flow, return to list ---
     if editing_scheduled_post_id:
          # Try to get channel ID from the cancelled state data
         channel_db_id = data.get("channel_id")
         if channel_db_id:
              await bot.send_message(call.from_user.id, "Действие отменено." if lang == "ru" else "Action cancelled.")
              await send_scheduled_posts_list(call.from_user.id, channel_db_id, lang, user_id)
              return

     # Default: return to main menu
     await bot.send_message(call.from_user.id, "Действие отменено." if lang == "ru" else "Action cancelled.", reply_markup=main_menu_keyboard(lang))


# --- Create Post Flow ---
@dp.message_handler(commands=['newpost', 'createpost'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["create_post"]["ru"], MENU_BUTTONS["create_post"]["en"]], state='*')
async def start_create_post(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        # If already in a state, try to cancel first? Or just inform? Inform is safer.
        await message.reply("Вы уже выполняете другое действие. Используйте /cancel для отмены." if lang == "ru" else "You are already performing another action. Use /cancel to cancel.")
        return

    res = supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    channels_access = res.data or []

    if not channels_access:
        await message.reply(TEXTS["no_edit_channels"][lang])
        return

    channel_db_ids = [entry["channel_id"] for entry in channels_access]
    res2 = supabase.table("channels").select("id, title").in_("id", channel_db_ids).execute()
    channels_list = res2.data or []

    if not channels_list: # Should not happen if channels_access is not empty, but safety check
         await message.reply(TEXTS["no_edit_channels"][lang])
         return

    kb = InlineKeyboardMarkup()
    for ch in channels_list:
        # Cache channel info
        channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
        channel_cache[ch["id"]]["title"] = ch["title"]
        # Add button
        kb.add(InlineKeyboardButton(ch["title"], callback_data=f"selch_post:{ch['id']}"))

    msg = await message.reply(TEXTS["choose_channel_post"][lang], reply_markup=kb)
    await state.update_data(select_msg_id=msg.message_id)
    await PostStates.waiting_for_channel.set()


# --- Input Handlers for Post Content ---
# Text input (can be actual text or /skip)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_text)
async def post_text_received(message: types.Message, state: FSMContext, lang: str):
    text = message.text

    # if text.lower().strip() in ["/skip", "скип", "пропустить"]:
    #     await state.update_data(content="")
    # else:
    await state.update_data(content=text) # Save text, even if it's /skip to be consistent

    # After receiving text, move to asking for media
    # If we were editing an existing scheduled post, go back to preview after setting text
    data = await state.get_data()
    post_db_id = data.get("post_db_id")

    if post_db_id is not None: # Editing an existing scheduled post
         try:
              # Update content in DB
              supabase.table("posts").update({"content": text}).eq("id", post_db_id).execute()
              await ScheduledPostsState.viewing_scheduled_post.set()
              await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, data.get("user_id")) # user_id is in middleware data
         except Exception as e:
              logger.error(f"Failed to update content for scheduled post {post_db_id}: {e}")
              await message.reply("Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
              await state.finish()

    else: # Creating a new post
        await PostStates.waiting_for_media.set()
        await message.reply(TEXTS["enter_post_media"][lang])


# Media input (can be media or /skip text)
@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT, ContentType.AUDIO, ContentType.ANIMATION, ContentType.TEXT], state=PostStates.waiting_for_media)
async def post_media_received(message: types.Message, state: FSMContext, lang: str):
    data = await state.get_data()
    post_db_id = data.get("post_db_id") # Check if editing scheduled post

    if message.content_type == ContentType.TEXT:
        if message.text.lower().strip() in ["/skip", "скип", "пропустить"]:
            await state.update_data(media_type=None, media_file_id=None)
            # Move to next step or back to preview
            if post_db_id is not None: # Editing existing scheduled post
                 try:
                     supabase.table("posts").update({"media_type": None, "media_file_id": None}).eq("id", post_db_id).execute()
                     await ScheduledPostsState.viewing_scheduled_post.set()
                     await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, data.get("user_id"))
                 except Exception as e:
                      logger.error(f"Failed to update media for scheduled post {post_db_id}: {e}")
                      await message.reply("Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
                      await state.finish()
            else: # Creating new post
                await PostStates.waiting_for_button_text.set()
                await message.reply(TEXTS["enter_button_text"][lang])

        # else: ignore other text input in media state (unless it's /cancel, handled by global)
        return

    # Handle media
    caption = message.caption or ""
    # Decide how to handle caption: append to existing content OR overwrite?
    # Let's append caption to the previously entered text content.
    current_content = data.get("content", "") # Get content from state
    if caption:
         if current_content:
              current_content += "\n\n" + caption # Add a separator
         else:
              current_content = caption # If no text was entered before media
    await state.update_data(content=current_content) # Update content with original text + caption

    media_type = None
    file_id = None
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id # Get the largest photo
    elif message.video:
        media_type = "video"
        file_id = message.video.file_id
    elif message.document:
        media_type = "document"
        file_id = message.document.file_id
    elif message.audio:
        media_type = "audio"
        file_id = message.audio.file_id
    elif message.animation:
        media_type = "animation"
        file_id = message.animation.file_id

    if media_type and file_id:
        await state.update_data(media_type=media_type, media_file_id=file_id)
    else:
         # This case should not happen if content_types are limited, but safety
         await state.update_data(media_type=None, media_file_id=None)


    # After receiving media, move to next step or back to preview
    if post_db_id is not None: # Editing existing scheduled post
         try:
             # Update content, media_type, media_file_id in DB
             update_data = {
                 "content": current_content,
                 "media_type": media_type,
                 "media_file_id": file_id
             }
             supabase.table("posts").update(update_data).eq("id", post_db_id).execute()
             await ScheduledPostsState.viewing_scheduled_post.set()
             await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, data.get("user_id"))
         except Exception as e:
              logger.error(f"Failed to update media/content for scheduled post {post_db_id}: {e}")
              await message.reply("Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
              await state.finish()

    else: # Creating new post
        await PostStates.waiting_for_button_text.set()
        await message.reply(TEXTS["enter_button_text"][lang])


# Button text input (can be text or /skip)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_text)
async def button_text_received(message: types.Message, state: FSMContext, lang: str):
    text = message.text.strip()
    data = await state.get_data()
    post_db_id = data.get("post_db_id") # Check if editing scheduled post

    if text.lower() in ["/skip", "скип", "пропустить"]:
        await state.update_data(buttons=[]) # Ensure buttons list is empty if skipped
        # Move directly to schedule options or back to preview
        if post_db_id is not None: # Editing existing scheduled post
             try:
                  # Update buttons in DB
                  supabase.table("posts").update({"buttons_json": None}).eq("id", post_db_id).execute() # Save None for empty buttons
                  await ScheduledPostsState.viewing_scheduled_post.set()
                  await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, data.get("user_id"))
             except Exception as e:
                  logger.error(f"Failed to update buttons for scheduled post {post_db_id}: {e}")
                  await message.reply("Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
                  await state.finish()
        else: # Creating new post
            await PostStates.waiting_for_schedule_options.set()
            kb = schedule_options_keyboard(lang)
            await message.reply(TEXTS["ask_schedule_options"][lang], reply_markup=kb)
        return

    # If not skip, save button text and ask for URL
    await state.update_data(current_button_text=text)
    prompt = TEXTS["enter_button_url"][lang].format(btn_text=text)
    await PostStates.waiting_for_button_url.set()
    await message.reply(prompt)

# Button URL input
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_url)
async def button_url_received(message: types.Message, state: FSMContext, lang: str):
    url = message.text.strip()
    # Basic URL validation (can be more robust)
    if not url.lower().startswith(("http://", "https://", "tg://")):
        await message.reply(TEXTS["invalid_input"][lang])
        return # Stay in the same state

    data = await state.get_data()
    btn_text = data.get("current_button_text")
    if not btn_text:
        # Should not happen if state is managed correctly
        logger.error("Button text missing in waiting_for_button_url state")
        await message.reply("Произошла ошибка. Начните создание поста заново." if lang == "ru" else "An error occurred. Please start post creation again.", reply_markup=main_menu_keyboard(lang))
        await state.finish()
        return

    buttons = data.get("buttons", [])
    buttons.append({"text": btn_text, "url": url})
    await state.update_data(buttons=buttons)

    kb = yes_no_keyboard(lang)
    await PostStates.waiting_for_add_more_buttons.set()
    await message.reply(TEXTS["ask_add_another_button"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_btn_yes", state=PostStates.waiting_for_add_more_buttons)
async def cb_add_button_yes(call: types.CallbackQuery, state: FSMContext, lang: str):
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
        pass
    await state.update_data(current_button_text=None) # Clear temp button text
    await PostStates.waiting_for_button_text.set() # Go back to button text input
    await bot.send_message(call.from_user.id, TEXTS["enter_button_text"][lang])

@dp.callback_query_handler(lambda c: c.data == "add_btn_no", state=PostStates.waiting_for_add_more_buttons)
async def cb_add_button_no(call: types.CallbackQuery, state: FSMContext, lang: str):
    await call.answer()
    try:
        await call.message.delete()
    except:
         await call.message.edit_reply_markup(reply_markup=None)
         pass
    await state.update_data(current_button_text=None) # Clear temp button text

    # After finishing buttons, move to schedule options or back to preview
    data = await state.get_data()
    post_db_id = data.get("post_db_id")

    if post_db_id is not None: # Editing existing scheduled post
         try:
              # Update buttons in DB
              buttons_to_save = data.get("buttons")
              supabase.table("posts").update({"buttons_json": json.dumps(buttons_to_save) if buttons_to_save else None}).eq("id", post_db_id).execute()
              await ScheduledPostsState.viewing_scheduled_post.set()
              await view_scheduled_post_by_id(call.from_user.id, post_db_id, lang, data.get("user_id"))
         except Exception as e:
              logger.error(f"Failed to update buttons for scheduled post {post_db_id}: {e}")
              await bot.send_message(call.from_user.id, "Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
              await state.finish()

    else: # Creating new post
        await PostStates.waiting_for_schedule_options.set()
        kb = schedule_options_keyboard(lang)
        await bot.send_message(call.from_user.id, TEXTS["ask_schedule_options"][lang], reply_markup=kb)


# --- Scheduling and Preview ---
@dp.callback_query_handler(lambda c: c.data == "edit_back_to_content", state=PostStates.waiting_for_schedule_options)
async def cb_back_to_content_edit(call: types.CallbackQuery, state: FSMContext, lang: str):
    """Callback to go back from schedule options to editing content."""
    await call.answer()
    try: await call.message.delete()
    except: await call.message.edit_reply_markup(reply_markup=None)

    # Go back to preview state (assuming editing always loops back to preview before scheduling)
    await PostStates.waiting_for_preview_confirm.set()
    await send_post_preview(call.from_user.id, state, lang)


@dp.callback_query_handler(lambda c: c.data in ["schedule_now", "schedule_later"], state=PostStates.waiting_for_schedule_options)
async def cb_schedule_options(call: types.CallbackQuery, state: FSMContext, lang: str, timezone: str):
    action = call.data
    await call.answer()

    data = await state.get_data()
    content = data.get("content") or ""
    media_file_id = data.get("media_file_id")

    # Ensure post content is not empty
    if not content and not media_file_id:
        try:
            await call.message.delete()
        except Exception:
             await call.message.edit_reply_markup(reply_markup=None)
             pass
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["post_content_empty"][lang], reply_markup=main_menu_keyboard(lang))
        return

    try: # Delete the schedule options message
         await call.message.delete()
    except Exception:
         await call.message.edit_reply_markup(reply_markup=None)
         pass

    if action == "schedule_now":
        # Build preview for immediate publishing
        await state.update_data(is_scheduled=False) # Flag for preview keyboard
        await PostStates.waiting_for_preview_confirm.set()
        await send_post_preview(call.from_user.id, state, lang)

    elif action == "schedule_later":
        # Move to waiting for datetime (using the state for *new* post scheduling)
        await state.update_data(is_scheduled=True) # Flag for preview keyboard
        await PostStates.waiting_for_datetime.set()
        prompt = TEXTS["prompt_schedule_datetime"][lang].format(timezone=timezone)
        await bot.send_message(call.from_user.id, prompt)

# Handler for datetime input when creating a *new* scheduled post
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_datetime)
async def post_datetime_received(message: types.Message, state: FSMContext, lang: str, timezone: str):
    datetime_str = message.text.strip()
    user_tz = pytz.timezone(timezone) if timezone in pytz.all_timezones_set else pytz.utc # Use user's timezone or UTC

    # Use dateparser for flexible parsing, then make it timezone-aware
    # Specify date_formats for better control over DD.MM.YYYY HH:MM
    # Set TIMEZONE to user's timezone so parse assumes input is in user's timezone
    settings = {'DATE_ORDER': 'DMY', 'RETURN_AS_TIMEZONE_AWARE': True, 'TIMEZONE': timezone, 'RETURN_AS_LOCALTIME': False} # Do not return as local time, keep timezone aware
    parsed_datetime = dateparser.parse(datetime_str, languages=[lang], settings=settings)

    if parsed_datetime is None:
        # dateparser failed, try strict parsing in user's timezone
        try:
             local_dt = dt.datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
             # Assume input is in user's local time, convert to timezone-aware using user's TZ
             parsed_datetime = user_tz.localize(local_dt)
        except (ValueError, pytz.UnknownTimeZoneError):
             parsed_datetime = None # Strict parsing failed too


    # Check if parsed datetime is in the future (compared to user's current time in their TZ)
    now_in_user_tz = datetime.now(user_tz)

    if parsed_datetime is None or parsed_datetime < now_in_user_tz:
        await message.reply(TEXTS["invalid_datetime_format"][lang])
        return # Stay in the same state

    # Convert to UTC for storage
    scheduled_at_utc = parsed_datetime.astimezone(pytz.utc)

    await state.update_data(scheduled_at=scheduled_at_utc.isoformat())
    # Move to preview state
    await PostStates.waiting_for_preview_confirm.set()
    await send_post_preview(message.chat.id, state, lang)


async def send_post_preview(chat_id: int, state: FSMContext, lang: str):
    """Sends a preview message to the user."""
    data = await state.get_data()
    content = data.get("content") or ""
    media_type = data.get("media_type")
    media_file_id = data.get("media_file_id")
    buttons = data.get("buttons")
    is_scheduled = data.get("is_scheduled", False) # Assume publish now if not set
    scheduled_at_utc_str = data.get("scheduled_at")
    channel_db_id = data.get("channel_id")
    post_db_id = data.get("post_db_id") # Will be None for new posts

    # Get channel title for context in preview
    channel_title = "..."
    if channel_db_id:
         channel_res = supabase.table("channels").select("title").eq("id", channel_db_id).execute()
         if channel_res.data:
              channel_title = channel_res.data[0]["title"]
         else:
              logger.warning(f"Channel DB ID {channel_db_id} not found for preview.")

    preview_header = f"_{TEXTS['confirm_post_preview_text'][lang]}_\n"
    preview_header += f"Канал: *{channel_title}*\n" if lang == "ru" else f"Channel: *{channel_title}*\n"
    if is_scheduled and scheduled_at_utc_str:
        try:
             scheduled_dt_utc = datetime.fromisoformat(scheduled_at_utc_str)
             # --- Modified: Get user's timezone correctly from cache based on chat_id/user_id ---
             user_tz_str = user_cache.get(chat_id, {}).get("timezone", "UTC") # chat_id is the user's ID here
             user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
             scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
             # Format time using standard library strftime
             scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')
             preview_header += f"Запланировано на: *{scheduled_time_display} ({user_tz_str})*\n" if lang == "ru" else f"Scheduled for: *{scheduled_time_display} ({user_tz_str})*\n"
        except Exception as e:
             logger.error(f"Error formatting scheduled time for preview: {e}")
             preview_header += f"Запланировано на: *{scheduled_at_utc_str}* (UTC)\n" if lang == "ru" else f"Scheduled for: *{scheduled_at_utc_str}* (UTC)\n"


    # Combine header with content
    final_content = preview_header + "\n" + (content if content else ("_(без текста)_" if lang == "ru" else "_(no text)_"))


    reply_markup = None
    if buttons:
        try:
            btn_list = buttons # Buttons are already a list of dicts from state
            if btn_list:
                reply_markup = InlineKeyboardMarkup()
                # Add buttons in rows of up to 2, or 1 if URL is long
                row_buttons = []
                for b in btn_list:
                     if len(row_buttons) < 2: # Try adding to current row
                          row_buttons.append(InlineKeyboardButton(b["text"], url=b["url"]))
                     else: # Row is full, add it and start a new row
                          reply_markup.row(*row_buttons)
                          row_buttons = [InlineKeyboardButton(b["text"], url=b["url"])]
                if row_buttons: # Add the last row if it has buttons
                     reply_markup.row(*row_buttons)

        except Exception as e:
            logger.error(f"Error building keyboard for preview: {e}")

    # Create preview keyboard with action and edit options
    preview_kb = post_preview_keyboard(lang, is_scheduled=is_scheduled, post_db_id=post_db_id)

    # Combine post buttons with action buttons if post buttons exist
    combined_kb = preview_kb
    if reply_markup:
        # Create a new combined keyboard
        combined_kb = InlineKeyboardMarkup()
        # Add post buttons first
        for row in reply_markup.inline_keyboard:
            combined_kb.row(*row) # Add each row from post buttons
        # Add action buttons from preview_kb (they were added one by one)
        for row in preview_kb.inline_keyboard:
             combined_kb.row(*row)


    try:
        # Attempt to delete the message that triggered the preview (e.g., datetime input, schedule option)
        # Note: This relies on the last message ID being stored in a state variable.
        # A better approach might be to edit the previous message if possible, or manage message IDs more robustly.
        # For now, let's just delete the schedule options message if it still exists.
        # The datetime input message will not be deleted by default here.
        pass # Skipping automatic deletion of previous message to avoid complexity


        sent_msg = None

        # Telegram maximum caption length is 1024, text is 4096
        if media_type and media_file_id:
             if len(final_content) > 1024:
                  final_content = final_content[:1021] + "..." # Truncate caption
                  logger.warning(f"Truncated caption for post preview {post_db_id if post_db_id else 'new'}.")
             # Ensure text is None for media posts
             send_text = None
             send_caption = final_content
        else:
             if len(final_content) > 4096:
                  final_content = final_content[:4093] + "..." # Truncate text
                  logger.warning(f"Truncated text for post preview {post_db_id if post_db_id else 'new'}.")
             # Ensure caption is None for text-only posts
             send_caption = None
             send_text = final_content


        if media_type and media_file_id:
            try:
                if media_type == "photo":
                    sent_msg = await bot.send_photo(chat_id, media_file_id, caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif media_type == "video":
                    sent_msg = await bot.send_video(chat_id, media_file_id, caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif media_type == "document":
                     sent_msg = await bot.send_document(chat_id, media_file_id, caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif media_type == "audio":
                     sent_msg = await bot.send_audio(chat_id, media_file_id, caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif media_type == "animation":
                     sent_msg = await bot.send_animation(chat_id, media_file_id, caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                else:
                     logger.warning(f"Unknown media type '{media_type}' for preview. Sending as text.")
                     sent_msg = await bot.send_message(chat_id, send_text or " ", reply_markup=combined_kb, parse_mode="Markdown")

            except TelegramAPIError as e:
                 logger.error(f"Error sending media preview: {e}")
                 # Fallback to sending text only or show error
                 fallback_text = f"{send_text or send_caption}\n\n*Ошибка при отправке медиа.*" if lang == "ru" else f"{send_text or send_caption}\n\n*Error sending media.*"
                 # Truncate fallback text if necessary
                 if len(fallback_text) > 4096: fallback_text = fallback_text[:4093] + "..."
                 sent_msg = await bot.send_message(chat_id, fallback_text, reply_markup=combined_kb, parse_mode="Markdown")

        else:
            # Text-only post
            sent_msg = await bot.send_message(chat_id, send_text or " ", reply_markup=combined_kb, parse_mode="Markdown")

        if sent_msg:
             await state.update_data(preview_msg_id=sent_msg.message_id)


    except Exception as e:
        logger.error(f"Failed to send preview message: {e}")
        await bot.send_message(chat_id, "Произошла ошибка при подготовке предпросмотра." if lang == "ru" else "An error occurred while preparing the preview.")
        await state.finish() # Exit the flow


# --- Preview Confirmation Handlers ---
@dp.callback_query_handler(lambda c: c.data == "confirm_publish", state=PostStates.waiting_for_preview_confirm)
async def cb_confirm_publish(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Публикую..." if lang == "ru" else "Publishing...")
    data = await state.get_data()
    channel_db_id = data.get("channel_id")
    content = data.get("content") or ""
    media_type = data.get("media_type")
    media_file_id = data.get("media_file_id")
    buttons = data.get("buttons") # This is the list of dicts
    preview_msg_id = data.get("preview_msg_id")
    post_db_id = data.get("post_db_id") # Check if we are publishing an *existing* scheduled post

    # Get channel Telegram ID
    channel_res = supabase.table("channels").select("channel_id").eq("id", channel_db_id).execute()
    if not channel_res.data:
         await call.answer("Ошибка: Канал не найден в базе.", show_alert=True)
         logger.error(f"Channel DB ID {channel_db_id} not found during publish confirmation.")
         # Clean up preview message if exists
         if preview_msg_id:
              try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
              except: pass
         await state.finish()
         return
    tg_channel_id = channel_res.data[0]["channel_id"]

    # Ensure user still has permission to publish to this channel
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         # Clean up preview message if exists
         if preview_msg_id:
              try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
              except: pass
         await state.finish()
         return


    reply_markup = None
    if buttons:
        try:
            btn_list = buttons
            if btn_list:
                reply_markup = InlineKeyboardMarkup()
                # Add buttons in rows of up to 2, or 1 if URL is long
                row_buttons = []
                for b in btn_list:
                     if len(row_buttons) < 2: # Try adding to current row
                          row_buttons.append(InlineKeyboardButton(b["text"], url=b["url"]))
                     else: # Row is full, add it and start a new row
                          reply_markup.row(*row_buttons)
                          row_buttons = [InlineKeyboardButton(b["text"], url=b["url"])]
                if row_buttons: # Add the last row if it has buttons
                     reply_markup.row(*row_buttons)
        except Exception as e:
            logger.error(f"Error building keyboard for publish: {e}")

    # Ensure post content is not empty before publishing
    if not content and not media_file_id:
        await call.answer(TEXTS["post_content_empty"][lang], show_alert=True)
        # Stay in preview state? Or return to menu? Return to menu is cleaner.
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["post_content_empty"][lang], reply_markup=main_menu_keyboard(lang))
        return

    try:
        logger.info(f"Attempting to publish post to channel {tg_channel_id}")
        # Use caption for media posts, text for text-only posts
        send_text = content if not (media_type and media_file_id) else None # Text only if NO media
        send_caption = content if media_type and media_file_id else None # Caption only if there's media

        if send_caption and len(send_caption) > 1024: send_caption = send_caption[:1021] + "..."
        if send_text and len(send_text) > 4096: send_text = send_text[:4093] + "..."


        if media_type and media_file_id:
            if media_type == "photo":
                await bot.send_photo(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
            elif media_type == "video":
                await bot.send_video(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
            elif media_type == "document":
                await bot.send_document(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
            elif media_type == "audio":
                await bot.send_audio(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
            elif media_type == "animation":
                await bot.send_animation(tg_channel_id, media_file_id, caption=send_caption, reply_markup=reply_markup, parse_mode="Markdown")
            else:
                 logger.warning(f"Unknown media type '{media_type}' during publish. Sending as text.")
                 await bot.send_message(tg_channel_id, send_text or " ", reply_markup=reply_markup, parse_mode="Markdown") # Send space if empty

        else:
            # Text-only post
            await bot.send_message(tg_channel_id, send_text or " ", reply_markup=reply_markup, parse_mode="Markdown") # Send space if empty


        # Save post to DB with status 'published' (optional, but good practice to record)
        # If this was an existing scheduled post being published now, update its status.
        # If it's a new post being published now, insert it.
        if post_db_id:
             # --- Modified: Update existing post, set status to published, clear job_id ---
             try:
                  # Get job_id before updating
                  res_job = supabase.table("posts").select("job_id").eq("id", post_db_id).execute()
                  current_job_id = res_job.data[0]["job_id"] if res_job.data else None
                  # Update
                  supabase.table("posts").update({
                      "content": content, # Update content just in case it was edited but not saved to DB yet (shouldn't happen with current flow, but safety)
                      "media_type": media_type,
                      "media_file_id": media_file_id,
                      "buttons_json": json.dumps(buttons) if buttons else None,
                      "status": "published",
                      "scheduled_at": datetime.now(pytz.utc).isoformat(), # Record publication time
                      "job_id": None # Clear job_id
                  }).eq("id", post_db_id).execute()
                  logger.info(f"Updated scheduled post {post_db_id} to status 'published'.")

                  # Cancel the job if it existed
                  if current_job_id:
                       try:
                           scheduler.remove_job(current_job_id)
                           logger.info(f"Cancelled job {current_job_id} for published post {post_db_id}.")
                       except Exception as e:
                           logger.error(f"Failed to cancel job {current_job_id} for published post {post_db_id}: {e}")

             except Exception as db_e:
                  logger.error(f"Failed to update scheduled post {post_db_id} to published status: {db_e}")
                  # Don't fail the user interaction just because DB record failed
        else:
            # --- Modified: Insert new post, job_id is NULL for published posts ---
            try:
                supabase.table("posts").insert({
                    "channel_id": channel_db_id,
                    "user_id": user_id,
                    "content": content,
                    "media_type": media_type,
                    "media_file_id": media_file_id,
                    "buttons_json": json.dumps(buttons) if buttons else None,
                    "status": "published",
                    "scheduled_at": datetime.now(pytz.utc).isoformat(), # Record publication time
                    "job_id": None # Published posts have no associated job
                }).execute()
                logger.info(f"Recorded new published post in DB for user {user_id}.")
            except Exception as db_e:
                logger.error(f"Failed to record new published post in DB for user {user_id}: {db_e}")
                # Don't fail the user interaction just because DB record failed


        await call.answer()
        if preview_msg_id:
             try: # Edit preview message to remove keyboard and mark as published
                 await call.message.edit_reply_markup(reply_markup=None)
                 # Try editing caption/text to add "Published" status
                 try:
                      current_caption_or_text = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).caption # Get current caption
                      if current_caption_or_text is None: # If no caption, it was a text message
                           current_caption_or_text = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).text
                      new_content = (current_caption_or_text or "") + ("\n\n*Опубликовано*" if lang=="ru" else "\n\n*Published*")
                      media_type = data.get("media_type")
                      media_file_id = data.get("media_file_id")

                      if media_type and media_file_id: # Edit caption
                           if len(new_content) > 1024: new_content = new_content[:1021] + "..."
                           await bot.edit_message_caption(chat_id=call.message.chat.id, message_id=preview_msg_id, caption=new_content, parse_mode="Markdown")
                      else: # Edit text
                           if len(new_content) > 4096: new_content = new_content[:4093] + "..."
                           await bot.edit_message_text(chat_id=call.message.chat.id, message_id=preview_msg_id, text=new_content, parse_mode="Markdown")
                 except Exception:
                      pass # Ignore caption/text editing errors

             except Exception: pass

        await bot.send_message(call.from_user.id, TEXTS["post_published_confirmation"][lang], reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except (ChatNotFound, ChatAdminRequired, BadRequest) as e:
        logger.error(f"Telegram API permissions/chat error publishing post to {tg_channel_id}: {e}")
        await call.answer(TEXTS["not_admin"][lang], show_alert=True) # Reuse not_admin text for general sending failure
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Ошибка при публикации поста. Проверьте права бота и пользователя." if lang == "ru" else "Error publishing post. Check bot and user permissions.", reply_markup=main_menu_keyboard(lang))

    except TelegramAPIError as e:
        logger.error(f"Telegram API Generic Error publishing post to {tg_channel_id}: {e}")
        await call.answer("Произошла ошибка Telegram API.", show_alert=True)
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Ошибка при публикации поста." if lang == "ru" else "Error publishing post.", reply_markup=main_menu_keyboard(lang))

    except Exception as e:
        logger.error(f"Unexpected error during publish: {e}")
        await call.answer("Произошла внутренняя ошибка.", show_alert=True)
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Произошла ошибка при публикации поста." if lang == "ru" else "Error publishing post.", reply_markup=main_menu_keyboard(lang))


@dp.callback_query_handler(lambda c: c.data == "confirm_schedule", state=PostStates.waiting_for_preview_confirm)
async def cb_confirm_schedule(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Планирую..." if lang == "ru" else "Scheduling...")
    data = await state.get_data()
    channel_db_id = data.get("channel_id")
    content = data.get("content") or ""
    media_type = data.get("media_type")
    media_file_id = data.get("media_file_id")
    buttons = data.get("buttons") # This is the list of dicts
    scheduled_at_utc_str = data.get("scheduled_at")
    preview_msg_id = data.get("preview_msg_id")
    post_db_id = data.get("post_db_id") # Check if we are scheduling an *existing* post (should be None here)

    # Ensure post_db_id is None, as this is for *new* scheduled posts.
    # Editing an existing scheduled post to change its time uses a different flow.
    if post_db_id is not None:
         logger.error(f"cb_confirm_schedule called with post_db_id={post_db_id}. This handler is for NEW posts.")
         await call.answer("Произошла внутренняя ошибка.", show_alert=True)
         await state.finish()
         # Clean up preview message if exists
         if preview_msg_id:
              try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
              except: pass
         return


    if not scheduled_at_utc_str:
        await call.answer("Ошибка: Время не указано.", show_alert=True)
        logger.error("Scheduled time missing during schedule confirmation.")
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        return

    # Ensure user still has permission to schedule to this channel
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         if preview_msg_id:
              try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
              except: pass
         await state.finish()
         return

    # Ensure post content is not empty before scheduling
    if not content and not media_file_id:
        await call.answer(TEXTS["post_content_empty"][lang], show_alert=True)
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["post_content_empty"][lang], reply_markup=main_menu_keyboard(lang))
        return

    # Ensure scheduled time is in the future (double check)
    try:
        scheduled_dt_utc = datetime.fromisoformat(scheduled_at_utc_str)
        if scheduled_dt_utc <= datetime.now(pytz.utc):
            await call.answer("Ошибка: Время в прошлом.", show_alert=True)
            logger.error(f"Attempted to schedule post in the past: {scheduled_dt_utc}")
            if preview_msg_id:
                 try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
                 except: pass
            await state.finish()
            await bot.send_message(call.from_user.id, TEXTS["invalid_datetime_format"][lang], reply_markup=main_menu_keyboard(lang))
            return
    except ValueError:
        await call.answer("Ошибка формата времени.", show_alert=True)
        logger.error(f"Invalid datetime format string: {scheduled_at_utc_str}")
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["invalid_datetime_format"][lang], reply_markup=main_menu_keyboard(lang))
        return


    try:
        # Save post to DB with status 'scheduled'
        # --- Modified: Insert new post without job_id initially ---
        res_insert = supabase.table("posts").insert({
            "channel_id": channel_db_id,
            "user_id": user_id,
            "content": content,
            "media_type": media_type,
            "media_file_id": media_file_id,
            "buttons_json": json.dumps(buttons) if buttons else None,
            "status": "scheduled",
            "scheduled_at": scheduled_at_utc_str,
            "job_id": None # job_id will be added after APScheduler job is created
        }).execute()

        if not res_insert.data:
             await call.answer("Ошибка при сохранении поста.", show_alert=True)
             logger.error(f"Failed to insert scheduled post for user {user_id}: {res_insert.error}")
             if preview_msg_id:
                 try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
                 except: pass
             await state.finish()
             await bot.send_message(call.from_user.id, "Ошибка при планировании поста." if lang == "ru" else "Error scheduling post.", reply_markup=main_menu_keyboard(lang))
             return

        post_rec = res_insert.data[0]
        post_db_id = post_rec["id"]
        scheduled_dt_utc = datetime.fromisoformat(scheduled_at_utc_str)

        # Add job to scheduler
        # --- Modified: Generate unique job ID and capture the result ---
        job_id_to_use = f"post_{post_db_id}_{scheduled_dt_utc.timestamp()}_{os.urandom(4).hex()}" # Ensure unique ID
        job = scheduler.add_job(
            schedule_post_job,
            trigger=DateTrigger(run_date=scheduled_dt_utc),
            args=[post_db_id],
            id=job_id_to_use, # Use the generated unique ID
            replace_existing=True # Use replace_existing just in case, though unique ID should prevent collisions
        )
        # --- Modified: Update post with the actual job.id returned by APScheduler ---
        try:
             supabase.table("posts").update({"job_id": job.id}).eq("id", post_db_id).execute()
             logger.info(f"Updated post {post_db_id} with job ID {job.id}.")
        except Exception as e:
             logger.error(f"Failed to update job_id {job.id} for post {post_db_id} after scheduling: {e}")
             # The job is scheduled, but the DB link might be broken. This is a critical consistency issue.
             # Consider logging this error for manual intervention. The job might run, but rescheduling/cancelling via bot might fail.
             pass # Continue user flow


        logger.info(f"Scheduled post {post_db_id} with job ID {job.id} for {scheduled_dt_utc}.")


        # Format local scheduled time for confirmation message
        # --- Modified: Get user's timezone from cache based on call.from_user.id ---
        user_tz_str = user_cache.get(call.from_user.id, {}).get("timezone", "UTC")
        user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
        scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
        scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')

        await call.answer()
        if preview_msg_id:
             try: # Edit preview message to remove keyboard and mark as scheduled
                 await call.message.edit_reply_markup(reply_markup=None)
                 # Try editing caption/text to add "Scheduled" status
                 try:
                      current_caption_or_text = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).caption # Get current caption
                      if current_caption_or_text is None: # If no caption, it was a text message
                           current_caption_or_text = (await bot.copy_message(call.message.chat.id, call.message.chat.id, preview_msg_id)).text
                      new_content = (current_caption_or_text or "") + ("\n\n*Запланировано*" if lang=="ru" else "\n\n*Scheduled*")
                      media_type = data.get("media_type")
                      media_file_id = data.get("media_file_id")

                      if media_type and media_file_id: # Edit caption
                           if len(new_content) > 1024: new_content = new_content[:1021] + "..."
                           await bot.edit_message_caption(chat_id=call.message.chat.id, message_id=preview_msg_id, caption=new_content, parse_mode="Markdown")
                      else: # Edit text
                           if len(new_content) > 4096: new_content = new_content[:4093] + "..."
                           await bot.edit_message_text(chat_id=call.message.chat.id, message_id=preview_msg_id, text=new_content, parse_mode="Markdown")
                 except Exception:
                      pass # Ignore caption/text editing errors
             except Exception: pass


        await bot.send_message(call.from_user.id, TEXTS["post_scheduled_confirmation"][lang].format(scheduled_at=scheduled_time_display), reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except Exception as e:
        logger.error(f"Unexpected error during scheduling: {e}")
        await call.answer("Произошла внутренняя ошибка.", show_alert=True)
        if preview_msg_id:
             try: await bot.edit_message_reply_markup(call.message.chat.id, preview_msg_id, reply_markup=None)
             except: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Произошла ошибка при планировании поста." if lang == "ru" else "Error scheduling post.", reply_markup=main_menu_keyboard(lang))


# --- Edit Handlers from Preview/Scheduled Post View ---
# These handlers transition back to specific input states
# This handler is for clicking EDIT buttons on the preview of a *new* post OR a *scheduled* post.
# It needs to store which post is being edited (new draft vs existing scheduled) and what field.
@dp.callback_query_handler(lambda c: c.data.startswith("edit_post:"), state=[PostStates.waiting_for_preview_confirm, ScheduledPostsState.viewing_scheduled_post])
async def cb_edit_post_content(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Режим редактирования..." if lang == "ru" else "Editing mode...")
    parts = call.data.split(":")
    # edit_context = parts[0] # edit_post - no longer needed as we removed edit_draft
    edit_type = parts[1] # text, media, buttons, time
    post_db_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() and int(parts[2]) != -1 else None # Use None for new drafts (-1)

    # Fetch current post data
    current_state_data = await state.get_data()

    if post_db_id: # Editing an existing scheduled post
        # --- Modified: Select job_id when fetching post for editing ---
        res = supabase.table("posts").select("*").eq("id", post_db_id).execute()
        if not res.data:
            await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
            await state.finish() # Exit current state (viewing_scheduled_post)
            try: await call.message.delete() # Clean up preview message
            except: pass
            return
        post_data = res.data[0]
        # Ensure user has edit permission (owner/editor) for this channel
        res_role = supabase.table("channel_editors").select("role").eq("channel_id", post_data["channel_id"]).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
        if not res_role.data:
             await call.answer(TEXTS["no_permission"][lang], show_alert=True)
             return
        # Load existing post data into state for modification
        await state.update_data(
             channel_id=post_data["channel_id"],
             content=post_data["content"],
             media_type=post_data["media_type"],
             media_file_id=post_data["media_file_id"],
             buttons=json.loads(post_data["buttons_json"]) if post_data["buttons_json"] else [],
             scheduled_at=post_data["scheduled_at"], # Keep original schedule time
             post_db_id=post_db_id, # Store post ID indicating we are editing existing
             is_scheduled=True # Always True for editing scheduled post
             # job_id is not stored in state for editing flow, only retrieved when needed for job management
        )
    else: # Editing a new post (draft) from the preview stage
        # Data is already in the state from the creation flow
        # Set post_db_id to None explicitly to indicate editing a draft
        await state.update_data(post_db_id=None)
        # is_scheduled flag should reflect whether the *new* post was intended to be scheduled
        # This flag is already in state from cb_schedule_options

    # Delete the preview message
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_reply_markup(reply_markup=None)
        pass

    # Transition to the correct state based on edit_type
    # Note: Input handlers for these states now need to check for post_db_id
    # and either update DB (if post_db_id is not None) or just update state (if post_db_id is None).
    if edit_type == "text":
        await PostStates.waiting_for_text.set() # Re-use the same state
        # No need to store post_db_id again, it's already in state
        await bot.send_message(call.from_user.id, TEXTS["enter_post_text"][lang])
    elif edit_type == "media":
        await PostStates.waiting_for_media.set() # Re-use the same state
        await bot.send_message(call.from_user.id, TEXTS["enter_post_media"][lang])
    elif edit_type == "buttons":
        # Clear current button data in state to start fresh for button editing flow
        await state.update_data(buttons=[], current_button_text=None)
        await PostStates.waiting_for_button_text.set() # Re-use the same state
        await bot.send_message(call.from_user.id, TEXTS["enter_button_text"][lang])
    elif edit_type == "time" and post_db_id: # Only for existing scheduled posts
         # This is the transition that caused the error, now the state exists
         await ScheduledPostsState.waiting_for_datetime.set() # Use a separate state for scheduled post editing time
         # Ensure post_db_id is in state for the handler
         await state.update_data(post_db_id=post_db_id)
         prompt = TEXTS["prompt_schedule_datetime"][lang].format(timezone=user_cache.get(user_id, {}).get("timezone", "UTC"))
         await bot.send_message(call.from_user.id, prompt)
    else:
        await call.answer("Неизвестный тип редактирования." if lang == "ru" else "Unknown edit type.", show_alert=True)
        # Should return to preview or main menu? Let's go back to the original context if possible
        if post_db_id: # Was editing a scheduled post
             await ScheduledPostsState.viewing_scheduled_post.set()
             # Re-fetch post data as editing state might have cleared it
             await view_scheduled_post_by_id(call.from_user.id, post_db_id, lang, user_id) # Show original preview
        else: # Was editing a new draft
             await PostStates.waiting_for_preview_confirm.set()
             # send_post_preview uses data from state, which was preserved
             await send_post_preview(call.from_user.id, state, lang) # Show original preview


# Handlers for input after editing states (text, media, buttons)
# These handlers will receive the *new* input after the user clicked an 'edit' button
# and entered the new content. They need to save the new content to state, and then
# return to the appropriate preview state or update DB.
# Note: These handlers are integrated into the original input handlers (post_text_received, post_media_received, etc.)
# They check for `post_db_id` in state to determine if editing a new draft or existing scheduled post.
# The logic to update DB and return to ScheduledPostsState.viewing_scheduled_post
# or just update state and return to PostStates.waiting_for_preview_confirm is added there.


# Handler for datetime input specifically when editing an *existing* scheduled post
@dp.message_handler(content_types=ContentType.TEXT, state=ScheduledPostsState.waiting_for_datetime)
async def scheduled_post_datetime_received(message: types.Message, state: FSMContext, lang: str, user_id: int, timezone: str):
    """Handle new datetime input when editing a scheduled post."""
    datetime_str = message.text.strip()
    user_tz = pytz.timezone(timezone) if timezone in pytz.all_timezones_set else pytz.utc

    # Use dateparser for flexible parsing, then make it timezone-aware
    settings = {'DATE_ORDER': 'DMY', 'RETURN_AS_TIMEZONE_AWARE': True, 'TIMEZONE': timezone, 'RETURN_AS_LOCALTIME': False}
    parsed_datetime = dateparser.parse(datetime_str, languages=[lang], settings=settings)

    if parsed_datetime is None:
        # dateparser failed, try strict parsing in user's timezone
        try:
             local_dt = dt.datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
             parsed_datetime = user_tz.localize(local_dt)
        except (ValueError, pytz.UnknownTimeZoneError):
             parsed_datetime = None # Strict parsing failed too


    # Check if parsed datetime is in the future (compared to user's current time in their TZ)
    now_in_user_tz = datetime.now(user_tz)

    if parsed_datetime is None or parsed_datetime < now_in_user_tz:
        await message.reply(TEXTS["invalid_datetime_format"][lang])
        return # Stay in the same state

    scheduled_at_utc = parsed_datetime.astimezone(pytz.utc)
    scheduled_at_utc_str = scheduled_at_utc.isoformat()

    data = await state.get_data()
    post_db_id = data.get("post_db_id") # Must be present when in this state

    if not post_db_id:
         await message.reply("Ошибка: ID поста не найден." if lang == "ru" else "Error: Post ID not found.", reply_markup=main_menu_keyboard(lang))
         await state.finish()
         return

    try:
        # Update post in DB with new scheduled time
        # --- Modified: Update scheduled_at and clear job_id temporarily ---
        # Clear job_id first to avoid race conditions or issues with old job_id if APScheduler updates DB
        supabase.table("posts").update({"scheduled_at": scheduled_at_utc_str, "job_id": None}).eq("id", post_db_id).execute()

        # Update scheduler job (cancel old, add new)
        # --- Modified: Pass new scheduled_at_utc_str ---
        await update_scheduled_post_job(post_db_id, scheduled_at_utc_str)

        # Return to viewing the scheduled post with updated time
        await ScheduledPostsState.viewing_scheduled_post.set() # Return to viewing state
        await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, user_id) # Show updated preview

    except Exception as e:
        logger.error(f"Failed to update scheduled time for post {post_db_id}: {e}")
        await message.reply("Ошибка при сохранении нового времени." if lang == "ru" else "Error saving new time.", reply_markup=main_menu_keyboard(lang))
        await state.finish()


async def update_scheduled_post_job(post_db_id: int, new_scheduled_at_utc_str: str):
    """Cancels old scheduler job for a post and creates a new one."""
    # --- Modified: Select job_id here ---
    res = supabase.table("posts").select("job_id").eq("id", post_db_id).execute()
    old_job_id = res.data[0]["job_id"] if res.data and res.data[0] and res.data[0].get("job_id") else None

    if old_job_id:
        try:
            # Remove job by its ID
            scheduler.remove_job(old_job_id)
            logger.info(f"Cancelled old scheduler job {old_job_id} for post {post_db_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel old scheduler job {old_job_id} for post {post_db_id}: {e}")

    # Add new job if time is in the future
    new_scheduled_dt_utc = datetime.fromisoformat(new_scheduled_at_utc_str)
    now_utc = datetime.now(pytz.utc)

    if new_scheduled_dt_utc > now_utc:
        # --- Modified: Generate unique job ID and capture result ---
        new_job_id_to_use = f"post_{post_db_id}_{new_scheduled_dt_utc.timestamp()}_{os.urandom(4).hex()}" # Generate unique ID for the new job
        job = scheduler.add_job(
            schedule_post_job,
            trigger=DateTrigger(run_date=new_scheduled_dt_utc),
            args=[post_db_id],
            id=new_job_id_to_use, # Use the new unique ID
            replace_existing=True # Use replace_existing just in case
        )
        # --- Modified: Update post with the new job.id returned by APScheduler ---
        try:
             # Also ensure status is 'scheduled' if it was potentially changed during error handling
             supabase.table("posts").update({"job_id": job.id, "status": "scheduled"}).eq("id", post_db_id).execute()
             logger.info(f"Added new scheduler job {job.id} for post {post_db_id} at {new_scheduled_dt_utc}.")
        except Exception as e:
             logger.error(f"Failed to update job_id for post {post_db_id} after rescheduling: {e}")
             # Job is in scheduler, but DB link might be broken. Log error.

    else:
         # New time is in the past (should be caught by validation, but double check)
         # Mark post as draft and remove job_id (already cleared above, but safety)
         logger.warning(f"Rescheduled time for post {post_db_id} is in the past ({new_scheduled_dt_utc}). Marking as draft.")
         try:
             # --- Modified: Ensure job_id is None when marking as draft ---
             supabase.table("posts").update({"status": "draft", "job_id": None}).eq("id", post_db_id).execute()
         except Exception as e:
              logger.error(f"Failed to mark post {post_db_id} as draft after past rescheduling time: {e}")


# --- Scheduled Posts Flow ---
@dp.message_handler(commands=['scheduled'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["scheduled_posts"]["ru"], MENU_BUTTONS["scheduled_posts"]["en"]], state='*')
async def view_scheduled_posts_menu(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply("Вы уже выполняете другое действие. Используйте /cancel для отмены." if lang == "ru" else "You are already performing another action. Use /cancel to cancel.")
        return

    # Get channels where user is owner/editor/viewer
    res = supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).execute()
    channels_access = res.data or []

    if not channels_access:
        await message.reply(TEXTS["no_scheduled_posts"][lang])
        return

    channel_db_ids = [entry["channel_id"] for entry in channels_access]
    # Filter channels to only show those with actual *scheduled* posts in the future?
    # Or show all channels the user has access to, and indicate if there are no scheduled posts after selecting?
    # Let's show all accessible channels first.
    res2 = supabase.table("channels").select("id, title").in_("id", channel_db_ids).execute()
    channels_list = res2.data or []

    if not channels_list: # Safety check - should be same as channels_access check
         await message.reply(TEXTS["no_scheduled_posts"][lang]) # Re-use text
         return

    # --- Added: Check if there are any scheduled posts at all before asking to choose channel ---
    now_utc = datetime.now(pytz.utc)
    # --- Modified: Select job_id here as well for potential future use ---
    res_any_scheduled = supabase.table("posts").select("id, channel_id, job_id").eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).in_("channel_id", channel_db_ids).execute()
    any_scheduled_posts = res_any_scheduled.data or []

    if not any_scheduled_posts:
         await message.reply(TEXTS["no_scheduled_posts"][lang])
         return

    # Filter channels_list to only include channels that actually have scheduled posts
    channels_with_scheduled = {p["channel_id"] for p in any_scheduled_posts}
    channels_list_filtered = [ch for ch in channels_list if ch["id"] in channels_with_scheduled]

    if not channels_list_filtered:
         # Should theoretically not happen if any_scheduled_posts is not empty, but safety
         await message.reply(TEXTS["no_scheduled_posts"][lang])
         return


    if len(channels_list_filtered) > 1:
        kb = InlineKeyboardMarkup()
        for ch in channels_list_filtered:
             # Cache channel info
            channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
            channel_cache[ch["id"]]["title"] = ch["title"]
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"viewsched_ch:{ch['id']}"))
        msg = await message.reply(TEXTS["choose_channel_scheduled"][lang], reply_markup=kb)
        await state.update_data(select_msg_id=msg.message_id) # Store message ID for cleanup
        await ScheduledPostsState.waiting_for_channel_selection.set() # Add state for channel selection
    else:
        # Only one channel with scheduled posts, show scheduled posts directly
        chan_db_id = channels_list_filtered[0]["id"]
        # No state needed for channel selection if only one channel
        await send_scheduled_posts_list(message.chat.id, chan_db_id, lang, user_id)


@dp.callback_query_handler(lambda c: c.data.startswith("viewsched_ch:"), state=ScheduledPostsState.waiting_for_channel_selection)
async def cb_choose_scheduled_channel(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])

    # Verify user has access (should be handled by the initial query, but good to double check)
    res = supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).eq("channel_id", chan_db_id).execute()
    if not res.data:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        await state.finish() # Exit flow if no permission
        try: await call.message.delete() # Clean up message
        except: await call.message.edit_reply_markup(reply_markup=None)
        return

    await call.answer()
    # Clean up the channel selection message
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
        pass

    await state.finish() # Exit channel selection state
    await send_scheduled_posts_list(call.from_user.id, chan_db_id, lang, user_id)


async def send_scheduled_posts_list(chat_id: int, channel_db_id: int, lang: str, user_id: int):
    """Sends a list of scheduled posts for a channel."""
    res_ch = supabase.table("channels").select("title").eq("id", channel_db_id).execute()
    title = res_ch.data[0]["title"] if res_ch.data else "Channel"
    # Fetch scheduled posts that are in the future
    now_utc = datetime.now(pytz.utc)
    # --- Modified: Select job_id here ---
    res_posts = supabase.table("posts").select("id, content, scheduled_at, job_id").eq("channel_id", channel_db_id).eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).order("scheduled_at").execute()
    scheduled_posts = res_posts.data or []

    if not scheduled_posts:
        await bot.send_message(chat_id, TEXTS["no_scheduled_posts"][lang])
        return

    header_text = TEXTS["scheduled_posts_header"][lang].format(channel=title)
    await bot.send_message(chat_id, header_text)

    # --- Modified: Get user's timezone correctly from cache based on chat_id/user_id ---
    user_tz_str = user_cache.get(chat_id, {}).get("timezone", "UTC")
    user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc

    # Determine user role to show/hide edit/delete buttons
    user_role = None
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).execute()
    if res_role.data:
        user_role = res_role.data[0]["role"]

    for post in scheduled_posts:
        post_id = post["id"]
        content_snippet = (post["content"][:50] + '...') if post["content"] and len(post["content"]) > 50 else (post["content"] or ("(без текста)" if lang == "ru" else "(no text)"))
        scheduled_dt_utc = datetime.fromisoformat(post["scheduled_at"])
        scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
        scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')

        # --- Modified: Add job_id to summary for debugging/info if needed (optional) ---
        # job_id_display = post.get("job_id", "N/A")[:8] + "..." if post.get("job_id") else "N/A"
        # post_summary = f"ID: `{post_id}` | Job: `{job_id_display}` | {scheduled_time_display} ({user_tz_str})\n{content_snippet}"

        post_summary = f"ID: `{post_id}` | {scheduled_time_display} ({user_tz_str})\n{content_snippet}"


        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("👁️ " + ("Просмотр" if lang == "ru" else "View"), callback_data=f"view_scheduled:{post_id}"))
        if user_role in ["owner", "editor"]:
             kb.add(InlineKeyboardButton("✏️ " + ("Редактировать" if lang == "ru" else "Edit"), callback_data=f"edit_scheduled:{post_id}"))
             # --- Modified: Call delete_scheduled with post_id ---
             kb.add(InlineKeyboardButton("🗑️ " + ("Удалить" if lang == "ru" else "Delete"), callback_data=f"delete_scheduled:{post_id}"))

        await bot.send_message(chat_id, post_summary, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query_handler(lambda c: c.data.startswith("view_scheduled:"), state=[None, ScheduledPostsState.waiting_for_channel_selection])
async def cb_view_scheduled_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Загрузка поста..." if lang == "ru" else "Loading post...")
    post_id = int(call.data.split(":")[1])

    # Delete the list item message
    try:
         await call.message.delete()
    except:
         await call.message.edit_reply_markup(reply_markup=None) # Fallback

    # --- Modified: Ensure view_scheduled_post_by_id fetches all necessary data including job_id if needed ---
    await view_scheduled_post_by_id(call.from_user.id, post_id, lang, user_id)
    # Set state after successful fetch and send
    # Need to finish the previous state (e.g., waiting_for_channel_selection) before setting viewing state
    await state.finish()
    await ScheduledPostsState.viewing_scheduled_post.set()
    await state.update_data(post_db_id=post_id) # Store post ID in state


async def view_scheduled_post_by_id(chat_id: int, post_id: int, lang: str, user_id: int):
    """Helper to fetch and send a single scheduled post preview."""
    # --- Modified: Select job_id here ---
    res = supabase.table("posts").select("*").eq("id", post_id).execute()
    if not res.data:
        await bot.send_message(chat_id, "Пост не найден." if lang == "ru" else "Post not found.", reply_markup=main_menu_keyboard(lang))
        return

    post = res.data[0]
    channel_db_id = post["channel_id"]
    # job_id = post.get("job_id") # Get job_id for display if needed

    # Verify user has access
    res_access = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).execute()
    if not res_access.data:
        await bot.send_message(chat_id, TEXTS["no_permission"][lang], reply_markup=main_menu_keyboard(lang))
        return
    user_role = res_access.data[0]["role"]

    # Get channel title and format scheduled time
    channel_res = supabase.table("channels").select("title").eq("id", channel_db_id).execute()
    channel_title = channel_res.data[0]["title"] if channel_res.data else "Channel"
    scheduled_dt_utc = datetime.fromisoformat(post["scheduled_at"])
    # --- Modified: Get user's timezone correctly from cache based on chat_id/user_id ---
    user_tz_str = user_cache.get(chat_id, {}).get("timezone", "UTC")
    user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
    scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
    scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')

    preview_text = TEXTS["view_scheduled_post_prompt_text"][lang].format(post_id=post_id, scheduled_at_local=scheduled_time_display)
    preview_text += f"\nКанал: *{channel_title}*\n\n" if lang == "ru" else f"\nChannel: *{channel_title}*\n\n"
    preview_text += post["content"] or ("_(без текста)_" if lang == "ru" else "_(no text)_")

    post_buttons_kb = None
    if post["buttons_json"]:
        try:
            btn_list = json.loads(post["buttons_json"])
            if btn_list:
                post_buttons_kb = InlineKeyboardMarkup()
                # Add buttons in rows
                row_buttons = []
                for b in btn_list:
                     if len(row_buttons) < 2:
                          row_buttons.append(InlineKeyboardButton(b["text"], url=b["url"]))
                     else:
                          post_buttons_kb.row(*row_buttons)
                          row_buttons = [InlineKeyboardButton(b["text"], url=b["url"])]
                if row_buttons:
                     post_buttons_kb.row(*row_buttons)
        except Exception as e:
            logger.error(f"Error building keyboard for scheduled post {post_id} preview: {e}")

    # Actions keyboard
    actions_kb = scheduled_post_actions_keyboard(lang, post_id) if user_role in ["owner", "editor"] else None

    # Combine post buttons with action buttons
    combined_kb = InlineKeyboardMarkup()
    if post_buttons_kb:
        for row in post_buttons_kb.inline_keyboard:
            combined_kb.row(*row) # Add post button rows
    if actions_kb:
         for row in actions_kb.inline_keyboard:
             combined_kb.row(*row) # Add action button rows


    # Truncate content if too long for preview
    final_content = preview_text
    if post["media_type"] and post["media_file_id"]:
         if len(final_content) > 1024: final_content = final_content[:1021] + "..."
         send_caption = final_content
         send_text = None
    else:
         if len(final_content) > 4096: final_content = final_content[:4093] + "..."
         send_text = final_content
         send_caption = None


    try:
        sent_msg = None
        if post["media_type"] and post["media_file_id"]:
            try:
                if post["media_type"] == "photo":
                    sent_msg = await bot.send_photo(chat_id, post["media_file_id"], caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif post["media_type"] == "video":
                    sent_msg = await bot.send_video(chat_id, post["media_file_id"], caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif post["media_type"] == "document":
                    sent_msg = await bot.send_document(chat_id, post["media_file_id"], caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif post["media_type"] == "audio":
                    sent_msg = await bot.send_audio(chat_id, post["media_file_id"], caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                elif post["media_type"] == "animation":
                    sent_msg = await bot.send_animation(chat_id, post["media_file_id"], caption=send_caption, reply_markup=combined_kb, parse_mode="Markdown")
                else:
                    logger.warning(f"Unknown media type '{post['media_type']}' for scheduled post preview {post_id}. Sending as text.")
                    sent_msg = await bot.send_message(chat_id, send_text or " ", reply_markup=combined_kb, parse_mode="Markdown")

            except TelegramAPIError as e:
                logger.error(f"Error sending scheduled post {post_id} media preview: {e}")
                # Fallback to sending text only or show error
                fallback_text = f"{send_text or send_caption}\n\n*Ошибка при отправке медиа.*" if lang == "ru" else f"{send_text or send_caption}\n\n*Error sending media.*"
                # Truncate fallback text if necessary
                if len(fallback_text) > 4096: fallback_text = fallback_text[:4093] + "..."
                sent_msg = await bot.send_message(chat_id, fallback_text, reply_markup=combined_kb, parse_mode="Markdown")

        else:
            sent_msg = await bot.send_message(chat_id, send_text or " ", reply_markup=combined_kb, parse_mode="Markdown")

        if sent_msg:
             # Store message ID for editing/deleting later if needed in this state
             state = dp.current_state(chat=chat_id, user=user_id) # Get state for the correct user/chat
             await state.update_data(preview_msg_id=sent_msg.message_id)


    except Exception as e:
        logger.error(f"Failed to send scheduled post {post_id} preview: {e}")
        await bot.send_message(chat_id, "Произошла ошибка при подготовке предпросмотра запланированного поста." if lang == "ru" else "An error occurred while preparing the scheduled post preview.")


@dp.callback_query_handler(lambda c: c.data == "back_to_scheduled_list", state=ScheduledPostsState.viewing_scheduled_post)
async def cb_back_to_scheduled_list(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer()
    data = await state.get_data()
    post_db_id = data.get("post_db_id")
    # Get the channel ID from the state data
    channel_db_id = data.get("channel_id")


    # Delete the preview message
    try: await call.message.delete()
    except: await call.message.edit_reply_markup(reply_markup=None) # Clean up preview message

    await state.finish() # Exit viewing state

    # Use the channel_db_id from state if available, otherwise try to fetch from DB (less reliable)
    if channel_db_id:
         await send_scheduled_posts_list(call.from_user.id, channel_db_id, lang, user_id)
         return
    elif post_db_id: # Fallback: try to get channel_id from DB using post_id
        res = supabase.table("posts").select("channel_id").eq("id", post_db_id).execute()
        if res.data:
            channel_db_id_fallback = res.data[0]["channel_id"]
            await send_scheduled_posts_list(call.from_user.id, channel_db_id_fallback, lang, user_id)
            return

    # If post_id or channel_id missing, just go to main menu
    await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))


@dp.callback_query_handler(lambda c: c.data.startswith("edit_scheduled:"), state=ScheduledPostsState.viewing_scheduled_post)
async def cb_edit_scheduled_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Редактирование...")
    post_id = int(call.data.split(":")[1])

    # Verify user has edit permission (owner/editor) for this channel
    res = supabase.table("posts").select("channel_id").eq("id", post_id).execute()
    if not res.data:
        await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
        await state.finish()
        try: await call.message.delete()
        except: pass
        return
    channel_db_id = res.data[0]["channel_id"]
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         return

    # Keep state as viewing_scheduled_post, but change the keyboard to edit options
    # Also need to load post data into state so edit handlers can access it
    # --- Modified: Select job_id when fetching post for editing ---
    post_res = supabase.table("posts").select("*").eq("id", post_id).execute()
    if not post_res.data:
         await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
         await state.finish()
         try: await call.message.delete()
         except: pass
         return
    post_data = post_res.data[0]
    # --- Modified: Ensure post_db_id, channel_id, etc are in state for subsequent editing steps ---
    await state.update_data(
        post_db_id=post_id, # Ensure post_id is in state
        channel_id=post_data["channel_id"],
        content=post_data["content"],
        media_type=post_data["media_type"],
        media_file_id=post_data["media_file_id"],
        buttons=json.loads(post_data["buttons_json"]) if post_data["buttons_json"] else [],
        scheduled_at=post_data["scheduled_at"],
        is_scheduled=True # It is a scheduled post
        # No need to store job_id in state here
    )


    kb = edit_scheduled_post_keyboard(lang, post_id)
    try:
        # Edit the current preview message to show edit options instead of view actions
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
         # If editing fails (e.g., message too old), send a new message with options
         await bot.send_message(call.from_user.id, TEXTS["edit_scheduled_post_options"][lang], reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("delete_scheduled:"), state=[ScheduledPostsState.viewing_scheduled_post, ScheduledPostsState.waiting_for_channel_selection, None]) # Allow deleting from list, view, or even no state if callback is old
async def cb_delete_scheduled_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
     # --- Modified: Extract post_id ---
     post_id = int(call.data.split(":")[1])

     # Verify user has edit permission (owner/editor) for this channel
     # --- Modified: Select job_id here to cancel the job ---
     res = supabase.table("posts").select("channel_id, job_id").eq("id", post_id).execute()
     if not res.data:
        await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
        # If state was viewing_scheduled_post, finish it and clean up
        current_state = await state.get_state()
        if current_state == ScheduledPostsState.viewing_scheduled_post.state:
             await state.finish()
             try: await call.message.delete() # Clean up old preview message
             except: pass
        # If state was waiting_for_channel_selection, finish it
        elif current_state == ScheduledPostsState.waiting_for_channel_selection.state:
             await state.finish() # Exit this state
             try: await call.message.delete() # Clean up the list item message
             except: pass

        return

     post_info = res.data[0]
     channel_db_id = post_info["channel_id"]
     job_id = post_info["job_id"] # Get job_id from DB

     res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
     if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         return

     # Cancel scheduler job
     if job_id:
         try:
             # Use the job_id retrieved from the DB
             scheduler.remove_job(job_id)
             logger.info(f"Cancelled scheduler job {job_id} for post {post_id} deletion.")
         except Exception as e:
             logger.warning(f"Failed to cancel scheduler job {job_id} for post {post_id} during deletion: {e}")

     # Delete from DB
     try:
          supabase.table("posts").delete().eq("id", post_id).execute()
          logger.info(f"Deleted post {post_id} from DB.")
     except Exception as e:
          logger.error(f"Failed to delete post {post_id} from DB: {e}")
          await call.answer("Ошибка при удалении поста из базы данных." if lang == "ru" else "Error deleting post from database.", show_alert=True)
          return # Stop here if DB deletion failed


     await call.answer(TEXTS["scheduled_post_deleted"][lang])

     current_state = await state.get_state()
     if current_state == ScheduledPostsState.viewing_scheduled_post.state:
         # If deleting from the preview/viewing state
         try: await call.message.delete() # Delete the preview message
         except: await call.message.edit_reply_markup(reply_markup=None)
         await state.finish() # Exit the viewing state
         await bot.send_message(call.from_user.id, TEXTS["scheduled_post_deleted"][lang], reply_markup=main_menu_keyboard(lang))
     elif current_state == ScheduledPostsState.waiting_for_channel_selection.state:
          # If deleting from the list view (message is one of the list items)
          try: await call.message.delete() # Delete the post item from the list
          except: pass # Ignore if message is gone
          # No state change needed here, the list view state remains active until user chooses a channel or cancels
     else: # Deleting from no state (old message)
          # Delete the list item message if it was an old list item
          try: await call.message.delete()
          except: pass
          # Send main menu as the flow is broken
          await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))


# --- Manage Channels Flow ---
@dp.message_handler(commands=['channels', 'manage'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["manage_channels"]["ru"], MENU_BUTTONS["manage_channels"]["en"]], state='*')
async def manage_channels_menu(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply("Вы уже выполняете другое действие. Используйте /cancel для отмены." if lang == "ru" else "You are already performing another action. Use /cancel to cancel.")
        return
    res = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
    channels_owned = res.data or []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
    if channels_owned:
        for ch in channels_owned:
            # Cache channel info
            channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
            channel_cache[ch["id"]]["title"] = ch["title"]
            # Add button
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
    text = TEXTS["manage_intro"][lang]
    if not channels_owned:
        text += "\n" + TEXTS["manage_intro_none"][lang]
    await message.reply(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_channel")
async def cb_add_channel(call: types.CallbackQuery, state: FSMContext, lang: str):
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
        pass
    await AddChannelState.waiting_for_channel_info.set()
    await bot.send_message(call.from_user.id, TEXTS["prompt_add_channel"][lang])

@dp.message_handler(commands=['add_channel'], state='*') # Allow adding via command with args
async def cmd_add_channel(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply("Вы уже выполняете другое действие. Используйте /cancel для отмены." if lang == "ru" else "You are already performing another action. Use /cancel to cancel.")
        return
    args = message.get_args()
    if args:
        # Process args directly
        await process_add_channel_input(message, state, lang, user_id, identifier=args.strip())
    else:
        # Prompt for input if no args
        await AddChannelState.waiting_for_channel_info.set()
        await message.reply(TEXTS["prompt_add_channel"][lang])

@dp.message_handler(state=AddChannelState.waiting_for_channel_info, content_types=ContentType.TEXT)
async def add_channel_received(message: types.Message, state: FSMContext, lang: str, user_id: int):
    identifier = message.text.strip()
    await process_add_channel_input(message, state, lang, user_id, identifier=identifier)

async def process_add_channel_input(message: types.Message, state: FSMContext, lang: str, user_id: int):
    """Helper to process channel identifier input for adding a channel."""
    chat_id = None
    title = None
    try:
        # Attempt to get chat info by identifier (username or ID)
        chat = await bot.get_chat(identifier)
        chat_id = chat.id
        # Ensure it's a channel
        if chat.type != types.ChatType.CHANNEL:
             await message.reply("Отправьте @username или ID именно канала." if lang == "ru" else "Please send the @username or ID of a channel.")
             # Stay in the same state to allow retry
             return

        title = chat.title or identifier # Use title if available, else identifier
        # Check bot's admin status and permissions
        bot_member = await bot.get_chat_member(chat_id, bot.id)
        if not bot_member.is_chat_admin:
             await message.reply(TEXTS["not_admin"][lang])
             await state.finish()
             return
        # Check if bot has required permissions
        required_permissions = ['can_post_messages']
        if not all(getattr(bot_member, perm, False) for perm in required_permissions):
             await message.reply(TEXTS["not_admin"][lang] + "\n" + ("Бот должен иметь права на публикацию сообщений." if lang == "ru" else "The bot must have permissions to post messages."))
             await state.finish()
             return

        # Check user's admin status in the channel
        member = await bot.get_chat_member(chat_id, message.from_user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply(TEXTS["not_admin"][lang] + "\n" + ("Вы должны быть администратором канала." if lang == "ru" else "You must be an administrator of the channel."))
            await state.finish()
            return

    except ChatNotFound:
        await message.reply(TEXTS["channel_not_found"][lang])
        await state.finish()
        return
    except ChatAdminRequired:
        # This can happen if the bot isn't an admin at all
        await message.reply(TEXTS["not_admin"][lang])
        await state.finish()
        return
    except BadRequest as e:
         logger.error(f"BadRequest when adding channel {identifier}: {e}")
         # Specific BadRequest might indicate invalid identifier format or access issue
         await message.reply(TEXTS["channel_not_found"][lang])
         await state.finish()
         return
    except Exception as e:
        logger.error(f"Unexpected error checking channel {identifier}: {e}")
        await message.reply("Произошла ошибка при проверке канала." if lang == "ru" else "An error occurred while checking the channel.")
        await state.finish()
        return


    # Check if channel already exists in DB
    res = supabase.table("channels").select("id").eq("channel_id", chat_id).execute()
    if res.data:
        await message.reply(TEXTS["channel_exists"][lang])
        await state.finish()
        return

    # Insert new channel
    new_channel = {"channel_id": chat_id, "title": title, "owner_id": user_id}
    res_insert = supabase.table("channels").insert(new_channel).execute()
    if not res_insert.data:
        logger.error(f"Failed to insert new channel {chat_id}: {res_insert.error}")
        await message.reply("Ошибка при добавлении канала." if lang == "ru" else "Error adding channel.")
        await state.finish()
        return

    channel_rec = res_insert.data[0]
    # Add owner as the first editor (role 'owner')
    try:
        supabase.table("channel_editors").insert({
            "channel_id": channel_rec["id"],
            "user_id": user_id,
            "role": "owner"
        }).execute()
        logger.info(f"Added owner {user_id} as owner for channel {channel_rec['id']}")
    except Exception as e:
         logger.error(f"Failed to add owner {user_id} as editor for new channel {channel_rec['id']}: {e}")
         # This is a critical error, the owner can't manage the channel. Should probably delete the channel row.
         # For simplicity, just log for now, but this needs better error handling.


    # Cache channel info
    channel_cache[channel_rec["id"]] = {"channel_id": chat_id, "title": title, "owner_id": user_id}

    await message.reply(TEXTS["channel_added"][lang], reply_markup=main_menu_keyboard(lang))
    await state.finish()


@dp.callback_query_handler(lambda c: c.data.startswith("manage:"))
async def cb_manage_channel(call: types.CallbackQuery, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])
    res = supabase.table("channels").select("id, title, owner_id").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    channel = res.data[0]
    title = channel["title"]

    # Cache channel info
    channel_cache[chan_db_id] = channel_cache.get(chan_db_id, {})
    channel_cache[chan_db_id]["title"] = title
    channel_cache[chan_db_id]["owner_id"] = channel["owner_id"]


    kb = manage_channel_keyboard(lang, chan_db_id)
    await call.answer()
    try:
        await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
    except Exception:
        # If edit fails (e.g., message too old), send a new one
        await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data == "back_to_manage")
async def cb_back_to_manage(call: types.CallbackQuery, lang: str, user_id: int):
    res = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
    channels_owned = res.data or []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
    if channels_owned:
        for ch in channels_owned:
             # Cache channel info
            channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
            channel_cache[ch["id"]]["title"] = ch["title"]
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
    text = TEXTS["manage_intro"][lang]
    if not channels_owned:
        text += "\n" + TEXTS["manage_intro_none"][lang]
    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("addedit:"))
async def cb_add_editor(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    await call.answer()
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_reply_markup(reply_markup=None)
        pass
    await state.update_data(channel_id=chan_db_id, channel_title=title)
    await AddEditorState.waiting_for_username.set()
    await bot.send_message(call.from_user.id, TEXTS["prompt_add_editor"][lang])

@dp.message_handler(state=AddEditorState.waiting_for_username, content_types=ContentType.TEXT)
async def add_editor_username(message: types.Message, state: FSMContext, lang: str):
    identifier = message.text.strip()
    target_user = None
    target_tg_id = None # Будет использован для поиска в Supabase

    if identifier.isdigit():
        target_tg_id = int(identifier)
        # Поиск пользователя по tg_id (BIGINT)
        res = supabase.table("users").select("*").eq("tg_id", target_tg_id).execute()
        if res.data:
            target_user = res.data[0]
    else:
        if identifier.startswith("@"):
            identifier = identifier[1:]
        # Поиск пользователя по имени (username), хранящемуся в поле 'name'
        # Это менее надежно, так как username может меняться или не быть установлен
        res = supabase.table("users").select("*").eq("name", "@" + identifier).execute()
        if res.data:
            target_user = res.data[0]
            target_tg_id = target_user["tg_id"] # Получаем tg_id из найденной записи

    # Если пользователь не найден ни по ID, ни по username
    if not target_user:
        await message.reply(TEXTS["user_not_found"][lang])
        # Остаемся в текущем состоянии для повторного ввода
        return

    # Используем Supabase ID пользователя для связей в других таблицах
    target_user_id = target_user["id"]
    target_user_name = target_user["name"]

    data = await state.get_data()
    channel_db_id = data.get("channel_id")

    # Check if user is already an editor/viewer/owner
    res_check = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", target_user_id).execute()
    if res_check.data:
        role_text_key = f"role_{res_check.data[0]['role']}"
        # Use .get() with a default for safety
        role_text = TEXTS.get(role_text_key, {}).get(lang, res_check.data[0]['role']) # Fallback to role name if text key missing
        await message.reply(TEXTS["user_already_editor"][lang].format(role=role_text))
        # Go back to manage channel menu
        title = data.get("channel_title", "Channel")
        kb = manage_channel_keyboard(lang, channel_db_id)
        await bot.send_message(message.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        await state.finish()
        return

    await state.update_data(new_editor_id=target_user_id, new_editor_name=target_user_name)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(TEXTS["role_editor"][lang], callback_data="role_editor"),
           InlineKeyboardButton(TEXTS["role_viewer"][lang], callback_data="role_viewer"))
    msg = await message.reply(TEXTS["choose_role"][lang], reply_markup=kb)
    await state.update_data(manage_msg_id=msg.message_id) # Store message ID to delete later
    await AddEditorState.waiting_for_role.set()

@dp.callback_query_handler(lambda c: c.data in ["role_editor", "role_viewer"], state=AddEditorState.waiting_for_role)
async def cb_select_role(call: types.CallbackQuery, state: FSMContext, lang: str):
    data = await state.get_data()
    channel_db_id = data.get("channel_id")
    new_user_id = data.get("new_editor_id")
    title = data.get("channel_title", "Channel")
    manage_msg_id = data.get("manage_msg_id")

    if not channel_db_id or not new_user_id:
        await call.answer("Error", show_alert=True)
        logger.error("Missing channel_id or new_user_id in cb_select_role state")
        await state.finish()
        # Clean up message if exists
        if manage_msg_id:
            try: await bot.delete_message(call.message.chat.id, manage_msg_id)
            except: pass
        return

    role = "editor" if call.data == "role_editor" else "viewer"
    role_text = TEXTS["role_editor"][lang] if role == "editor" else TEXTS["role_viewer"][lang]

    try:
        supabase.table("channel_editors").insert({
            "channel_id": channel_db_id,
            "user_id": new_user_id,
            "role": role
        }).execute()

        await call.answer()
        # Clean up the role selection message
        if manage_msg_id:
            try: await bot.delete_message(call.message.chat.id, manage_msg_id)
            except: await call.message.edit_reply_markup(reply_markup=None) # Fallback

        await bot.send_message(call.from_user.id, TEXTS["editor_added"][lang].format(role_text=role_text))

        # Go back to manage channel menu for the specific channel
        kb = manage_channel_keyboard(lang, channel_db_id)
        await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        await state.finish()

    except Exception as e:
        logger.error(f"Failed to add editor {new_user_id} to channel {channel_db_id}: {e}")
        await call.answer("Ошибка при добавлении пользователя." if lang == "ru" else "Error adding user.", show_alert=True)
         # Clean up message if exists
        if manage_msg_id:
            try: await bot.delete_message(call.message.chat.id, manage_msg_id)
            except: pass
        await state.finish()
         # Go back to main menu on error
        await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))



@dp.callback_query_handler(lambda c: c.data.startswith("remedit:"))
async def cb_remove_editor_menu(call: types.CallbackQuery, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]

    # Fetch all editors/viewers for this channel (excluding the owner)
    res_editors = supabase.table("channel_editors").select("user_id, role").eq("channel_id", chan_db_id).neq("role", "owner").execute()
    editors = res_editors.data or []

    if not editors:
        await call.answer("Редакторы или наблюдатели не найдены." if lang == "ru" else "No editors or viewers found.", show_alert=True)
        # Stay on the manage channel menu - re-fetch and re-send it?
        kb = manage_channel_keyboard(lang, chan_db_id)
        try: await call.message.edit_reply_markup(reply_markup=kb) # Update keyboard in case
        except: pass
        return


    user_ids = [e["user_id"] for e in editors]
    # Fetch names for these users
    res_users = supabase.table("users").select("id, name").in_("id", user_ids).execute()
    users = res_users.data or []
    name_map = {u["id"]: u["name"] for u in users}

    kb = InlineKeyboardMarkup()
    for e in editors:
        uid = e["user_id"]
        role = e["role"]
        name = name_map.get(uid, f"ID: {uid}") # Fallback if user not found in DB (shouldn't happen with FK)
        role_text = TEXTS["role_editor"][lang] if role == "editor" else TEXTS["role_viewer"][lang]
        btn_text = f"{name} ({role_text})"
        kb.add(InlineKeyboardButton(btn_text, callback_data=f"confirmremedit:{chan_db_id}:{uid}")) # Ask for confirmation
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"manage:{chan_db_id}"))

    await call.answer()
    try:
        # Edit the current manage channel message to show the remove editor list
        await call.message.edit_text(TEXTS["remove_editor_prompt"][lang], reply_markup=kb)
    except Exception:
        # If edit fails, send a new message
        await bot.send_message(call.from_user.id, TEXTS["remove_editor_prompt"][lang], reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("confirmremedit:"))
async def cb_confirm_remove_user(call: types.CallbackQuery, lang: str, user_id: int):
    parts = call.data.split(":")
    chan_db_id = int(parts[1])
    user_to_remove_id = int(parts[2])

    # Verify ownership again for safety
    res_owner = supabase.table("channels").select("owner_id").eq("id", chan_db_id).execute()
    if not res_owner.data or res_owner.data[0]["owner_id"] != user_id:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         # Go back to the remove list or manage menu
         kb = manage_channel_keyboard(lang, chan_db_id) # Assuming we were on manage screen before remove list
         try: await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=channel_cache.get(chan_db_id, {}).get("title", "Channel")), reply_markup=kb)
         except: pass
         return

    # Fetch user name for confirmation message
    user_res = supabase.table("users").select("name").eq("id", user_to_remove_id).execute()
    user_name = user_res.data[0]["name"] if user_res.data else f"ID: {user_to_remove_id}"

    # Ensure the user being removed is not the owner (double check)
    if user_to_remove_id == user_id:
        await call.answer("Нельзя удалить владельца канала." if lang == "ru" else "Cannot remove channel owner.", show_alert=True)
        # Go back to the remove list
        kb = InlineKeyboardMarkup() # Build the remove editor list again
        res_editors = supabase.table("channel_editors").select("user_id, role").eq("channel_id", chan_db_id).neq("role", "owner").execute()
        editors = res_editors.data or []
        user_ids = [e["user_id"] for e in editors]
        res_users = supabase.table("users").select("id, name").in_("id", user_ids).execute()
        name_map = {u["id"]: u["name"] for u in res_users.data or []}
        for e in editors:
             uid = e["user_id"]
             role = e["role"]
             name = name_map.get(uid, f"ID: {uid}")
             role_text = TEXTS["role_editor"][lang] if role == "editor" else TEXTS["role_viewer"][lang]
             btn_text = f"{name} ({role_text})"
             kb.add(InlineKeyboardButton(btn_text, callback_data=f"confirmremedit:{chan_db_id}:{uid}"))
        kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"remedit:{chan_db_id}"))
        try: await call.message.edit_text(TEXTS["remove_editor_prompt"][lang], reply_markup=kb)
        except: pass
        return


    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ " + ("Подтвердить" if lang == "ru" else "Confirm"), callback_data=f"removeuser:{chan_db_id}:{user_to_remove_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data=f"remedit:{chan_db_id}")) # Go back to the remove editor list

    await call.answer()
    try:
        # Edit the current remove list message to ask for confirmation
        await call.message.edit_text(f"Вы уверены, что хотите удалить пользователя {user_name}?" if lang == "ru" else f"Are you sure you want to remove user {user_name}?", reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, f"Вы уверены, что хотите удалить пользователя {user_name}?" if lang == "ru" else f"Are you sure you want to remove user {user_name}?", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("removeuser:"))
async def cb_remove_user(call: types.CallbackQuery, lang: str, user_id: int):
    parts = call.data.split(":")
    chan_db_id = int(parts[1])
    user_to_remove_id = int(parts[2])

    # Verify ownership again for safety
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]

    # Ensure the user being removed is not the owner (triple check)
    if user_to_remove_id == user_id:
        await call.answer("Нельзя удалить владельца канала." if lang == "ru" else "Cannot remove channel owner.", show_alert=True)
        # Go back to the remove list (or manage menu if that fails)
        kb = manage_channel_keyboard(lang, chan_db_id) # Assuming we were on manage screen before remove list
        try: await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        except: pass
        return

    try:
        supabase.table("channel_editors").delete().eq("channel_id", chan_db_id).eq("user_id", user_to_remove_id).execute()

        await call.answer(TEXTS["user_removed"][lang])
        # Go back to the manage channel menu
        kb = manage_channel_keyboard(lang, chan_db_id)
        try:
            # Attempt to edit the current confirmation message
            await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        except Exception:
            # Fallback if edit fails
            await bot.send_message(call.from_user.id, TEXTS["user_removed"][lang], reply_markup=main_menu_keyboard(lang)) # Go to main menu on failure
            # Optionally, send the manage menu separately if main menu is not desired fallback
            # await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
            pass # Or just send main menu

    except Exception as e:
         logger.error(f"Failed to remove editor {user_to_remove_id} from channel {chan_db_id}: {e}")
         await call.answer("Ошибка при удалении пользователя." if lang == "ru" else "Error removing user.", show_alert=True)
         # Go back to manage channel menu
         kb = manage_channel_keyboard(lang, chan_db_id)
         try:
              await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
         except Exception:
              await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("delchan:"))
async def cb_delete_channel_confirm(call: types.CallbackQuery, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])
    res = supabase.table("channels").select("title, owner_id").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ " + ("Удалить" if lang == "ru" else "Yes"), callback_data=f"confirm_del:{chan_db_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data=f"manage:{chan_db_id}")) # Go back to manage menu

    await call.answer()
    try:
        # Edit the current manage channel message to show confirmation
        await call.message.edit_text(TEXTS["confirm_delete_channel"][lang].format(title=title), reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, TEXTS["confirm_delete_channel"][lang].format(title=title), reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("confirm_del:"))
async def cb_delete_channel(call: types.CallbackQuery, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])
    res = supabase.table("channels").select("title, owner_id").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]

    try:
        # Get job_ids for scheduled posts in this channel to cancel them
        # --- Modified: Select job_id here ---
        res_posts = supabase.table("posts").select("job_id").eq("channel_id", chan_db_id).execute() # Check all posts for job_id just in case
        for post in res_posts.data or []:
            if post.get("job_id"): # Use .get() for safety
                try:
                    # Remove job by its ID
                    scheduler.remove_job(post["job_id"])
                    logger.info(f"Cancelled scheduler job {post['job_id']} for channel deletion.")
                except Exception as e:
                    logger.warning(f"Failed to cancel job {post['job_id']} during channel deletion: {e}")

        # Delete related entries first due to foreign keys
        # Supabase usually supports cascading deletes if configured, but explicit deletion is safer
        supabase.table("posts").delete().eq("channel_id", chan_db_id).execute()
        supabase.table("channel_editors").delete().eq("channel_id", chan_db_id).execute()
        # Delete the channel itself
        supabase.table("channels").delete().eq("id", chan_db_id).execute()

        # Remove channel from cache
        if chan_db_id in channel_cache:
            del channel_cache[chan_db_id]
            logger.info(f"Removed channel {chan_db_id} from cache.")

        await call.answer(TEXTS["channel_removed"][lang].format(title=title))

        # Show updated manage channels menu
        res2 = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
        channels_owned = res2.data or []
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
        if channels_owned:
            for ch in channels_owned:
                 # Cache channel info
                channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
                channel_cache[ch["id"]]["title"] = ch["title"]
                kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
        text = TEXTS["channel_removed"][lang].format(title=title) + "\n\n" + TEXTS["manage_intro"][lang]
        if not channels_owned:
            text += "\n" + TEXTS["manage_intro_none"][lang]

        try:
            # Edit the current confirmation message to show the updated manage menu
            await call.message.edit_text(text, reply_markup=kb)
        except Exception:
            # Fallback if edit fails
            await bot.send_message(call.from_user.id, text, reply_markup=kb)


    except Exception as e:
        logger.error(f"Failed to delete channel {chan_db_id}: {e}")
        await call.answer("Ошибка при удалении канала." if lang == "ru" else "Error deleting channel.", show_alert=True)
        # Go back to main menu on error, as the channel might be partially deleted
        await bot.send_message(call.from_user.id, "Произошла ошибка при удалении канала." if lang == "ru" else "An error occurred while deleting the channel.", reply_markup=main_menu_keyboard(lang))


# --- Settings Flow (Language and Timezone) ---
@dp.message_handler(commands=['settings'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["settings"]["ru"], MENU_BUTTONS["settings"]["en"]], state='*')
async def open_settings(message: types.Message, state: FSMContext, lang: str):
    if await state.get_state() is not None:
        await message.reply("Вы уже выполняете другое действие. Используйте /cancel для отмены." if lang == "ru" else "You are already performing another action. Use /cancel to cancel.")
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🌐 " + ("Язык" if lang == "ru" else "Language"), callback_data="settings_lang"))
    kb.add(InlineKeyboardButton("⏰ " + ("Часовой пояс" if lang == "ru" else "Timezone"), callback_data="settings_timezone"))
    await message.reply(TEXTS["settings"][lang] + ":", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "settings_lang")
async def cb_open_language_settings(call: types.CallbackQuery, lang: str):
    await call.answer()
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Русский", callback_data="lang_ru"), InlineKeyboardButton("English", callback_data="lang_en"))
    try:
        await call.message.edit_text(TEXTS["language_prompt"][lang], reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, TEXTS["language_prompt"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data in ["lang_ru", "lang_en"])
async def cb_set_language(call: types.CallbackQuery, lang: str, user_id: int):
    new_lang = "ru" if call.data == "lang_ru" else "en"
    try:
        # Обновление языка пользователя по его Supabase ID
        supabase.table("users").update({"language": new_lang}).eq("id", user_id).execute()
        # Update cache
        user_cache[call.from_user.id] = user_cache.get(call.from_user.id, {}) # Ensure user exists in cache
        user_cache[call.from_user.id]["language"] = new_lang # Use 'language' key as in DB schema

        await call.answer(TEXTS["language_changed"][new_lang])
        try:
            # Edit the language selection message to confirmation text
            await call.message.edit_text(TEXTS["language_changed"][new_lang])
            # No need to send main menu separately if edited successfully
        except Exception:
            # If edit fails, send a new confirmation message and the menu
            await bot.send_message(call.from_user.id, TEXTS["language_changed"][new_lang])
        # Always send the main menu in the new language
        await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][new_lang], reply_markup=main_menu_keyboard(new_lang))

    except Exception as e:
         logger.error(f"Failed to set language for user {user_id}: {e}")
         await call.answer("Ошибка при изменении языка." if lang == "ru" else "Error changing language.", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "settings_timezone")
async def cb_open_timezone_settings(call: types.CallbackQuery, state: FSMContext, lang: str):
     await call.answer()
     try:
         await call.message.delete()
     except Exception:
         await call.message.edit_reply_markup(reply_markup=None)
         pass
     await SettingsState.waiting_for_timezone.set()
     await bot.send_message(call.from_user.id, TEXTS["timezone_prompt"][lang], disable_web_page_preview=True)


@dp.message_handler(state=SettingsState.waiting_for_timezone, content_types=ContentType.TEXT)
async def timezone_received(message: types.Message, state: FSMContext, lang: str, user_id: int):
    timezone_str = message.text.strip()
    valid_timezone = None

    # Attempt to parse using dateparser's timezone handling
    # dateparser settings can return timezone name if valid
    settings = {'RETURN_AS_TIMEZONE_AWARE': True, 'TIMEZONE': timezone_str, 'TIMEZONES': pytz.all_timezones_set} # Try user input as TIMEZONE
    parsed_dt = dateparser.parse('now', settings=settings) # Parse a simple string like 'now' to test if timezone is recognized

    if parsed_dt and parsed_dt.tzinfo is not None:
         # dateparser successfully parsed 'now' with the given timezone string
         # Get the canonical timezone name from the parsed object
         # pytz timezones have a .tzinfo.zone attribute
         if hasattr(parsed_dt.tzinfo, 'zone'):
              valid_timezone = parsed_dt.tzinfo.zone
         elif str(parsed_dt.tzinfo).startswith('UTC'):
              # Handle simple UTC offsets returned by dateparser
              # We should store UTC offsets like 'UTC+03:00' or 'Etc/GMT-3' consistently
              # dateparser often returns 'UTC+HH:MM'
              valid_timezone = str(parsed_dt.tzinfo) # e.g., 'UTC+03:00'
              # Try converting 'UTC+03:00' style to Etc/GMT style for better compatibility if needed
              try:
                   offset_td = parsed_dt.utcoffset()
                   total_minutes = int(offset_td.total_seconds() / 60)
                   if total_minutes % 60 == 0: # Only handle full hour offsets for Etc/GMT
                        hours_offset = total_minutes // 60
                        # Etc/GMT sign is opposite of UTC sign
                        gmt_offset_str = f"Etc/GMT{-hours_offset}"
                        if gmt_offset_str in pytz.all_timezones_set:
                             valid_timezone = gmt_offset_str
                             logger.info(f"Converted UTC offset {timezone_str} to pytz canonical {valid_timezone}")
              except Exception:
                   pass # Keep the UTC offset string if Etc/GMT conversion fails


    # Fallback check if dateparser didn't give a valid timezone name
    if valid_timezone is None:
         # Check if it's a standard pytz timezone name
         if timezone_str in pytz.all_timezones_set:
              valid_timezone = timezone_str
         else:
             # Try UTC±HH:MM format explicitly if pytz name check fails
             try:
                  import re
                  # Match UTC, GMT, or Z (for UTC+0) followed by optional +/-HH:MM or +/-HH
                  match = re.match(r'^(UTC|GMT|Z)([+-]\d{1,2})?(:(\d{2}))?$', timezone_str.upper())
                  if match:
                      base = match.group(1)
                      offset_sign_hours_str = match.group(2)
                      offset_minutes_str = match.group(4)

                      if base == 'Z' or (base in ['UTC', 'GMT'] and offset_sign_hours_str is None):
                          # UTC or GMT or Z without offset means UTC+0
                          valid_timezone = 'UTC'
                      elif offset_sign_hours_str:
                          hours = int(offset_sign_hours_str)
                          minutes = int(offset_minutes_str) if offset_minutes_str else 0
                          if abs(hours) <= 14 and minutes >= 0 and minutes < 60: # Max UTC offset is around +/- 14
                              # Construct a consistent UTC+HH:MM string format
                               sign = '+' if hours >= 0 else '-'
                               valid_timezone = f"UTC{sign}{abs(hours):02d}:{minutes:02d}"
                               # Attempt Etc/GMT conversion again for standard names preference
                               try:
                                   sign_multiplier = 1 if sign == '+' else -1
                                   total_seconds = (hours * 3600 + sign_multiplier * minutes * 60)
                                   offset_td = dt.timedelta(seconds=total_seconds)
                                   offset_minutes_total = int(offset_td.total_seconds() / 60)
                                   if offset_minutes_total % 60 == 0:
                                        hours_offset = offset_minutes_total // 60
                                        gmt_offset_str = f"Etc/GMT{-hours_offset}" # Etc/GMT sign is opposite
                                        if gmt_offset_str in pytz.all_timezones_set:
                                             valid_timezone = gmt_offset_str
                               except Exception:
                                    pass # Keep UTC+HH:MM if Etc/GMT conversion fails


             except Exception:
                  pass # Regex or parsing failed


    if valid_timezone is None:
        await message.reply(TEXTS["invalid_timezone"][lang])
        return # Stay in state

    try:
        # Store the validated timezone string
        # Обновление часового пояса пользователя по его Supabase ID
        supabase.table("users").update({"timezone": valid_timezone}).eq("id", user_id).execute()
        # Update cache
        user_cache[message.from_user.id] = user_cache.get(message.from_user.id, {})
        user_cache[message.from_user.id]["timezone"] = valid_timezone

        await message.reply(TEXTS["timezone_updated"][lang].format(timezone=valid_timezone), reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except Exception as e:
        logger.error(f"Failed to set timezone for user {user_id}: {e}")
        await message.reply("Ошибка при сохранении часового пояса." if lang == "ru" else "Error saving timezone.")
        await state.finish()


# --- Fallback Handlers ---
# Handlers for commands need state='*' to work globally
# Handlers with specific states run before global state='*'
# So, command handlers like /cancel will intercept messages first.
# This handler catches non-command text or other content types when in NO specific state.
@dp.message_handler(state=None, content_types=ContentType.ANY)
async def handle_unknown_message_no_state(message: types.Message, lang: str):
    # If not in a state and not a recognized command (commands are handled first),
    # it's likely an unknown message. Prompt main menu.
    await message.reply(TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))

# This handler catches messages when in a specific state, but the input type/content doesn't match
# any handler defined for that state.
@dp.message_handler(state='*', content_types=ContentType.ANY)
async def handle_unknown_message_in_state(message: types.Message, lang: str):
     # If in a specific state, but input doesn't match any handler for that state (incl. global commands),
     # it's invalid input for the current state.
     await message.reply(TEXTS["invalid_input"][lang])


# This handler catches callbacks when not in a state.
# It means the callback came from an old or unexpected inline keyboard.
@dp.callback_query_handler(state=None)
async def handle_unknown_callback_no_state(call: types.CallbackQuery):
     # Just answer the callback query silently.
     await call.answer()
     # Optionally send the main menu, but might be spammy if user clicks old buttons repeatedly.
     # await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))


# This handler catches callbacks when in a state, but the callback data doesn't match
# any handler defined for that state.
@dp.callback_query_handler(state='*')
async def handle_unknown_callback_in_state(call: types.CallbackQuery, lang: str):
     await call.answer("Неожиданное действие." if lang == "ru" else "Unexpected action.", show_alert=True)
     # No state transition needed, stay in the current state.


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
