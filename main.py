
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
# Import the async client parts if available or just use the standard client and await execute()
from supabase import create_client, Client
from aiogram.utils.exceptions import ChatNotFound, ChatAdminRequired, BadRequest, TelegramAPIError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import dateparser # Helps parse date/time strings
import datetime as dt # Use dt for datetime module to avoid conflict with datetime object
import re # For timezone parsing


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
    raise Exception("Missing BOT_TOKEN or Supabase configuration.")

# Initialize Supabase client (NOTE: is_async=True is required for await on execute())
# Create synchronously, methods become async
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, is_async=True)


# Initialize bot, dispatcher, storage, and scheduler
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

# In-memory cache for user data (telegram_user_id -> {id, name, username, language, timezone})
user_cache = {}
# In-memory cache for channel data (db_id -> {channel_id, title, owner_id})
channel_cache = {}

# Menu button texts (Keep existing from provided code)
MENU_BUTTONS = {
    "create_post": {"ru": "Создать пост", "en": "Create Post"},
    "scheduled_posts": {"ru": "Запланированные посты", "en": "Scheduled Posts"},
    "settings": {"ru": "Настройки", "en": "Settings"},
    "manage_channels": {"ru": "Управление каналами", "en": "Manage Channels"}
}

# Text prompts in both languages (Keep existing from provided code, add/adjust if needed)
TEXTS = {
    "welcome": {
        "ru": "Привет! Я бот для управления постами в ваших Telegram-каналах.", # Slightly rephrase
        "en": "Hello! I am a bot to help you manage posts in your Telegram channels."
    },
    "menu_prompt": {
        "ru": "Выберите действие в меню ниже.",
        "en": "Please choose an action from the menu below."
    },
    "registration_success": { # New text for successful registration
        "ru": "Вы успешно зарегистрированы!",
        "en": "You have been successfully registered!"
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


# Keyboard builders (Keep existing from provided code)
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

        if user and user.id:
            telegram_user_id = user.id
            username = user.username
            name = user.first_name
            if user.last_name:
                name += " " + user.last_name

            # Check cache first
            user_record = user_cache.get(telegram_user_id)

            if not user_record:
                # Try fetching from DB using telegram_user_id
                try:
                    # Use async execute()
                    res = await supabase.table("users").select("*").eq("telegram_user_id", telegram_user_id).execute()
                    if res.data:
                        user_record = res.data[0]
                        # Update name and username in DB if changed
                        # Ensure `name` and `username` columns exist based on schema
                        update_data = {}
                        if user_record.get("name") != name:
                            update_data["name"] = name
                        if user_record.get("username") != username:
                             update_data["username"] = username

                        if update_data:
                             # Use async execute()
                             await supabase.table("users").update(update_data).eq("id", user_record["id"]).execute()
                             # Update cached version
                             if "name" in update_data: user_record["name"] = name
                             if "username" in update_data: user_record["username"] = username

                        user_cache[telegram_user_id] = user_record # Cache the full record
                        logger.info(f"User data updated in cache for {telegram_user_id}.")
                    else:
                        # Insert new user with telegram_user_id
                        # Use async execute()
                        insert_data = {"telegram_user_id": telegram_user_id, "name": name}
                        if username:
                             insert_data["username"] = username
                        res_insert = await supabase.table("users").insert(insert_data).execute()

                        if res_insert.data:
                            user_record = res_insert.data[0]
                            user_cache[telegram_user_id] = user_record # Cache new record
                            logger.info(f"New user registered: {telegram_user_id} ({name})")
                            # If this is the *first* registration, the /start handler will send the welcome message.
                        else:
                            logger.error(f"Failed to insert new user {telegram_user_id}: {res_insert.error}")
                            # Cannot proceed without a user record
                            # Attempt to notify the user and consume the update
                            try:
                                if update.message:
                                    await bot.send_message(telegram_user_id, "Произошла внутренняя ошибка при регистрации пользователя. Пожалуйста, попробуйте позже." if user_cache.get(telegram_user_id, {}).get("language", "ru") == "ru" else "An internal error occurred during user registration. Please try again later.")
                                elif update.callback_query:
                                     await bot.answer_callback_query(update.callback_query.id, "Произошла внутренняя ошибка.", show_alert=True)
                            except Exception as e:
                                logger.error(f"Failed to send error message to user {telegram_user_id}: {e}")

                            update.consumed = True # Consume update if we can't get user info
                            return # Stop processing this update

                except Exception as e:
                    logger.error(f"Database error during user check/registration for {telegram_user_id}: {e}")
                    # Handle critical error - cannot proceed for this user
                    # Attempt to notify the user and consume the update
                    try:
                         # Use a generic error message as language might not be fetched
                         error_msg = "Произошла ошибка при обработке запроса. Пожалуйста, попробуйте позже."
                         if update.message:
                             await bot.send_message(telegram_user_id, error_msg)
                         elif update.callback_query:
                              await bot.answer_callback_query(update.callback_query.id, "Произошла ошибка.", show_alert=True)
                    except Exception as err_send:
                        logger.error(f"Failed to send database error message to user {telegram_user_id}: {err_send}")

                    update.consumed = True # Consume update if we can't get user info
                    return # Stop processing this update

            if user_record:
                # Pass Supabase ID (UUID), language, and timezone to data for handlers
                # The user_record['id'] is the UUID from Supabase
                data["user_id"] = user_record["id"]
                data["lang"] = user_record.get("language", "ru") # Default to ru if not set
                data["timezone"] = user_record.get("timezone", "UTC") # Default to UTC if not set
            else:
                 # This case should ideally not be reached due to the 'return' statements above
                 logger.error(f"User record is None after DB check/insert logic for telegram_user_id {telegram_user_id}")
                 # As a fallback safety, consume the update if user_record is still None
                 update.consumed = True
                 return


dp.middleware.setup(DBMiddleware())

# FSM state groups (Keep existing from provided code)
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


# Scheduler helper function (Modify to use await)
async def schedule_post_job(post_id: int):
    """Fetches post from DB, publishes it, updates status."""
    try:
        # Use async execute()
        res = await supabase.table("posts").select("id, channel_id, content, media_type, media_file_id, buttons_json, status, job_id").eq("id", post_id).execute()
        if not res.data:
            logger.warning(f"Scheduler job failed: Post {post_id} not found.")
            # Attempt to remove job if it exists by job_id - This cleanup is better handled in load_scheduled_posts or a dedicated cleanup task
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
                  # Use async execute()
                  await supabase.table("posts").update({"job_id": None}).eq("id", post_id).execute()
             except Exception as e:
                  logger.error(f"Failed to clear job_id for post {post_id} with status {post['status']}: {e}")

             return # Exit if not scheduled

        # Get channel Telegram ID
        # Use async execute()
        channel_res = await supabase.table("channels").select("channel_id").eq("id", channel_db_id).execute()
        if not channel_res.data:
             logger.error(f"Scheduler job failed: Channel DB ID {channel_db_id} not found for post {post_id}. Cannot publish.")
             # Mark post status as failed publishing and clear job_id
             try:
                  # Use async execute()
                  await supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
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
            # Use async execute()
            await supabase.table("posts").update({"status": "published", "posted_at": datetime.now(pytz.utc).isoformat(), "job_id": None}).eq("id", post_id).execute()
            logger.info(f"Post {post_id} successfully published to {tg_channel_id}.")

            # Remove the job from APScheduler explicitly after successful execution
            if current_job_id:
                 try:
                     scheduler.remove_job(current_job_id)
                     logger.info(f"Removed job {current_job_id} from scheduler after successful publication of post {post_id}.")
                 except Exception as e:
                     logger.error(f"Failed to remove job {current_job_id} from scheduler after publication of post {post_id}: {e}")


        except (ChatNotFound, ChatAdminRequired, BadRequest) as e:
             logger.error(f"Telegram API permissions/chat error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
             # Mark post status as failed publishing and clear job_id
             try:
                  # Use async execute()
                  await supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
             except Exception as e:
                  logger.error(f"Failed to mark post {post_id} as publishing_failed and clear job_id: {e}")
             pass # Optionally notify owner?

        except TelegramAPIError as e:
            logger.error(f"Telegram API Generic Error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
            # Mark post status as failed publishing and clear job_id
            try:
                 # Use async execute()
                 await supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
            except Exception as e:
                 logger.error(f"Failed to mark post {post_id} as publishing_failed and clear job_id: {e}")
            pass # Or update status?

        except Exception as e:
            logger.error(f"Unexpected error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
            try:
                 # Use async execute()
                 await supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
            except Exception as e:
                 logger.error(f"Failed to mark post {post_id} as publishing_failed: {e}")


    except Exception as e:
        logger.error(f"Error in schedule_post_job for post {post_id}: {e}")
        # If an error occurs before accessing the post or getting channel_id
        try:
             # Use async execute()
             await supabase.table("posts").update({"status": "publishing_failed", "job_id": None}).eq("id", post_id).execute()
             logger.info(f"Marked post {post_id} as publishing_failed due to error before sending.")
        except Exception as db_err:
             logger.error(f"Failed to mark post {post_id} as publishing_failed after error: {db_err}")


async def load_scheduled_posts():
    """Loads scheduled posts from DB and adds them to the scheduler."""
    now_utc = datetime.now(pytz.utc)
    # Only load posts with status 'scheduled' and scheduled in the future
    # Use async execute()
    res = await supabase.table("posts").select("id, scheduled_at, job_id").eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).execute()
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
             # This check is implicitly covered by the first condition (job.id not in db_job_ids)
             # and the check for job.args. Skipping explicit check here to avoid complexity.


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
                  # Use async execute()
                  await supabase.table("posts").update({"status": "draft", "job_id": None}).eq("id", post_id).execute()
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
                 # Use async execute()
                 await supabase.table("posts").update({"job_id": job.id}).eq("id", post_id).execute()
                 logger.info(f"Updated job_id in DB for post {post_id} to {job.id}.")
            else:
                 logger.info(f"Verified job_id {job.id} in DB matches APScheduler job for post {post_id}.")


            logger.info(f"Loaded scheduled post {post_id} with job ID {job.id} for {scheduled_time_utc}.")

        except Exception as e:
            logger.error(f"Failed to load scheduled post {post_id} into scheduler: {e}")


async def on_startup(dp):
    # Delete webhook to ensure updates are received via polling
    # Ensure async call
    await bot.delete_webhook(drop_pending_updates=True)
    scheduler.start()
    # Load scheduled posts into scheduler on startup
    await load_scheduled_posts()
    logger.info("Bot started and scheduler loaded.")

# --- General Handlers ---
@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, lang: str): # lang is passed by middleware
    # The DBMiddleware ensures the user is registered before this handler runs.
    # This handler just sends the welcome message and main menu.
    welcome_text = TEXTS["welcome"][lang] + "\n" + TEXTS["menu_prompt"][lang]
    await message.reply(welcome_text, reply_markup=main_menu_keyboard(lang))


# Keep other handlers as they are, ensuring await is added before supabase calls

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

    # Use async execute()
    res = await supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    channels_access = res.data or []

    if not channels_access:
        await message.reply(TEXTS["no_edit_channels"][lang])
        return

    channel_db_ids = [entry["channel_id"] for entry in channels_access]
    # Use async execute()
    res2 = await supabase.table("channels").select("id, title").in_("id", channel_db_ids).execute()
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

@dp.callback_query_handler(lambda c: c.data.startswith("selch_post:"), state=PostStates.waiting_for_channel)
async def select_channel_for_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer()
    channel_db_id = int(call.data.split(":")[1])

    # Verify user has edit permission (owner/editor) for this channel
    # Use async execute()
    res_role = await supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         await state.finish() # Exit the creation flow
         # Delete the channel selection message if it still exists
         data = await state.get_data()
         select_msg_id = data.get("select_msg_id")
         if select_msg_id:
              try: await bot.delete_message(call.message.chat.id, select_msg_id)
              except: await call.message.edit_reply_markup(reply_markup=None)
         return

    # Delete the channel selection message
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_reply_markup(reply_markup=None)
        pass


    await state.update_data(channel_id=channel_db_id, content=None, media_type=None, media_file_id=None, buttons=[])
    await PostStates.waiting_for_text.set()
    await bot.send_message(call.from_user.id, TEXTS["enter_post_text"][lang])


# --- Input Handlers for Post Content ---
# Text input (can be actual text or /skip)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_text)
async def post_text_received(message: types.Message, state: FSMContext, lang: str):
    text = message.text.strip() # Use strip()
    data = await state.get_data()
    post_db_id = data.get("post_db_id") # Check if editing scheduled post

    if text.lower() in ["/skip", "скип", "пропустить"]:
        text_to_save = None # Save None for empty text in DB
        # If there was previous content (e.g., editing a post), clear it
        data["content"] = None
        await state.update_data(data)
        # await state.update_data(content=None) # Save None for empty text
    else:
        text_to_save = text
        await state.update_data(content=text_to_save)


    # After receiving text, move to asking for media
    # If we were editing an existing scheduled post, go back to preview after setting text
    if post_db_id is not None: # Editing an existing scheduled post
         try:
              # Update content in DB
              # Use async execute()
              await supabase.table("posts").update({"content": text_to_save}).eq("id", post_db_id).execute()
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
                     # Use async execute()
                     await supabase.table
