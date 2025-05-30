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
        "en": "Please send the channel @username or ID that you want to add. Make sure the bot is an administrator in that channel."
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
    kb.add(InlineKeyboardButton("✏️ Изменить текст" if lang == "ru" else "✏️ Edit Text", callback_data=f"edit_post:text:{post_db_id}" if post_db_id else "edit_draft:text"))
    kb.add(InlineKeyboardButton("🖼️ Изменить медиа" if lang == "ru" else "🖼️ Edit Media", callback_data=f"edit_post:media:{post_db_id}" if post_db_id else "edit_draft:media"))
    kb.add(InlineKeyboardButton("🔘 Изменить кнопки" if lang == "ru" else "🔘 Edit Buttons", callback_data=f"edit_post:buttons:{post_db_id}" if post_db_id else "edit_draft:buttons"))
    if is_scheduled: # Option to change schedule time only applies if it's scheduled
         kb.add(InlineKeyboardButton("⏰ Изменить время" if lang == "ru" else "⏰ Edit Time", callback_data=f"edit_post:time:{post_db_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data="cancel_post_creation"))
    return kb

def scheduled_post_actions_keyboard(lang: str, post_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✏️ " + ("Редактировать" if lang == "ru" else "Edit"), callback_data=f"edit_scheduled:{post_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить" if lang == "ru" else "Delete"), callback_data=f"delete_scheduled:{post_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"back_to_scheduled_list")) # Needs channel_id? Or just go back to menu? Back to menu is simpler for now.
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
            tg_id = user.id
            username = user.username
            name = user.first_name
            if user.last_name:
                name += " " + user.last_name
            if username:
                 name = "@" + username # Prefer @username if available

            user_record = user_cache.get(tg_id)

            if not user_record:
                # Try fetching from DB
                res = supabase.table("users").select("*").eq("tg_id", tg_id).execute()
                if res.data:
                    user_record = res.data[0]
                    # Update name in DB if changed
                    if user_record["name"] != name:
                         supabase.table("users").update({"name": name}).eq("id", user_record["id"]).execute()
                         user_record["name"] = name # Update cached version too
                    user_cache[tg_id] = user_record # Cache the full record
                else:
                    # Insert new user
                    res_insert = supabase.table("users").insert({"tg_id": tg_id, "name": name}).execute()
                    if res_insert.data:
                        user_record = res_insert.data[0]
                        user_cache[tg_id] = user_record # Cache new record
                        logger.info(f"New user registered: {tg_id} ({name})")
                    else:
                        logger.error(f"Failed to insert new user {tg_id}: {res_insert.error}")
                        # Cannot proceed without a user record
                        return

            if user_record:
                data["user_id"] = user_record["id"]
                data["lang"] = user_record.get("language", "ru")
                data["timezone"] = user_record.get("timezone", "UTC") # Default to UTC if not set
            else:
                 logger.error(f"User record is None after DB check/insert for tg_id {tg_id}")
                 # Handle critical error - cannot proceed for this user
                 if update.message:
                     await bot.send_message(tg_id, "Произошла внутренняя ошибка. Пожалуйста, попробуйте позже.")
                 elif update.callback_query:
                      await bot.answer_callback_query(update.callback_query.id, "Произошла внутренняя ошибка.", show_alert=True)
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
    waiting_for_datetime = State()
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


# Scheduler helper function
async def schedule_post_job(post_id: int):
    """Fetches post from DB, publishes it, updates status."""
    try:
        res = supabase.table("posts").select("*").eq("id", post_id).execute()
        if not res.data:
            logger.warning(f"Scheduler job failed: Post {post_id} not found.")
            # Attempt to remove job if it exists by job_id
            jobs = scheduler.get_jobs()
            for job in jobs:
                 if job.args and len(job.args) > 0 and job.args[0] == post_id:
                      try:
                          scheduler.remove_job(job.id)
                          supabase.table("posts").update({"job_id": None}).eq("id", post_id).execute()
                          logger.info(f"Removed orphaned scheduler job {job.id} for missing post {post_id}")
                      except Exception as e:
                           logger.error(f"Failed to remove orphaned scheduler job {job.id} for post {post_id}: {e}")
                      break
            return

        post = res.data[0]
        channel_db_id = post["channel_id"]
        content = post["content"] or ""
        media_type = post["media_type"]
        media_file_id = post["media_file_id"]
        buttons_json = post["buttons_json"]

        # Get channel Telegram ID
        channel_res = supabase.table("channels").select("channel_id").eq("id", channel_db_id).execute()
        if not channel_res.data:
             logger.error(f"Scheduler job failed: Channel DB ID {channel_db_id} not found for post {post_id}.")
             # Maybe mark post status as error? Or leave it scheduled/draft?
             return
        tg_channel_id = channel_res.data[0]["channel_id"]

        reply_markup = None
        if buttons_json:
            try:
                btn_list = json.loads(buttons_json)
                if btn_list:
                    reply_markup = InlineKeyboardMarkup()
                    for b in btn_list:
                        if "url" in b:
                            reply_markup.add(InlineKeyboardButton(b["text"], url=b["url"]))
            except json.JSONDecodeError:
                logger.error(f"Failed to decode buttons_json for post {post_id}")
            except Exception as e:
                logger.error(f"Error building keyboard for post {post_id}: {e}")


        try:
            logger.info(f"Attempting to send scheduled post {post_id} to channel {tg_channel_id}")
            if media_type and media_file_id:
                if media_type == "photo":
                    await bot.send_photo(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
                elif media_type == "video":
                    await bot.send_video(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
                elif media_type == "document":
                    await bot.send_document(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
                elif media_type == "audio":
                    await bot.send_audio(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
                elif media_type == "animation":
                    await bot.send_animation(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
                else:
                    logger.warning(f"Unknown media type '{media_type}' for post {post_id}. Sending as text.")
                    await bot.send_message(tg_channel_id, content if content else " ", reply_markup=reply_markup)
            else:
                await bot.send_message(tg_channel_id, content if content else " ", reply_markup=reply_markup)

            # Update post status and remove job_id
            supabase.table("posts").update({"status": "published", "job_id": None}).eq("id", post_id).execute()
            logger.info(f"Post {post_id} successfully published to {tg_channel_id}.")

        except TelegramAPIError as e:
            logger.error(f"Telegram API Error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
            # Consider marking post status as failed? Or notifying the owner user?
            # For now, just log and leave the status as 'scheduled' or maybe 'publishing_failed'
            # It will NOT be re-attempted by the scheduler unless explicitly added again
            pass # Or update status?

        except Exception as e:
            logger.error(f"Unexpected error publishing scheduled post {post_id} to {tg_channel_id}: {e}")
             # Similar to TelegramAPIError handling

    except Exception as e:
        logger.error(f"Error in schedule_post_job for post {post_id}: {e}")


async def load_scheduled_posts():
    """Loads scheduled posts from DB and adds them to the scheduler."""
    now_utc = datetime.now(pytz.utc)
    res = supabase.table("posts").select("*").eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).execute()
    scheduled_posts = res.data or []
    logger.info(f"Found {len(scheduled_posts)} scheduled posts to load.")

    for post in scheduled_posts:
        post_id = post["id"]
        scheduled_time_utc = datetime.fromisoformat(post["scheduled_at"])
        try:
            # Remove any existing job with the same job_id from a previous run
            if post["job_id"]:
                 try:
                      scheduler.remove_job(post["job_id"])
                      logger.info(f"Removed old scheduler job {post['job_id']} for post {post_id}")
                 except Exception: # Job might not exist if bot crashed before adding it
                      pass

            # Add new job
            job = scheduler.add_job(
                schedule_post_job,
                trigger=DateTrigger(run_date=scheduled_time_utc),
                args=[post_id],
                id=f"post_{post_id}_{scheduled_time_utc.timestamp()}", # Generate a unique ID
                replace_existing=True # Replace if ID is already there (shouldn't happen with unique ID)
            )
            # Update post with new job_id
            supabase.table("posts").update({"job_id": job.id}).eq("id", post_id).execute()
            logger.info(f"Loaded scheduled post {post_id} with job ID {job.id} for {scheduled_time_utc}.")

        except Exception as e:
            logger.error(f"Failed to load scheduled post {post_id} into scheduler: {e}")


async def on_startup(dp):
    await bot.delete_webhook(drop_pending_updates=True)
    scheduler.start()
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

    if msg_to_delete_id:
        try:
            await bot.delete_message(message.chat.id, msg_to_delete_id)
        except Exception:
            pass # Ignore errors if message already deleted or inaccessible

    if preview_msg_id:
        try:
             # Edit preview message to indicate cancellation instead of deleting it
             await bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=preview_msg_id, reply_markup=None)
             await bot.edit_message_caption(chat_id=message.chat.id, message_id=preview_msg_id, caption=(data.get("content") or "") + ("\n\n*Отменено*" if lang=="ru" else "\n\n*Cancelled*"))
        except Exception:
            pass # Ignore errors


    await state.finish()
    await message.reply("Действие отменено." if lang == "ru" else "Action cancelled.", reply_markup=main_menu_keyboard(lang))

@dp.callback_query_handler(lambda c: c.data == "cancel_post_creation", state=PostStates.waiting_for_preview_confirm)
async def cb_cancel_post_creation(call: types.CallbackQuery, state: FSMContext, lang: str):
     data = await state.get_data()
     preview_msg_id = data.get("preview_msg_id")

     if preview_msg_id:
        try:
             # Edit preview message to indicate cancellation instead of deleting it
             await bot.edit_message_reply_markup(chat_id=call.message.chat.id, message_id=preview_msg_id, reply_markup=None)
             await bot.edit_message_caption(chat_id=call.message.chat.id, message_id=preview_msg_id, caption=(data.get("content") or "") + ("\n\n*Отменено*" if lang=="ru" else "\n\n*Cancelled*"))
        except Exception:
            pass # Ignore errors

     await call.answer("Отменено." if lang == "ru" else "Cancelled.")
     await state.finish()
     await bot.send_message(call.from_user.id, "Действие отменено." if lang == "ru" else "Action cancelled.", reply_markup=main_menu_keyboard(lang))


# --- Create Post Flow ---
@dp.message_handler(commands=['newpost', 'createpost'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["create_post"]["ru"], MENU_BUTTONS["create_post"]["en"]], state='*')
async def start_create_post(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply(TEXTS["invalid_input"][lang])
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

@dp.callback_query_handler(lambda c: c.data.startswith("selch_post:"), state=PostStates.waiting_for_channel)
async def cb_select_channel_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    chan_db_id = int(call.data.split(":")[1])

    # Verify user has permission for this channel
    res = supabase.table("channel_editors").select("role").eq("channel_id", chan_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res.data:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        await state.finish()
        # Remove the selection message if possible
        data = await state.get_data()
        mid = data.get("select_msg_id")
        if mid:
            try: await bot.delete_message(call.message.chat.id, mid)
            except: pass
        return

    await state.update_data(channel_id=chan_db_id)
    await call.answer()
    try:
        await call.message.delete() # Delete the channel selection message
    except Exception:
         await call.message.edit_reply_markup(reply_markup=None) # Fallback to removing keyboard
         pass

    await PostStates.waiting_for_text.set()
    await bot.send_message(call.from_user.id, TEXTS["enter_post_text"][lang])


# --- Input Handlers for Post Content ---
# Text input (can be actual text or /skip)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_text)
async def post_text_received(message: types.Message, state: FSMContext, lang: str):
    text = message.text
    # Commands are handled by the command handler with state='*'
    # if text.lower().strip() in ["/skip", "скип", "пропустить"]:
    #     await state.update_data(content="")
    # else:
    await state.update_data(content=text) # Save text, even if it's /skip to be consistent

    await PostStates.waiting_for_media.set()
    await message.reply(TEXTS["enter_post_media"][lang])

# Media input (can be media or /skip text)
@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT, ContentType.AUDIO, ContentType.ANIMATION, ContentType.TEXT], state=PostStates.waiting_for_media)
async def post_media_received(message: types.Message, state: FSMContext, lang: str):
    if message.content_type == ContentType.TEXT:
        if message.text.lower().strip() in ["/skip", "скип", "пропустить"]:
            await state.update_data(media_type=None, media_file_id=None)
            await PostStates.waiting_for_button_text.set()
            await message.reply(TEXTS["enter_button_text"][lang])
        # else: ignore other text input in media state
        return

    # Handle media
    caption = message.caption or ""
    data = await state.get_data()
    # If user sent text first, append caption. If not, caption is the content.
    # Decide how to handle this - let's overwrite content if media caption is provided
    # or keep old text if no caption? The requirement seems sequential: text, then media.
    # Let's update the content only IF the user sent text-with-media in the *first* step.
    # However, the current state machine takes text *then* media. So caption on media is *additional* text.
    # Let's append the caption to the existing content if it exists.
    current_content = data.get("content", "")
    if caption:
         if current_content:
              current_content += "\n" + caption
         else:
              current_content = caption
    await state.update_data(content=current_content)


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
         # This case should not happen if content_types are limited
         await state.update_data(media_type=None, media_file_id=None)

    await PostStates.waiting_for_button_text.set()
    await message.reply(TEXTS["enter_button_text"][lang])

# Button text input (can be text or /skip)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_text)
async def button_text_received(message: types.Message, state: FSMContext, lang: str):
    text = message.text
    if text.lower().strip() in ["/skip", "скип", "пропустить"]:
        await state.update_data(buttons=[]) # Ensure buttons list is empty if skipped
        # Move directly to schedule options
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
    await PostStates.waiting_for_button_text.set()
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
    # Move to schedule options
    await PostStates.waiting_for_schedule_options.set()
    kb = schedule_options_keyboard(lang)
    await bot.send_message(call.from_user.id, TEXTS["ask_schedule_options"][lang], reply_markup=kb)

# --- Scheduling and Preview ---
@dp.callback_query_handler(lambda c: c.data in ["schedule_now", "schedule_later", "edit_back_to_content"], state=PostStates.waiting_for_schedule_options)
async def cb_schedule_options(call: types.CallbackQuery, state: FSMContext, lang: str, timezone: str):
    action = call.data
    await call.answer()

    if action == "edit_back_to_content": # Go back from schedule options to content editing
         # This callback is actually triggered from the preview screen's edit buttons now
         # Need to adjust logic - this state is only entered *before* preview
         pass # This specific handler is not needed anymore based on the new keyboard flow

    data = await state.get_data()
    content = data.get("content")
    media_file_id = data.get("media_file_id")

    if not content and not media_file_id:
        # Post is empty, cannot proceed to scheduling/publishing
        try:
            await call.message.delete()
        except Exception:
             await call.message.edit_reply_markup(reply_markup=None)
             pass
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["post_content_empty"][lang], reply_markup=main_menu_keyboard(lang))
        return

    if action == "schedule_now":
        # Build preview for immediate publishing
        await state.update_data(is_scheduled=False) # Flag for preview keyboard
        await PostStates.waiting_for_preview_confirm.set()
        await send_post_preview(call.from_user.id, state, lang)

    elif action == "schedule_later":
        # Move to waiting for datetime
        await state.update_data(is_scheduled=True) # Flag for preview keyboard
        await PostStates.waiting_for_datetime.set()
        prompt = TEXTS["prompt_schedule_datetime"][lang].format(timezone=timezone)
        try:
            await call.message.delete() # Delete schedule options message
        except Exception:
            await call.message.edit_reply_markup(reply_markup=None)
            pass
        await bot.send_message(call.from_user.id, prompt)

@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_datetime)
async def post_datetime_received(message: types.Message, state: FSMContext, lang: str, timezone: str):
    datetime_str = message.text.strip()
    user_tz = pytz.timezone(timezone) if timezone in pytz.all_timezones_set else pytz.utc # Use user's timezone or UTC

    # Use dateparser for flexible parsing, then make it timezone-aware
    # Specify date_formats for better control over DD.MM.YYYY HH:MM
    parsed_datetime = dateparser.parse(datetime_str, languages=[lang], settings={'DATE_ORDER': 'DMY', 'RETURN_AS_TIMEZONE_AWARE': True, 'TIMEZONES': [timezone]})

    if parsed_datetime is None:
        # Fallback to strict DD.MM.YYYY HH:MM parsing if dateparser fails
        try:
             local_dt = datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
             # Assume input is in user's local time, convert to timezone-aware
             parsed_datetime = user_tz.localize(local_dt)
        except (ValueError, pytz.UnknownTimeZoneError):
             parsed_datetime = None

    if parsed_datetime is None or parsed_datetime < datetime.now(user_tz):
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

    # Get channel title for context in preview
    channel_title = "..."
    if channel_db_id:
         channel_res = supabase.table("channels").select("title").eq("id", channel_db_id).execute()
         if channel_res.data:
              channel_title = channel_res.data[0]["title"]
         else:
              logger.warning(f"Channel DB ID {channel_db_id} not found for preview.")

    preview_text = f"_{TEXTS['confirm_post_preview_text'][lang]}_\n"
    preview_text += f"Канал: *{channel_title}*\n" if lang == "ru" else f"Channel: *{channel_title}*\n"
    if is_scheduled and scheduled_at_utc_str:
        try:
             scheduled_dt_utc = datetime.fromisoformat(scheduled_at_utc_str)
             user_tz_str = user_cache.get(chat_id, {}).get("timezone", "UTC")
             user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
             scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
             preview_text += f"Запланировано на: *{scheduled_dt_local.strftime('%d.%m.%Y %H:%M')} ({user_tz_str})*\n" if lang == "ru" else f"Scheduled for: *{scheduled_dt_local.strftime('%d.%m.%Y %H:%M')} ({user_tz_str})*\n"
        except Exception as e:
             logger.error(f"Error formatting scheduled time for preview: {e}")
             preview_text += f"Запланировано на: *{scheduled_at_utc_str}*\n" if lang == "ru" else f"Scheduled for: *{scheduled_at_utc_str}*\n"


    preview_text += "\n" + (content if content else ("_(без текста)_" if lang == "ru" else "_(no text)_"))

    reply_markup = None
    if buttons:
        try:
            btn_list = buttons # Buttons are already a list of dicts from state
            if btn_list:
                reply_markup = InlineKeyboardMarkup()
                for b in btn_list:
                    if "url" in b:
                        reply_markup.add(InlineKeyboardButton(b["text"], url=b["url"]))
        except Exception as e:
            logger.error(f"Error building keyboard for preview: {e}")

    # Create preview keyboard with action and edit options
    preview_kb = post_preview_keyboard(lang, is_scheduled=is_scheduled) # No post_db_id yet for new post

    try:
        # Attempt to delete previous message if exists (e.g., timezone prompt, schedule options)
        prev_msg_id = data.get("select_msg_id") # Re-using this state key for the last message before preview
        if prev_msg_id:
             try:
                 await bot.delete_message(chat_id, prev_msg_id)
             except Exception:
                  pass # Ignore errors

        if media_type and media_file_id:
            # Media posts must have caption for text and buttons
            sent_msg = None
            try:
                if media_type == "photo":
                    sent_msg = await bot.send_photo(chat_id, media_file_id, caption=preview_text, reply_markup=reply_markup)
                elif media_type == "video":
                    sent_msg = await bot.send_video(chat_id, media_file_id, caption=preview_text, reply_markup=reply_markup)
                elif media_type == "document":
                     sent_msg = await bot.send_document(chat_id, media_file_id, caption=preview_text, reply_markup=reply_markup)
                elif media_type == "audio":
                     sent_msg = await bot.send_audio(chat_id, media_file_id, caption=preview_text, reply_markup=reply_markup)
                elif media_type == "animation":
                     sent_msg = await bot.send_animation(chat_id, media_file_id, caption=preview_text, reply_markup=reply_markup)
                else:
                     logger.warning(f"Unknown media type '{media_type}' for preview. Sending as text.")
                     sent_msg = await bot.send_message(chat_id, preview_text, reply_markup=reply_markup)

                # Edit the sent message with the complex keyboard afterwards
                if sent_msg:
                    await bot.edit_message_reply_markup(chat_id, sent_msg.message_id, reply_markup=preview_kb)
                    await state.update_data(preview_msg_id=sent_msg.message_id)

            except TelegramAPIError as e:
                 logger.error(f"Error sending media preview: {e}")
                 # Fallback to sending text only or show error
                 await bot.send_message(chat_id, f"{preview_text}\n\n*Ошибка при отправке медиа.*" if lang == "ru" else f"{preview_text}\n\n*Error sending media.*", reply_markup=preview_kb)
                 # Store a placeholder message ID if media failed, so we can still edit/delete the text part
                 sent_msg = await bot.send_message(chat_id, preview_text, reply_markup=preview_kb)
                 if sent_msg: await state.update_data(preview_msg_id=sent_msg.message_id)


        else:
            # Text-only post
            sent_msg = await bot.send_message(chat_id, preview_text, reply_markup=reply_markup)
            # Edit the sent message with the complex keyboard afterwards
            await bot.edit_message_reply_markup(chat_id, sent_msg.message_id, reply_markup=preview_kb)
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

    # Get channel Telegram ID
    channel_res = supabase.table("channels").select("channel_id").eq("id", channel_db_id).execute()
    if not channel_res.data:
         await call.answer("Ошибка: Канал не найден в базе.", show_alert=True)
         logger.error(f"Channel DB ID {channel_db_id} not found during publish confirmation.")
         await state.finish()
         return
    tg_channel_id = channel_res.data[0]["channel_id"]


    reply_markup = None
    if buttons:
        try:
            btn_list = buttons
            if btn_list:
                reply_markup = InlineKeyboardMarkup()
                for b in btn_list:
                    if "url" in b:
                        reply_markup.add(InlineKeyboardButton(b["text"], url=b["url"]))
        except Exception as e:
            logger.error(f"Error building keyboard for publish: {e}")


    try:
        logger.info(f"Attempting to publish post to channel {tg_channel_id}")
        if media_type and media_file_id:
            if media_type == "photo":
                await bot.send_photo(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
            elif media_type == "video":
                await bot.send_video(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
            elif media_type == "document":
                await bot.send_document(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
            elif media_type == "audio":
                await bot.send_audio(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
            elif media_type == "animation":
                await bot.send_animation(tg_channel_id, media_file_id, caption=content if content else None, reply_markup=reply_markup)
            else:
                 logger.warning(f"Unknown media type '{media_type}' during publish. Sending as text.")
                 await bot.send_message(tg_channel_id, content if content else " ", reply_markup=reply_markup)

        else:
            await bot.send_message(tg_channel_id, content if content else " ", reply_markup=reply_markup)

        # Save post to DB with status 'published' (optional, but good practice to record)
        try:
            supabase.table("posts").insert({
                "channel_id": channel_db_id,
                "user_id": user_id,
                "content": content,
                "media_type": media_type,
                "media_file_id": media_file_id,
                "buttons_json": json.dumps(buttons) if buttons else None,
                "status": "published",
                "scheduled_at": datetime.now(pytz.utc).isoformat() # Record publication time
            }).execute()
        except Exception as db_e:
            logger.error(f"Failed to record published post in DB: {db_e}")
            # Don't fail the user interaction just because DB record failed

        await call.answer()
        try: # Edit preview message to remove keyboard
             await call.message.edit_reply_markup(reply_markup=None)
             await call.message.edit_caption(caption=(call.message.caption or "") + ("\n\n*Опубликовано*" if lang=="ru" else "\n\n*Published*"))
        except Exception: pass

        await bot.send_message(call.from_user.id, TEXTS["post_published_confirmation"][lang], reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except TelegramAPIError as e:
        logger.error(f"Telegram API Error publishing post to {tg_channel_id}: {e}")
        await call.answer(TEXTS["not_admin"][lang], show_alert=True) # Reuse not_admin text for general sending failure
        try: # Edit preview message to remove keyboard
             await call.message.edit_reply_markup(reply_markup=None)
        except Exception: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Ошибка при публикации поста." if lang == "ru" else "Error publishing post.", reply_markup=main_menu_keyboard(lang))

    except Exception as e:
        logger.error(f"Unexpected error during publish: {e}")
        await call.answer("Произошла внутренняя ошибка.", show_alert=True)
        try: # Edit preview message to remove keyboard
             await call.message.edit_reply_markup(reply_markup=None)
        except Exception: pass
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

    if not scheduled_at_utc_str:
        await call.answer("Ошибка: Время не указано.", show_alert=True)
        logger.error("Scheduled time missing during schedule confirmation.")
        await state.finish()
        return

    try:
        # Save post to DB with status 'scheduled'
        res_insert = supabase.table("posts").insert({
            "channel_id": channel_db_id,
            "user_id": user_id,
            "content": content,
            "media_type": media_type,
            "media_file_id": media_file_id,
            "buttons_json": json.dumps(buttons) if buttons else None,
            "status": "scheduled",
            "scheduled_at": scheduled_at_utc_str
        }).execute()

        if not res_insert.data:
             await call.answer("Ошибка при сохранении поста.", show_alert=True)
             logger.error(f"Failed to insert scheduled post for user {user_id}: {res_insert.error}")
             try: # Edit preview message to remove keyboard
                 await call.message.edit_reply_markup(reply_markup=None)
             except Exception: pass
             await state.finish()
             await bot.send_message(call.from_user.id, "Ошибка при планировании поста." if lang == "ru" else "Error scheduling post.", reply_markup=main_menu_keyboard(lang))
             return

        post_rec = res_insert.data[0]
        post_db_id = post_rec["id"]
        scheduled_dt_utc = datetime.fromisoformat(scheduled_at_utc_str)

        # Add job to scheduler
        job = scheduler.add_job(
            schedule_post_job,
            trigger=DateTrigger(run_date=scheduled_dt_utc),
            args=[post_db_id],
            id=f"post_{post_db_id}_{scheduled_dt_utc.timestamp()}", # Generate a unique ID
            replace_existing=True
        )
        # Update post with job_id
        supabase.table("posts").update({"job_id": job.id}).eq("id", post_db_id).execute()

        # Format local scheduled time for confirmation message
        user_tz_str = user_cache.get(call.from_user.id, {}).get("timezone", "UTC")
        user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
        scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
        scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')

        await call.answer()
        try: # Edit preview message to remove keyboard
             await call.message.edit_reply_markup(reply_markup=None)
             await call.message.edit_caption(caption=(call.message.caption or "") + ("\n\n*Запланировано*" if lang=="ru" else "\n\n*Scheduled*"))

        except Exception: pass

        await bot.send_message(call.from_user.id, TEXTS["post_scheduled_confirmation"][lang].format(scheduled_at=scheduled_time_display), reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except Exception as e:
        logger.error(f"Unexpected error during scheduling: {e}")
        await call.answer("Произошла внутренняя ошибка.", show_alert=True)
        try: # Edit preview message to remove keyboard
             await call.message.edit_reply_markup(reply_markup=None)
        except Exception: pass
        await state.finish()
        await bot.send_message(call.from_user.id, "Произошла ошибка при планировании поста." if lang == "ru" else "Error scheduling post.", reply_markup=main_menu_keyboard(lang))


# --- Edit Handlers from Preview/Scheduled Post View ---
# These handlers transition back to specific input states
@dp.callback_query_handler(lambda c: c.data.startswith("edit_post:") or c.data.startswith("edit_draft:"), state=[PostStates.waiting_for_preview_confirm, ScheduledPostsState.viewing_scheduled_post])
async def cb_edit_post_content(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Режим редактирования..." if lang == "ru" else "Editing mode...")
    parts = call.data.split(":")
    edit_type = parts[1] # text, media, buttons, time
    # post_db_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None # Only for scheduled posts

    # Fetch current post data (either from state for new post, or from DB for scheduled post)
    current_state_data = await state.get_data()
    post_db_id = current_state_data.get("post_db_id") # This will be None for new drafts

    if post_db_id: # Editing an existing scheduled post
        res = supabase.table("posts").select("*").eq("id", post_db_id).execute()
        if not res.data:
            await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
            await state.finish()
            return
        post_data = res.data[0]
        # Ensure user has edit permission (owner/editor) for this channel
        res_role = supabase.table("channel_editors").select("role").eq("channel_id", post_data["channel_id"]).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
        if not res_role.data:
             await call.answer(TEXTS["no_permission"][lang], show_alert=True)
             return
        # Load existing post data into state
        await state.update_data(
             channel_id=post_data["channel_id"],
             content=post_data["content"],
             media_type=post_data["media_type"],
             media_file_id=post_data["media_file_id"],
             buttons=json.loads(post_data["buttons_json"]) if post_data["buttons_json"] else [],
             scheduled_at=post_data["scheduled_at"], # Keep original schedule time
             post_db_id=post_db_id, # Keep post ID
             is_scheduled=True # Always True for editing scheduled post
        )
        current_state = ScheduledPostsState.viewing_scheduled_post
    else: # Editing a new post before saving/scheduling
        post_data = current_state_data
        current_state = PostStates.waiting_for_preview_confirm # Need to know the previous state

    # Delete the preview message or current message
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_reply_markup(reply_markup=None)
        pass

    # Transition to the correct state based on edit_type
    if edit_type == "text":
        await PostStates.waiting_for_text.set() # Re-use the same state
        await state.update_data(editing_post_id=post_db_id) # Store post ID if editing existing
        await bot.send_message(call.from_user.id, TEXTS["enter_post_text"][lang])
    elif edit_type == "media":
        await PostStates.waiting_for_media.set() # Re-use the same state
        await state.update_data(editing_post_id=post_db_id) # Store post ID if editing existing
        await bot.send_message(call.from_user.id, TEXTS["enter_post_media"][lang])
    elif edit_type == "buttons":
        # Clear current button data in state to start fresh
        await state.update_data(buttons=[], current_button_text=None, editing_post_id=post_db_id)
        await PostStates.waiting_for_button_text.set() # Re-use the same state
        await bot.send_message(call.from_user.id, TEXTS["enter_button_text"][lang])
    elif edit_type == "time" and post_db_id: # Only for existing scheduled posts
         await state.update_data(editing_post_id=post_db_id)
         await ScheduledPostsState.waiting_for_datetime.set() # Use a separate state for scheduled post editing time
         prompt = TEXTS["prompt_schedule_datetime"][lang].format(timezone=user_cache.get(user_id, {}).get("timezone", "UTC"))
         await bot.send_message(call.from_user.id, prompt)
    else:
        await call.answer("Неизвестный тип редактирования." if lang == "ru" else "Unknown edit type.", show_alert=True)
        # Should return to preview or main menu? Let's go to main menu for safety.
        await state.finish()
        await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))

# Need handlers for input after editing states
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_text) # This is also the handler after editing text
@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT, ContentType.AUDIO, ContentType.ANIMATION, ContentType.TEXT], state=PostStates.waiting_for_media) # Handler after editing media
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_text) # Handler after editing buttons (text)
@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_url) # Handler after editing buttons (url)
@dp.callback_query_handler(lambda c: c.data == "add_btn_yes", state=PostStates.waiting_for_add_more_buttons) # Handler after editing buttons (add more yes)
@dp.callback_query_handler(lambda c: c.data == "add_btn_no", state=PostStates.waiting_for_add_more_buttons) # Handler after editing buttons (add more no)
async def post_edit_input_received(message: types.Message, state: FSMContext, lang: str, user_id: int):
    # This is a simplified catch-all. In a real app, you'd duplicate/adapt the original input handlers
    # (post_text_received, post_media_received, button_text_received, button_url_received, cb_add_button_yes/no)
    # to check if `editing_post_id` is in state data. If yes, after receiving input,
    # update the state data and transition BACK to the preview state for the *scheduled post*,
    # rather than continuing the new post creation flow.

    # Simplified approach: After *any* input in an edit state, just go back to the appropriate preview
    current_state_name = await state.get_state()
    data = await state.get_data()
    post_db_id = data.get("editing_post_id") # Check if we were editing an existing post

    # Process the input using the original handlers for the current state
    if current_state_name == PostStates.waiting_for_text.state:
        await post_text_received(message, state, lang) # Call original handler
    elif current_state_name == PostStates.waiting_for_media.state:
         # Handle media input, requires the message object
         await post_media_received(message, state, lang) # Call original handler
    elif current_state_name == PostStates.waiting_for_button_text.state:
         await button_text_received(message, state, lang) # Call original handler
    elif current_state_name == PostStates.waiting_for_button_url.state:
         await button_url_received(message, state, lang) # Call original handler
    # Note: Callback handlers (yes/no for buttons) need their own separate handlers
    # after editing, which would also transition back to preview. This requires
    # duplicating or adapting cb_add_button_yes/no.

    # After processing input (and state transition might have already happened in the handler):
    data = await state.get_data() # Re-fetch state data as it might have changed
    post_db_id = data.get("editing_post_id") # Check again if it was an edit session

    if post_db_id is not None:
        # If we were editing an existing scheduled post, go back to scheduled post preview state
        # Need to *update* the post in the DB first with the new data from state
        try:
             update_data = {
                 "content": data.get("content"),
                 "media_type": data.get("media_type"),
                 "media_file_id": data.get("media_file_id"),
                 "buttons_json": json.dumps(data.get("buttons")) if data.get("buttons") else None,
             }
             supabase.table("posts").update(update_data).eq("id", post_db_id).execute()
             # If time was edited, need to cancel/add scheduler job
             if current_state_name == ScheduledPostsState.waiting_for_datetime.state and data.get("scheduled_at"):
                  await update_scheduled_post_job(post_db_id, data["scheduled_at"])

             await state.update_data(editing_post_id=None) # Clear editing flag
             await ScheduledPostsState.viewing_scheduled_post.set() # Return to viewing state
             await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, user_id) # Show updated preview
        except Exception as e:
             logger.error(f"Failed to update scheduled post {post_db_id} after edit input: {e}")
             await message.reply("Ошибка при сохранении изменений." if lang == "ru" else "Error saving changes.", reply_markup=main_menu_keyboard(lang))
             await state.finish()
    else:
        # If we were editing a new draft, stay in the create post flow and move to the *next* step (which is handled by the original handlers)
        # The original handlers already moved to the next state (media -> buttons, buttons -> schedule options)
        # We just need to ensure the next step is shown correctly.
        # The original handlers already send the next prompt. So no extra action needed here after the handler call.
        pass


@dp.message_handler(content_types=ContentType.TEXT, state=ScheduledPostsState.waiting_for_datetime)
async def scheduled_post_datetime_received(message: types.Message, state: FSMContext, lang: str, timezone: str):
    """Handle new datetime input when editing a scheduled post."""
    datetime_str = message.text.strip()
    user_tz = pytz.timezone(timezone) if timezone in pytz.all_timezones_set else pytz.utc

    parsed_datetime = dateparser.parse(datetime_str, languages=[lang], settings={'DATE_ORDER': 'DMY', 'RETURN_AS_TIMEZONE_AWARE': True, 'TIMEZONES': [timezone]})

    if parsed_datetime is None:
        try:
             local_dt = datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
             parsed_datetime = user_tz.localize(local_dt)
        except (ValueError, pytz.UnknownTimeZoneError):
             parsed_datetime = None

    if parsed_datetime is None or parsed_datetime < datetime.now(user_tz):
        await message.reply(TEXTS["invalid_datetime_format"][lang])
        return # Stay in the same state

    scheduled_at_utc = parsed_datetime.astimezone(pytz.utc)
    scheduled_at_utc_str = scheduled_at_utc.isoformat()

    data = await state.get_data()
    post_db_id = data.get("editing_post_id") # Must be present

    if not post_db_id:
         await message.reply("Ошибка: ID поста не найден." if lang == "ru" else "Error: Post ID not found.", reply_markup=main_menu_keyboard(lang))
         await state.finish()
         return

    try:
        # Update post in DB with new scheduled time
        supabase.table("posts").update({"scheduled_at": scheduled_at_utc_str}).eq("id", post_db_id).execute()

        # Update scheduler job
        await update_scheduled_post_job(post_db_id, scheduled_at_utc_str)

        await state.update_data(editing_post_id=None) # Clear editing flag
        await ScheduledPostsState.viewing_scheduled_post.set() # Return to viewing state
        await view_scheduled_post_by_id(message.chat.id, post_db_id, lang, user_id) # Show updated preview

    except Exception as e:
        logger.error(f"Failed to update scheduled time for post {post_db_id}: {e}")
        await message.reply("Ошибка при сохранении нового времени." if lang == "ru" else "Error saving new time.", reply_markup=main_menu_keyboard(lang))
        await state.finish()


async def update_scheduled_post_job(post_db_id: int, new_scheduled_at_utc_str: str):
    """Cancels old scheduler job for a post and creates a new one."""
    res = supabase.table("posts").select("job_id").eq("id", post_db_id).execute()
    old_job_id = res.data[0]["job_id"] if res.data and res.data[0]["job_id"] else None

    if old_job_id:
        try:
            scheduler.remove_job(old_job_id)
            logger.info(f"Cancelled old scheduler job {old_job_id} for post {post_db_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel old scheduler job {old_job_id} for post {post_db_id}: {e}")

    # Add new job
    new_scheduled_dt_utc = datetime.fromisoformat(new_scheduled_at_utc_str)
    if new_scheduled_dt_utc > datetime.now(pytz.utc):
        job = scheduler.add_job(
            schedule_post_job,
            trigger=DateTrigger(run_date=new_scheduled_dt_utc),
            args=[post_db_id],
            id=f"post_{post_db_id}_{new_scheduled_dt_utc.timestamp()}", # Generate a unique ID
            replace_existing=True
        )
        # Update post with new job_id
        supabase.table("posts").update({"job_id": job.id}).eq("id", post_db_id).execute()
        logger.info(f"Added new scheduler job {job.id} for post {post_db_id} at {new_scheduled_dt_utc}.")
    else:
         # New time is in the past (should be caught by validation, but double check)
         # Mark post as draft/failed and remove job_id
         supabase.table("posts").update({"status": "draft", "job_id": None}).eq("id", post_db_id).execute()
         logger.warning(f"Scheduled time for post {post_db_id} is in the past. Marked as draft.")


# --- Scheduled Posts Flow ---
@dp.message_handler(commands=['scheduled'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["scheduled_posts"]["ru"], MENU_BUTTONS["scheduled_posts"]["en"]], state='*')
async def view_scheduled_posts_menu(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply(TEXTS["invalid_input"][lang])
        return

    # Get channels where user is owner/editor/viewer
    res = supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).execute()
    channels_access = res.data or []

    if not channels_access:
        await message.reply(TEXTS["no_scheduled_posts"][lang])
        return

    channel_db_ids = [entry["channel_id"] for entry in channels_access]
    res2 = supabase.table("channels").select("id, title").in_("id", channel_db_ids).execute()
    channels_list = res2.data or []

    if not channels_list: # Safety check
         await message.reply(TEXTS["no_scheduled_posts"][lang])
         return

    if len(channels_list) > 1:
        kb = InlineKeyboardMarkup()
        for ch in channels_list:
             # Cache channel info
            channel_cache[ch["id"]] = channel_cache.get(ch["id"], {})
            channel_cache[ch["id"]]["title"] = ch["title"]
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"viewsched_ch:{ch['id']}"))
        await message.reply(TEXTS["choose_channel_scheduled"][lang], reply_markup=kb)
        await ScheduledPostsState.waiting_for_channel_selection.set() # Add state for channel selection
    else:
        # Only one channel, show drafts directly
        chan_db_id = channels_list[0]["id"]
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
        except: pass
        return

    await call.answer()
    try:
        await call.message.delete() # Delete channel selection message
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
    res_posts = supabase.table("posts").select("id, content, scheduled_at").eq("channel_id", channel_db_id).eq("status", "scheduled").gt("scheduled_at", now_utc.isoformat()).order("scheduled_at").execute()
    scheduled_posts = res_posts.data or []

    if not scheduled_posts:
        await bot.send_message(chat_id, TEXTS["no_scheduled_posts"][lang])
        return

    header_text = TEXTS["scheduled_posts_header"][lang].format(channel=title)
    await bot.send_message(chat_id, header_text)

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

        post_summary = f"ID: `{post_id}` | {scheduled_time_display} ({user_tz_str})\n{content_snippet}"

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("👁️ " + ("Просмотр" if lang == "ru" else "View"), callback_data=f"view_scheduled:{post_id}"))
        if user_role in ["owner", "editor"]:
             kb.add(InlineKeyboardButton("✏️ " + ("Редактировать" if lang == "ru" else "Edit"), callback_data=f"edit_scheduled:{post_id}"))
             kb.add(InlineKeyboardButton("🗑️ " + ("Удалить" if lang == "ru" else "Delete"), callback_data=f"delete_scheduled:{post_id}"))

        await bot.send_message(chat_id, post_summary, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query_handler(lambda c: c.data.startswith("view_scheduled:"))
async def cb_view_scheduled_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer("Загрузка поста..." if lang == "ru" else "Loading post...")
    post_id = int(call.data.split(":")[1])

    await view_scheduled_post_by_id(call.from_user.id, post_id, lang, user_id, from_callback=call)
    # Set state after successful fetch and send
    await ScheduledPostsState.viewing_scheduled_post.set()
    await state.update_data(post_db_id=post_id) # Store post ID in state


async def view_scheduled_post_by_id(chat_id: int, post_id: int, lang: str, user_id: int, from_callback: types.CallbackQuery = None):
    """Helper to fetch and send a single scheduled post preview."""
    res = supabase.table("posts").select("*").eq("id", post_id).execute()
    if not res.data:
        if from_callback: await from_callback.answer("Пост не найден.", show_alert=True)
        else: await bot.send_message(chat_id, "Пост не найден." if lang == "ru" else "Post not found.", reply_markup=main_menu_keyboard(lang))
        return

    post = res.data[0]
    channel_db_id = post["channel_id"]

    # Verify user has access
    res_access = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).execute()
    if not res_access.data:
        if from_callback: await from_callback.answer(TEXTS["no_permission"][lang], show_alert=True)
        else: await bot.send_message(chat_id, TEXTS["no_permission"][lang], reply_markup=main_menu_keyboard(lang))
        return
    user_role = res_access.data[0]["role"]

    # Get channel title and format scheduled time
    channel_res = supabase.table("channels").select("title").eq("id", channel_db_id).execute()
    channel_title = channel_res.data[0]["title"] if channel_res.data else "Channel"
    scheduled_dt_utc = datetime.fromisoformat(post["scheduled_at"])
    user_tz_str = user_cache.get(chat_id, {}).get("timezone", "UTC")
    user_tz = pytz.timezone(user_tz_str) if user_tz_str in pytz.all_timezones_set else pytz.utc
    scheduled_dt_local = scheduled_dt_utc.astimezone(user_tz)
    scheduled_time_display = scheduled_dt_local.strftime('%d.%m.%Y %H:%M')

    preview_text = TEXTS["view_scheduled_post_prompt_text"][lang].format(post_id=post_id, scheduled_at_local=scheduled_time_display)
    preview_text += f"\nКанал: *{channel_title}*\n\n" if lang == "ru" else f"\nChannel: *{channel_title}*\n\n"
    preview_text += post["content"] or ("_(без текста)_" if lang == "ru" else "_(no text)_")

    reply_markup = None
    if post["buttons_json"]:
        try:
            btn_list = json.loads(post["buttons_json"])
            if btn_list:
                reply_markup = InlineKeyboardMarkup()
                for b in btn_list:
                    if "url" in b:
                        reply_markup.add(InlineKeyboardButton(b["text"], url=b["url"]))
        except Exception as e:
            logger.error(f"Error building keyboard for scheduled post {post_id} preview: {e}")

    # Actions keyboard
    actions_kb = scheduled_post_actions_keyboard(lang, post_id) if user_role in ["owner", "editor"] else None
    # Note: Need to combine the post buttons with the action buttons. Telegram API allows only one InlineKeyboardMarkup per message.
    # Option 1: Show post buttons in preview, send separate message with actions.
    # Option 2: Combine all buttons into one keyboard (can get long). Let's go with option 2 for simplicity here.

    combined_kb = InlineKeyboardMarkup()
    if reply_markup:
        for row in reply_markup.inline_keyboard:
             for btn in row:
                  combined_kb.add(btn) # Add buttons one by one

    if actions_kb:
         for row in actions_kb.inline_keyboard:
             for btn in row:
                 combined_kb.add(btn) # Add action buttons

    try:
        # Delete the previous message (e.g., list item) if coming from a callback
        if from_callback:
             try:
                 await from_callback.message.delete()
             except Exception:
                 await from_callback.message.edit_reply_markup(reply_markup=None) # Fallback
                 pass

        if post["media_type"] and post["media_file_id"]:
            sent_msg = None
            try:
                if post["media_type"] == "photo":
                    sent_msg = await bot.send_photo(chat_id, post["media_file_id"], caption=preview_text, reply_markup=combined_kb)
                elif post["media_type"] == "video":
                    sent_msg = await bot.send_video(chat_id, post["media_file_id"], caption=preview_text, reply_markup=combined_kb)
                elif post["media_type"] == "document":
                    sent_msg = await bot.send_document(chat_id, post["media_file_id"], caption=preview_text, reply_markup=combined_kb)
                elif post["media_type"] == "audio":
                    sent_msg = await bot.send_audio(chat_id, post["media_file_id"], caption=preview_text, reply_markup=combined_kb)
                elif post["media_type"] == "animation":
                    sent_msg = await bot.send_animation(chat_id, post["media_file_id"], caption=preview_text, reply_markup=combined_kb)
                else:
                    logger.warning(f"Unknown media type '{post['media_type']}' for scheduled post preview {post_id}. Sending as text.")
                    sent_msg = await bot.send_message(chat_id, preview_text, reply_markup=combined_kb)

            except TelegramAPIError as e:
                logger.error(f"Error sending scheduled post {post_id} media preview: {e}")
                # Fallback to sending text only or show error
                sent_msg = await bot.send_message(chat_id, f"{preview_text}\n\n*Ошибка при отправке медиа.*" if lang == "ru" else f"{preview_text}\n\n*Error sending media.*", reply_markup=combined_kb)

            if sent_msg:
                 # Store message ID for editing/deleting later if needed in this state
                 if from_callback: await from_callback.message.delete() # Delete the *old* preview message if coming from editing state
                 # Store the *new* preview message ID
                 state = dp.current_state(chat=chat_id, user=user_id) # Get state for the correct user/chat
                 await state.update_data(preview_msg_id=sent_msg.message_id)


        else:
            sent_msg = await bot.send_message(chat_id, preview_text, reply_markup=combined_kb)
            if sent_msg:
                 # Store message ID
                 if from_callback: await from_callback.message.delete() # Delete the *old* preview message if coming from editing state
                 # Store the *new* preview message ID
                 state = dp.current_state(chat=chat_id, user=user_id)
                 await state.update_data(preview_msg_id=sent_msg.message_id)


    except Exception as e:
        logger.error(f"Failed to send scheduled post {post_id} preview: {e}")
        await bot.send_message(chat_id, "Произошла ошибка при подготовке предпросмотра запланированного поста." if lang == "ru" else "An error occurred while preparing the scheduled post preview.")


@dp.callback_query_handler(lambda c: c.data == "back_to_scheduled_list", state=ScheduledPostsState.viewing_scheduled_post)
async def cb_back_to_scheduled_list(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
    await call.answer()
    data = await state.get_data()
    post_db_id = data.get("post_db_id")
    if post_db_id:
        res = supabase.table("posts").select("channel_id").eq("id", post_db_id).execute()
        if res.data:
            channel_db_id = res.data[0]["channel_id"]
            try: await call.message.delete()
            except: await call.message.edit_reply_markup(reply_markup=None) # Clean up preview message
            await state.finish() # Exit viewing state
            await send_scheduled_posts_list(call.from_user.id, channel_db_id, lang, user_id)
            return

    # If post_id or channel_id missing, just go to main menu
    await state.finish()
    try: await call.message.delete()
    except: await call.message.edit_reply_markup(reply_markup=None)
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
        return
    channel_db_id = res.data[0]["channel_id"]
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         return

    # Keep state as viewing_scheduled_post, but show edit options
    await state.update_data(post_db_id=post_id) # Ensure post_id is in state
    kb = edit_scheduled_post_keyboard(lang, post_id)
    try:
        # Edit the current preview message
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
         await bot.send_message(call.from_user.id, TEXTS["edit_scheduled_post_options"][lang], reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("delete_scheduled:"), state=[ScheduledPostsState.viewing_scheduled_post, None]) # Allow deleting from list or view
async def cb_delete_scheduled_post(call: types.CallbackQuery, state: FSMContext, lang: str, user_id: int):
     post_id = int(call.data.split(":")[1])

     # Verify user has edit permission (owner/editor) for this channel
     res = supabase.table("posts").select("channel_id, job_id").eq("id", post_id).execute()
     if not res.data:
        await call.answer("Пост не найден." if lang == "ru" else "Post not found.", show_alert=True)
        # If state was viewing_scheduled_post, finish it
        current_state = await state.get_state()
        if current_state == ScheduledPostsState.viewing_scheduled_post.state:
             await state.finish()
             try: await call.message.delete() # Clean up old preview message
             except: pass
        return

     post_info = res.data[0]
     channel_db_id = post_info["channel_id"]
     job_id = post_info["job_id"]

     res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
     if not res_role.data:
         await call.answer(TEXTS["no_permission"][lang], show_alert=True)
         return

     # Cancel scheduler job
     if job_id:
         try:
             scheduler.remove_job(job_id)
             logger.info(f"Cancelled scheduler job {job_id} for post {post_id}")
         except Exception as e:
             logger.warning(f"Failed to cancel scheduler job {job_id} for post {post_id}: {e}")

     # Delete from DB
     supabase.table("posts").delete().eq("id", post_id).execute()

     await call.answer("Пост удалён." if lang == "ru" else "Post deleted.")

     current_state = await state.get_state()
     if current_state == ScheduledPostsState.viewing_scheduled_post.state:
         # If deleting from the preview/viewing state
         try: await call.message.delete() # Delete the preview message
         except: await call.message.edit_reply_markup(reply_markup=None)
         await state.finish() # Exit the viewing state
         await bot.send_message(call.from_user.id, TEXTS["scheduled_post_deleted"][lang], reply_markup=main_menu_keyboard(lang))
     else:
         # If deleting from the list view
         try: await call.message.delete() # Delete the post item from the list
         except: pass # Ignore if message is gone
         # No state change needed if not in viewing state


# --- Manage Channels Flow ---
@dp.message_handler(commands=['channels', 'manage'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["manage_channels"]["ru"], MENU_BUTTONS["manage_channels"]["en"]], state='*')
async def manage_channels_menu(message: types.Message, state: FSMContext, lang: str, user_id: int):
    if await state.get_state() is not None:
        await message.reply(TEXTS["invalid_input"][lang])
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
async def cb_add_channel(call: types.CallbackQuery, lang: str):
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
        await message.reply(TEXTS["invalid_input"][lang])
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

async def process_add_channel_input(message: types.Message, state: FSMContext, lang: str, user_id: int, identifier: str):
    """Helper to process channel identifier input for adding a channel."""
    if identifier.startswith("@"):
        identifier = identifier
    else:
         # Try converting to int if it looks like an ID, otherwise keep as is
         try:
             int(identifier)
         except ValueError:
              pass # Keep it as is if not purely numeric

    chat_id = None
    title = None
    try:
        chat = await bot.get_chat(identifier)
        chat_id = chat.id
        title = chat.title or identifier # Use title if available, else identifier
        # Check bot's admin status and permissions
        bot_member = await bot.get_chat_member(chat_id, bot.id)
        if not bot_member.is_chat_admin:
             await message.reply(TEXTS["not_admin"][lang])
             await state.finish()
             return
        # Check if bot can post messages
        if not bot_member.can_post_messages:
             await message.reply(TEXTS["not_admin"][lang])
             await state.finish()
             return
        # Check user's admin status
        member = await bot.get_chat_member(chat_id, message.from_user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply(TEXTS["not_admin"][lang])
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
         await message.reply(TEXTS["channel_not_found"][lang])
         await state.finish()
         return
    except Exception as e:
        logger.error(f"Unexpected error checking channel {identifier}: {e}")
        await message.reply("Произошла ошибка при проверке канала." if lang == "ru" else "An error occurred while checking the channel.")
        await state.finish()
        return


    res = supabase.table("channels").select("id").eq("channel_id", chat_id).execute()
    if res.data:
        await message.reply(TEXTS["channel_exists"][lang])
        await state.finish()
        return

    new_channel = {"channel_id": chat_id, "title": title, "owner_id": user_id}
    res_insert = supabase.table("channels").insert(new_channel).execute()
    if not res_insert.data:
        logger.error(f"Failed to insert new channel {chat_id}: {res_insert.error}")
        await message.reply("Ошибка при добавлении канала." if lang == "ru" else "Error adding channel.")
        await state.finish()
        return

    channel_rec = res_insert.data[0]
    # Add owner as the first editor
    supabase.table("channel_editors").insert({
        "channel_id": channel_rec["id"],
        "user_id": user_id,
        "role": "owner"
    }).execute()

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
    target_tg_id = None

    if identifier.isdigit():
        target_tg_id = int(identifier)
        res = supabase.table("users").select("*").eq("tg_id", target_tg_id).execute()
        if res.data:
            target_user = res.data[0]
    else:
        if identifier.startswith("@"):
            identifier = identifier[1:]
        # Search by username part, case-insensitive
        res = supabase.table("users").select("*").ilike("name", "%@" + identifier).execute()
        if res.data:
             # If multiple matches, ideally show a list. For simplicity, take the first one.
             target_user = res.data[0]
             target_tg_id = target_user["tg_id"]


    if not target_user:
        await message.reply(TEXTS["user_not_found"][lang])
        # Stay in the same state to allow retrying
        return

    target_user_id = target_user["id"]
    target_user_name = target_user["name"]

    data = await state.get_data()
    channel_db_id = data.get("channel_id")

    # Check if user is already an editor/viewer/owner
    res_check = supabase.table("channel_editors").select("role").eq("channel_id", channel_db_id).eq("user_id", target_user_id).execute()
    if res_check.data:
        role_text_key = f"role_{res_check.data[0]['role']}"
        role_text = TEXTS.get(role_text_key, {}).get(lang, res_check.data[0]['role']) # Fallback to role name
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

    res_editors = supabase.table("channel_editors").select("user_id, role").eq("channel_id", chan_db_id).neq("role", "owner").execute()
    editors = res_editors.data or []

    if not editors:
        await call.answer("Редакторы не найдены." if lang == "ru" else "No editors found.", show_alert=True)
        # Stay on the manage channel menu
        return

    user_ids = [e["user_id"] for e in editors]
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
        await call.message.edit_text(TEXTS["remove_editor_prompt"][lang], reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, TEXTS["remove_editor_prompt"][lang], reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("confirmremedit:"))
async def cb_confirm_remove_user(call: types.CallbackQuery, lang: str):
    parts = call.data.split(":")
    chan_db_id = int(parts[1])
    user_to_remove_id = int(parts[2])

    # Fetch user name for confirmation message
    user_res = supabase.table("users").select("name").eq("id", user_to_remove_id).execute()
    user_name = user_res.data[0]["name"] if user_res.data else f"ID: {user_to_remove_id}"

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ " + ("Подтвердить" if lang == "ru" else "Confirm"), callback_data=f"removeuser:{chan_db_id}:{user_to_remove_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data=f"remedit:{chan_db_id}")) # Go back to the remove editor list

    await call.answer()
    try:
        await call.message.edit_text(f"Вы уверены, что хотите удалить пользователя {user_name}?" if lang == "ru" else f"Are you sure you want to remove user {user_name}?", reply_markup=kb)
    except Exception:
        await bot.send_message(call.from_user.id, f"Вы уверены, что хотите удалить пользователя {user_name}?" if lang == "ru" else f"Are you sure you want to remove user {user_name}?", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("removeuser:"))
async def cb_remove_user(call: types.CallbackQuery, lang: str, user_id: int):
    parts = call.data.split(":")
    chan_db_id = int(parts[1])
    user_to_remove_id = int(parts[2])

    res = supabase.table("channels").select("owner_id, title").eq("id", chan_db_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]

    # Ensure the user being removed is not the owner
    if user_to_remove_id == user_id:
        await call.answer("Нельзя удалить владельца канала." if lang == "ru" else "Cannot remove channel owner.", show_alert=True)
        return

    try:
        supabase.table("channel_editors").delete().eq("channel_id", chan_db_id).eq("user_id", user_to_remove_id).execute()

        await call.answer(TEXTS["user_removed"][lang])
        # Go back to the manage channel menu
        kb = manage_channel_keyboard(lang, chan_db_id)
        try:
            # Attempt to edit the current confirmation/remove list message
            await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        except Exception:
            # Fallback if edit fails
            await bot.send_message(call.from_user.id, TEXTS["user_removed"][lang], reply_markup=main_menu_keyboard(lang)) # Go to main menu if edit fails
            # Send manage menu separately if needed (might result in two messages)
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
        res_posts = supabase.table("posts").select("job_id").eq("channel_id", chan_db_id).eq("status", "scheduled").execute()
        for post in res_posts.data or []:
            if post["job_id"]:
                try:
                    scheduler.remove_job(post["job_id"])
                    logger.info(f"Cancelled scheduler job {post['job_id']} for channel deletion.")
                except Exception as e:
                    logger.warning(f"Failed to cancel job {post['job_id']} during channel deletion: {e}")

        # Delete related entries first due to foreign keys
        supabase.table("posts").delete().eq("channel_id", chan_db_id).execute()
        supabase.table("channel_editors").delete().eq("channel_id", chan_db_id).execute()
        # Delete the channel itself
        supabase.table("channels").delete().eq("id", chan_db_id).execute()

        # Remove channel from cache
        if chan_db_id in channel_cache:
            del channel_cache[chan_db_id]

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
            await call.message.edit_text(text, reply_markup=kb)
        except Exception:
            await bot.send_message(call.from_user.id, text, reply_markup=kb)


    except Exception as e:
        logger.error(f"Failed to delete channel {chan_db_id}: {e}")
        await call.answer("Ошибка при удалении канала." if lang == "ru" else "Error deleting channel.", show_alert=True)
        # Go back to manage channel menu for the channel (if it wasn't deleted)
        kb = manage_channel_keyboard(lang, chan_db_id) # This might fail if channel is gone
        try:
            await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
        except Exception:
             # If edit fails, probably channel is gone, go to main menu
             await bot.send_message(call.from_user.id, "Ошибка при удалении канала." if lang == "ru" else "Error deleting channel.", reply_markup=main_menu_keyboard(lang))


# --- Settings Flow (Language and Timezone) ---
@dp.message_handler(commands=['settings'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["settings"]["ru"], MENU_BUTTONS["settings"]["en"]], state='*')
async def open_settings(message: types.Message, state: FSMContext, lang: str):
    if await state.get_state() is not None:
        await message.reply(TEXTS["invalid_input"][lang])
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
        supabase.table("users").update({"language": new_lang}).eq("id", user_id).execute()
        # Update cache
        if call.from_user.id in user_cache:
            user_cache[call.from_user.id]["lang"] = new_lang

        await call.answer(TEXTS["language_changed"][new_lang])
        try:
            await call.message.edit_text(TEXTS["language_changed"][new_lang])
        except Exception:
            pass # Ignore if message couldn't be edited
        await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][new_lang], reply_markup=main_menu_keyboard(new_lang)) # Send menu in new language
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

    # Try standard pytz timezones
    if timezone_str in pytz.all_timezones_set:
        valid_timezone = timezone_str
    else:
        # Try parsing UTC offsets like UTC+3, UTC-5 etc.
        try:
             if timezone_str.upper().startswith('UTC'):
                 offset_str = timezone_str[3:].strip()
                 if not offset_str: # Just UTC
                      valid_timezone = 'UTC'
                 else:
                      # Handle +HH, -HH, +HH:MM, -HH:MM
                      offset_parts = offset_str.split(':')
                      hours = int(offset_parts[0])
                      minutes = int(offset_parts[1]) if len(offset_parts) > 1 else 0
                      if abs(hours) <= 12 and minutes >= 0 and minutes < 60:
                           # Create a fixed offset timezone string like 'UTC+03:00'
                           sign = '+' if hours >= 0 else '-'
                           offset_td = datetime.timedelta(hours=abs(hours), minutes=minutes)
                           total_minutes = int(offset_td.total_seconds() / 60)
                           # pytz needs specific format for fixed offsets, like 'Etc/GMT-3' or 'Etc/GMT+4'
                           # Note the sign is reversed in Etc/GMT
                           gmt_offset_str = f"Etc/GMT{sign}{abs(hours)}"
                           if minutes > 0:
                               # pytz doesn't easily handle minutes in Etc/GMT, fall back or require standard names
                               logger.warning(f"UTC offset with minutes received: {timezone_str}. pytz does not support these easily.")
                               valid_timezone = None # Invalid for pytz fixed offsets

                           if gmt_offset_str in pytz.all_timezones_set:
                                valid_timezone = gmt_offset_str
                           else: # Some offsets like +0 are UTC
                                if offset_td == datetime.timedelta(0):
                                    valid_timezone = 'UTC'


        except Exception:
             valid_timezone = None # Parsing failed

    if valid_timezone is None:
        await message.reply(TEXTS["invalid_timezone"][lang])
        return # Stay in state

    try:
        supabase.table("users").update({"timezone": valid_timezone}).eq("id", user_id).execute()
        # Update cache
        if message.from_user.id in user_cache:
            user_cache[message.from_user.id]["timezone"] = valid_timezone

        await message.reply(TEXTS["timezone_updated"][lang].format(timezone=valid_timezone), reply_markup=main_menu_keyboard(lang))
        await state.finish()

    except Exception as e:
        logger.error(f"Failed to set timezone for user {user_id}: {e}")
        await message.reply("Ошибка при сохранении часового пояса." if lang == "ru" else "Error saving timezone.")
        await state.finish()


# --- Fallback Handlers ---
@dp.message_handler(state='*', content_types=ContentType.ANY)
async def handle_unknown_message(message: types.Message, state: FSMContext, lang: str):
    current_state = await state.get_state()
    if current_state:
        # If in a specific state but input doesn't match, it's invalid input
        # (Command handlers with state='*' are checked before this)
        await message.reply(TEXTS["invalid_input"][lang])
    else:
        # If not in a state and not a recognized command, maybe prompt main menu
        await message.reply(TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))


@dp.callback_query_handler(state='*')
async def handle_unknown_callback(call: types.CallbackQuery, state: FSMContext, lang: str):
     # If in a specific state, but callback doesn't match any handler for that state
     # (Callback handlers are checked based on data and state)
     # This handler will only be reached if the callback data doesn't match *any*
     # handler that is active for the current state.
     current_state = await state.get_state()
     if current_state:
         await call.answer("Неожиданное действие." if lang == "ru" else "Unexpected action.", show_alert=True)
     else:
          # If not in a state, might be an old inline keyboard. Just answer.
          await call.answer()
          await bot.send_message(call.from_user.id, TEXTS["menu_prompt"][lang], reply_markup=main_menu_keyboard(lang))


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
