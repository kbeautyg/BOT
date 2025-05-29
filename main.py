import asyncio
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from supabase import create_client, Client
import json
import re

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not BOT_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing BOT_TOKEN or SUPABASE_URL or SUPABASE_KEY in environment")

class SupabaseDB:
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)
    
    def init_schema(self):
        """Ensure the necessary tables exist (or create/alter them if possible)."""
        try:
            # Check if essential tables exist by querying a small portion
            self.client.table("channels").select("id").limit(1).execute()
            self.client.table("posts").select("id").limit(1).execute()
            self.client.table("users").select("user_id").limit(1).execute()
        except Exception:
            # Attempt to create missing tables and columns via SQL
            schema_sql = """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                timezone TEXT DEFAULT 'UTC',
                language TEXT DEFAULT 'ru',
                date_format TEXT DEFAULT 'YYYY-MM-DD',
                time_format TEXT DEFAULT 'HH:MM',
                notify_before INTEGER DEFAULT 0,
                current_project BIGINT
            );
            CREATE TABLE IF NOT EXISTS projects (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name TEXT,
                owner_id BIGINT
            );
            CREATE TABLE IF NOT EXISTS user_projects (
                user_id BIGINT,
                project_id BIGINT,
                role TEXT,
                PRIMARY KEY (user_id, project_id)
            );
            ALTER TABLE user_projects
              ADD FOREIGN KEY (user_id) REFERENCES users(user_id),
              ADD FOREIGN KEY (project_id) REFERENCES projects(id);
            ALTER TABLE channels 
              ADD COLUMN IF NOT EXISTS project_id BIGINT;
            ALTER TABLE posts 
              ADD COLUMN IF NOT EXISTS project_id BIGINT;
            ALTER TABLE channels 
              DROP CONSTRAINT IF EXISTS channels_user_id_chat_id_key;
            ALTER TABLE channels 
              ADD CONSTRAINT channels_project_chat_unique UNIQUE(project_id, chat_id);
            ALTER TABLE channels 
              ADD FOREIGN KEY (project_id) REFERENCES projects(id);
            ALTER TABLE posts 
              ADD FOREIGN KEY (project_id) REFERENCES projects(id);
            """
            try:
                self.client.postgrest.rpc("sql", {"sql": schema_sql}).execute()
            except Exception:
                # If unable to create/alter via API (e.g., insufficient permissions)
                pass

    # User management
    def get_user(self, user_id: int):
        """Retrieve user settings by Telegram user_id."""
        res = self.client.table("users").select("*").eq("user_id", user_id).execute()
        data = res.data or []
        return data[0] if data else None

    def ensure_user(self, user_id: int, default_lang: str = None):
        """Ensure a user exists in the users table. Creates with defaults if not present, and initializes default project."""
        user = self.get_user(user_id)
        if user:
            # If user exists but has no current_project (older data), create default project
            if not user.get("current_project"):
                # Create a default project for existing user
                lang = user.get("language", "ru")
                proj_name = "Мой проект" if lang == "ru" else "My Project"
                project = self.create_project(user_id, proj_name)
                if project:
                    user = self.update_user(user_id, {"current_project": project["id"]})
            return user
        # Create new user with default settings
        lang = default_lang or 'ru'
        new_user = {
            "user_id": user_id,
            "timezone": "UTC",
            "language": lang,
            "date_format": "YYYY-MM-DD",
            "time_format": "HH:MM",
            "notify_before": 0,
            "current_project": None
        }
        res_user = self.client.table("users").insert(new_user).execute()
        created_user = res_user.data[0] if res_user.data else None
        if created_user:
            # Create default project for new user
            proj_name = "Мой проект" if lang == "ru" else "My Project"
            project = self.create_project(user_id, proj_name)
            if project:
                created_user = self.update_user(user_id, {"current_project": project["id"]})
        return created_user

    def update_user(self, user_id: int, updates: dict):
        """Update user settings and return the updated record."""
        if not updates:
            return None
        res = self.client.table("users").update(updates).eq("user_id", user_id).execute()
        return res.data[0] if res.data else None

    # Project management
    def create_project(self, owner_id: int, name: str):
        """Create a new project and assign owner as admin."""
        proj_data = {"name": name, "owner_id": owner_id}
        res_proj = self.client.table("projects").insert(proj_data).execute()
        project = res_proj.data[0] if res_proj.data else None
        if project:
            # Add owner to user_projects with role 'owner'
            member_data = {"user_id": owner_id, "project_id": project["id"], "role": "owner"}
            self.client.table("user_projects").insert(member_data).execute()
        return project

    def list_projects(self, user_id: int):
        """List all projects that a user is a member of (with role info)."""
        # Get all project memberships for the user
        res = self.client.table("user_projects").select("*").eq("user_id", user_id).execute()
        memberships = res.data or []
        project_ids = [m["project_id"] for m in memberships]
        if not project_ids:
            return []
        res_proj = self.client.table("projects").select("*").in_("id", project_ids).execute()
        projects = res_proj.data or []
        # Optionally attach role info
        for proj in projects:
            for m in memberships:
                if m["project_id"] == proj["id"]:
                    proj["role"] = m.get("role")
                    break
        return projects

    def get_project(self, project_id: int):
        """Retrieve a project by ID."""
        res = self.client.table("projects").select("*").eq("id", project_id).execute()
        data = res.data or []
        return data[0] if data else None

    def is_user_in_project(self, user_id: int, project_id: int):
        """Check if a user is a member of the given project."""
        res = self.client.table("user_projects").select("user_id").eq("user_id", user_id).eq("project_id", project_id).execute()
        return bool(res.data)

    def add_user_to_project(self, user_id: int, project_id: int, role: str = "admin"):
        """Add a user to a project with the given role."""
        data = {"user_id": user_id, "project_id": project_id, "role": role}
        try:
            self.client.table("user_projects").insert(data).execute()
            return True
        except Exception:
            return False

    # Channel management
    def add_channel(self, user_id: int, chat_id: int, name: str, project_id: int):
        """Add a new channel to the project (or update its name if it exists)."""
        res = self.client.table("channels").select("*").eq("project_id", project_id).eq("chat_id", chat_id).execute()
        if res.data:
            # Update name if channel exists in this project
            self.client.table("channels").update({"name": name}).eq("project_id", project_id).eq("chat_id", chat_id).execute()
            return res.data[0]
        data = {"user_id": user_id, "project_id": project_id, "name": name, "chat_id": chat_id}
        res_insert = self.client.table("channels").insert(data).execute()
        return res_insert.data[0] if res_insert.data else None

    def list_channels(self, user_id: int = None, project_id: int = None):
        """List all channels, optionally filtered by project or user (membership)."""
        query = self.client.table("channels").select("*")
        if project_id is not None:
            query = query.eq("project_id", project_id)
        elif user_id is not None:
            # Find all projects for this user and list channels in those projects
            res = self.client.table("user_projects").select("project_id").eq("user_id", user_id).execute()
            memberships = res.data or []
            proj_ids = [m["project_id"] for m in memberships]
            if proj_ids:
                query = query.in_("project_id", proj_ids)
            else:
                query = query.eq("project_id", -1)  # no projects, will return empty
        res = query.execute()
        return res.data or []

    def remove_channel(self, project_id: int, identifier: str):
        """Remove a channel (by chat_id or internal id) from the given project."""
        channel_to_delete = None
        if identifier.startswith("@"):
            return False  # Removing by username not supported
        try:
            cid = int(identifier)
        except ValueError:
            return False
        # Try identifier as chat_id
        res = self.client.table("channels").select("*").eq("project_id", project_id).eq("chat_id", cid).execute()
        if res.data:
            channel_to_delete = res.data[0]
        else:
            # Try identifier as internal channel id
            res = self.client.table("channels").select("*").eq("project_id", project_id).eq("id", cid).execute()
            if res.data:
                channel_to_delete = res.data[0]
        if not channel_to_delete:
            return False
        chan_id = channel_to_delete.get("id")
        # Delete channel and any related posts
        self.client.table("channels").delete().eq("id", chan_id).execute()
        self.client.table("posts").delete().eq("channel_id", chan_id).execute()
        return True

    def get_channel(self, channel_id: int):
        """Retrieve a single channel by internal ID."""
        res = self.client.table("channels").select("*").eq("id", channel_id).execute()
        data = res.data or []
        return data[0] if data else None

    def get_channel_by_chat_id(self, chat_id: int):
        """Retrieve a single channel by Telegram chat_id (first match)."""
        res = self.client.table("channels").select("*").eq("chat_id", chat_id).execute()
        data = res.data or []
        return data[0] if data else None

    # Post management
    def add_post(self, post_data: dict):
        """Insert a new post into the database. Returns the inserted record."""
        if "buttons" in post_data and isinstance(post_data["buttons"], list):
            post_data["buttons"] = json.dumps(post_data["buttons"])
        res = self.client.table("posts").insert(post_data).execute()
        return res.data[0] if res.data else None

    def get_post(self, post_id: int):
        """Retrieve a single post by id."""
        res = self.client.table("posts").select("*").eq("id", post_id).execute()
        data = res.data or []
        return data[0] if data else None

    def list_posts(self, user_id: int = None, project_id: int = None, only_pending: bool = True):
        """List posts, optionally filtered by user or project and published status."""
        query = self.client.table("posts").select("*")
        if only_pending:
            query = query.eq("published", False)
        if project_id is not None:
            query = query.eq("project_id", project_id)
        elif user_id is not None:
            query = query.eq("user_id", user_id)
        query = query.order("publish_time", desc=False)
        res = query.execute()
        return res.data or []

    def update_post(self, post_id: int, updates: dict):
        """Update fields of a post and return the updated record."""
        if "buttons" in updates and isinstance(updates["buttons"], list):
            updates["buttons"] = json.dumps(updates["buttons"])
        res = self.client.table("posts").update(updates).eq("id", post_id).execute()
        return res.data[0] if res.data else None

    def delete_post(self, post_id: int):
        """Delete a post by id."""
        self.client.table("posts").delete().eq("id", post_id).execute()

    def get_due_posts(self, current_time):
        """Get all posts due at or before current_time (not published and not drafts)."""
        now_str = current_time.strftime("%Y-%m-%dT%H:%M:%S%z") if hasattr(current_time, "strftime") else str(current_time)
        res = self.client.table("posts").select("*").eq("published", False).eq("draft", False).lte("publish_time", now_str).execute()
        return res.data or []

    def mark_post_published(self, post_id: int):
        """Mark a post as published."""
        self.client.table("posts").update({"published": True}).eq("id", post_id).execute()

class DBContainer: pass
supabase_db = DBContainer()
supabase_db.db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY)
supabase_db.db.init_schema()

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(storage=MemoryStorage())

# State definitions
class CreatePost(StatesGroup):
    text = State()
    media = State()
    format = State()
    buttons = State()
    time = State()
    repeat = State()
    channel = State()
    confirm = State()

class EditPost(StatesGroup):
    text = State()
    media = State()
    format = State()
    buttons = State()
    time = State()
    repeat = State()
    channel = State()
    confirm = State()

class NewProject(StatesGroup):
    name = State()

TEXTS = {
    'ru': {
        'start_welcome': "Привет! Я бот для отложенного постинга.\nИспользуйте /help для списка команд.",
        'help': ("Команды:\n"
                 "/create – создать пост\n"
                 "/list – список отложенных постов\n"
                 "/view <ID> – просмотреть пост\n"
                 "/edit <ID> – редактировать пост\n"
                 "/reschedule <ID> <дата/время> – перенести время публикации\n"
                 "/delete <ID> – удалить пост\n"
                 "/channels – управление каналами\n"
                 "/project – проекты (смена/создание)\n"
                 "/settings – настройки пользователя\n"
                 "/cancel – отменить ввод"),
        'channels_no_channels': "Список каналов пуст. Добавьте канал:\n/channels add <ID_канала или @username>",
        'channels_list_title': "Подключенные каналы:",
        'channels_item': "- {name} (ID: {id})",
        'channels_add_usage': "Использование:\n/channels add <ID_канала или @username>",
        'channels_remove_usage': "Использование:\n/channels remove <ID_канала>",
        'channels_added': "Канал «{name}» добавлен.",
        'channels_add_error': "Не удалось получить канал: {error}",
        'channels_removed': "Канал удалён.",
        'channels_not_found': "Канал не найден.",
        'channels_unknown_command': "Неизвестная подкоманда. Используйте /channels add | remove",
        'channels_remove_confirm': "Удалить канал «{name}»? Все связанные посты будут удалены.",
        'no_channels': "Нет доступных каналов. Сначала добавьте канал через /channels.",
        'create_step1': "Шаг 1/8: отправьте текст поста (или /skip).",
        'create_step2': "Шаг 2/8: пришлите фото или видео, или /skip.",
        'create_step2_retry': "Отправьте фото или видео, или /skip.",
        'create_step3': "Шаг 3/8: выберите формат (Markdown, HTML или Без форматирования).",
        'create_step4': ("Шаг 4/8: отправьте кнопки.\n"
                         "Каждая кнопка на новой строке: «Текст | URL».\n"
                         "Если кнопки не нужны – отправьте /skip."),
        'create_step5': "Шаг 5/8: отправьте дату и время публикации в формате {format}.",
        'create_time_error': "Неверный формат. Пример: {example}.",
        'time_past_error': "Указанное время уже прошло. Пожалуйста, укажите время в будущем.",
        'create_step6': ("Шаг 6/8: интервал повторения поста.\n"
                         "Напр.: 1d (ежедневно), 7d (еженедельно), 12h (каждые 12 часов), 0 или /skip – без повтора."),
        'create_repeat_error': "Неверный формат интервала. Примеры: 0, 1d, 12h, 30m.",
        'create_step7': "Шаг 7/8: выберите канал для публикации (введите номер).",
        'create_channel_error': "Канал не найден. Введите номер или ID.",
        'confirm_post_scheduled': "Пост запланирован ✅",
        'confirm_post_draft': "Черновик сохранён ✅",
        'confirm_post_cancel': "Отменено.",
        'view_usage': "Использование: /view <ID поста>",
        'view_invalid_id': "Некорректный ID поста.",
        'view_not_found': "Пост с таким ID не найден.",
        'edit_usage': "Использование: /edit <ID поста>",
        'edit_invalid_id': "Некорректный ID поста.",
        'edit_post_not_found': "Пост с таким ID не найден.",
        'edit_post_published': "Этот пост уже опубликован, редактирование невозможно.",
        'edit_begin': "Редактирование поста #{id}.\nТекущий текст: \"{text}\"\nОтправьте новый текст или /skip, чтобы оставить без изменений.",
        'edit_current_media': "Текущее медиа: {info} прикреплено.\nОтправьте новое фото или видео, чтобы заменить, или /skip, чтобы оставить, или введите 'нет' для удаления медиа.",
        'edit_no_media': "Для поста нет медиа.\nОтправьте фото или видео, чтобы добавить, или /skip, чтобы пропустить.",
        'edit_current_format': "Текущий формат: {format}. Выберите новый формат или отправьте /skip для сохранения текущего.",
        'edit_current_buttons': "Текущие кнопки:\n{buttons_list}\nОтправьте новые кнопки (Текст | URL), или /skip для сохранения, или 'нет' для удаления всех.",
        'edit_no_buttons': "Для поста нет кнопок.\nОтправьте кнопки в формате Текст | URL, или /skip, чтобы пропустить, или 'нет' чтобы оставить без кнопок.",
        'edit_current_time': "Текущее время публикации: {time}\nВведите новую дату/время в формате {format}, или /skip для сохранения, или 'none' для удаления времени (черновик).",
        'edit_time_error': "Неверный формат. Введите в формате {format} или /skip.",
        'edit_current_repeat': "Текущий интервал повтора: {repeat}\nВведите новый интервал (0 — без повтора) или /skip для сохранения.",
        'edit_repeat_error': "Неверный формат интервала. Примеры: 0, 1d, 12h, 30m.",
        'edit_choose_channel': "Выберите новый канал для поста (или отправьте /skip, чтобы оставить текущий):",
        'edit_keep_current_channel': "Оставить текущий",
        'confirm_changes_saved': "Изменения сохранены для поста #{id}.",
        'edit_cancelled': "Редактирование поста отменено.",
        'edit_saved_notify': "Пост отредактирован ✅",
        'edit_cancel_notify': "Редактирование отменено ❌",
        'reschedule_usage': "Использование: /reschedule <ID поста> <дата и время>",
        'reschedule_invalid_id': "Некорректный ID поста.",
        'reschedule_not_found': "Пост с таким ID не найден.",
        'reschedule_post_published': "Этот пост уже был опубликован, его нельзя перенести.",
        'reschedule_success': "Пост #{id} перенесён.",
        'no_posts': "Нет запланированных постов.",
        'scheduled_posts_title': "Запланированные посты:",
        'delete_usage': "Использование: /delete <ID поста>",
        'delete_invalid_id': "Некорректный ID поста.",
        'delete_not_found': "Пост с таким ID не найден.",
        'delete_already_published': "Этот пост уже был опубликован, его нельзя удалить.",
        'delete_success': "Пост #{id} удалён.",
        'delete_confirm': "Удалить пост #{id}? Это действие необратимо.",
        'no_text': "(без текста)",
        'media_photo': "фото",
        'media_video': "видео",
        'media_media': "медиа",
        'settings_current': ("Ваши настройки:\n"
                             "Часовой пояс: {tz}\n"
                             "Язык: {lang}\n"
                             "Формат даты: {date_fmt}\n"
                             "Формат времени: {time_fmt}\n"
                             "Уведомления: {notify}"),
        'settings_timezone_usage': "Использование:\n/settings tz <часовой пояс>",
        'settings_language_usage': "Использование:\n/settings lang <ru|en>",
        'settings_datefmt_usage': "Использование:\n/settings datefmt <формат даты> (например, DD.MM.YYYY)",
        'settings_timefmt_usage': "Использование:\n/settings timefmt <формат времени> (например, HH:MM)",
        'settings_notify_usage': "Использование:\n/settings notify <минут до уведомления> (0 для выкл.)",
        'settings_unknown': "Неизвестная настройка. Доступно: tz, lang, datefmt, timefmt, notify",
        'settings_tz_set': "Часовой пояс обновлен: {tz}",
        'settings_lang_set': "Язык интерфейса обновлен: {lang_name}",
        'settings_datefmt_set': "Формат даты обновлен: {fmt}",
        'settings_timefmt_set': "Формат времени обновлен: {fmt}",
        'settings_notify_set': "Уведомления перед публикацией: {minutes_str}",
        'settings_invalid_tz': "Неправильный часовой пояс. Пример: Europe/Moscow или UTC+3",
        'settings_invalid_lang': "Неподдерживаемый язык. Доступно: ru, en",
        'settings_invalid_datefmt': "Неверный формат даты.",
        'settings_invalid_timefmt': "Неверный формат времени.",
        'settings_invalid_notify': "Неверное значение (в минутах).",
        'lang_ru': "Русский",
        'lang_en': "Английский",
        'notify_message': "⌛️ Скоро будет опубликован пост #{id} в канале {channel} (через {minutes} мин.).",
        'notify_message_less_min': "⌛️ Скоро будет опубликован пост #{id} в канале {channel} (менее чем через минуту).",
        'error_post_failed': "⚠️ Не удалось отправить пост #{id} в канал {channel}: {error}",
        'projects_list_title': "Ваши проекты:",
        'projects_item': "- {name}",
        'projects_item_current': "- {name} (текущий)",
        'projects_created': "Проект \"{name}\" создан ✅",
        'projects_switched': "Переключено на проект \"{name}\" ✅",
        'projects_not_found': "Проект не найден или доступ запрещен.",
        'projects_invite_usage': "Использование:\n/project invite <ID пользователя>",
        'projects_invite_success': "Пользователь {user_id} добавлен в проект.",
        'projects_invite_not_found': "Пользователь не найден или не запускал бота.",
        'projects_invited_notify': "Вас добавили в проект \"{project}\" пользователем {user}. Используйте /project для переключения.",
        'yes_btn': "Да",
        'no_btn': "Нет"
    },
    'en': {
        'start_welcome': "Hello! I'm a bot for scheduling posts.\nUse /help to see available commands.",
        'help': ("Commands:\n"
                 "/create – create a post\n"
                 "/list – list scheduled posts\n"
                 "/view <ID> – view a post\n"
                 "/edit <ID> – edit a post\n"
                 "/reschedule <ID> <datetime> – reschedule a post\n"
                 "/delete <ID> – delete a post\n"
                 "/channels – manage channels\n"
                 "/project – projects (switch/create)\n"
                 "/settings – user settings\n"
                 "/cancel – cancel input"),
        'channels_no_channels': "No channels added. Add a channel via:\n/channels add <channel_id or @username>",
        'channels_list_title': "Connected channels:",
        'channels_item': "- {name} (ID: {id})",
        'channels_add_usage': "Usage:\n/channels add <channel_id or @username>",
        'channels_remove_usage': "Usage:\n/channels remove <channel_id>",
        'channels_added': "Channel \"{name}\" added.",
        'channels_add_error': "Failed to get channel: {error}",
        'channels_removed': "Channel removed.",
        'channels_not_found': "Channel not found.",
        'channels_unknown_command': "Unknown subcommand. Use /channels add | remove",
        'channels_remove_confirm': "Remove channel \"{name}\"? All associated posts will be deleted.",
        'no_channels': "No channels available. Please add a channel via /channels first.",
        'create_step1': "Step 1/8: send the post text (or /skip).",
        'create_step2': "Step 2/8: send a photo or video, or /skip.",
        'create_step2_retry': "Please send a photo or video, or /skip.",
        'create_step3': "Step 3/8: choose format (Markdown, HTML or None).",
        'create_step4': ("Step 4/8: send buttons.\n"
                         "One button per line: Text | URL.\n"
                         "If no buttons needed, send /skip."),
        'create_step5': "Step 5/8: send the date/time in format {format}.",
        'create_time_error': "Invalid format. Example: {example}.",
        'time_past_error': "The specified time is in the past. Please provide a future time.",
        'create_step6': ("Step 6/8: set repeat interval.\n"
                         "E.g. 1d (daily), 7d (weekly), 12h (every 12 hours), 0 or /skip for no repeat."),
        'create_repeat_error': "Invalid interval format. Examples: 0, 1d, 12h, 30m.",
        'create_step7': "Step 7/8: choose a channel for posting (enter number).",
        'create_channel_error': "Channel not found. Enter a number or ID.",
        'confirm_post_scheduled': "Post scheduled ✅",
        'confirm_post_draft': "Draft saved ✅",
        'confirm_post_cancel': "Cancelled.",
        'view_usage': "Usage: /view <post ID>",
        'view_invalid_id': "Invalid post ID.",
        'view_not_found': "Post not found.",
        'edit_usage': "Usage: /edit <post ID>",
        'edit_invalid_id': "Invalid post ID.",
        'edit_post_not_found': "Post not found.",
        'edit_post_published': "This post has already been published and cannot be edited.",
        'edit_begin': "Editing post #{id}.\nCurrent text: \"{text}\"\nSend new text or /skip to leave unchanged.",
        'edit_current_media': "Current media: {info} attached.\nSend a new photo or video to replace, or /skip to keep, or type 'none' to remove.",
        'edit_no_media': "This post has no media.\nSend a photo or video to add, or /skip to continue.",
        'edit_current_format': "Current format: {format}. Choose a new format or send /skip to keep current.",
        'edit_current_buttons': "Current buttons:\n{buttons_list}\nSend new buttons (Text | URL), or /skip to keep, or 'none' to remove all.",
        'edit_no_buttons': "This post has no buttons.\nSend buttons in Text | URL format to add, or /skip to skip, or 'none' to keep none.",
        'edit_current_time': "Current scheduled time: {time}\nEnter a new date/time in format {format}, or /skip to keep, or 'none' to unschedule (draft).",
        'edit_time_error': "Invalid format. Use {format} or /skip.",
        'edit_current_repeat': "Current repeat interval: {repeat}\nEnter a new interval (0 for none) or /skip to keep.",
        'edit_repeat_error': "Invalid interval format. Examples: 0, 1d, 12h, 30m.",
        'edit_choose_channel': "Choose a new channel for the post (or send /skip to keep the current one):",
        'edit_keep_current_channel': "Keep current",
        'confirm_changes_saved': "Changes saved for post #{id}.",
        'edit_cancelled': "Post editing cancelled.",
        'edit_saved_notify': "Post edited ✅",
        'edit_cancel_notify': "Edit cancelled ❌",
        'reschedule_usage': "Usage: /reschedule <post ID> <datetime>",
        'reschedule_invalid_id': "Invalid post ID.",
        'reschedule_not_found': "Post not found.",
        'reschedule_post_published': "This post has already been published and cannot be rescheduled.",
        'reschedule_success': "Post #{id} rescheduled.",
        'no_posts': "No scheduled posts.",
        'scheduled_posts_title': "Scheduled posts:",
        'delete_usage': "Usage: /delete <post ID>",
        'delete_invalid_id': "Invalid post ID.",
        'delete_not_found': "Post not found.",
        'delete_already_published': "This post has already been published and cannot be deleted.",
        'delete_success': "Post #{id} deleted.",
        'delete_confirm': "Delete post #{id}? This action cannot be undone.",
        'no_text': "(no text)",
        'media_photo': "photo",
        'media_video': "video",
        'media_media': "media",
        'settings_current': ("Your settings:\n"
                             "Timezone: {tz}\n"
                             "Language: {lang}\n"
                             "Date format: {date_fmt}\n"
                             "Time format: {time_fmt}\n"
                             "Notifications: {notify}"),
        'settings_timezone_usage': "Usage:\n/settings tz <timezone>",
        'settings_language_usage': "Usage:\n/settings lang <ru|en>",
        'settings_datefmt_usage': "Usage:\n/settings datefmt <date format> (e.g. DD.MM.YYYY)",
        'settings_timefmt_usage': "Usage:\n/settings timefmt <time format> (e.g. HH:MM)",
        'settings_notify_usage': "Usage:\n/settings notify <minutes before> (0 to disable)",
        'settings_unknown': "Unknown setting. Available: tz, lang, datefmt, timefmt, notify",
        'settings_tz_set': "Timezone updated to {tz}",
        'settings_lang_set': "Language updated to {lang_name}",
        'settings_datefmt_set': "Date format updated to {fmt}",
        'settings_timefmt_set': "Time format updated to {fmt}",
        'settings_notify_set': "Notification lead time set to {minutes_str}",
        'settings_invalid_tz': "Invalid timezone. Example: Europe/Moscow or UTC+3",
        'settings_invalid_lang': "Unsupported language. Available: ru, en",
        'settings_invalid_datefmt': "Invalid date format.",
        'settings_invalid_timefmt': "Invalid time format.",
        'settings_invalid_notify': "Invalid notification value.",
        'lang_ru': "Russian",
        'lang_en': "English",
        'notify_message': "⌛️ Post #{id} in channel {channel} will be posted in {minutes} min.",
        'notify_message_less_min': "⌛️ Post #{id} in channel {channel} will be posted in less than a minute.",
        'error_post_failed': "⚠️ Failed to send post #{id} to channel {channel}: {error}",
        'projects_list_title': "Your projects:",
        'projects_item': "- {name}",
        'projects_item_current': "- {name} (current)",
        'projects_created': "Project \"{name}\" created ✅",
        'projects_switched': "Switched to project \"{name}\" ✅",
        'projects_not_found': "Project not found or access denied.",
        'projects_invite_usage': "Usage:\n/project invite <user_id>",
        'projects_invite_success': "User {user_id} added to the project.",
        'projects_invite_not_found': "User not found or has not started the bot.",
        'projects_invited_notify': "You have been added to project \"{project}\" by {user}. Use /project to switch to it.",
        'yes_btn': "Yes",
        'no_btn': "No"
    }
}

# Date/time parsing utilities
TOKEN_MAP = {"YYYY": "%Y", "YY": "%y",
             "MM": "%m",   "DD": "%d",
             "HH": "%H",   "hh": "%I",
             "mm": "%M",   "SS": "%S",
             "AM": "%p",   "PM": "%p",
             "am": "%p",   "pm": "%p",
}
_rx = re.compile("|".join(sorted(TOKEN_MAP, key=len, reverse=True)))

def format_to_strptime(date_fmt: str, time_fmt: str) -> str:
    return _rx.sub(lambda m: TOKEN_MAP[m.group(0)], f"{date_fmt} {time_fmt}")

def parse_time(user: dict, text: str):
    date_fmt = user.get("date_format", "YYYY-MM-DD")
    time_fmt = user.get("time_format", "HH:mm")
    tz_name = user.get("timezone", "UTC")
    # Adjust format to avoid conflict between month and minute tokens
    if "MM" in time_fmt:
        time_fmt = time_fmt.replace("MM", "mm")
    fmt = format_to_strptime(date_fmt, time_fmt)
    dt = datetime.strptime(text, fmt)
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    local_dt = dt.replace(tzinfo=tz)
    utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    return utc_dt

def format_example(user: dict):
    date_fmt = user.get("date_format", "YYYY-MM-DD")
    time_fmt = user.get("time_format", "HH:mm")
    if "MM" in time_fmt:
        time_fmt = time_fmt.replace("MM", "mm")
    fmt = format_to_strptime(date_fmt, time_fmt)
    now = datetime.now()
    try:
        return now.strftime(fmt)
    except Exception:
        return now.strftime("%Y-%m-%d %H:%M")

# Start command handler
start_router = Router()
@start_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    # Determine default language from user's Telegram settings
    lang_code = message.from_user.language_code or ""
    default_lang = "ru"
    if lang_code.startswith("en"):
        default_lang = "en"
    elif lang_code.startswith("ru"):
        default_lang = "ru"
    # Ensure user exists with default settings (and default project)
    user = supabase_db.db.ensure_user(user_id, default_lang=default_lang)
    # Greet in user's language
    lang = user.get("language", default_lang) if user else default_lang
    await message.answer(TEXTS[lang]['start_welcome'])

@start_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    user_id = message.from_user.id
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    if not current_state:
        await message.answer(TEXTS[lang]['confirm_post_cancel'])
    else:
        await state.clear()
        await message.answer(TEXTS[lang]['confirm_post_cancel'])

# Help command handler
help_router = Router()
@help_router.message(Command("help"))
async def cmd_help(message: Message):
    user_id = message.from_user.id
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    await message.answer(TEXTS[lang]['help'])

# Channels command handler
channels_router = Router()
@channels_router.message(Command("channels"))
async def cmd_channels(message: Message, bot: Bot):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=2)
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    project_id = user.get("current_project") if user else None
    # If just /channels, list channels in current project
    if len(args) == 1:
        if not project_id:
            await message.answer(TEXTS[lang]['channels_no_channels'])
            return
        channels = supabase_db.db.list_channels(project_id=project_id)
        if not channels:
            await message.answer(TEXTS[lang]['channels_no_channels'])
            return
        await message.answer(TEXTS[lang]['channels_list_title'])
        for ch in channels:
            cid = ch["chat_id"]
            title = ch.get("name") or str(cid)
            text = TEXTS[lang]['channels_item'].format(name=title, id=cid)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑️ Remove", callback_data=f"remove_channel:{ch['id']}")]
            ])
            await message.answer(text, reply_markup=kb)
        return

    sub = args[1].lower()
    if sub == "add":
        if len(args) < 3:
            await message.answer(TEXTS[lang]['channels_add_usage'])
            return
        if not project_id:
            await message.answer(TEXTS[lang]['channels_add_error'].format(error="No active project"))
            return
        ident = args[2]
        try:
            chat = await bot.get_chat(ident)
            member = await bot.get_chat_member(chat.id, user_id)
            if member.status not in ("administrator", "creator"):
                await message.answer("❌ Вы должны быть администратором канала." if lang == "ru" else "❌ You must be an administrator of that channel.")
                return
            bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
            if bot_member.status not in ("administrator", "creator"):
                await message.answer("❌ Бот не является администратором этого канала." if lang == "ru" else "❌ The bot is not an admin in that channel.")
                return
            supabase_db.db.add_channel(user_id, chat.id, chat.title or chat.username or str(chat.id), project_id)
            await message.answer(TEXTS[lang]['channels_added'].format(name=chat.title or chat.username or chat.id))
        except Exception as e:
            await message.answer(TEXTS[lang]['channels_add_error'].format(error=e))
    elif sub in ("remove", "delete"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['channels_remove_usage'])
            return
        if not project_id:
            await message.answer(TEXTS[lang]['channels_not_found'])
            return
        identifier = args[2]
        chan = None
        if identifier.isdigit():
            chan_list = supabase_db.db.list_channels(project_id=project_id)
            for ch in chan_list:
                if ch.get("chat_id") == int(identifier) or ch.get("id") == int(identifier):
                    chan = ch
                    break
        if not chan:
            await message.answer(TEXTS[lang]['channels_not_found'])
            return
        title = chan.get("name") or str(chan.get("chat_id"))
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=TEXTS[lang]['yes_btn'], callback_data=f"confirm_remove_channel:{chan['id']}"),
                InlineKeyboardButton(text=TEXTS[lang]['no_btn'], callback_data=f"cancel_remove_channel_text:{chan['id']}")
            ]
        ])
        await message.answer(TEXTS[lang]['channels_remove_confirm'].format(name=title), reply_markup=confirm_kb)
    else:
        await message.answer(TEXTS[lang]['channels_unknown_command'])

@channels_router.callback_query(lambda c: c.data and c.data.startswith("remove_channel:"))
async def on_remove_channel_button(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        chan_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    channel = supabase_db.db.get_channel(chan_id)
    if not channel or not user or not supabase_db.db.is_user_in_project(user_id, channel.get("project_id")):
        await callback.answer(TEXTS[lang]['channels_not_found'], show_alert=True)
        return
    title = channel.get("name") or str(channel.get("chat_id"))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=TEXTS[lang]['yes_btn'], callback_data=f"confirm_remove_channel:{chan_id}"),
            InlineKeyboardButton(text=TEXTS[lang]['no_btn'], callback_data=f"cancel_remove_channel:{chan_id}")
        ]
    ])
    try:
        await callback.message.edit_text(TEXTS[lang]['channels_remove_confirm'].format(name=title), reply_markup=kb)
    except:
        pass
    await callback.answer()

@channels_router.callback_query(lambda c: c.data and c.data.startswith("confirm_remove_channel:"))
async def on_confirm_remove_channel(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        chan_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    channel = supabase_db.db.get_channel(chan_id)
    project_id = channel.get("project_id") if channel else None
    success = False
    if project_id and user and supabase_db.db.is_user_in_project(user_id, project_id):
        success = supabase_db.db.remove_channel(project_id, str(chan_id))
    if success:
        await callback.message.edit_text(TEXTS[lang]['channels_removed'])
    else:
        await callback.message.edit_text(TEXTS[lang]['channels_not_found'])
    await callback.answer()

@channels_router.callback_query(lambda c: c.data and c.data.startswith("cancel_remove_channel:"))
async def on_cancel_remove_channel(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        chan_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    channel = supabase_db.db.get_channel(chan_id)
    if channel:
        cid = channel.get("chat_id")
        title = channel.get("name") or str(cid)
        text = TEXTS[lang]['channels_item'].format(name=title, id=cid)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ Remove", callback_data=f"remove_channel:{channel['id']}")]
        ])
        await callback.message.edit_text(text, reply_markup=kb)
    else:
        await callback.message.edit_text(TEXTS[lang]['confirm_post_cancel'])
    await callback.answer(TEXTS[lang]['confirm_post_cancel'])

# Delete post command handler
delete_router = Router()
@delete_router.message(Command("delete"))
async def cmd_delete(message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    if len(args) < 2:
        await message.answer(TEXTS[lang]['delete_usage'])
        return
    try:
        post_id = int(args[1])
    except:
        await message.answer(TEXTS[lang]['delete_invalid_id'])
        return
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await message.answer(TEXTS[lang]['delete_not_found'])
        return
    if post.get("published"):
        await message.answer(TEXTS[lang]['delete_already_published'])
        return
    supabase_db.db.delete_post(post_id)
    await message.answer(TEXTS[lang]['delete_success'].format(id=post_id))

# Edit post command handler
edit_router = Router()
@edit_router.message(Command("edit"))
async def cmd_edit(message: Message, state: FSMContext):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    if len(args) < 2:
        await message.answer(TEXTS[lang]['edit_usage'])
        return
    try:
        post_id = int(args[1])
    except:
        await message.answer(TEXTS[lang]['edit_invalid_id'])
        return
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await message.answer(TEXTS[lang]['edit_post_not_found'])
        return
    if post.get("published"):
        await message.answer(TEXTS[lang]['edit_post_published'])
        return
    await state.update_data(orig_post=post, user_settings=(user or supabase_db.db.ensure_user(user_id, default_lang=lang)))
    await state.set_state(EditPost.text)
    current_text = post.get("text") or ""
    await message.answer(TEXTS[lang]['edit_begin'].format(id=post_id, text=current_text))

@edit_router.message(EditPost.text, Command("skip"))
async def skip_edit_text(message: Message, state: FSMContext):
    await state.update_data(new_text=None)
    await ask_edit_media(message, state)

@edit_router.message(EditPost.text)
async def edit_step_text(message: Message, state: FSMContext):
    await state.update_data(new_text=message.text or "")
    await ask_edit_media(message, state)

async def ask_edit_media(message: Message, state: FSMContext):
    await state.set_state(EditPost.media)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    lang = data.get("user_settings", {}).get("language", "ru")
    if orig_post.get("media_id"):
        info = TEXTS[lang]['media_photo'] if orig_post.get("media_type") == "photo" else TEXTS[lang]['media_video'] if orig_post.get("media_type") == "video" else TEXTS[lang]['media_media']
        await message.answer(TEXTS[lang]['edit_current_media'].format(info=info))
    else:
        await message.answer(TEXTS[lang]['edit_no_media'])

@edit_router.message(EditPost.media, Command("skip"))
async def skip_edit_media(message: Message, state: FSMContext):
    await state.update_data(new_media_id=None, new_media_type=None)
    await ask_edit_format(message, state)

@edit_router.message(EditPost.media, F.photo)
async def edit_step_media_photo(message: Message, state: FSMContext):
    await state.update_data(new_media_id=message.photo[-1].file_id, new_media_type="photo")
    await ask_edit_format(message, state)

@edit_router.message(EditPost.media, F.video)
async def edit_step_media_video(message: Message, state: FSMContext):
    await state.update_data(new_media_id=message.video.file_id, new_media_type="video")
    await ask_edit_format(message, state)

@edit_router.message(EditPost.media)
async def edit_step_media_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("user_settings", {}).get("language", "ru")
    if data.get("orig_post", {}).get("media_id"):
        info = TEXTS[lang]['media_media']
        await message.answer(TEXTS[lang]['edit_current_media'].format(info=info))
    else:
        await message.answer(TEXTS[lang]['edit_no_media'])

async def ask_edit_format(message: Message, state: FSMContext):
    await state.set_state(EditPost.format)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    lang = data.get("user_settings", {}).get("language", "ru")
    current_format = orig_post.get("format") or "none"
    await message.answer(TEXTS[lang]['edit_current_format'].format(format=current_format))

@edit_router.message(EditPost.format)
async def edit_step_format(message: Message, state: FSMContext):
    raw = (message.text or "").strip().lower()
    new_fmt = None
    if raw:
        if raw.startswith("markdown"):
            new_fmt = "markdown"
        elif raw.startswith("html") or raw.startswith("htm"):
            new_fmt = "html"
        elif raw in ("none", "без", "без форматирования"):
            new_fmt = "none"
    if new_fmt is None:
        data = await state.get_data()
        lang = data.get("user_settings", {}).get("language", "ru")
        new_fmt = (data.get("orig_post", {}).get("format") or "none")
    await state.update_data(new_format=new_fmt)
    await ask_edit_buttons(message, state)

async def ask_edit_buttons(message: Message, state: FSMContext):
    await state.set_state(EditPost.buttons)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    lang = data.get("user_settings", {}).get("language", "ru")
    if orig_post.get("buttons"):
        btns = orig_post.get("buttons")
        if isinstance(btns, str):
            try:
                btns = json.loads(btns)
            except:
                btns = []
        if not isinstance(btns, list):
            btns = []
        if btns:
            buttons_list = "\n".join([f"- {b['text']} | {b['url']}" if isinstance(b, dict) else f"- {b}" for b in btns])
        else:
            buttons_list = "-"
        await message.answer(TEXTS[lang]['edit_current_buttons'].format(buttons_list=buttons_list))
    else:
        await message.answer(TEXTS[lang]['edit_no_buttons'])

@edit_router.message(EditPost.buttons)
async def edit_step_buttons(message: Message, state: FSMContext):
    text = message.text or ""
    if text.strip().lower() in ("нет", "none"):
        await state.update_data(new_buttons=[])
    else:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        new_buttons = []
        for line in lines:
            parts = line.split("|")
            if len(parts) >= 2:
                btn_text = parts[0].strip()
                btn_url = parts[1].strip()
                if btn_text and btn_url:
                    new_buttons.append({"text": btn_text, "url": btn_url})
        await state.update_data(new_buttons=new_buttons)
    await ask_edit_time(message, state)

async def ask_edit_time(message: Message, state: FSMContext):
    await state.set_state(EditPost.time)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    user = data.get("user_settings", {}) or {}
    lang = user.get("language", "ru")
    if orig_post.get("publish_time"):
        orig_time = orig_post.get("publish_time")
        try:
            pub_dt = datetime.fromisoformat(orig_time) if isinstance(orig_time, str) else orig_time
        except:
            pub_dt = datetime.strptime(orig_time, "%Y-%m-%dT%H:%M:%S")
            pub_dt = pub_dt.replace(tzinfo=ZoneInfo("UTC"))
        tz_name = user.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_name)
        except:
            tz = ZoneInfo("UTC")
        local_dt = pub_dt.astimezone(tz)
        fmt = format_to_strptime(user.get("date_format", "YYYY-MM-DD"), user.get("time_format", "HH:mm"))
        current_time_str = local_dt.strftime(fmt)
        await message.answer(TEXTS[lang]['edit_current_time'].format(time=current_time_str, format=f"{user.get('date_format', 'YYYY-MM-DD')} {user.get('time_format', 'HH:mm')}"))
    else:
        await message.answer(TEXTS[lang]['edit_current_time'].format(time="(черновик)" if lang == "ru" else "(draft)", format=f"{user.get('date_format', 'YYYY-MM-DD')} {user.get('time_format', 'HH:mm')}"))

@edit_router.message(EditPost.time)
async def edit_step_time(message: Message, state: FSMContext):
    data = await state.get_data()
    user = data.get("user_settings", {}) or {}
    lang = user.get("language", "ru")
    text_val = (message.text or "").strip()
    if text_val.lower() in ("none", "нет"):
        await state.update_data(new_publish_time=None)
    else:
        try:
            new_time = parse_time(user, text_val)
        except:
            await message.answer(TEXTS[lang]['edit_time_error'].format(format=f"{user.get('date_format', 'YYYY-MM-DD')} {user.get('time_format', 'HH:mm')}"))
            return
        now = datetime.now(ZoneInfo("UTC"))
        if new_time <= now:
            await message.answer(TEXTS[lang]['time_past_error'])
            return
        await state.update_data(new_publish_time=new_time)
    await ask_edit_repeat(message, state)

async def ask_edit_repeat(message: Message, state: FSMContext):
    await state.set_state(EditPost.repeat)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    user = data.get("user_settings", {}) or {}
    lang = user.get("language", "ru")
    current_repeat = orig_post.get("repeat_interval") or 0
    current_repeat_str = "0"
    if current_repeat % 86400 == 0 and current_repeat > 0:
        days = current_repeat // 86400
        current_repeat_str = f"{days}d"
    elif current_repeat % 3600 == 0 and current_repeat > 0:
        hours = current_repeat // 3600
        current_repeat_str = f"{hours}h"
    elif current_repeat % 60 == 0 and current_repeat > 0:
        minutes = current_repeat // 60
        current_repeat_str = f"{minutes}m"
    await message.answer(TEXTS[lang]['edit_current_repeat'].format(repeat=current_repeat_str))

@edit_router.message(EditPost.repeat)
async def edit_step_repeat(message: Message, state: FSMContext):
    data = await state.get_data()
    user = data.get("user_settings", {}) or {}
    lang = user.get("language", "ru")
    raw = (message.text or "").strip().lower()
    new_interval = None
    if raw in ("0", "none", "нет", "/skip"):
        new_interval = 0
    else:
        unit = raw[-1] if raw else ""
        try:
            value = int(raw[:-1])
        except:
            value = None
        if not value or unit not in ("d", "h", "m"):
            await message.answer(TEXTS[lang]['edit_repeat_error'])
            return
        if unit == "d":
            new_interval = value * 86400
        elif unit == "h":
            new_interval = value * 3600
        elif unit == "m":
            new_interval = value * 60
    if new_interval is None:
        new_interval = 0
    await state.update_data(new_repeat_interval=new_interval)
    await ask_edit_channel(message, state)

async def ask_edit_channel(message: Message, state: FSMContext):
    await state.set_state(EditPost.channel)
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    lang = data.get("user_settings", {}).get("language", "ru")
    channels_list = supabase_db.db.list_channels(project_id=data.get("user_settings", {}).get("current_project"))
    if not channels_list:
        await message.answer(TEXTS[lang]['channels_no_channels'])
        return
    current_channel_name = "(unknown)"
    chan_id = orig_post.get("channel_id"); chat_id = orig_post.get("chat_id")
    for ch in channels_list:
        if chan_id and ch.get("id") == chan_id:
            current_channel_name = ch.get("name") or str(ch.get("chat_id"))
            break
        if chat_id and ch.get("chat_id") == chat_id:
            current_channel_name = ch.get("name") or str(ch.get("chat_id"))
            break
    if lang == "ru":
        lines = [f"Текущий канал: {current_channel_name}", "Выберите новый канал или отправьте /skip, чтобы оставить текущий:"]
    else:
        lines = [f"Current channel: {current_channel_name}", "Choose a new channel or send /skip to keep the current one:"]
    for i, ch in enumerate(channels_list, start=1):
        name = ch.get("name") or str(ch.get("chat_id"))
        lines.append(f"{i}. {name}")
    await state.update_data(_chan_map=channels_list)
    await message.answer("\n".join(lines))

@edit_router.message(EditPost.channel, Command("skip"))
async def skip_edit_channel(message: Message, state: FSMContext):
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    new_channel_id = orig_post.get("channel_id")
    new_chat_id = orig_post.get("chat_id")
    new_channel_name = None
    channels_list = supabase_db.db.list_channels(project_id=data.get("user_settings", {}).get("current_project"))
    for ch in channels_list:
        if ch.get("id") == new_channel_id or ch.get("chat_id") == new_chat_id:
            new_channel_name = ch.get("name") or str(ch.get("chat_id"))
            break
    await state.update_data(new_channel_db_id=new_channel_id, new_channel_chat_id=new_chat_id, new_channel_name=new_channel_name)
    await show_edit_preview(message, state)

@edit_router.message(EditPost.channel)
async def choose_edit_channel(message: Message, state: FSMContext):
    data = await state.get_data()
    channels_list = data.get("_chan_map", [])
    raw = (message.text or "").strip()
    chosen = None
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(channels_list):
            chosen = channels_list[idx-1]
    else:
        for ch in channels_list:
            if str(ch['chat_id']) == raw or (ch.get('name') and ('@' + ch['name']) == raw):
                chosen = ch
                break
    if not chosen:
        lang = data.get("user_settings", {}).get("language", "ru")
        await message.answer(TEXTS[lang].get('edit_channel_error', TEXTS[lang]['edit_post_not_found']))
        return
    await state.update_data(new_channel_db_id=chosen.get('id'), new_channel_chat_id=chosen.get('chat_id'), new_channel_name=chosen.get('name') or str(chosen.get('chat_id')))
    await show_edit_preview(message, state)

async def show_edit_preview(message: Message, state: FSMContext):
    data = await state.get_data()
    orig_post = data.get('orig_post', {})
    user = data.get('user_settings', {}) or {}
    lang = user.get('language', 'ru')
    text = data.get('new_text', orig_post.get('text', '')) or ''
    media_id = data.get('new_media_id', orig_post.get('media_id'))
    media_type = data.get('new_media_type', orig_post.get('media_type'))
    fmt = data.get('new_format', orig_post.get('format') or 'none')
    buttons = data.get('new_buttons', orig_post.get('buttons') or [])
    btn_list = []
    if isinstance(buttons, str):
        try:
            btn_list = json.loads(buttons) if buttons else []
        except Exception:
            btn_list = []
    elif isinstance(buttons, list):
        btn_list = buttons
    markup = None
    if btn_list:
        kb = []
        for btn in btn_list:
            if isinstance(btn, dict):
                btn_text = btn.get('text'); btn_url = btn.get('url')
            elif isinstance(btn, (list, tuple)) and len(btn) >= 2:
                btn_text, btn_url = btn[0], btn[1]
            else:
                continue
            if btn_text and btn_url:
                kb.append([InlineKeyboardButton(text=btn_text, url=btn_url)])
        if kb:
            markup = InlineKeyboardMarkup(inline_keyboard=kb)
    parse_mode = None
    if fmt and fmt.lower() == "markdown":
        parse_mode = "Markdown"
    elif fmt and fmt.lower() == "html":
        parse_mode = "HTML"
    try:
        if media_id and media_type:
            if media_type.lower() == "photo":
                await message.answer_photo(media_id, caption=text or TEXTS[lang]['no_text'], parse_mode=parse_mode, reply_markup=markup)
            elif media_type.lower() == "video":
                await message.answer_video(media_id, caption=text or TEXTS[lang]['no_text'], parse_mode=parse_mode, reply_markup=markup)
            else:
                await message.answer(text or TEXTS[lang]['no_text'], parse_mode=parse_mode, reply_markup=markup)
        else:
            await message.answer(text or TEXTS[lang]['no_text'], parse_mode=parse_mode, reply_markup=markup)
    except Exception as e:
        await message.answer(f"Предпросмотр сообщения недоступен: {e}" if lang == "ru" else f"Preview unavailable: {e}")
    confirm_text = ("Подтвердите изменение поста через кнопки ниже." if lang == "ru" else "Please confirm or cancel the changes using the buttons below.")
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=TEXTS[lang]['yes_btn'], callback_data="confirm_edit"),
            InlineKeyboardButton(text=TEXTS[lang]['no_btn'], callback_data="cancel_edit")
        ]
    ])
    await message.answer(confirm_text, reply_markup=confirm_kb)
    await state.set_state(EditPost.confirm)

@edit_router.callback_query(F.data == "confirm_edit")
async def on_confirm_edit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    orig_post = data.get("orig_post", {})
    post_id = orig_post.get("id")
    latest = supabase_db.db.get_post(post_id)
    user = data.get("user_settings", {})
    lang = user.get("language", "ru") if user else "ru"
    if not latest or latest.get("published"):
        await callback.message.edit_text(TEXTS[lang]['edit_post_published'])
        await state.clear()
        await callback.answer()
        return
    updates = {}
    if "new_text" in data:
        updates["text"] = data["new_text"]
    if "new_media_id" in data:
        updates["media_id"] = data["new_media_id"]
        updates["media_type"] = data.get("new_media_type")
    if "new_format" in data:
        updates["format"] = data["new_format"]
    if "new_buttons" in data:
        updates["buttons"] = data["new_buttons"]
    if "new_publish_time" in data:
        pub_time = data["new_publish_time"]
        if isinstance(pub_time, datetime):
            updates["publish_time"] = pub_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            updates["publish_time"] = pub_time
        if pub_time is None:
            updates["draft"] = True
            updates["published"] = False
            updates["repeat_interval"] = 0
        else:
            updates["draft"] = False
    if "new_repeat_interval" in data:
        updates["repeat_interval"] = data["new_repeat_interval"]
    if "new_channel_db_id" in data:
        updates["channel_id"] = data["new_channel_db_id"]
        updates["chat_id"] = data.get("new_channel_chat_id")
    supabase_db.db.update_post(post_id, updates)
    await callback.message.edit_text(TEXTS[lang]['confirm_changes_saved'].format(id=post_id))
    await state.clear()
    await callback.answer()

@edit_router.callback_query(F.data == "cancel_edit")
async def on_cancel_edit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user = data.get("user_settings", {})
    lang = user.get("language", "ru") if user else "ru"
    await callback.message.edit_text(TEXTS[lang]['edit_cancelled'])
    await state.clear()
    await callback.answer()

# List posts and related commands
list_router = Router()
@list_router.message(Command("list"))
async def cmd_list(message: Message):
    user_id = message.from_user.id
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    project_id = user.get("current_project") if user else None
    if not project_id:
        await message.answer(TEXTS[lang]['no_posts'])
        return
    posts = supabase_db.db.list_posts(project_id=project_id, only_pending=True)
    if not posts:
        await message.answer(TEXTS[lang]['no_posts'])
    else:
        await message.answer(TEXTS[lang]['scheduled_posts_title'])
        for post in posts:
            pid = post.get("id")
            chan_name = ""
            chan_id = post.get("channel_id"); chat_id = post.get("chat_id")
            channel = None
            if chan_id:
                channel = supabase_db.db.get_channel(chan_id)
            if not channel and chat_id:
                channel = supabase_db.db.get_channel_by_chat_id(chat_id)
            if channel:
                chan_name = channel.get("name") or str(channel.get("chat_id"))
            else:
                chan_name = str(chat_id) if chat_id else ""
            if post.get("draft"):
                time_str = "(черновик)" if lang == "ru" else "(draft)"
            else:
                pub_time = post.get("publish_time")
                time_str = str(pub_time)
                try:
                    pub_dt = None
                    if isinstance(pub_time, str):
                        try:
                            pub_dt = datetime.fromisoformat(pub_time)
                        except:
                            pub_dt = datetime.strptime(pub_time, "%Y-%m-%dT%H:%M:%S")
                        pub_dt = pub_dt.replace(tzinfo=ZoneInfo("UTC"))
                    elif isinstance(pub_time, datetime):
                        pub_dt = pub_time
                    tz_name = user.get("timezone", "UTC") if user else "UTC"
                    tz = ZoneInfo(tz_name)
                    pub_local = pub_dt.astimezone(tz) if pub_dt else None
                    if pub_local:
                        date_fmt = user.get("date_format", "YYYY-MM-DD") if user else "YYYY-MM-DD"
                        time_fmt = user.get("time_format", "HH:mm") if user else "HH:mm"
                        fmt = date_fmt.replace("YYYY", "%Y").replace("YY", "%y")
                        fmt = fmt.replace("MM", "%m").replace("DD", "%d") + " " + time_fmt.replace("HH", "%H").replace("H", "%H").replace("MM", "%M").replace("M", "%M")
                        time_str = pub_local.strftime(fmt)
                    else:
                        time_str = str(pub_time)
                except Exception:
                    time_str = str(pub_time)
            repeat_flag = ""
            if post.get("repeat_interval") and post["repeat_interval"] > 0:
                repeat_flag = " 🔁"
            full_text = (post.get("text") or "").replace("\n", " ")
            preview = full_text[:30]
            if len(full_text) > 30:
                preview += "..."
            line = f"ID {pid}: {chan_name} | {time_str}{repeat_flag} | {preview}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="👁️ View", callback_data=f"view_post:{pid}"),
                    InlineKeyboardButton(text="✏️ Edit", callback_data=f"edit_post:{pid}"),
                    InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_post:{pid}")
                ]
            ])
            await message.answer(line, reply_markup=kb)

@list_router.message(Command("view"))
async def cmd_view(message: Message):
    user_id = message.from_user.id
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(TEXTS[lang]['view_usage'])
        return
    try:
        post_id = int(args[1])
    except:
        await message.answer(TEXTS[lang]['view_invalid_id'])
        return
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await message.answer(TEXTS[lang]['view_not_found'])
        return
    text = post.get("text") or TEXTS[lang]['no_text']
    media_id = post.get("media_id")
    media_type = post.get("media_type")
    fmt = post.get("format") or "none"
    buttons = post.get("buttons") or []
    parse_mode = None
    if fmt.lower() == "markdown":
        parse_mode = "Markdown"
    elif fmt.lower() == "html":
        parse_mode = "HTML"
    btn_list = []
    if isinstance(buttons, str):
        try:
            btn_list = json.loads(buttons) if buttons else []
        except:
            btn_list = []
    elif isinstance(buttons, list):
        btn_list = buttons
    markup = None
    if btn_list:
        kb = []
        for btn in btn_list:
            if isinstance(btn, dict):
                btn_text = btn.get('text'); btn_url = btn.get('url')
            elif isinstance(btn, (list, tuple)) and len(btn) >= 2:
                btn_text, btn_url = btn[0], btn[1]
            else:
                continue
            if btn_text and btn_url:
                kb.append([InlineKeyboardButton(text=btn_text, url=btn_url)])
        if kb:
            markup = InlineKeyboardMarkup(inline_keyboard=kb)
    try:
        if media_id and media_type:
            if media_type.lower() == 'photo':
                await message.answer_photo(media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
            elif media_type.lower() == 'video':
                await message.answer_video(media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
            else:
                await message.answer(text, parse_mode=parse_mode, reply_markup=markup)
        else:
            await message.answer(text, parse_mode=parse_mode, reply_markup=markup)
    except Exception as e:
        err_msg = f"Не удалось показать пост: {e}" if lang == 'ru' else f"Failed to display post: {e}"
        await message.answer(err_msg)

@list_router.message(Command("reschedule"))
async def cmd_reschedule(message: Message):
    user_id = message.from_user.id
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer(TEXTS[lang]['reschedule_usage'])
        return
    try:
        post_id = int(args[1])
    except:
        await message.answer(TEXTS[lang]['reschedule_invalid_id'])
        return
    time_str = args[2]
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await message.answer(TEXTS[lang]['reschedule_not_found'])
        return
    if post.get("published"):
        await message.answer(TEXTS[lang]['reschedule_post_published'])
        return
    user_settings = supabase_db.db.get_user(user_id) or {}
    try:
        new_dt = parse_time(user_settings, time_str)
    except Exception:
        await message.answer(TEXTS[lang]['create_time_error'].format(example=format_example(user_settings)))
        return
    now = datetime.now(ZoneInfo("UTC"))
    if new_dt <= now:
        await message.answer(TEXTS[lang]['time_past_error'])
        return
    updates = {}
    updates["publish_time"] = new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    updates["published"] = False
    updates["repeat_interval"] = post.get("repeat_interval", 0)
    updates["notified"] = False
    supabase_db.db.update_post(post_id, updates)
    await message.answer(TEXTS[lang]['reschedule_success'].format(id=post_id))

# Callback handlers for interactive post actions
@list_router.callback_query(lambda c: c.data and c.data.startswith("view_post:"))
async def on_view_post(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        post_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await callback.answer(TEXTS[lang]['view_not_found'], show_alert=True)
        return
    text = post.get("text") or TEXTS[lang]['no_text']
    media_id = post.get("media_id")
    media_type = post.get("media_type")
    fmt = post.get("format") or "none"
    buttons = post.get("buttons") or []
    parse_mode = None
    if fmt.lower() == "markdown":
        parse_mode = "Markdown"
    elif fmt.lower() == "html":
        parse_mode = "HTML"
    btn_list = []
    if isinstance(buttons, str):
        try:
            btn_list = json.loads(buttons) if buttons else []
        except:
            btn_list = []
    elif isinstance(buttons, list):
        btn_list = buttons
    markup = None
    if btn_list:
        kb = []
        for btn in btn_list:
            if isinstance(btn, dict):
                btn_text = btn.get('text'); btn_url = btn.get('url')
            elif isinstance(btn, (list, tuple)) and len(btn) >= 2:
                btn_text, btn_url = btn[0], btn[1]
            else:
                continue
            if btn_text and btn_url:
                kb.append([InlineKeyboardButton(text=btn_text, url=btn_url)])
        if kb:
            markup = InlineKeyboardMarkup(inline_keyboard=kb)
    try:
        if media_id and media_type:
            if media_type.lower() == 'photo':
                await callback.message.answer_photo(media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
            elif media_type.lower() == 'video':
                await callback.message.answer_video(media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
            else:
                await callback.message.answer(text, parse_mode=parse_mode, reply_markup=markup)
        else:
            await callback.message.answer(text, parse_mode=parse_mode, reply_markup=markup)
    except Exception as e:
        err = f"Failed to display post: {e}" if lang != 'ru' else f"Не удалось показать пост: {e}"
        await callback.message.answer(err)
    await callback.answer()

@list_router.callback_query(lambda c: c.data and c.data.startswith("edit_post:"))
async def on_edit_post(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        post_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await callback.answer(TEXTS[lang]['edit_post_not_found'], show_alert=True)
        return
    if post.get("published"):
        await callback.answer(TEXTS[lang]['edit_post_published'], show_alert=True)
        return
    await state.update_data(orig_post=post, user_settings=(user or supabase_db.db.ensure_user(user_id, default_lang=lang)))
    await state.set_state(EditPost.text)
    current_text = post.get("text") or ""
    await callback.message.answer(TEXTS[lang]['edit_begin'].format(id=post_id, text=current_text))
    await callback.answer()

@list_router.callback_query(lambda c: c.data and c.data.startswith("delete_post:"))
async def on_delete_post(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        post_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    post = supabase_db.db.get_post(post_id)
    if not post or not supabase_db.db.is_user_in_project(user_id, post.get("project_id", -1)):
        await callback.answer(TEXTS[lang]['delete_not_found'], show_alert=True)
        return
    if post.get("published"):
        await callback.answer(TEXTS[lang]['delete_already_published'], show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=TEXTS[lang]['yes_btn'], callback_data=f"confirm_delete_post:{post_id}"),
         InlineKeyboardButton(text=TEXTS[lang]['no_btn'], callback_data=f"cancel_delete_post:{post_id}")]
    ])
    await callback.message.edit_text(TEXTS[lang]['delete_confirm'].format(id=post_id), reply_markup=kb)
    await callback.answer()

@list_router.callback_query(lambda c: c.data and c.data.startswith("confirm_delete_post:"))
async def on_confirm_delete_post(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        post_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    post = supabase_db.db.get_post(post_id)
    if not post:
        try:
            await callback.message.edit_text(TEXTS[lang]['delete_not_found'])
        except:
            pass
        await callback.answer()
        return
    if post.get("published"):
        try:
            await callback.message.edit_text(TEXTS[lang]['delete_already_published'])
        except:
            pass
        await callback.answer()
        return
    supabase_db.db.delete_post(post_id)
    try:
        await callback.message.edit_text(TEXTS[lang]['delete_success'].format(id=post_id))
    except:
        await callback.answer(TEXTS[lang]['delete_success'].format(id=post_id), show_alert=True)
    await callback.answer()

@list_router.callback_query(lambda c: c.data and c.data.startswith("cancel_delete_post:"))
async def on_cancel_delete_post(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        post_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    post = supabase_db.db.get_post(post_id)
    if post:
        chan_name = ""
        chan_id = post.get("channel_id"); chat_id = post.get("chat_id")
        channel = supabase_db.db.get_channel(chan_id) if post.get("channel_id") else None
        if not channel and chat_id:
            channel = supabase_db.db.get_channel_by_chat_id(chat_id)
        if channel:
            chan_name = channel.get("name") or str(channel.get("chat_id"))
        else:
            chan_name = str(chat_id) if chat_id else ""
        if post.get("draft"):
            time_str = "(черновик)" if lang == "ru" else "(draft)"
        else:
            pub_time = post.get("publish_time")
            time_str = str(pub_time)
            try:
                pub_dt = None
                if isinstance(pub_time, str):
                    try:
                        pub_dt = datetime.fromisoformat(pub_time)
                    except:
                        pub_dt = datetime.strptime(pub_time, "%Y-%m-%dT%H:%M:%S")
                        pub_dt = pub_dt.replace(tzinfo=ZoneInfo("UTC"))
                elif isinstance(pub_time, datetime):
                    pub_dt = pub_time
                tz_name = user.get("timezone", "UTC") if user else "UTC"
                tz = ZoneInfo(tz_name)
                pub_local = pub_dt.astimezone(tz) if pub_dt else None
                if pub_local:
                    date_fmt = user.get("date_format", "YYYY-MM-DD") if user else "YYYY-MM-DD"
                    time_fmt = user.get("time_format", "HH:mm") if user else "HH:mm"
                    fmt = date_fmt.replace("YYYY", "%Y").replace("YY", "%y")
                    fmt = fmt.replace("MM", "%m").replace("DD", "%d") + " " + time_fmt.replace("HH", "%H").replace("H", "%H").replace("MM", "%M").replace("M", "%M")
                    time_str = pub_local.strftime(fmt)
                else:
                    time_str = str(pub_time)
            except:
                time_str = str(pub_time)
        repeat_flag = ""
        if post.get("repeat_interval") and post["repeat_interval"] > 0:
            repeat_flag = " 🔁"
        full_text = (post.get("text") or "").replace("\n", " ")
        preview = full_text[:30]
        if len(full_text) > 30:
            preview += "..."
        line = f"ID {post_id}: {chan_name} | {time_str}{repeat_flag} | {preview}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👁️ View", callback_data=f"view_post:{post_id}"),
                InlineKeyboardButton(text="✏️ Edit", callback_data=f"edit_post:{post_id}"),
                InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_post:{post_id}")
            ]
        ])
        await callback.message.edit_text(line, reply_markup=kb)
    else:
        await callback.message.edit_text(TEXTS[lang]['confirm_post_cancel'])
    await callback.answer(TEXTS[lang]['confirm_post_cancel'])

# Settings command handler
settings_router = Router()
@settings_router.message(Command("settings"))
async def cmd_settings(message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=2)
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    if len(args) == 1:
        if not user:
            user = supabase_db.db.ensure_user(user_id)
            lang = user.get("language", lang)
        tz = user.get("timezone", "UTC")
        lang_name = TEXTS[lang]['lang_ru'] if user.get("language") == "ru" else TEXTS[lang]['lang_en']
        date_fmt = user.get("date_format", "YYYY-MM-DD")
        time_fmt = user.get("time_format", "HH:mm")
        notify_val = user.get("notify_before", 0)
        notify_str = (str(notify_val) + (" мин." if lang == "ru" else " min")) if notify_val else (("выкл." if lang == "ru" else "off"))
        msg = (
            f"Ваши настройки:\n"
            f"Часовой пояс: {tz}\n"
            f"Язык: {lang_name}\n"
            f"Формат даты: {date_fmt}\n"
            f"Формат времени: {time_fmt}\n"
            f"Уведомления: {notify_str}\n\n"
            "/settings tz Europe/Moscow — установить часовой пояс\n"
            "/settings lang ru — язык интерфейса (ru, en)\n"
            "/settings datefmt YYYY-MM-DD — формат даты (например: YYYY-MM-DD)\n"
            "/settings timefmt HH:MM — формат времени (например: HH:MM)\n"
            "/settings notify 10 — напоминание за N минут до публикации (0 — выкл.)"
        ) if lang == "ru" else (
            f"Your settings:\n"
            f"Timezone: {tz}\n"
            f"Language: {lang_name}\n"
            f"Date format: {date_fmt}\n"
            f"Time format: {time_fmt}\n"
            f"Notifications: {notify_str}\n\n"
            "/settings tz Europe/Moscow — set timezone\n"
            "/settings lang en — interface language (ru, en)\n"
            "/settings datefmt YYYY-MM-DD — date format (e.g., YYYY-MM-DD)\n"
            "/settings timefmt HH:MM — time format (e.g., HH:MM)\n"
            "/settings notify 10 — reminder N minutes before posting (0 to disable)"
        )
        await message.answer(msg)
        return
    sub = args[1].lower()
    if sub in ("tz", "timezone", "часовой", "часовой_пояс"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['settings_timezone_usage'])
            return
        tz_name = args[2]
        try:
            ZoneInfo(tz_name)
        except:
            await message.answer(TEXTS[lang]['settings_invalid_tz'])
            return
        supabase_db.db.update_user(user_id, {"timezone": tz_name})
        await message.answer(TEXTS[lang]['settings_tz_set'].format(tz=tz_name))
    elif sub in ("lang", "language", "язык"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['settings_language_usage'])
            return
        val = args[2].lower()
        if val in ("ru", "русский", "rus"):
            new_lang = "ru"
        elif val in ("en", "eng", "english", "английский"):
            new_lang = "en"
        else:
            await message.answer(TEXTS[lang]['settings_invalid_lang'])
            return
        supabase_db.db.update_user(user_id, {"language": new_lang})
        lang_name = "Русский" if new_lang == "ru" else "English"
        await message.answer(TEXTS[new_lang]['settings_lang_set'].format(lang_name=lang_name))
    elif sub in ("datefmt", "date_format", "формат_даты"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['settings_datefmt_usage'])
            return
        fmt = args[2].upper()
        if not ("Y" in fmt and "M" in fmt and "D" in fmt):
            await message.answer(TEXTS[lang]['settings_invalid_datefmt'])
            return
        supabase_db.db.update_user(user_id, {"date_format": fmt})
        await message.answer(TEXTS[lang]['settings_datefmt_set'].format(fmt=fmt))
    elif sub in ("timefmt", "time_format", "формат_времени"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['settings_timefmt_usage'])
            return
        fmt = args[2]
        if "H" not in fmt.upper() or "M" not in fmt.upper():
            await message.answer(TEXTS[lang]['settings_invalid_timefmt'])
            return
        supabase_db.db.update_user(user_id, {"time_format": fmt})
        await message.answer(TEXTS[lang]['settings_timefmt_set'].format(fmt=fmt))
    elif sub in ("notify", "notifications", "уведомления"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['settings_notify_usage'])
            return
        val = args[2].lower()
        if val in ("0", "off", "нет", "none"):
            minutes = 0
        else:
            try:
                minutes = int(val)
            except:
                minutes = None
        if minutes is None or minutes < 0:
            await message.answer(TEXTS[lang]['settings_invalid_notify'])
            return
        supabase_db.db.update_user(user_id, {"notify_before": minutes})
        msg = TEXTS[lang]['settings_notify_set'].format(minutes_str=("выключены" if lang == "ru" else "disabled") if minutes == 0 else (str(minutes) + (" мин." if lang == "ru" else " min")))
        await message.answer(msg)
    else:
        await message.answer(TEXTS[lang]['settings_unknown'])

# Projects command handler
projects_router = Router()
@projects_router.message(Command(commands=["project", "projects"]))
async def cmd_project(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=2)
    lang = "ru"
    user = supabase_db.db.get_user(user_id)
    if user:
        lang = user.get("language", "ru")
    if len(args) == 1:
        if not user:
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        projects = supabase_db.db.list_projects(user_id)
        if not projects:
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        lines = [TEXTS[lang]['projects_list_title']]
        current_proj = user.get("current_project")
        for proj in projects:
            name = proj.get("name", "Unnamed")
            if current_proj and proj["id"] == current_proj:
                lines.append(TEXTS[lang]['projects_item_current'].format(name=name))
            else:
                lines.append(TEXTS[lang]['projects_item'].format(name=name))
        buttons = []
        for proj in projects:
            name = proj.get("name", "Unnamed")
            buttons.append([InlineKeyboardButton(text=name + (" ✅" if current_proj and proj["id"] == current_proj else ""), callback_data=f"proj_switch:{proj['id']}")])
        buttons.append([InlineKeyboardButton(text="➕ Новый проект" if lang == "ru" else "➕ New Project", callback_data="proj_new")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer("\n".join(lines), reply_markup=kb)
        return
    sub = args[1].lower()
    if sub in ("new", "create"):
        if len(args) < 3:
            await message.answer(TEXTS[lang]['projects_invite_usage'] if sub == "invite" else ("Введите название проекта:" if lang == "ru" else "Please provide a project name."))
            return
        proj_name = args[2].strip()
        if not proj_name:
            await message.answer(("Название проекта не может быть пустым." if lang == "ru" else "Project name cannot be empty."))
            return
        project = supabase_db.db.create_project(user_id, proj_name)
        if not project:
            await message.answer(("Ошибка: не удалось создать проект." if lang == "ru" else "Error: Failed to create project."))
            return
        supabase_db.db.update_user(user_id, {"current_project": project["id"]})
        await message.answer(TEXTS[lang]['projects_created'].format(name=proj_name))
    elif sub == "switch":
        if len(args) < 3:
            await message.answer(("Использование:\n/project switch <project_id>" if lang == "ru" else "Usage:\n/project switch <project_id>"))
            return
        try:
            pid = int(args[2])
        except:
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        if not supabase_db.db.is_user_in_project(user_id, pid):
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        project = supabase_db.db.get_project(pid)
        if not project:
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        supabase_db.db.update_user(user_id, {"current_project": pid})
        await message.answer(TEXTS[lang]['projects_switched'].format(name=project.get("name", "")))
    elif sub == "invite":
        if len(args) < 3:
            await message.answer(TEXTS[lang]['projects_invite_usage'])
            return
        target = args[2].strip()
        try:
            invitee_id = int(target)
        except:
            invitee_id = None
        if not invitee_id:
            await message.answer(TEXTS[lang]['projects_invite_usage'])
            return
        if not user or not user.get("current_project"):
            await message.answer(TEXTS[lang]['projects_not_found'])
            return
        proj_id = user["current_project"]
        invitee_user = supabase_db.db.get_user(invitee_id)
        if not invitee_user:
            await message.answer(TEXTS[lang]['projects_invite_not_found'])
            return
        added = supabase_db.db.add_user_to_project(invitee_id, proj_id, role="admin")
        if not added:
            await message.answer(("Пользователь уже в проекте." if lang == "ru" else "User is already a member of the project."))
            return
        await message.answer(TEXTS[lang]['projects_invite_success'].format(user_id=invitee_id))
        proj = supabase_db.db.get_project(proj_id)
        inviter_name = message.from_user.full_name or f"user {user_id}"
        invitee_lang = invitee_user.get("language", "ru")
        notify_text = TEXTS[invitee_lang]['projects_invited_notify'].format(project=proj.get("name", ""), user=inviter_name)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Переключиться" if invitee_lang == "ru" else "🔄 Switch to project", callback_data=f"proj_switch:{proj_id}")]
        ])
        try:
            await bot.send_message(invitee_id, notify_text, reply_markup=kb)
        except Exception:
            pass
    else:
        await message.answer(TEXTS[lang]['projects_not_found'])

@projects_router.callback_query(lambda c: c.data and c.data.startswith("proj_switch:"))
async def on_switch_project(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        proj_id = int(callback.data.split(":", 1)[1])
    except:
        await callback.answer()
        return
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    if not supabase_db.db.is_user_in_project(user_id, proj_id):
        await callback.answer(TEXTS[lang]['projects_not_found'], show_alert=True)
        return
    project = supabase_db.db.get_project(proj_id)
    if not project:
        await callback.answer(TEXTS[lang]['projects_not_found'], show_alert=True)
        return
    supabase_db.db.update_user(user_id, {"current_project": proj_id})
    try:
        await callback.message.edit_text(TEXTS[lang]['projects_switched'].format(name=project.get("name", "")))
    except:
        await callback.answer(TEXTS[lang]['projects_switched'].format(name=project.get("name", "")), show_alert=True)
    await callback.answer()

@projects_router.callback_query(F.data == "proj_new")
async def on_new_project(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    await callback.message.answer(("Введите название нового проекта:" if lang == "ru" else "Please send the new project name:"))
    await state.set_state(NewProject.name)
    await callback.answer()

@projects_router.message(NewProject.name)
async def create_new_project_name(message: Message, state: FSMContext):
    user_id = message.from_user.id
    project_name = message.text.strip()
    user = supabase_db.db.get_user(user_id)
    lang = user.get("language", "ru") if user else "ru"
    if not project_name:
        await message.answer(("Название проекта не может быть пустым." if lang == "ru" else "Project name cannot be empty."))
        return
    project = supabase_db.db.create_project(user_id, project_name)
    if not project:
        await message.answer(("Ошибка при создании проекта." if lang == "ru" else "Failed to create project."))
        await state.clear()
        return
    supabase_db.db.update_user(user_id, {"current_project": project["id"]})
    await message.answer(TEXTS[lang]['projects_created'].format(name=project_name))
    await state.clear()

# Include routers
dp.include_router(start_router)
dp.include_router(help_router)
dp.include_router(channels_router)
dp.include_router(create_router)
dp.include_router(edit_router)
dp.include_router(list_router)
dp.include_router(delete_router)
dp.include_router(settings_router)
dp.include_router(projects_router)

# Scheduler for auto-posting
async def start_scheduler(bot: Bot, check_interval: int = 5):
    while True:
        now_utc = datetime.now(timezone.utc)
        due_posts = supabase_db.db.get_due_posts(now_utc)
        for post in due_posts:
            post_id = post.get("id")
            user_id = post.get("user_id")
            chat_id = None
            if post.get("chat_id"):
                chat_id = post.get("chat_id")
            else:
                chan_id = post.get("channel_id")
                if chan_id:
                    channel = supabase_db.db.get_channel(chan_id)
                    if channel:
                        chat_id = channel.get("chat_id")
            if not chat_id:
                supabase_db.db.mark_post_published(post_id)
                continue
            text = post.get("text") or ""
            media_id = post.get("media_id")
            media_type = post.get("media_type")
            fmt = post.get("format") or ""
            buttons = []
            markup = None
            if post.get("buttons"):
                try:
                    buttons = json.loads(post.get("buttons")) if isinstance(post.get("buttons"), str) else post.get("buttons")
                except Exception:
                    buttons = post.get("buttons") or []
            if buttons:
                kb = []
                for btn in buttons:
                    if isinstance(btn, dict):
                        btn_text = btn.get("text"); btn_url = btn.get("url")
                    elif isinstance(btn, (list, tuple)) and len(btn) >= 2:
                        btn_text, btn_url = btn[0], btn[1]
                    else:
                        continue
                    if btn_text and btn_url:
                        kb.append([InlineKeyboardButton(text=btn_text, url=btn_url)])
                if kb:
                    markup = InlineKeyboardMarkup(inline_keyboard=kb)
            parse_mode = None
            if fmt.lower() == "markdown":
                parse_mode = "Markdown"
            elif fmt.lower() == "html":
                parse_mode = "HTML"
            try:
                if media_id and media_type:
                    if media_type.lower() == "photo":
                        await bot.send_photo(chat_id, photo=media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
                    elif media_type.lower() == "video":
                        await bot.send_video(chat_id, video=media_id, caption=text, parse_mode=parse_mode, reply_markup=markup)
                    else:
                        await bot.send_message(chat_id, text or TEXTS['en']['no_text'], parse_mode=parse_mode, reply_markup=markup)
                else:
                    await bot.send_message(chat_id, text or TEXTS['en']['no_text'], parse_mode=parse_mode, reply_markup=markup)
            except Exception as e:
                error_msg = str(e)
                if user_id:
                    chan_name = str(chat_id)
                    channel = supabase_db.db.get_channel_by_chat_id(chat_id)
                    if channel:
                        chan_name = channel.get("name") or str(chat_id)
                    lang = "ru"
                    user = supabase_db.db.get_user(user_id)
                    if user:
                        lang = user.get("language", "ru")
                    msg_text = TEXTS[lang]['error_post_failed'].format(id=post_id, channel=chan_name, error=error_msg)
                    try:
                        await bot.send_message(user_id, msg_text)
                    except Exception:
                        pass
                supabase_db.db.mark_post_published(post_id)
                continue
            repeat_int = post.get("repeat_interval") or 0
            if repeat_int > 0:
                try:
                    pub_time_str = post.get("publish_time")
                    if pub_time_str:
                        try:
                            current_dt = datetime.fromisoformat(pub_time_str)
                        except Exception:
                            current_dt = datetime.strptime(pub_time_str, "%Y-%m-%dT%H:%M:%S")
                    else:
                        current_dt = now_utc
                    next_time = current_dt + timedelta(seconds=repeat_int)
                    supabase_db.db.update_post(post_id, {"publish_time": next_time.strftime("%Y-%m-%dT%H:%M:%S%z"), "published": False, "notified": False})
                    continue
                except Exception as e:
                    print(f"Failed to schedule next repeat for post {post_id}: {e}")
            supabase_db.db.mark_post_published(post_id)
        upcoming_posts = supabase_db.db.list_posts(only_pending=True)
        for post in upcoming_posts:
            if post.get("published") or post.get("draft"):
                continue
            user_id = post.get("user_id")
            if not user_id:
                continue
            user = supabase_db.db.get_user(user_id)
            if not user:
                continue
            notify_before = user.get("notify_before", 0)
            if notify_before and notify_before > 0:
                try:
                    pub_time_str = post.get("publish_time")
                    if not pub_time_str:
                        continue
                    try:
                        pub_dt = datetime.fromisoformat(pub_time_str)
                    except Exception:
                        pub_dt = datetime.strptime(pub_time_str, "%Y-%m-%dT%H:%M:%S")
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    threshold = pub_dt - timedelta(minutes=notify_before)
                    if threshold <= now < pub_dt and not post.get("notified"):
                        lang = user.get("language", "ru")
                        chan_name = ""
                        chan_id = post.get("channel_id"); chat_id = post.get("chat_id")
                        channel = None
                        if chan_id:
                            channel = supabase_db.db.get_channel(chan_id)
                        if not channel and chat_id:
                            channel = supabase_db.db.get_channel_by_chat_id(chat_id)
                        if channel:
                            chan_name = channel.get("name") or str(channel.get("chat_id"))
                        else:
                            chan_name = str(chat_id) if chat_id else ""
                        minutes_left = int((pub_dt - now).total_seconds() // 60)
                        if minutes_left < 1:
                            notify_text = TEXTS[lang]['notify_message_less_min'].format(id=post.get('id'), channel=chan_name)
                        else:
                            notify_text = TEXTS[lang]['notify_message'].format(id=post.get('id'), channel=chan_name, minutes=minutes_left)
                        try:
                            await bot.send_message(user_id, notify_text)
                            supabase_db.db.update_post(post.get('id'), {"notified": True})
                        except Exception as e:
                            print(f"Failed to send notification to user {user_id}: {e}")
                except Exception as e:
                    print(f"Notification check failed for post {post.get('id')}: {e}")
        await asyncio.sleep(check_interval)

async def main():
    # Start scheduler for auto-posting and notifications
    asyncio.create_task(start_scheduler(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
