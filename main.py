import os
import json
import re
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging # Import logging module

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from supabase import create_client, Client
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# CONFIG & INIT
load_dotenv() # подтягиваем .env

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all((BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY)):
    logging.error("❌ BOT_TOKEN / SUPABASE_URL / SUPABASE_KEY – проверь .env")
    raise SystemExit("❌ BOT_TOKEN / SUPABASE_URL / SUPABASE_KEY – проверь .env")

bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(storage=MemoryStorage())

# SUPABASE THIN WRAPPER
class SupabaseDB:
    """Простейший слой вокруг Supabase/PostgREST со схемой, нужной боту."""
    def __init__(self, url: str, key: str): # Added __init__ method
        self.client: Client = create_client(url, key)
        logging.info("Supabase client initialized.")

    # ---------- USERS ----------
    def get_user(self, user_id: int):
        try:
            res = self.client.table("users").select("*").eq("user_id", user_id).execute()
            return (res.data or [None])[0]
        except Exception as e:
            logging.error(f"Error getting user {user_id}: {e}")
            return None

    def ensure_user(self, user_id: int, default_lang="ru"):
        try:
            user = self.get_user(user_id)
            if user:
                return user
            tpl = dict(user_id=user_id, language=default_lang, timezone="UTC", date_format="YYYY-MM-DD", time_format="HH:mm", notify_before=0)
            res = self.client.table("users").insert(tpl).execute()
            logging.info(f"User {user_id} ensured/created.")
            return res.data[0]
        except Exception as e:
            logging.error(f"Error ensuring user {user_id}: {e}")
            return None

    def update_user(self, user_id: int, fields: dict):
        if not fields:
            return
        try:
            self.client.table("users").update(fields).eq("user_id", user_id).execute()
            logging.info(f"User {user_id} updated with fields: {fields}")
        except Exception as e:
            logging.error(f"Error updating user {user_id} with fields {fields}: {e}")

    # ---------- PROJECTS ----------
    def create_project(self, owner_id: int, name: str):
        try:
            proj = self.client.table("projects").insert({"name": name, "owner_id": owner_id}).execute().data[0]
            self.client.table("user_projects").insert({"user_id": owner_id, "project_id": proj["id"], "role": "owner"}).execute()
            logging.info(f"Project '{name}' created by user {owner_id}.")
            return proj
        except Exception as e:
            logging.error(f"Error creating project '{name}' for owner {owner_id}: {e}")
            return None

    def list_projects(self, user_id: int):
        try:
            memberships = self.client.table("user_projects").select("project_id, role").eq("user_id", user_id).execute().data
            pids = [m["project_id"] for m in memberships]
            if not pids:
                return []
            projects_data = self.client.table("projects").select("*").in_("id", pids).execute().data
            
            project_roles = {m["project_id"]: m["role"] for m in memberships}
            for project in projects_data:
                project["role"] = project_roles.get(project["id"])
            return projects_data
        except Exception as e:
            logging.error(f"Error listing projects for user {user_id}: {e}")
            return []

    def is_member(self, user_id: int, project_id: int):
        try:
            return bool(self.client.table("user_projects").select("user_id")
                        .eq("user_id", user_id).eq("project_id", project_id).execute().data)
        except Exception as e:
            logging.error(f"Error checking membership for user {user_id} in project {project_id}: {e}")
            return False

    def get_user_project_role(self, user_id: int, project_id: int):
        try:
            res = self.client.table("user_projects").select("role").eq("user_id", user_id).eq("project_id", project_id).execute()
            return (res.data or [None])[0]["role"] if res.data else None
        except Exception as e:
            logging.error(f"Error getting role for user {user_id} in project {project_id}: {e}")
            return None

    def add_user_to_project(self, user_id: int, project_id: int, role: str):
        try:
            data = {"user_id": user_id, "project_id": project_id, "role": role}
            res = self.client.table("user_projects").upsert(data, on_conflict="user_id,project_id").execute().data[0]
            logging.info(f"User {user_id} added/updated in project {project_id} with role {role}.")
            return res
        except Exception as e:
            logging.error(f"Error adding user {user_id} to project {project_id} with role {role}: {e}")
            return None

    def remove_user_from_project(self, user_id: int, project_id: int):
        try:
            self.client.table("user_projects").delete().eq("user_id", user_id).eq("project_id", project_id).execute()
            logging.info(f"User {user_id} removed from project {project_id}.")
        except Exception as e:
            logging.error(f"Error removing user {user_id} from project {project_id}: {e}")

    def list_project_members(self, project_id: int):
        try:
            return self.client.table("user_projects").select("user_id, role").eq("project_id", project_id).execute().data
        except Exception as e:
            logging.error(f"Error listing members for project {project_id}: {e}")
            return []

    # ---------- CHANNELS ----------
    def add_channel(self, project_id: int, chat_id: int, title: str):
        try:
            data = {"project_id": project_id, "chat_id": chat_id, "name": title}
            res = self.client.table("channels").upsert(data, on_conflict="project_id,chat_id").execute().data[0]
            logging.info(f"Channel '{title}' ({chat_id}) added to project {project_id}.")
            return res
        except Exception as e:
            logging.error(f"Error adding channel '{title}' ({chat_id}) to project {project_id}: {e}")
            return None

    def list_channels(self, project_id: int):
        try:
            return self.client.table("channels").select("*").eq("project_id", project_id).execute().data
        except Exception as e:
            logging.error(f"Error listing channels for project {project_id}: {e}")
            return []

    def del_channel(self, chan_internal_id: int):
        try:
            self.client.table("channels").delete().eq("id", chan_internal_id).execute()
            logging.info(f"Channel {chan_internal_id} deleted.")
        except Exception as e:
            logging.error(f"Error deleting channel {chan_internal_id}: {e}")

    # ---------- POSTS ----------
    def new_post(self, fields: dict):
        try:
            if "buttons" in fields and isinstance(fields["buttons"], list):
                fields["buttons"] = json.dumps(fields["buttons"])
            res = self.client.table("posts").insert(fields).execute().data[0]
            logging.info(f"New post created: {res.get('id')}")
            return res
        except Exception as e:
            logging.error(f"Error creating new post with fields {fields}: {e}")
            return None

    def get_post(self, post_id: int):
        try:
            return (self.client.table("posts").select("*").eq("id", post_id).execute().data or [None])[0]
        except Exception as e:
            logging.error(f"Error getting post {post_id}: {e}")
            return None

    def update_post(self, post_id: int, fields: dict):
        try:
            if "buttons" in fields and isinstance(fields["buttons"], list):
                fields["buttons"] = json.dumps(fields["buttons"])
            self.client.table("posts").update(fields).eq("id", post_id).execute()
            logging.info(f"Post {post_id} updated with fields: {fields}")
        except Exception as e:
            logging.error(f"Error updating post {post_id} with fields {fields}: {e}")

    def delete_post(self, post_id: int):
        try:
            self.client.table("posts").delete().eq("id", post_id).execute()
            logging.info(f"Post {post_id} deleted.")
        except Exception as e:
            logging.error(f"Error deleting post {post_id}: {e}")

    def pending_posts(self, now_iso: str):
        try:
            return self.client.table("posts") \
                .select("*") \
                .eq("published", False) \
                .eq("draft", False) \
                .lte("publish_time", now_iso) \
                .execute().data
        except Exception as e:
            logging.error(f"Error getting pending posts for {now_iso}: {e}")
            return []

    def mark_published(self, post_id: int):
        try:
            self.update_post(post_id, {"published": True})
            logging.info(f"Post {post_id} marked as published.")
        except Exception as e:
            logging.error(f"Error marking post {post_id} as published: {e}")

db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY) # Instantiated db object correctly

# GLOBAL CONSTANTS / TEXTS
TEXTS = {
    "ru": {
        "start": "🤖 Бот готов. /help — список команд.",
        "help": ("/create — новый пост\n"
                 "/list — посты\n"
                 "/channels — каналы\n"
                 "/project — проекты\n"
                 "/settings — настройки\n"
                 "/cancel — отмена\n"
                 "/current_project — текущий проект\n"
                 "/project_select — выбрать проект\n"
                 "/project_add — создать проект\n"
                 "/manage_users — управление пользователями проекта\n"),
        "no_channels": "Сначала добавь канал через /channels add",
        "enter_post_text": "Введи текст поста:",
        "attach_media": "Прикрепить фото/видео? (пришли файл или /skip)",
        "enter_buttons": "Кнопки (каждая на новой строке: Текст | url). Если не нужно — /skip",
        "publish_time": "Когда публиковать? (пример: 2025-12-01 18:45)\nИли /skip для немедленно",
        "invalid_date_format": "❌ Формат даты неверный. Пример: 2025-12-01 18:45",
        "repeat_post": "Повторять пост? (0 — не повторять, иначе число минут)",
        "invalid_repeat_format": "❌ Введи число минут (0 — не повторять)",
        "no_projects": "Нет проектов. Сначала создай проект через /project_add",
        "pick_channel": "В какой канал публиковать?",
        "post_saved": "✅ Пост сохранён и будет опубликован по расписанию.",
        "post_cancelled": "❌ Отменено",
        "no_channels_to_delete": "Нет каналов для удаления.",
        "select_channel_to_delete": "Выбери канал для удаления:",
        "channel_deleted": "Канал удалён.",
        "enter_channel_info": "Введи @username или ID канала:",
        "channel_not_found": "❌ Канал не найден. Проверь, что бот админ.",
        "only_channels_supported": "❌ Только каналы/группы поддерживаются",
        "channel_added": "✅ Канал добавлен.",
        "your_channels": "Твои каналы:\n",
        "your_projects": "Твои проекты:\n",
        "enter_project_name": "Введи название нового проекта:",
        "project_created": "✅ Проект создан: ",
        "settings_menu": "Настройки:\nЯзык: {language}\nТаймзона: {timezone}\nФормат даты: {date_format}\nФормат времени: {time_format}",
        "select_language": "Выбери язык:",
        "language_updated": "Язык обновлен.",
        "post_not_found": "Пост не найден.",
        "no_posts_yet": "Постов пока нет.",
        "post_deleted": "Пост удалён.",
        "enter_new_post_text": "Введи новый текст поста:",
        "text_updated": "Текст обновлён.",
        "no_projects_found": "Нет проектов.",
        "no_channels_found": "Нет каналов.",
        "add_channel_first": "Сначала добавь канал через /channels add",
        "select_project": "Выбери проект:",
        "project_selected": "✅ Проект '{project_name}' выбран.",
        "project_not_found": "Проект не найден.",
        "current_project": "Текущий проект: {project_name}",
        "no_active_project": "Нет активного проекта. Выбери проект через /project_select",
        "access_denied": "У тебя нет доступа к этому проекту или команде.",
        "user_not_found": "Пользователь не найден.",
        "enter_user_id_or_username": "Введи ID пользователя или @username для добавления/изменения роли:",
        "select_role": "Выбери роль для пользователя {user_info}:",
        "role_updated": "Роль пользователя {user_info} обновлена на '{role}'.",
        "user_added_to_project": "Пользователь {user_info} добавлен в проект '{project_name}' с ролью '{role}'.",
        "user_already_member": "Пользователь {user_info} уже является участником проекта '{project_name}'.",
        "user_removed_from_project": "Пользователь {user_info} удален из проекта '{project_name}'.",
        "confirm_remove_user": "Ты уверен, что хочешь удалить пользователя {user_info} из проекта '{project_name}'?",
        "yes": "Да",
        "no": "Нет",
        "user_management_menu": "Управление пользователями проекта '{project_name}':",
        "add_user": "Добавить пользователя",
        "remove_user": "Удалить пользователя",
        "list_users": "Список пользователей",
        "manage_users": "Управление пользователями",
        "project_users": "Пользователи проекта '{project_name}':",
        "no_users_in_project": "В этом проекте нет других пользователей.",
        "select_user_to_remove": "Выбери пользователя для удаления:",
        "user_removed": "Пользователь удален.",
        "set_timezone": "Введи свою таймзону (например, Europe/Moscow или America/New_York):",
        "invalid_timezone": "❌ Неверный формат таймзоны. Попробуй еще раз (например, Europe/Moscow).",
        "timezone_updated": "Таймзона обновлена.",
        "set_datetime_format": "Введи формат даты (например, YYYY-MM-DD) и времени (например, HH:mm) через пробел:",
        "invalid_datetime_format": "❌ Неверный формат. Используй YYYY, MM, DD, HH, mm. Пример: YYYY-MM-DD HH:mm",
        "datetime_format_updated": "Формат даты/времени обновлен.",
        "error": "Ошибка",
        "not_owner": "Только владелец проекта может управлять пользователями.",
    },
    "en": {
        "start": "🤖 Bot is ready. /help for commands.",
        "help": ("/create — new post\n"
                 "/list — posts\n"
                 "/channels — channels\n"
                 "/project — projects\n"
                 "/settings — settings\n"
                 "/cancel — cancel\n"
                 "/current_project — current project\n"
                 "/project_select — select project\n"
                 "/project_add — create project\n"
                 "/manage_users — manage project users\n"),
        "no_channels": "Add a channel first via /channels add",
        "enter_post_text": "Enter post text:",
        "attach_media": "Attach photo/video? (send file or /skip)",
        "enter_buttons": "Buttons (each on a new line: Text | url). If not needed — /skip",
        "publish_time": "When to publish? (example: 2025-12-01 18:45)\nOr /skip for immediate",
        "invalid_date_format": "❌ Invalid date format. Example: 2025-12-01 18:45",
        "repeat_post": "Repeat post? (0 — no repeat, otherwise number of minutes)",
        "invalid_repeat_format": "❌ Enter number of minutes (0 — no repeat)",
        "no_projects": "No projects. Create a project first via /project_add",
        "pick_channel": "Which channel to publish to?",
        "post_saved": "✅ Post saved and will be published on schedule.",
        "post_cancelled": "❌ Cancelled",
        "no_channels_to_delete": "No channels to delete.",
        "select_channel_to_delete": "Select channel to delete:",
        "channel_deleted": "Channel deleted.",
        "enter_channel_info": "Enter @username or channel ID:",
        "channel_not_found": "❌ Channel not found. Make sure the bot is an admin.",
        "only_channels_supported": "❌ Only channels/groups are supported",
        "channel_added": "✅ Channel added.",
        "your_channels": "Your channels:\n",
        "your_projects": "Your projects:\n",
        "enter_project_name": "Enter new project name:",
        "project_created": "✅ Project created: ",
        "settings_menu": "Settings:\nLanguage: {language}\nTimezone: {timezone}\nDate format: {date_format}\nTime format: {time_format}",
        "select_language": "Select language:",
        "language_updated": "Language updated.",
        "post_not_found": "Post not found.",
        "no_posts_yet": "No posts yet.",
        "post_deleted": "Post deleted.",
        "enter_new_post_text": "Enter new post text:",
        "text_updated": "Text updated.",
        "no_projects_found": "No projects.",
        "no_channels_found": "No channels.",
        "add_channel_first": "Add a channel first via /channels add",
        "select_project": "Select project:",
        "project_selected": "✅ Project '{project_name}' selected.",
        "project_not_found": "Project not found.",
        "current_project": "Current project: {project_name}",
        "no_active_project": "No active project. Select a project via /project_select",
        "access_denied": "You do not have access to this project or command.",
        "user_not_found": "User not found.",
        "enter_user_id_or_username": "Enter user ID or @username to add/change role:",
        "select_role": "Select role for user {user_info}:",
        "role_updated": "User {user_info}'s role updated to '{role}'.",
        "user_added_to_project": "User {user_info} added to project '{project_name}' with role '{role}'.",
        "user_already_member": "User {user_info} is already a member of project '{project_name}'.",
        "user_removed_from_project": "User {user_info} removed from project '{project_name}'.",
        "confirm_remove_user": "Are you sure you want to remove user {user_info} from project '{project_name}'?",
        "yes": "Yes",
        "no": "No",
        "user_management_menu": "Project '{project_name}' user management:",
        "add_user": "Add user",
        "remove_user": "Remove user",
        "list_users": "List users",
        "manage_users": "Manage users",
        "project_users": "Users in project '{project_name}':",
        "no_users_in_project": "No other users in this project.",
        "select_user_to_remove": "Select user to remove:",
        "user_removed": "User removed.",
        "set_timezone": "Enter your timezone (e.g., Europe/Moscow or America/New_York):",
        "invalid_timezone": "❌ Invalid timezone format. Please try again (e.g., Europe/Moscow).",
        "timezone_updated": "Timezone updated.",
        "set_datetime_format": "Enter date format (e.g., YYYY-MM-DD) and time format (e.g., HH:mm) separated by space:",
        "invalid_datetime_format": "❌ Invalid format. Use YYYY, MM, DD, HH, mm. Example: YYYY-MM-DD HH:mm",
        "datetime_format_updated": "Date/time format updated.",
        "error": "Error",
        "not_owner": "Only project owner can manage users.",
    }
}

# HELPERS
TOKEN_MAP = {"YYYY": "%Y", "YY": "%y", "MM": "%m", "DD": "%d", "HH": "%H", "mm": "%M"}
_rx = re.compile("|".join(sorted(TOKEN_MAP, key=len, reverse=True)))

def fmt2strptime(dfmt: str, tfmt: str) -> str:
    return _rx.sub(lambda m: TOKEN_MAP[m.group(0)], f"{dfmt} {tfmt}")

def parse_dt(user_cfg: dict, text: str) -> datetime:
    dfmt, tfmt = user_cfg["date_format"], user_cfg["time_format"]
    fmt = fmt2strptime(dfmt, tfmt)
    dt = datetime.strptime(text, fmt)
    tz = ZoneInfo(user_cfg.get("timezone", "UTC"))
    return dt.replace(tzinfo=tz).astimezone(ZoneInfo("UTC"))

# FSM STATES
class CreatePost(StatesGroup):
    text = State()
    media = State()
    buttons = State()
    datetime = State()
    repeat = State()
    channel = State()
    confirm = State()

class ProjectStates(StatesGroup):
    new_project_name = State()
    select_project = State()
    manage_users = State()
    add_user_id = State()
    add_user_role = State()
    remove_user_confirm = State()
    remove_user_select = State()

class ChannelStates(StatesGroup):
    add_channel_name = State()

class SettingsStates(StatesGroup):
    set_timezone = State()
    set_datetime_format = State()

# ROUTERS (start / help / cancel)
base_router = Router()
channels_router = Router()
projects_router = Router()
posts_router = Router()

@base_router.message(Command("start"))
async def cmd_start(m: Message):
    u = db.ensure_user(m.from_user.id, default_lang=(m.from_user.language_code or "ru")[:2])
    await m.answer(TEXTS[u["language"]]["start"])

@base_router.message(Command("help"))
async def cmd_help(m: Message):
    lang = (db.get_user(m.from_user.id) or {}).get("language", "ru")
    await m.answer(TEXTS[lang]["help"])

@base_router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    lang = (db.get_user(m.from_user.id) or {}).get("language", "ru")
    await m.answer(TEXTS[lang]["post_cancelled"])

# КАНАЛЫ (добавить/удалить/список)
@channels_router.message(Command("channels"))
async def channels_menu(m: Message):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return

    chans = db.list_channels(active_project_id)
    if not chans:
        await m.answer(TEXTS[lang]["no_channels"])
        return
    txt = TEXTS[lang]["your_channels"] + "\n".join(f"{c['name']} — {c['chat_id']}" for c in chans)
    await m.answer(txt)

@channels_router.message(Command("add_channel"))
async def add_channel(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return
    
    await state.update_data({"add_channel_project_id": active_project_id})
    await m.answer(TEXTS[lang]["enter_channel_info"])
    await state.set_state(ChannelStates.add_channel_name)

@channels_router.message(F.text, ChannelStates.add_channel_name)
async def add_channel_save(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    data = await state.get_data()
    project_id = data.get("add_channel_project_id")
    input_txt = m.text.strip()
    try:
        chat = await bot.get_chat(input_txt)
    except Exception:
        await m.answer(TEXTS[lang]["channel_not_found"])
        await state.clear()
        return
    
    if chat.type not in ["channel", "supergroup", "group"]:
        await m.answer(TEXTS[lang]["only_channels_supported"])
        await state.clear()
        return
    
    db.add_channel(project_id, chat.id, chat.title or chat.username or str(chat.id))
    await m.answer(TEXTS[lang]["channel_added"])
    await state.clear()

@channels_router.message(Command("remove_channel"))
async def remove_channel(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return
    
    chans = db.list_channels(active_project_id)
    if not chans:
        await m.answer(TEXTS[lang]["no_channels_to_delete"])
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(c["name"], callback_data=f"delch_{c['id']}")] for c in chans
    ])
    await m.answer(TEXTS[lang]["select_channel_to_delete"], reply_markup=kb)

@channels_router.callback_query(F.data.startswith("delch_"))
async def remove_channel_cb(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    ch_id = int(q.data[6:])
    db.del_channel(ch_id)
    await q.message.edit_text(TEXTS[lang]["channel_deleted"])
    await q.answer()

# ПРОЕКТЫ (создать/список/выбор)
@projects_router.message(Command("project"))
async def projects_menu(m: Message):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    projs = db.list_projects(m.from_user.id)
    if not projs:
        await m.answer(TEXTS[lang]["no_projects"])
        return
    
    txt = TEXTS[lang]["your_projects"] + "\n".join(f"{p['id']}: {p['name']} (Role: {p['role']})" for p in projs)
    await m.answer(txt)

@projects_router.message(Command("project_add"))
async def project_add(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    await m.answer(TEXTS[lang]["enter_project_name"])
    await state.set_state(ProjectStates.new_project_name)

@projects_router.message(F.text, ProjectStates.new_project_name)
async def project_add_save(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    name = m.text.strip()
    p = db.create_project(m.from_user.id, name)
    await m.answer(TEXTS[lang]["project_created"] + name)
    await state.clear()

@projects_router.message(Command("project_select"))
async def project_select(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    projs = db.list_projects(m.from_user.id)
    if not projs:
        await m.answer(TEXTS[lang]["no_projects"])
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(f"{p['name']} ({p['role']})", callback_data=f"selproj_{p['id']}")] for p in projs
    ])
    await m.answer(TEXTS[lang]["select_project"], reply_markup=kb)

@projects_router.callback_query(F.data.startswith("selproj_"))
async def project_selected_cb(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    project_id = int(q.data[8:])
    
    # Check if user is a member of this project
    if not db.is_member(q.from_user.id, project_id):
        await q.answer(TEXTS[lang]["access_denied"], show_alert=True)
        return

    db.update_user(q.from_user.id, {"active_project_id": project_id})
    
    # Get project name for confirmation message
    project = (db.client.table("projects").select("name").eq("id", project_id).execute().data or [None])[0]
    project_name = project["name"] if project else "Unknown"

    await q.message.edit_text(TEXTS[lang]["project_selected"].format(project_name=project_name))
    await q.answer()

@projects_router.message(Command("current_project"))
async def current_project(m: Message):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return
    
    project = (db.client.table("projects").select("name").eq("id", active_project_id).execute().data or [None])[0]
    if project:
        await m.answer(TEXTS[lang]["current_project"].format(project_name=project["name"]))
    else:
        await m.answer(TEXTS[lang]["project_not_found"])

@projects_router.message(Command("manage_users"))
async def manage_users_menu(m: Message):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]

    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return
    
    user_role = db.get_user_project_role(m.from_user.id, active_project_id)
    if user_role != "owner":
        await m.answer(TEXTS[lang]["not_owner"])
        return
    
    project = (db.client.table("projects").select("name").eq("id", active_project_id).execute().data or [None])[0]
    project_name = project["name"] if project else "Unknown"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(TEXTS[lang]["add_user"], callback_data="manage_add_user")],
        [InlineKeyboardButton(TEXTS[lang]["remove_user"], callback_data="manage_remove_user")],
        [InlineKeyboardButton(TEXTS[lang]["list_users"], callback_data="manage_list_users")],
    ])
    await m.answer(TEXTS[lang]["user_management_menu"].format(project_name=project_name), reply_markup=kb)

@projects_router.callback_query(F.data == "manage_add_user")
async def manage_add_user_start(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    await q.message.edit_text(TEXTS[lang]["enter_user_id_or_username"])
    await state.set_state(ProjectStates.add_user_id)
    await q.answer()

@projects_router.message(F.text, ProjectStates.add_user_id)
async def manage_add_user_id(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    user_input = m.text.strip()
    target_user_id = None
    user_info_display = user_input

    try:
        target_user_id = int(user_input)
        target_user_tg = await bot.get_chat(target_user_id)
        user_info_display = target_user_tg.full_name or target_user_tg.username or str(target_user_id)
    except ValueError: # Not an integer, try as username
        if user_input.startswith('@'):
            try:
                target_user_tg = await bot.get_chat(user_input)
                target_user_id = target_user_tg.id
                user_info_display = target_user_tg.full_name or target_user_tg.username or str(target_user_id)
            except Exception:
                await m.answer(TEXTS[lang]["user_not_found"])
                await state.clear()
                return
        else:
            await m.answer(TEXTS[lang]["user_not_found"])
            await state.clear()
            return
    except Exception: # Telegram API error for get_chat
        await m.answer(TEXTS[lang]["user_not_found"])
        await state.clear()
        return

    if target_user_id == m.from_user.id:
        await m.answer("Ты не можешь изменить свою собственную роль через эту команду.")
        await state.clear()
        return

    active_project_id = u.get("active_project_id")
    if not active_project_id: # Should be checked by manage_users_menu
        await m.answer(TEXTS[lang]["no_active_project"])
        await state.clear()
        return

    await state.update_data({"target_user_id": target_user_id, "user_info_display": user_info_display})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Member", callback_data="role_member")],
        [InlineKeyboardButton("Admin", callback_data="role_admin")],
        [InlineKeyboardButton("Owner", callback_data="role_owner")],
    ])
    await m.answer(TEXTS[lang]["select_role"].format(user_info=user_info_display), reply_markup=kb)
    await state.set_state(ProjectStates.add_user_role)

@projects_router.callback_query(F.data.startswith("role_"), ProjectStates.add_user_role)
async def manage_add_user_role(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    user_info_display = data["user_info_display"]
    role = q.data[5:]

    active_project_id = u.get("active_project_id")
    project = (db.client.table("projects").select("name").eq("id", active_project_id).execute().data or [None])[0]
    project_name = project["name"] if project else "Unknown"

    db.add_user_to_project(target_user_id, active_project_id, role)
    await q.message.edit_text(TEXTS[lang]["user_added_to_project"].format(user_info=user_info_display, project_name=project_name, role=role))
    await state.clear()
    await q.answer()

@projects_router.callback_query(F.data == "manage_list_users")
async def manage_list_users(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]

    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await q.answer(TEXTS[lang]["no_active_project"], show_alert=True)
        return
    
    user_role = db.get_user_project_role(q.from_user.id, active_project_id)
    if user_role != "owner":
        await q.answer(TEXTS[lang]["not_owner"], show_alert=True)
        return

    members = db.list_project_members(active_project_id)
    project = (db.client.table("projects").select("name").eq("id", active_project_id).execute().data or [None])[0]
    project_name = project["name"] if project else "Unknown"

    if not members:
        await q.message.edit_text(TEXTS[lang]["no_users_in_project"])
        await q.answer()
        return

    txt = TEXTS[lang]["project_users"].format(project_name=project_name) + "\n"
    for member in members:
        try:
            member_tg = await bot.get_chat(member["user_id"])
            member_info = member_tg.full_name or member_tg.username or str(member["user_id"])
        except Exception:
            member_info = f"ID: {member['user_id']}"
        txt += f"- {member_info} ({member['role']})\n"
    
    await q.message.edit_text(txt)
    await q.answer()

@projects_router.callback_query(F.data == "manage_remove_user")
async def manage_remove_user_start(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]

    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await q.answer(TEXTS[lang]["no_active_project"], show_alert=True)
        return
    
    user_role = db.get_user_project_role(q.from_user.id, active_project_id)
    if user_role != "owner":
        await q.answer(TEXTS[lang]["not_owner"], show_alert=True)
        return

    members = db.list_project_members(active_project_id)
    if not members or (len(members) == 1 and members[0]["user_id"] == q.from_user.id):
        await q.message.edit_text(TEXTS[lang]["no_users_in_project"])
        await q.answer()
        return

    kb_buttons = []
    for member in members:
        if member["user_id"] != q.from_user.id: # Cannot remove self
            try:
                member_tg = await bot.get_chat(member["user_id"])
                member_info = member_tg.full_name or member_tg.username or str(member["user_id"])
            except Exception:
                member_info = f"ID: {member['user_id']}"
            kb_buttons.append([InlineKeyboardButton(f"{member_info} ({member['role']})", callback_data=f"rmuser_{member['user_id']}")])
    
    if not kb_buttons:
        await q.message.edit_text(TEXTS[lang]["no_users_in_project"])
        await q.answer()
        return

    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await q.message.edit_text(TEXTS[lang]["select_user_to_remove"], reply_markup=kb)
    await state.set_state(ProjectStates.remove_user_select)
    await q.answer()

@projects_router.callback_query(F.data.startswith("rmuser_"), ProjectStates.remove_user_select)
async def manage_remove_user_confirm(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    
    target_user_id = int(q.data[7:])
    
    active_project_id = u.get("active_project_id")
    project = (db.client.table("projects").select("name").eq("id", active_project_id).execute().data or [None])[0]
    project_name = project["name"] if project else "Unknown"

    try:
        target_user_tg = await bot.get_chat(target_user_id)
        user_info_display = target_user_tg.full_name or target_user_tg.username or str(target_user_id)
    except Exception:
        user_info_display = f"ID: {target_user_id}"

    await state.update_data({"target_user_id_to_remove": target_user_id, "user_info_display_to_remove": user_info_display})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(TEXTS[lang]["yes"], callback_data="confirm_remove_yes")],
        [InlineKeyboardButton(TEXTS[lang]["no"], callback_data="confirm_remove_no")],
    ])
    await q.message.edit_text(TEXTS[lang]["confirm_remove_user"].format(user_info=user_info_display, project_name=project_name), reply_markup=kb)
    await state.set_state(ProjectStates.remove_user_confirm)
    await q.answer()

@projects_router.callback_query(F.data.startswith("confirm_remove_"), ProjectStates.remove_user_confirm)
async def manage_remove_user_execute(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    data = await state.get_data()
    target_user_id = data["target_user_id_to_remove"]
    user_info_display = data["user_info_display_to_remove"]

    active_project_id = u.get("active_project_id")

    if q.data == "confirm_remove_yes":
        db.remove_user_from_project(target_user_id, active_project_id)
        await q.message.edit_text(TEXTS[lang]["user_removed_from_project"].format(user_info=user_info_display, project_name="")) # Project name already in previous message
    else:
        await q.message.edit_text(TEXTS[lang]["post_cancelled"]) # Reusing cancel text
    
    await state.clear()
    await q.answer()

# НАСТРОЙКИ (таймзона, язык, формат)
@projects_router.message(Command("settings"))
async def settings_menu(m: Message, state: FSMContext):
    u = db.get_user(m.from_user.id)
    txt = TEXTS[u["language"]]["settings_menu"].format(
        language=u['language'],
        timezone=u['timezone'],
        date_format=u['date_format'],
        time_format=u['time_format']
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(TEXTS[u["language"]]["select_language"], callback_data="set_lang")],
            [InlineKeyboardButton(TEXTS[u["language"]]["set_timezone"], callback_data="set_tz")],
            [InlineKeyboardButton(TEXTS[u["language"]]["set_datetime_format"], callback_data="set_fmt")],
        ]
    )
    await m.answer(txt, reply_markup=kb)

@projects_router.callback_query(F.data == "set_lang")
async def set_lang(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("Русский", callback_data="lang_ru")],
            [InlineKeyboardButton("English", callback_data="lang_en")],
        ]
    )
    await q.message.edit_text(TEXTS[lang]["select_language"], reply_markup=kb)
    await q.answer()

@projects_router.callback_query(F.data.in_(["lang_ru", "lang_en"]))
async def lang_selected(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    selected_lang = "ru" if q.data == "lang_ru" else "en"
    db.update_user(q.from_user.id, {"language": selected_lang})
    await q.message.edit_text(TEXTS[selected_lang]["language_updated"])
    await q.answer()

@projects_router.callback_query(F.data == "set_tz")
async def set_tz_start(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    await q.message.edit_text(TEXTS[lang]["set_timezone"])
    await state.set_state(SettingsStates.set_timezone)
    await q.answer()

@projects_router.message(F.text, SettingsStates.set_timezone)
async def set_tz_save(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    timezone_str = m.text.strip()
    try:
        ZoneInfo(timezone_str) # Validate timezone string
        db.update_user(m.from_user.id, {"timezone": timezone_str})
        await m.answer(TEXTS[lang]["timezone_updated"])
        await state.clear()
    except Exception:
        await m.answer(TEXTS[lang]["invalid_timezone"])

@projects_router.callback_query(F.data == "set_fmt")
async def set_fmt_start(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    await q.message.edit_text(TEXTS[lang]["set_datetime_format"])
    await state.set_state(SettingsStates.set_datetime_format)
    await q.answer()

@projects_router.message(F.text, SettingsStates.set_datetime_format)
async def set_fmt_save(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    parts = m.text.strip().split()
    if len(parts) == 2:
        dfmt, tfmt = parts
        # Basic validation for format tokens
        # This regex checks for YYYY, MM, DD, HH, mm patterns
        date_tokens_valid = all(token in TOKEN_MAP for token in re.findall(r'(YYYY|YY|MM|DD)', dfmt))
        time_tokens_valid = all(token in TOKEN_MAP for token in re.findall(r'(HH|mm)', tfmt))

        if date_tokens_valid and time_tokens_valid:
            db.update_user(m.from_user.id, {"date_format": dfmt, "time_format": tfmt})
            await m.answer(TEXTS[lang]["datetime_format_updated"])
            await state.clear()
            return
    await m.answer(TEXTS[lang]["invalid_datetime_format"])

# ПОСТЫ: СОЗДАНИЕ ЧЕРЕЗ FSM (полный wizard)
@posts_router.message(Command("create"))
async def create_post_start(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return
    
    await state.update_data({"project_id": active_project_id}) # Store project_id for the post
    await m.answer(TEXTS[lang]["enter_post_text"])
    await state.set_state(CreatePost.text)

@posts_router.message(CreatePost.text)
async def post_text(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    await state.update_data({"text": m.text})
    await m.answer(TEXTS[lang]["attach_media"])
    await state.set_state(CreatePost.media)

@posts_router.message(F.photo | F.video, CreatePost.media)
async def post_media(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    file_id = None
    if m.photo:
        file_id = m.photo[-1].file_id
    elif m.video:
        file_id = m.video.file_id
    await state.update_data({"media": file_id})
    await m.answer(TEXTS[lang]["enter_buttons"])
    await state.set_state(CreatePost.buttons)

@posts_router.message(Command("skip"), CreatePost.media)
async def post_media_skip(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    await state.update_data({"media": None})
    await m.answer(TEXTS[lang]["enter_buttons"])
    await state.set_state(CreatePost.buttons)

@posts_router.message(CreatePost.buttons)
async def post_buttons(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    lines = m.text.strip().splitlines()
    buttons = []
    for line in lines:
        if "|" in line:
            txt, url = map(str.strip, line.split("|", 1))
            buttons.append({"text": txt, "url": url})
    await state.update_data({"buttons": buttons})
    await m.answer(TEXTS[lang]["publish_time"])
    await state.set_state(CreatePost.datetime)

@posts_router.message(Command("skip"), CreatePost.buttons)
async def post_buttons_skip(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    await state.update_data({"buttons": []})
    await m.answer(TEXTS[lang]["publish_time"])
    await state.set_state(CreatePost.datetime)

@posts_router.message(CreatePost.datetime)
async def post_datetime(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    try:
        dt = parse_dt(u, m.text.strip()) # Use parse_dt helper
    except Exception:
        await m.answer(TEXTS[lang]["invalid_date_format"])
        return
    await state.update_data({"datetime": dt.isoformat()})
    await m.answer(TEXTS[lang]["repeat_post"], reply_markup=None)
    await state.set_state(CreatePost.repeat)

@posts_router.message(Command("skip"), CreatePost.datetime)
async def post_datetime_skip(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    now = datetime.now(timezone.utc).isoformat()
    await state.update_data({"datetime": now})
    await m.answer(TEXTS[lang]["repeat_post"])
    await state.set_state(CreatePost.repeat)

@posts_router.message(CreatePost.repeat)
async def post_repeat(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    repeat = 0
    try:
        repeat = int(m.text.strip())
    except Exception:
        await m.answer(TEXTS[lang]["invalid_repeat_format"])
        return
    await state.update_data({"repeat": repeat})
    
    data = await state.get_data()
    active_project_id = data.get("project_id") # Get project_id from state
    
    if not active_project_id: # Should not happen if /create checks for it
        await m.answer(TEXTS[lang]["no_active_project"])
        await state.clear()
        return

    chans = db.list_channels(active_project_id)
    if not chans:
        await m.answer(TEXTS[lang]["no_channels"])
        await state.clear()
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(c["name"], callback_data=f"pickch_{c['id']}")] for c in chans
    ])
    await m.answer(TEXTS[lang]["pick_channel"], reply_markup=kb)
    await state.set_state(CreatePost.channel)

@posts_router.callback_query(F.data.startswith("pickch_"), CreatePost.channel)
async def post_pick_channel(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    ch_id = int(q.data[7:])
    await state.update_data({"channel_id": ch_id})
    data = await state.get_data()
    
    await q.message.edit_text(TEXTS[lang]["post_saved"]) # Update message to indicate saving
    
    db.new_post({
        "text": data.get("text"),
        "media": data.get("media"),
        "buttons": data.get("buttons"),
        "publish_time": data.get("datetime"),
        "repeat_minutes": data.get("repeat"),
        "channel_id": ch_id,
        "published": False,
        "draft": False,
        "project_id": data.get("project_id") # Ensure project_id is saved with the post
    })
    await state.clear()
    await q.message.answer(TEXTS[lang]["post_saved"]) # Send final confirmation
    await q.answer()

# ЛИСТИНГ, УДАЛЕНИЕ, РЕДАКТИРОВАНИЕ ПОСТОВ
@posts_router.message(Command("list"))
async def list_posts(m: Message):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    
    active_project_id = u.get("active_project_id")
    if not active_project_id:
        await m.answer(TEXTS[lang]["no_active_project"])
        return

    posts = db.client.table("posts").select("*").eq("project_id", active_project_id).order("publish_time", desc=False).execute().data
    
    if not posts:
        await m.answer(TEXTS[lang]["no_posts_yet"])
        return
    
    for p in posts:
        txt = (p["text"] or "")[:60] + ("…" if p["text"] and len(p["text"]) > 60 else "")
        dt = p["publish_time"][:16].replace("T", " ")
        status = "✅" if p.get("published") else "🕓"
        
        await m.answer(
            f"{status} <b>{dt}</b>\n{txt}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton("👁️", callback_data=f"prev_{p['id']}"),
                        InlineKeyboardButton("✏️", callback_data=f"edit_{p['id']}"),
                        InlineKeyboardButton("🗑️", callback_data=f"del_{p['id']}")
                    ]
                ]
            )
        )

@posts_router.callback_query(F.data.startswith("prev_"))
async def preview_post(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    post_id = int(q.data[5:])
    p = db.get_post(post_id)
    if not p:
        await q.answer(TEXTS[lang]["post_not_found"], show_alert=True)
        return
    
    txt = p["text"]
    btns = None
    try:
        if p.get("buttons"):
            btns_list = json.loads(p["buttons"])
            if btns_list:
                btns = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(b["text"], url=b["url"])] for b in btns_list
                ])
    except Exception:
        pass # Malformed JSON or other button issues
    
    if p.get("media"):
        try:
            await bot.send_photo(q.from_user.id, p["media"], caption=txt, reply_markup=btns)
        except Exception:
            await q.message.answer(txt, reply_markup=btns) # Fallback if photo fails
    else:
        await q.message.answer(txt, reply_markup=btns)
    await q.answer()

@posts_router.callback_query(F.data.startswith("del_"))
async def delete_post_cb(q: CallbackQuery):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    post_id = int(q.data[4:])
    db.delete_post(post_id)
    await q.message.edit_text(TEXTS[lang]["post_deleted"])
    await q.answer()

@posts_router.callback_query(F.data.startswith("edit_"))
async def edit_post_cb(q: CallbackQuery, state: FSMContext):
    u = db.ensure_user(q.from_user.id)
    lang = u["language"]
    post_id = int(q.data[5:])
    p = db.get_post(post_id)
    if not p:
        await q.answer(TEXTS[lang]["post_not_found"], show_alert=True)
        return
    
    await state.update_data({"edit_id": post_id})
    await q.message.answer(TEXTS[lang]["enter_new_post_text"])
    await state.set_state("edit_post_text") # This state needs to be defined in FSMStates if it's not
    await q.answer()

@posts_router.message(F.text, lambda m, state: state.get_state() == "edit_post_text") # This state needs to be defined in FSMStates
async def edit_post_text(m: Message, state: FSMContext):
    u = db.ensure_user(m.from_user.id)
    lang = u["language"]
    data = await state.get_data()
    post_id = data.get("edit_id")
    db.update_post(post_id, {"text": m.text})
    await m.answer(TEXTS[lang]["text_updated"])
    await state.clear()

# АВТОПУБЛИКАЦИЯ — ПЕРИОДИЧЕСКИЙ LOOP
async def autoposter():
    while True:
        try:
            now = datetime.now(timezone.utc).isoformat(timespec="minutes")
            pending = db.pending_posts(now)
            
            for p in pending:
                ch = db.client.table("channels").select("*").eq("id", p["channel_id"]).execute().data
                if not ch:
                    logging.warning(f"Channel with ID {p['channel_id']} not found for post {p['id']}. Skipping.")
                    continue
                
                ch_id = ch[0]["chat_id"]
                btns = None
                try:
                    if p.get("buttons"):
                        btns_list = json.loads(p["buttons"])
                        if btns_list:
                            btns = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(b["text"], url=b["url"])] for b in btns_list
                            ])
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing buttons for post {p['id']}: {e}. Buttons will not be sent.")
                    btns = None
                except Exception as e:
                    logging.error(f"Unexpected error with buttons for post {p['id']}: {e}. Buttons will not be sent.")
                    btns = None
                
                try:
                    if p.get("media"):
                        await bot.send_photo(ch_id, p["media"], caption=p["text"], reply_markup=btns)
                    else:
                        await bot.send_message(ch_id, p["text"], reply_markup=btns)
                    
                    db.mark_published(p["id"]) # Mark as published after successful sending
                    logging.info(f"Post {p['id']} successfully published to channel {ch_id}.")
                    
                    # Повтор? Генерим копию с новым временем, если repeat_minutes > 0
                    if p.get("repeat_minutes") and int(p["repeat_minutes"]) > 0:
                        dt = datetime.fromisoformat(p["publish_time"])
                        new_dt = dt + timedelta(minutes=int(p["repeat_minutes"]))
                        
                        # Ensure project_id is passed for new post
                        project_id = ch[0]["project_id"] # Get project_id from channel
                        
                        db.new_post({
                            "text": p["text"],
                            "media": p.get("media"),
                            "buttons": p.get("buttons"),
                            "publish_time": new_dt.isoformat(),
                            "repeat_minutes": p["repeat_minutes"],
                            "channel_id": p["channel_id"],
                            "published": False,
                            "draft": False,
                            "project_id": project_id # Add project_id to the new post
                        })
                        logging.info(f"New repeat post created for post {p['id']} with publish time {new_dt.isoformat()}.")
                except Exception as ex:
                    logging.error(f"Error publishing post {p['id']} to channel {ch_id}: {ex}", exc_info=True)
                    # Optionally, mark post as failed or retry later
            
            await asyncio.sleep(30) # Check every 30 seconds
        except Exception as e:
            logging.critical(f"Critical error in autoposter loop: {e}", exc_info=True)
            await asyncio.sleep(60) # Wait longer if a critical error occurs to prevent rapid looping

# ОБЩИЙ СТАРТ, ОБРАБОТКА ОШИБОК, ПОДКЛЮЧЕНИЕ ROUTERS
dp.include_router(base_router)
dp.include_router(channels_router)
dp.include_router(projects_router)
dp.include_router(posts_router)

@dp.errors()
async def error_handler(update, error):
    try:
        msg = getattr(update, "message", None) or getattr(update, "callback_query", None)
        user_id = msg.from_user.id if msg else None
        
        logging.error(f"Update: {update} caused error: {error}", exc_info=True)
        
        if user_id:
            u = db.get_user(user_id)
            lang = u["language"] if u else "ru"
            error_message = f"{TEXTS[lang]['error']}: {error}"
            if "A request to the Telegram API was unsuccessful" in str(error):
                error_message = f"{TEXTS[lang]['error']}: Произошла ошибка при взаимодействии с Telegram API. Возможно, бот не является администратором канала или канал недоступен."
            elif "Bad Request: chat not found" in str(error):
                error_message = f"{TEXTS[lang]['error']}: Канал не найден. Убедитесь, что ID канала верен и бот добавлен в канал."
            elif "Forbidden: bot was blocked by the user" in str(error):
                error_message = f"{TEXTS[lang]['error']}: Бот был заблокирован пользователем. Пожалуйста, разблокируйте бота."
            
            if msg:
                await msg.answer(error_message)
    except Exception as e:
        logging.critical(f"Error in error_handler itself: {e}", exc_info=True)
        if msg:
            await msg.answer("Произошла неизвестная ошибка в обработчике ошибок.")

async def main():
    logging.info("Starting bot...")
    # Можно сразу гонять два корутины — polling и автопостер
    await asyncio.gather(
        dp.start_polling(bot, skip_updates=True),
        autoposter()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
