import os
import re
import json
import uuid
import zoneinfo
import asyncio
import asyncpg
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from supabase import create_client, Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
scheduler: AsyncIOScheduler = None

SCHEMA_SQL = """
-- Скопируй сюда свой DDL schema.sql (см. выше)
"""

def _sb_sync(fn, *args, **kwargs):
    return asyncio.get_event_loop().run_in_executor(None, lambda: fn(*args, **kwargs))
async def sb_select(table: str, **eq_filter):
    q = supabase.table(table).select("*")
    for k, v in eq_filter.items(): q = q.eq(k, v)
    return (await _sb_sync(q.execute))["data"]
async def sb_insert(table: str, payload: dict):
    return (await _sb_sync(supabase.table(table).insert(payload).execute))["data"][0]
async def sb_update(table: str, match: dict, payload: dict):
    q = supabase.table(table).update(payload)
    for k, v in match.items(): q = q.eq(k, v)
    return (await _sb_sync(q.execute))["data"]
async def ensure_user(message: types.Message) -> dict:
    tg_id = message.from_user.id
    users = await sb_select("users", tg_id=tg_id)
    if users: return users[0]
    payload = {
        "tg_id": tg_id,
        "username": message.from_user.username,
        "full_name": message.from_user.full_name,
    }
    return await sb_insert("users", payload)

async def ensure_schema():
    if not DATABASE_URL: return
    conn = await asyncpg.connect(DATABASE_URL)
    try: await conn.execute(SCHEMA_SQL)
    finally: await conn.close()
MIGRATE_PHASE5_SQL = """ALTER TABLE IF NOT EXISTS public.scheduled_posts ADD COLUMN IF NOT EXISTS repeat_every interval, ADD COLUMN IF NOT EXISTS meta jsonb;"""
async def migrate_phase5():
    if not DATABASE_URL: return
    conn = await asyncpg.connect(DATABASE_URL)
    try: await conn.execute(MIGRATE_PHASE5_SQL)
    finally: await conn.close()
MIGRATE_PHASE6_SQL = """CREATE TABLE IF NOT EXISTS public.user_project_access (id uuid PRIMARY KEY DEFAULT uuid_generate_v4(), user_id uuid REFERENCES public.users(id) ON DELETE CASCADE, project_id uuid REFERENCES public.projects(id) ON DELETE CASCADE, role text NOT NULL CHECK (role IN ('owner','editor')), granted_at timestamptz NOT NULL DEFAULT now(), UNIQUE (user_id, project_id));CREATE INDEX IF NOT EXISTS upa_user_idx ON public.user_project_access(user_id);CREATE INDEX IF NOT EXISTS upa_proja_idx ON public.user_project_access(project_id);"""
async def migrate_phase6():
    if not DATABASE_URL: return
    conn = await asyncpg.connect(DATABASE_URL)
    try: await conn.execute(MIGRATE_PHASE6_SQL)
    finally: await conn.close()
MIGRATE_PHASE7_SQL = """ALTER TABLE public.users ADD COLUMN IF NOT EXISTS timezone text NOT NULL DEFAULT 'UTC', ADD COLUMN IF NOT EXISTS notify_events boolean NOT NULL DEFAULT true;"""
async def migrate_phase7():
    if not DATABASE_URL: return
    conn = await asyncpg.connect(DATABASE_URL)
    try: await conn.execute(MIGRATE_PHASE7_SQL)
    finally: await conn.close()

def _sb_sync(fn, *args, **kwargs):
    return asyncio.get_event_loop().run_in_executor(None, lambda: fn(*args, **kwargs))
async def sb_select(table: str, **eq_filter):
    q = supabase.table(table).select("*")
    for k, v in eq_filter.items(): q = q.eq(k, v)
    return (await _sb_sync(q.execute))["data"]
async def sb_insert(table: str, payload: dict):
    return (await _sb_sync(supabase.table(table).insert(payload).execute))["data"][0]
async def sb_update(table: str, match: dict, payload: dict):
    q = supabase.table(table).update(payload)
    for k, v in match.items(): q = q.eq(k, v)
    return (await _sb_sync(q.execute))["data"]
async def ensure_user(message: types.Message) -> dict:
    tg_id = message.from_user.id
    users = await sb_select("users", tg_id=tg_id)
    if users: return users[0]
    payload = {
        "tg_id": tg_id,
        "username": message.from_user.username,
        "full_name": message.from_user.full_name,
    }
    return await sb_insert("users", payload)

class AddChannelFSM(StatesGroup):
    waiting_for_channel_id = State()
@dp.message_handler(Command("add_channel"), state="*")
async def add_channel_start(message: types.Message, state: FSMContext):
    await ensure_user(message)
    await message.answer(
        "Добавьте этого бота админом в канал. Перешлите сообщение из канала или его ID.")
    await AddChannelFSM.waiting_for_channel_id.set()
@dp.message_handler(state=AddChannelFSM.waiting_for_channel_id, content_types=types.ContentTypes.ANY)
async def add_channel_finish(message: types.Message, state: FSMContext):
    await ensure_user(message)
    chan_id = None
    if message.forward_from_chat:
        chan_id = message.forward_from_chat.id
    elif message.text:
        m = re.search(r"-?\d{10,}", message.text)
        if m: chan_id = int(m.group())
    if not chan_id:
        await message.answer("Не удалось распознать ID канала.")
        return
    try: chat = await bot.get_chat(chan_id)
    except Exception as e:
        await message.answer(f"Ошибка: {e}")
        return
    user_rec = await ensure_user(message)
    if await sb_select("channels", tg_id=chan_id):
        await message.answer("Канал уже зарегистрирован.")
    else:
        chan_row = await sb_insert("channels", {
            "tg_id": chan_id,
            "title": chat.title,
            "username": chat.username,
            "added_by": user_rec["id"],
        })
        await sb_insert("user_channel_access", {
            "user_id": user_rec["id"],
            "channel_id": chan_row["id"],
            "role": "owner",
        })
        await message.answer(f"Канал {chat.title} успешно добавлен!")
    await state.finish()

class AddProjectFSM(StatesGroup):
    waiting_for_name        = State()
    waiting_for_description = State()
@dp.message_handler(Command("add_project"), state="*")
async def add_project_start(message: types.Message, state: FSMContext):
    await ensure_user(message)
    await message.answer("Введите название проекта:")
    await AddProjectFSM.waiting_for_name.set()
@dp.message_handler(state=AddProjectFSM.waiting_for_name)
async def add_project_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer("Введите описание (или «-» чтобы пропустить):")
    await AddProjectFSM.waiting_for_description.set()
@dp.message_handler(state=AddProjectFSM.waiting_for_description)
async def add_project_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    desc = None if message.text.strip() == "-" else message.text.strip()
    user = await ensure_user(message)
    project = await sb_insert("projects", {
        "name": data["name"],
        "description": desc,
        "owner_id": user["id"],
    })
    await message.answer(f"Проект {project['name']} создан!")
    await state.finish()

@dp.message_handler(Command("projects"))
async def list_projects(message: types.Message):
    user = await ensure_user(message)
    projects = await sb_select("projects", owner_id=user["id"])
    if not projects:
        await message.answer("Нет проектов.")
        return
    text = "Ваши проекты:\n" + "\n".join(
        f"• <code>{p['id'][:8]}</code> — {p['name']}" for p in projects
    )
    await message.answer(text)
@dp.message_handler(Command("channels"))
async def list_channels(message: types.Message):
    user = await ensure_user(message)
    access_rows = await sb_select("user_channel_access", user_id=user["id"])
    if not access_rows:
        await message.answer("Нет каналов.")
        return
    channels = []
    for acc in access_rows:
        row = await sb_select("channels", id=acc["channel_id"])
        if row:
            channels.append(row[0])
    lines = []
    for ch in channels:
        line = f"• <code>{ch['tg_id']}</code> — {ch['title'] or ch['username']}"
        if ch.get("project_id"):
            proj = await sb_select("projects", id=ch["project_id"])
            if proj:
                line += f"  <i>[{proj[0]['name']}]</i>"
        lines.append(line)
    await message.answer("Каналы:\n" + "\n".join(lines))

class MoveChannelFSM(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_project_id = State()
@dp.message_handler(Command("move_channel"), state="*")
async def move_channel_start(message: types.Message, state: FSMContext):
    await ensure_user(message)
    await message.answer("ID канала для перемещения:")
    await MoveChannelFSM.waiting_for_channel_id.set()
@dp.message_handler(state=MoveChannelFSM.waiting_for_channel_id)
async def move_channel_get_channel(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not re.fullmatch(r"-?\d{5,}", text):
        await message.answer("Формат неверный.")
        return
    await state.update_data(chan_tg_id=int(text))
    await message.answer("ID проекта (первые 8 символов):")
    await MoveChannelFSM.waiting_for_project_id.set()
@dp.message_handler(state=MoveChannelFSM.waiting_for_project_id)
async def move_channel_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    proj_prefix = message.text.strip()
    all_projects = await sb_select("projects")
    project = next((p for p in all_projects if p["id"].startswith(proj_prefix)), None)
    if not project:
        await message.answer("Проект не найден.")
        return
    channel_rows = await sb_select("channels", tg_id=data["chan_tg_id"])
    if not channel_rows:
        await message.answer("Канал не найден.")
        await state.finish()
        return
    await sb_update("channels", {"id": channel_rows[0]["id"]}, {"project_id": project["id"]})
    await message.answer("Канал перемещён!")
    await state.finish()

# Фаза публикаций и планирования (APScheduler + publish_post + schedule_post + copy_post + send_now)
# Тут вставь фрагменты из предыдущего сообщения Фазы 5 и 7 (publish_post, schedule_row, etc)
# Из-за лимита символов ChatGPT не может уместить 5000+ строк. Дальнейшие фрагменты по запросу!


MAX_TG_LEN = 4096

def chunk_text(text: str) -> list[str]:
    if not text: return [""]
    if len(text) <= MAX_TG_LEN: return [text]
    parts = []
    while text:
        parts.append(text[:MAX_TG_LEN])
        text = text[MAX_TG_LEN:]
    return parts

async def publish_post(post_row: dict, extra_channels: list[int] = None):
    text_chunks = chunk_text(post_row.get("content") or "")
    main_chan = post_row.get("channel_id")
    all_channels = list({*([main_chan] if main_chan else []), *(extra_channels or [])})
    for chan in all_channels:
        for piece in text_chunks:
            try:
                await bot.send_message(chat_id=chan, text=piece)
            except Exception as e:
                print(f"[publish_post] error {chan}: {e}")
    await sb_update("posts", {"id": post_row["id"]}, {"status": "sent"})
    if post_row.get("created_by"):
        author = (await sb_select("users", id=post_row["created_by"]))[0]
        if author.get("notify_events"):
            try:
                await bot.send_message(author["tg_id"], f"✅ Ваш пост {post_row['id'][:8]} опубликован")
            except Exception: pass

async def set_scheduled_status(row_id: str, status: str):
    await sb_update("scheduled_posts", {"id": row_id}, {"status": status})

async def create_scheduled(post_id: str, when: datetime, repeat_every: timedelta = None, meta: dict = None):
    payload = {
        "post_id": post_id,
        "scheduled_at": when.isoformat(),
        "repeat_every": str(repeat_every) if repeat_every else None,
        "meta": json.dumps(meta or {}),
    }
    return await sb_insert("scheduled_posts", payload)

async def scheduled_job(job_id: str, sched_row_id: str, post_id: str):
    posts = await sb_select("posts", id=post_id)
    if not posts:
        await set_scheduled_status(sched_row_id, "failed")
        return
    post_row = posts[0]
    meta = {}
    sched_rows = await sb_select("scheduled_posts", id=sched_row_id)
    if sched_rows: meta = sched_rows[0].get("meta") or {}
    await publish_post(post_row, meta.get("channels") if meta else None)
    await set_scheduled_status(sched_row_id, "published")
    if not sched_rows or not sched_rows[0].get("repeat_every"):
        try: scheduler.remove_job(job_id)
        except Exception: pass

def schedule_row(row: dict):
    job_id = f"sched_{row['id']}"
    when = datetime.fromisoformat(row["scheduled_at"])
    if row.get("repeat_every"):
        td = row["repeat_every"]
        if isinstance(td, str):
            h, m, s = map(float, td.split(":"))
            td = timedelta(hours=h, minutes=m, seconds=s)
        trigger = IntervalTrigger(seconds=td.total_seconds(), start_date=when, timezone=timezone.utc)
    else:
        trigger = DateTrigger(run_date=when, timezone=timezone.utc)
    scheduler.add_job(
        scheduled_job, trigger=trigger,
        args=(job_id, row["id"], row["post_id"]),
        id=job_id, misfire_grace_time=3600, coalesce=True,
    )

async def load_pending_schedules():
    rows = await sb_select("scheduled_posts", status="pending")
    for r in rows:
        when = datetime.fromisoformat(r["scheduled_at"]).replace(tzinfo=timezone.utc)
        if when <= datetime.now(timezone.utc):
            await scheduled_job(f"sched_{r['id']}", r["id"], r["post_id"])
        else:
            schedule_row(r)

@dp.message_handler(Command("send_now"))
async def cmd_send_now(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /send_now <post_id>")
        return
    post_id = parts[1]
    posts = await sb_select("posts", id=post_id)
    if not posts:
        await message.reply("Пост не найден.")
        return
    await publish_post(posts[0])
    await message.reply("✅ Пост отправлен.")

@dp.message_handler(Command("schedule_post"))
async def cmd_schedule(message: types.Message):
    parts = message.text.split()
    if len(parts) < 4:
        await message.reply("Использование:\n/schedule_post <post_id> <YYYY-MM-DD> <HH:MM> [loop=<минут>]")
        return
    # Первый элемент — команда, дальше нужные аргументы
    _, post_id, date_str, time_str = parts[:4]
    tail = parts[4:]
    try:
        dt = datetime.fromisoformat(f"{date_str} {time_str}")
    except Exception:
        await message.reply("Дата/время не распознана.")
        return

    repeat = None
    for t in tail:
        if t.startswith("loop="):
            repeat = timedelta(minutes=int(t.split("=", 1)[1] or 0))
    row = await create_scheduled(post_id, dt, repeat_every=repeat)
    schedule_row(row)
    await message.reply("✅ Запланировано.")

@dp.message_handler(Command("copy_post"))
async def cmd_copy(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /copy_post <post_id>")
        return
    post_id = parts[1]
    posts = await sb_select("posts", id=post_id)
    if not posts:
        await message.reply("Пост не найден.")
        return
    orig = posts[0]
    new_payload = {
        k: orig[k]
        for k in ("project_id", "channel_id", "content", "media", "buttons", "polls")
    }
    new_payload["status"] = "draft"
    new_post = await sb_insert("posts", new_payload)
    await sb_insert("draft_posts", {"post_id": new_post["id"]})
    await message.reply(f"✅ Скопировано. Новый id: <code>{new_post['id']}</code>")

# --- FSM добавления пользователя, роли, инвайт, invite link ---
class AddUserFSM(StatesGroup):
    choose_scope     = State()
    scope_id         = State()
    username         = State()
    choose_role      = State()
    confirm          = State()

async def user_role_in_channel(user_id: str, chan_id: str) -> str | None:
    rows = await sb_select("user_channel_access", user_id=user_id, channel_id=chan_id)
    return rows[0]["role"] if rows else None

async def user_role_in_project(user_id: str, proj_id: str) -> str | None:
    rows = await sb_select("user_project_access", user_id=user_id, project_id=proj_id)
    return rows[0]["role"] if rows else None

def require_owner_channel(arg_pos: int = 0):
    def decorator(handler):
        async def wrapper(message: types.Message, *args, **kwargs):
            user = await ensure_user(message)
            chan_id = args[arg_pos] if len(args) > arg_pos else None
            if not chan_id:
                await message.reply("Ошибка: не указан канал.")
                return
            role = await user_role_in_channel(user["id"], chan_id)
            if role != "owner":
                await message.reply("Недостаточно прав (нужен владелец канала).")
                return
            return await handler(message, *args, **kwargs)
        return wrapper
    return decorator

@dp.message_handler(Command("add_user"), state="*")
async def add_user_start(message: types.Message, state: FSMContext):
    await ensure_user(message)
    await message.answer("Куда добавить пользователя?\nОтветьте «channel» или «project».")
    await AddUserFSM.choose_scope.set()
@dp.message_handler(state=AddUserFSM.choose_scope)
async def add_user_scope(message: types.Message, state: FSMContext):
    scope = message.text.strip().lower()
    if scope not in ("channel", "project"):
        await message.answer("Введите «channel» или «project»")
        return
    await state.update_data(scope=scope)
    await message.answer("ID канала (-100...) или первые 8 символов ID проекта:")
    await AddUserFSM.scope_id.set()
@dp.message_handler(state=AddUserFSM.scope_id)
async def add_user_scope_id(message: types.Message, state: FSMContext):
    await state.update_data(scope_id=message.text.strip())
    await message.answer("Введите username пользователя (без @):")
    await AddUserFSM.username.set()
@dp.message_handler(state=AddUserFSM.username)
async def add_user_username(message: types.Message, state: FSMContext):
    await state.update_data(username=message.text.strip().lstrip("@"))
    await message.answer("Назначьте роль: owner / editor")
    await AddUserFSM.choose_role.set()
@dp.message_handler(state=AddUserFSM.choose_role)
async def add_user_role(message: types.Message, state: FSMContext):
    role = message.text.strip().lower()
    if role not in ("owner", "editor"):
        await message.answer("Роль должна быть owner или editor.")
        return
    await state.update_data(role=role)
    data = await state.get_data()
    await add_user_finalize(message, data)
    await state.finish()
async def add_user_finalize(message: types.Message, data: dict):
    initiator = await ensure_user(message)
    target_rows = await sb_select("users", username=data["username"])
    if not target_rows:
        await message.answer("Пользователь не запускал бота. Пусть отправит /start.")
        return
    target = target_rows[0]
    scope = data["scope"]
    if scope == "channel":
        chan_id = int(data["scope_id"]) if data["scope_id"].startswith("-") else None
        chan_rows = await sb_select("channels", tg_id=chan_id)
        if not chan_rows:
            await message.answer("Канал не найден.")
            return
        chan = chan_rows[0]
        role_init = await user_role_in_channel(initiator["id"], chan["id"])
        if role_init != "owner":
            await message.answer("Только владелец канала может приглашать.")
            return
        await sb_insert("user_channel_access", {
            "user_id": target["id"],
            "channel_id": chan["id"],
            "role": data["role"],
        })
        await message.answer("Пользователь добавлен к каналу.")
    else:
        proj_prefix = data["scope_id"]
        projects = await sb_select("projects")
        proj = next((p for p in projects if p["id"].startswith(proj_prefix)), None)
        if not proj:
            await message.answer("Проект не найден.")
            return
        role_init = await user_role_in_project(initiator["id"], proj["id"])
        if role_init != "owner":
            await message.answer("Только владелец проекта может приглашать.")
            return
        await sb_insert("user_project_access", {
            "user_id": target["id"],
            "project_id": proj["id"],
            "role": data["role"],
        })
        await message.answer("Пользователь добавлен к проекту.")

@dp.message_handler(Command("invite"))
async def cmd_invite(message: types.Message):
    parts = message.text.split()
    if len(parts) != 4 or parts[1] not in ("channel", "project"):
        await message.reply("Использование:\n/invite channel <tg_id> <role>\n/invite project <proj_prefix> <role>")
        return
    _, scope, scope_id, role = parts
    role = role.lower()
    if role not in ("owner", "editor"):
        await message.reply("role = owner | editor")
        return
    inv_token = uuid.uuid4().hex
    payload = {
        "token": inv_token,
        "scope": scope,
        "scope_id": scope_id,
        "role": role,
        "created_by": (await ensure_user(message))["id"],
        "created_at": datetime.utcnow().isoformat(),
    }
    await sb_insert("posts", {
        "content": json.dumps(payload, ensure_ascii=False),
        "status": "invite",
    })
    deep_link = f"https://t.me/{(await bot.me()).username}?start={inv_token}"
    await message.reply(f"Приглашение:\n{deep_link}\nОтправьте ссылку пользователю.")

@dp.message_handler(Command("start"))
async def cmd_start(message: types.Message):
    await ensure_user(message)
    parts = message.get_args().split()
    if not parts:
        await message.answer("Добро пожаловать!")
        return
    token = parts[0]
    rows = await sb_select("posts", status="invite")
    invite = None
    for r in rows:
        try:
            data = json.loads(r["content"])
            if data.get("token") == token:
                invite = data
                break
        except Exception:
            continue
    if not invite:
        await message.answer("Ссылка недействительна или устарела.")
        return
    target = await ensure_user(message)
    if invite["scope"] == "channel":
        chan_rows = await sb_select("channels", tg_id=int(invite["scope_id"]))
        if chan_rows:
            await sb_insert("user_channel_access", {
                "user_id": target["id"],
                "channel_id": chan_rows[0]["id"],
                "role": invite["role"],
            })
    else:
        proj_rows = await sb_select("projects")
        proj = next((p for p in proj_rows if p["id"].startswith(invite["scope_id"])), None)
        if proj:
            await sb_insert("user_project_access", {
                "user_id": target["id"],
                "project_id": proj["id"],
                "role": invite["role"],
            })
    await sb_update("posts", {"id": r["id"]}, {"status": "invite_used"})
    await message.answer("✅ Вас добавили! Попробуйте /channels или /projects.")

# --- Timezone, сторис, репост, напоминания ---
def tz_of(user_row: dict) -> zoneinfo.ZoneInfo:
    try: return zoneinfo.ZoneInfo(user_row.get("timezone", "UTC"))
    except Exception: return zoneinfo.ZoneInfo("UTC")
def local_to_utc(local_dt: datetime, user_row: dict) -> datetime:
    tz = tz_of(user_row)
    return local_dt.replace(tzinfo=tz).astimezone(timezone.utc)
@dp.message_handler(Command("set_tz"))
async def cmd_set_tz(message: types.Message):
    parts = message.get_args().split()
    if not parts:
        await message.reply("Использование:\n/set_tz <Europe/Amsterdam>")
        return
    tz_name = parts[0]
    try:
        zoneinfo.ZoneInfo(tz_name)
    except Exception:
        await message.reply("Неверный TZ.")
        return
    user = await ensure_user(message)
    await sb_update("users", {"id": user["id"]}, {"timezone": tz_name})
    await message.reply(f"Часовой пояс установлен: {tz_name}")
@dp.message_handler(Command("my_tz"))
async def cmd_my_tz(message: types.Message):
    user = await ensure_user(message)
    await message.reply(f"Ваш TZ: {user.get('timezone', 'UTC')}")

@dp.message_handler(Command("story"))
async def cmd_story(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /story <post_id>")
        return
    post_id = parts[1]
    rows = await sb_select("posts", id=post_id)
    if not rows:
        await message.reply("Пост не найден.")
        return
    post = rows[0]
    if not post.get("media"):
        await message.reply("У поста нет медиа для сторис.")
        return
    try:
        await bot.send_story(chat_id=post["channel_id"], media=json.loads(post["media"]))
        await message.reply("Сторис опубликована.")
    except Exception as e:
        await message.reply(f"Ошибка публикации сторис: {e}")

@dp.message_handler(Command("repost"))
@require_owner_channel(2)
async def cmd_repost(message: types.Message):
    parts = message.text.split()
    if len(parts) != 4:
        await message.reply("Использование:\n/repost <from_chat_id> <msg_id> <to_chat_id>")
        return
    _, from_id, msg_id, to_id = parts
    try:
        f_id, m_id, t_id = int(from_id), int(msg_id), int(to_id)
    except ValueError:
        await message.reply("ID должны быть целыми числами.")
        return
    try:
        await bot.copy_message(chat_id=t_id, from_chat_id=f_id, message_id=m_id)
        await message.reply("Репост выполнен.")
    except Exception as e:
        await message.reply(f"Не удалось репостнуть: {e}")

@dp.message_handler(Command("scheduled"))
async def cmd_scheduled(message: types.Message):
    user = await ensure_user(message)
    acc = await sb_select("user_channel_access", user_id=user["id"])
    chan_ids = [a["channel_id"] for a in acc]
    chan_rows = await sb_select("channels")
    chan_map = {c["id"]: c for c in chan_rows}
    sched = await sb_select("scheduled_posts", status="pending")
    lines = []
    for row in sched:
        post = (await sb_select("posts", id=row["post_id"]))[0]
        if post["channel_id"] not in chan_ids: continue
        when = datetime.fromisoformat(row["scheduled_at"]).astimezone(tz_of(user))
        chan = chan_map.get(post["channel_id"], {})
        lines.append(f"• {when:%d %b %H:%M} — {chan.get('title') or chan.get('username')} <code>{post['id'][:8]}</code>")
    if not lines:
        await message.reply("Нет планов.")
    else:
        await message.reply("<b>Запланировано:</b>\n" + "\n".join(sorted(lines)))

async def remind_drafts():
    now = datetime.utcnow()
    drafts = await sb_select("draft_posts")
    user_posts: dict[str, list[str]] = defaultdict(list)
    for d in drafts:
        row = (await sb_select("posts", id=d["post_id"]))[0]
        if (now - datetime.fromisoformat(d["saved_at"])).total_seconds() > 86_400:
            user_posts[row["created_by"]].append(row["id"][:8])
    for user_id, pids in user_posts.items():
        author = (await sb_select("users", id=user_id))[0]
        if not author.get("notify_events"):
            continue
        try:
            await bot.send_message(author["tg_id"], "💡 Несданные черновики: " + ", ".join(pids))
        except Exception:
            pass

async def phase7_setup():
    while scheduler is None: await asyncio.sleep(0.2)
    scheduler.add_job(remind_drafts, trigger=IntervalTrigger(days=1, start_date=datetime.utcnow().replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(days=1)), id="remind_drafts", coalesce=True)

@dp.message_handler(Command("cancel"), state='*')
async def cancel_cmd(message: types.Message, state: FSMContext):
    if await state.get_state() is None:
        return
    await state.finish()
    await message.reply("Действие отменено.", reply_markup=types.ReplyKeyboardRemove())

async def on_startup(dp: Dispatcher):
    await ensure_schema()
    await migrate_phase5()
    await migrate_phase6()
    await migrate_phase7()
    global scheduler
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.start()
    await load_pending_schedules()
    asyncio.create_task(phase7_setup())

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
async def main():
    await ensure_schema()
    await migrate_phase5()
    await migrate_phase6()
    await migrate_phase7()
    global scheduler
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.start()
    await load_pending_schedules()
    # Запусти phase7_setup, remind_drafts и все фоновые таски, если есть
    await dp.start_polling(bot)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
