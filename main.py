import os
import json
import logging
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ContentType
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import executor
from supabase import create_client, Client
from aiogram.utils.exceptions import ChatNotFound, ChatAdminRequired, BadRequest

logging.basicConfig(level=logging.INFO)

API_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not API_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing BOT_TOKEN or Supabase configuration.")

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory cache for user data
user_cache = {}

# Menu button texts
MENU_BUTTONS = {
    "create_post": {"ru": "Создать пост", "en": "Create Post"},
    "view_drafts": {"ru": "Просмотр черновиков", "en": "View Drafts"},
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
        "ru": "Теперь отправьте изображение или другое медиа для поста, или /skip, чтобы пропустить добавление медиа.",
        "en": "Now send an image or other media for the post, or /skip to skip attaching media."
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
    "draft_saved": {
        "ru": "Пост сохранён как черновик.",
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
    "post_published": {
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
        "ru": "Отправьте @username или ID канала, который вы хотите добавить:",
        "en": "Please send the channel @username or ID that you want to add:"
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
        "ru": "Вы не администратор этого канала или бот не добавлен в администраторы.",
        "en": "You are not an admin of this channel, or the bot is not added as an admin."
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
        "ru": "Пользователь не найден. Убедитесь, что он запустил бота.",
        "en": "User not found. Make sure they have started the bot."
    },
    "user_already_editor": {
        "ru": "Этот пользователь уже имеет доступ к каналу.",
        "en": "This user already has access to the channel."
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
    "editor_added": {
        "ru": "Пользователь добавлен в канал как {role}.",
        "en": "User has been added to the channel as {role}."
    },
    "remove_editor_prompt": {
        "ru": "Выберите пользователя для удаления из редакторов:",
        "en": "Select a user to remove:"
    },
    "user_removed": {
        "ru": "Пользователь удалён из редакторов.",
        "en": "The user has been removed from editors."
    },
    "confirm_delete_channel": {
        "ru": "Вы уверены, что хотите удалить канал \"{title}\" из системы? Все черновики будут удалены.",
        "en": "Are you sure you want to remove channel \"{title}\" from the system? All drafts will be deleted."
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
    "no_permission": {
        "ru": "У вас нет прав для выполнения этого действия.",
        "en": "You do not have permission to perform this action."
    },
    "invalid_input": {
        "ru": "Неправильный формат. Попробуйте снова.",
        "en": "Invalid input format. Please try again."
    }
}

def main_menu_keyboard(lang: str) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(MENU_BUTTONS["create_post"][lang], MENU_BUTTONS["view_drafts"][lang])
    kb.row(MENU_BUTTONS["settings"][lang], MENU_BUTTONS["manage_channels"][lang])
    return kb

# Middleware to ensure user registration
class DBMiddleware(BaseMiddleware):
    async def on_pre_process_update(self, update: types.Update, data: dict):
        user = None
        if update.message:
            user = update.message.from_user
        elif update.callback_query:
            user = update.callback_query.from_user
        if user:
            tg_id = user.id
            username = user.username
            if username:
                name = "@" + username
            else:
                name = user.first_name
                if user.last_name:
                    name += " " + user.last_name
            if tg_id in user_cache:
                if user_cache[tg_id].get("name") != name:
                    supabase.table("users").update({"name": name}).eq("tg_id", tg_id).execute()
                    user_cache[tg_id]["name"] = name
            else:
                res = supabase.table("users").select("*").eq("tg_id", tg_id).execute()
                if res.data:
                    user_record = res.data[0]
                    if user_record["name"] != name:
                        supabase.table("users").update({"name": name}).eq("id", user_record["id"]).execute()
                        user_record["name"] = name
                    user_cache[tg_id] = {
                        "id": user_record["id"],
                        "name": user_record["name"],
                        "lang": user_record.get("language", "ru") if "language" in user_record else "ru"
                    }
                else:
                    res_insert = supabase.table("users").insert({"tg_id": tg_id, "name": name}).execute()
                    new_user = res_insert.data[0] if res_insert.data else None
                    if new_user:
                        user_cache[tg_id] = {
                            "id": new_user["id"],
                            "name": new_user["name"],
                            "lang": new_user.get("language", "ru") if "language" in new_user else "ru"
                        }
                    else:
                        logging.error("Failed to insert new user into database.")
            data["lang"] = user_cache[tg_id]["lang"]
            data["user_id"] = user_cache[tg_id]["id"]

dp.middleware.setup(DBMiddleware())

# FSM state groups
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_media = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    waiting_for_add_more = State()

class AddChannelState(StatesGroup):
    waiting_for_channel_info = State()

class AddEditorState(StatesGroup):
    waiting_for_username = State()
    waiting_for_role = State()

@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message):
    tg_id = message.from_user.id
    lang = user_cache.get(tg_id, {}).get("lang", "ru")
    welcome_text = TEXTS["welcome"][lang] + "\n" + TEXTS["menu_prompt"][lang]
    await message.reply(welcome_text, reply_markup=main_menu_keyboard(lang))

@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if not current_state:
        return
    # Remove any pending inline keyboards if applicable
    if current_state == PostStates.waiting_for_channel.state:
        data = await state.get_data()
        mid = data.get("select_msg_id")
        if mid:
            try:
                await bot.delete_message(message.chat.id, mid)
            except:
                pass
    if current_state == AddEditorState.waiting_for_role.state:
        data = await state.get_data()
        mid = data.get("manage_msg_id")
        if mid:
            try:
                await bot.delete_message(message.chat.id, mid)
            except:
                pass
    await state.finish()
    tg_id = message.from_user.id
    lang = user_cache.get(tg_id, {}).get("lang", "ru")
    await message.reply("Действие отменено." if lang == "ru" else "Action cancelled.", reply_markup=main_menu_keyboard(lang))

# Create Post flow
@dp.message_handler(commands=['newpost', 'createpost'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["create_post"]["ru"], MENU_BUTTONS["create_post"]["en"]], state='*')
async def start_create_post(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        tg_id = message.from_user.id
        lang = user_cache.get(tg_id, {}).get("lang", "ru")
        await message.reply(TEXTS["invalid_input"][lang])
        return
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channel_editors").select("channel_id, role").eq("user_id", user_id).in_("role", ["owner", "editor"]).execute()
    channels_access = res.data or []
    if not channels_access:
        await message.reply(TEXTS["no_edit_channels"][lang])
        return
    channel_ids = [entry["channel_id"] for entry in channels_access]
    res2 = supabase.table("channels").select("id, title").in_("id", channel_ids).execute()
    channels_list = res2.data or []
    if len(channels_list) > 1:
        kb = InlineKeyboardMarkup()
        for ch in channels_list:
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"selch:{ch['id']}"))
        msg = await message.reply(TEXTS["choose_channel_post"][lang], reply_markup=kb)
        await state.update_data(select_msg_id=msg.message_id)
        await PostStates.waiting_for_channel.set()
    else:
        channel_id = channels_list[0]["id"]
        await state.update_data(channel_id=channel_id)
        await PostStates.waiting_for_text.set()
        await message.reply(TEXTS["enter_post_text"][lang])

@dp.callback_query_handler(lambda c: c.data.startswith("selch:"), state=PostStates.waiting_for_channel)
async def cb_select_channel(call: types.CallbackQuery, state: FSMContext):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channel_editors").select("role").eq("channel_id", chan_id).eq("user_id", user_id).execute()
    if not res.data or res.data[0]["role"] not in ["owner", "editor"]:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        await state.finish()
        return
    await state.update_data(channel_id=chan_id)
    try:
        await call.message.delete()
    except:
        pass
    await call.answer()
    await PostStates.waiting_for_text.set()
    await bot.send_message(call.from_user.id, TEXTS["enter_post_text"][lang])

@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_text)
async def post_text_received(message: types.Message, state: FSMContext):
    text = message.text
    if text.startswith("/"):
        return  # ignore other commands
    await state.update_data(content=text)
    await PostStates.waiting_for_media.set()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["enter_post_media"][lang])

@dp.message_handler(commands=['skip'], state=PostStates.waiting_for_text)
async def skip_post_text(message: types.Message, state: FSMContext):
    await state.update_data(content="")
    await PostStates.waiting_for_media.set()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["enter_post_media"][lang])

@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT, ContentType.AUDIO, ContentType.ANIMATION], state=PostStates.waiting_for_text)
async def post_text_media_received(message: types.Message, state: FSMContext):
    caption = message.caption or ""
    await state.update_data(content=caption)
    media_type = None
    file_id = None
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
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
        await state.update_data(media_type=None, media_file_id=None)
    await PostStates.waiting_for_button_text.set()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["enter_button_text"][lang])

@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT, ContentType.AUDIO, ContentType.ANIMATION], state=PostStates.waiting_for_media)
async def post_media_received(message: types.Message, state: FSMContext):
    media_type = None
    file_id = None
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
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
    await state.update_data(media_type=media_type, media_file_id=file_id)
    await PostStates.waiting_for_button_text.set()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["enter_button_text"][lang])

@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_media)
async def wrong_media_input(message: types.Message, state: FSMContext):
    # If not using /skip, just remind to send media
    if message.text and not message.text.startswith("/"):
        tg_id = message.from_user.id
        lang = user_cache[tg_id]["lang"]
        await message.reply(TEXTS["enter_post_media"][lang])

@dp.message_handler(commands=['skip'], state=PostStates.waiting_for_media)
async def skip_post_media(message: types.Message, state: FSMContext):
    await state.update_data(media_type=None, media_file_id=None)
    await PostStates.waiting_for_button_text.set()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["enter_button_text"][lang])

@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_text)
async def button_text_received(message: types.Message, state: FSMContext):
    text = message.text
    if text.startswith("/"):
        return  # skip or other commands handled separately
    await state.update_data(current_button_text=text)
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    prompt = TEXTS["enter_button_url"][lang].format(btn_text=text)
    await PostStates.waiting_for_button_url.set()
    await message.reply(prompt)

@dp.message_handler(commands=['skip'], state=PostStates.waiting_for_button_text)
async def skip_buttons(message: types.Message, state: FSMContext):
    data = await state.get_data()
    channel_id = data.get("channel_id")
    user_id = user_cache[message.from_user.id]["id"]
    content = data.get("content", "")
    media_type = data.get("media_type")
    media_file_id = data.get("media_file_id")
    buttons = []
    supabase.table("posts").insert({
        "channel_id": channel_id,
        "user_id": user_id,
        "content": content,
        "media_type": media_type if media_type else None,
        "media_file_id": media_file_id if media_file_id else None,
        "buttons_json": None,
        "status": "draft"
    }).execute()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    await message.reply(TEXTS["draft_saved"][lang], reply_markup=main_menu_keyboard(lang))
    await state.finish()

@dp.message_handler(content_types=ContentType.TEXT, state=PostStates.waiting_for_button_url)
async def button_url_received(message: types.Message, state: FSMContext):
    url = message.text.strip()
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    if not url.lower().startswith(("http://", "https://", "tg://")):
        await message.reply(TEXTS["invalid_input"][lang])
        return
    data = await state.get_data()
    btn_text = data.get("current_button_text")
    if not btn_text:
        return
    buttons = data.get("buttons", [])
    buttons.append({"text": btn_text, "url": url})
    await state.update_data(buttons=buttons)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Да" if lang == "ru" else "Yes", callback_data="add_btn_yes"),
           InlineKeyboardButton("Нет" if lang == "ru" else "No", callback_data="add_btn_no"))
    await PostStates.waiting_for_add_more.set()
    await message.reply(TEXTS["ask_add_another_button"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_btn_yes", state=PostStates.waiting_for_add_more)
async def cb_add_button_yes(call: types.CallbackQuery, state: FSMContext):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
    await PostStates.waiting_for_button_text.set()
    await bot.send_message(call.from_user.id, TEXTS["enter_button_text"][lang])

@dp.callback_query_handler(lambda c: c.data == "add_btn_no", state=PostStates.waiting_for_add_more)
async def cb_add_button_no(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    channel_id = data.get("channel_id")
    user_id = user_cache[call.from_user.id]["id"]
    content = data.get("content", "")
    media_type = data.get("media_type")
    media_file_id = data.get("media_file_id")
    buttons = data.get("buttons", [])
    supabase.table("posts").insert({
        "channel_id": channel_id,
        "user_id": user_id,
        "content": content,
        "media_type": media_type if media_type else None,
        "media_file_id": media_file_id if media_file_id else None,
        "buttons_json": json.dumps(buttons) if buttons else None,
        "status": "draft"
    }).execute()
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
    await bot.send_message(call.from_user.id, TEXTS["draft_saved"][lang], reply_markup=main_menu_keyboard(lang))
    await state.finish()

# View Drafts flow
@dp.message_handler(commands=['drafts'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["view_drafts"]["ru"], MENU_BUTTONS["view_drafts"]["en"]], state='*')
async def view_drafts(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        tg_id = message.from_user.id
        lang = user_cache[tg_id]["lang"]
        await message.reply(TEXTS["invalid_input"][lang])
        return
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channel_editors").select("channel_id").eq("user_id", user_id).execute()
    channels_access = res.data or []
    if not channels_access:
        await message.reply(TEXTS["no_drafts"][lang])
        return
    channel_ids = [entry["channel_id"] for entry in channels_access]
    res2 = supabase.table("channels").select("id, title").in_("id", channel_ids).execute()
    channels_list = res2.data or []
    if len(channels_list) > 1:
        kb = InlineKeyboardMarkup()
        for ch in channels_list:
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"drafts:{ch['id']}"))
        await message.reply(TEXTS["choose_channel_drafts"][lang], reply_markup=kb)
    else:
        chan_id = channels_list[0]["id"]
        await send_drafts_list(message.chat.id, chan_id, lang)

@dp.callback_query_handler(lambda c: c.data.startswith("drafts:"))
async def cb_choose_drafts_channel(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channel_editors").select("role").eq("channel_id", chan_id).eq("user_id", user_id).execute()
    if not res.data:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    await call.answer()
    await send_drafts_list(call.from_user.id, chan_id, lang)
    try:
        await call.message.delete()
    except:
        pass

async def send_drafts_list(chat_id: int, channel_id: int, lang: str):
    res_ch = supabase.table("channels").select("title").eq("id", channel_id).execute()
    title = res_ch.data[0]["title"] if res_ch.data else "Channel"
    res_posts = supabase.table("posts").select("*").eq("channel_id", channel_id).eq("status", "draft").execute()
    drafts = res_posts.data or []
    if not drafts:
        await bot.send_message(chat_id, TEXTS["no_drafts"][lang])
        return
    header_text = TEXTS["drafts_header"][lang].format(channel=title)
    await bot.send_message(chat_id, header_text)
    # Determine user role
    user_entry = user_cache.get(chat_id)
    if not user_entry:
        res_user = supabase.table("users").select("id").eq("tg_id", chat_id).execute()
        user_entry = res_user.data[0] if res_user.data else None
        if user_entry:
            user_cache[chat_id] = {"id": user_entry["id"], "name": None, "lang": lang}
    user_role = None
    if user_entry:
        user_id = user_entry["id"] if isinstance(user_entry, dict) else user_entry.get("id")
        res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_id).eq("user_id", user_id).execute()
        if res_role.data:
            user_role = res_role.data[0]["role"]
    for post in drafts:
        content = post["content"] or ""
        media_type = post["media_type"]
        media_file_id = post["media_file_id"]
        buttons_json = post["buttons_json"]
        keyboard = None
        if buttons_json:
            try:
                btn_list = json.loads(buttons_json)
            except:
                btn_list = []
            if btn_list:
                keyboard = InlineKeyboardMarkup()
                for b in btn_list:
                    if "url" in b:
                        keyboard.add(InlineKeyboardButton(b["text"], url=b["url"]))
        if user_role in ["owner", "editor"]:
            if not keyboard:
                keyboard = InlineKeyboardMarkup()
            pub_cb = f"pub:{post['id']}"
            del_cb = f"delpost:{post['id']}"
            keyboard.add(InlineKeyboardButton("✅ Опубликовать" if lang == "ru" else "Publish", callback_data=pub_cb),
                         InlineKeyboardButton("🗑️ Удалить" if lang == "ru" else "Delete", callback_data=del_cb))
        try:
            if media_type and media_file_id:
                if media_type == "photo":
                    await bot.send_photo(chat_id, media_file_id, caption=content if content else None, reply_markup=keyboard)
                elif media_type == "video":
                    await bot.send_video(chat_id, media_file_id, caption=content if content else None, reply_markup=keyboard)
                elif media_type == "document":
                    await bot.send_document(chat_id, media_file_id, caption=content if content else None, reply_markup=keyboard)
                elif media_type == "audio":
                    await bot.send_audio(chat_id, media_file_id, caption=content if content else None, reply_markup=keyboard)
                elif media_type == "animation":
                    await bot.send_animation(chat_id, media_file_id, caption=content if content else None, reply_markup=keyboard)
                else:
                    await bot.send_message(chat_id, content, reply_markup=keyboard)
            else:
                await bot.send_message(chat_id, content if content else ("(без текста)" if lang == "ru" else "(no text)"), reply_markup=keyboard)
        except Exception as e:
            logging.error(f"Failed to send draft preview: {e}")

@dp.callback_query_handler(lambda c: c.data.startswith("pub:"))
async def cb_publish_post(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    post_id = int(call.data.split(":")[1])
    res = supabase.table("posts").select("*").eq("id", post_id).execute()
    if not res.data:
        await call.answer("Post not found.", show_alert=True)
        return
    post = res.data[0]
    channel_id = post["channel_id"]
    user_id = user_cache[tg_id]["id"]
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_id).eq("user_id", user_id).execute()
    if not res_role.data or res_role.data[0]["role"] not in ["owner", "editor"]:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    content = post["content"] or ""
    media_type = post["media_type"]
    media_file_id = post["media_file_id"]
    buttons_json = post["buttons_json"]
    reply_markup = None
    if buttons_json:
        try:
            btn_list = json.loads(buttons_json)
        except:
            btn_list = []
        if btn_list:
            reply_markup = InlineKeyboardMarkup()
            for b in btn_list:
                if "url" in b:
                    reply_markup.add(InlineKeyboardButton(b["text"], url=b["url"]))
    try:
        if media_type and media_file_id:
            if media_type == "photo":
                await bot.send_photo(post["channel_id"], media_file_id, caption=content if content else "", reply_markup=reply_markup)
            elif media_type == "video":
                await bot.send_video(post["channel_id"], media_file_id, caption=content if content else "", reply_markup=reply_markup)
            elif media_type == "document":
                await bot.send_document(post["channel_id"], media_file_id, caption=content if content else "", reply_markup=reply_markup)
            elif media_type == "audio":
                await bot.send_audio(post["channel_id"], media_file_id, caption=content if content else "", reply_markup=reply_markup)
            elif media_type == "animation":
                await bot.send_animation(post["channel_id"], media_file_id, caption=content if content else "", reply_markup=reply_markup)
            else:
                await bot.send_message(post["channel_id"], content, reply_markup=reply_markup)
        else:
            await bot.send_message(post["channel_id"], content if content else " ", reply_markup=reply_markup)
    except Exception as e:
        logging.error(f"Failed to publish post: {e}")
        await call.answer(TEXTS["not_admin"][lang], show_alert=True)
        return
    supabase.table("posts").update({"status": "published"}).eq("id", post_id).execute()
    try:
        await call.message.delete()
    except:
        pass
    await call.answer()
    await bot.send_message(call.from_user.id, TEXTS["post_published"][lang])

@dp.callback_query_handler(lambda c: c.data.startswith("delpost:"))
async def cb_delete_post(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    post_id = int(call.data.split(":")[1])
    res = supabase.table("posts").select("channel_id").eq("id", post_id).execute()
    if not res.data:
        await call.answer("Not found.", show_alert=True)
        return
    channel_id = res.data[0]["channel_id"]
    user_id = user_cache[tg_id]["id"]
    res_role = supabase.table("channel_editors").select("role").eq("channel_id", channel_id).eq("user_id", user_id).execute()
    if not res_role.data or res_role.data[0]["role"] not in ["owner", "editor"]:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    supabase.table("posts").delete().eq("id", post_id).execute()
    try:
        await call.message.delete()
    except:
        pass
    await call.answer()
    await bot.send_message(call.from_user.id, TEXTS["post_deleted"][lang])

# Manage Channels flow
@dp.message_handler(commands=['channels', 'manage'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["manage_channels"]["ru"], MENU_BUTTONS["manage_channels"]["en"]], state='*')
async def manage_channels_menu(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        tg_id = message.from_user.id
        lang = user_cache[tg_id]["lang"]
        await message.reply(TEXTS["invalid_input"][lang])
        return
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
    channels_owned = res.data or []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
    if channels_owned:
        for ch in channels_owned:
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
    text = TEXTS["manage_intro"][lang]
    if not channels_owned:
        text += "\n" + TEXTS["manage_intro_none"][lang]
    await message.reply(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_channel")
async def cb_add_channel(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    await call.answer()
    try:
        await call.message.delete()
    except:
        pass
    await AddChannelState.waiting_for_channel_info.set()
    await bot.send_message(call.from_user.id, TEXTS["prompt_add_channel"][lang])

@dp.message_handler(commands=['add_channel'], state='*')
async def cmd_add_channel(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        tg_id = message.from_user.id
        lang = user_cache[tg_id]["lang"]
        await message.reply(TEXTS["invalid_input"][lang])
        return
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    args = message.get_args()
    if args:
        channel_identifier = args.strip()
        if not channel_identifier:
            await message.reply(TEXTS["prompt_add_channel"][lang])
            return
        if channel_identifier.startswith("@"):
            channel_identifier = channel_identifier
        try:
            member = await bot.get_chat_member(channel_identifier, message.from_user.id)
            if member.status not in ("administrator", "creator"):
                await message.reply(TEXTS["not_admin"][lang])
                return
            chat = await bot.get_chat(channel_identifier)
            chat_id = chat.id
            title = chat.title or channel_identifier
        except ChatNotFound:
            await message.reply(TEXTS["channel_not_found"][lang])
            return
        except ChatAdminRequired:
            await message.reply(TEXTS["not_admin"][lang])
            return
        except BadRequest:
            await message.reply(TEXTS["channel_not_found"][lang])
            return
        res = supabase.table("channels").select("id, owner_id").eq("channel_id", chat_id).execute()
        if res.data:
            await message.reply(TEXTS["channel_exists"][lang])
            return
        new_channel = {"channel_id": chat_id, "title": title, "owner_id": user_cache[tg_id]["id"]}
        res_insert = supabase.table("channels").insert(new_channel).execute()
        if not res_insert.data:
            await message.reply("Failed to add channel.")
            return
        channel_rec = res_insert.data[0]
        supabase.table("channel_editors").insert({
            "channel_id": channel_rec["id"],
            "user_id": user_cache[tg_id]["id"],
            "role": "owner"
        }).execute()
        await message.reply(TEXTS["channel_added"][lang], reply_markup=main_menu_keyboard(lang))
    else:
        await AddChannelState.waiting_for_channel_info.set()
        await message.reply(TEXTS["prompt_add_channel"][lang])

@dp.message_handler(state=AddChannelState.waiting_for_channel_info, content_types=ContentType.TEXT)
async def add_channel_received(message: types.Message, state: FSMContext):
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    identifier = message.text.strip()
    if identifier.startswith("@"):
        identifier = identifier
    try:
        member = await bot.get_chat_member(identifier, message.from_user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply(TEXTS["not_admin"][lang])
            await state.finish()
            return
        chat = await bot.get_chat(identifier)
        chat_id = chat.id
        title = chat.title or identifier
    except ChatNotFound:
        await message.reply(TEXTS["channel_not_found"][lang])
        await state.finish()
        return
    except ChatAdminRequired:
        await message.reply(TEXTS["not_admin"][lang])
        await state.finish()
        return
    except BadRequest:
        await message.reply(TEXTS["channel_not_found"][lang])
        await state.finish()
        return
    res = supabase.table("channels").select("id, owner_id").eq("channel_id", chat_id).execute()
    if res.data:
        await message.reply(TEXTS["channel_exists"][lang])
        await state.finish()
        return
    new_channel = {"channel_id": chat_id, "title": title, "owner_id": user_cache[tg_id]["id"]}
    res_insert = supabase.table("channels").insert(new_channel).execute()
    if not res_insert.data:
        await message.reply("Ошибка при добавлении канала." if lang == "ru" else "Error adding channel.")
        await state.finish()
        return
    channel_rec = res_insert.data[0]
    supabase.table("channel_editors").insert({
        "channel_id": channel_rec["id"],
        "user_id": user_cache[tg_id]["id"],
        "role": "owner"
    }).execute()
    await message.reply(TEXTS["channel_added"][lang], reply_markup=main_menu_keyboard(lang))
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("manage:"))
async def cb_manage_channel(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("id, title, owner_id").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    channel = res.data[0]
    title = channel["title"]
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить редактора" if lang == "ru" else "Add Editor"), callback_data=f"addedit:{chan_id}"))
    kb.add(InlineKeyboardButton("➖ " + ("Удалить редактора" if lang == "ru" else "Remove Editor"), callback_data=f"remedit:{chan_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить канал" if lang == "ru" else "Delete Channel"), callback_data=f"delchan:{chan_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data="back_to_manage"))
    await call.answer()
    try:
        await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "back_to_manage")
async def cb_back_to_manage(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
    channels_owned = res.data or []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
    if channels_owned:
        for ch in channels_owned:
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
    text = TEXTS["manage_intro"][lang]
    if not channels_owned:
        text += "\n" + TEXTS["manage_intro_none"][lang]
    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("addedit:"))
async def cb_add_editor(call: types.CallbackQuery, state: FSMContext):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
    await state.update_data(channel_id=chan_id, channel_title=title)
    await AddEditorState.waiting_for_username.set()
    await bot.send_message(call.from_user.id, TEXTS["prompt_add_editor"][lang])

@dp.message_handler(state=AddEditorState.waiting_for_username, content_types=ContentType.TEXT)
async def add_editor_username(message: types.Message, state: FSMContext):
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    identifier = message.text.strip()
    target_user = None
    if identifier.isdigit():
        tid = int(identifier)
        res = supabase.table("users").select("*").eq("tg_id", tid).execute()
        if res.data:
            target_user = res.data[0]
    else:
        if identifier.startswith("@"):
            identifier = identifier[1:]
        res = supabase.table("users").select("*").eq("name", "@" + identifier).execute()
        if res.data:
            target_user = res.data[0]
    if not target_user:
        await message.reply(TEXTS["user_not_found"][lang])
        await state.finish()
        return
    target_user_id = target_user["id"]
    target_user_name = target_user["name"]
    data = await state.get_data()
    channel_id = data.get("channel_id")
    res_check = supabase.table("channel_editors").select("role").eq("channel_id", channel_id).eq("user_id", target_user_id).execute()
    if res_check.data:
        await message.reply(TEXTS["user_already_editor"][lang])
        await state.finish()
        return
    await state.update_data(new_editor_id=target_user_id, new_editor_name=target_user_name)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(TEXTS["role_editor"][lang], callback_data="role_editor"),
           InlineKeyboardButton(TEXTS["role_viewer"][lang], callback_data="role_viewer"))
    msg = await message.reply(TEXTS["choose_role"][lang], reply_markup=kb)
    await state.update_data(manage_msg_id=msg.message_id)
    await AddEditorState.waiting_for_role.set()

@dp.callback_query_handler(lambda c: c.data in ["role_editor", "role_viewer"], state=AddEditorState.waiting_for_role)
async def cb_select_role(call: types.CallbackQuery, state: FSMContext):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    data = await state.get_data()
    channel_id = data.get("channel_id")
    new_user_id = data.get("new_editor_id")
    title = data.get("channel_title", "")
    if not channel_id or not new_user_id:
        await call.answer("Error", show_alert=True)
        await state.finish()
        return
    role = "editor" if call.data == "role_editor" else "viewer"
    supabase.table("channel_editors").insert({
        "channel_id": channel_id,
        "user_id": new_user_id,
        "role": role
    }).execute()
    role_text = TEXTS["role_editor"][lang] if role == "editor" else TEXTS["role_viewer"][lang]
    await call.answer()
    try:
        await call.message.delete()
    except:
        await call.message.edit_reply_markup(reply_markup=None)
    await bot.send_message(call.from_user.id, TEXTS["editor_added"][lang].format(role=role_text))
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить редактора" if lang == "ru" else "Add Editor"), callback_data=f"addedit:{channel_id}"))
    kb.add(InlineKeyboardButton("➖ " + ("Удалить редактора" if lang == "ru" else "Remove Editor"), callback_data=f"remedit:{channel_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить канал" if lang == "ru" else "Delete Channel"), callback_data=f"delchan:{channel_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data="back_to_manage"))
    await bot.send_message(call.from_user.id, TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("remedit:"))
async def cb_remove_editor_menu(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    res_editors = supabase.table("channel_editors").select("user_id, role").eq("channel_id", chan_id).neq("role", "owner").execute()
    editors = res_editors.data or []
    if not editors:
        await call.answer("No editors.", show_alert=True)
        return
    user_ids = [e["user_id"] for e in editors]
    res_users = supabase.table("users").select("id, name").in_("id", user_ids).execute()
    users = res_users.data or []
    name_map = {u["id"]: u["name"] for u in users}
    kb = InlineKeyboardMarkup()
    for e in editors:
        uid = e["user_id"]
        role = e["role"]
        name = name_map.get(uid, str(uid))
        btn_text = f"{name} ({TEXTS['role_editor'][lang] if role == 'editor' else TEXTS['role_viewer'][lang]})"
        kb.add(InlineKeyboardButton(btn_text, callback_data=f"removeuser:{chan_id}:{uid}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data=f"manage:{chan_id}"))
    await call.answer()
    try:
        await call.message.edit_text(TEXTS["remove_editor_prompt"][lang], reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, TEXTS["remove_editor_prompt"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("removeuser:"))
async def cb_remove_user(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    parts = call.data.split(":")
    chan_id = int(parts[1])
    user_to_remove_id = int(parts[2])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("owner_id, title").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    supabase.table("channel_editors").delete().eq("channel_id", chan_id).eq("user_id", user_to_remove_id).execute()
    await call.answer()
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить редактора" if lang == "ru" else "Add Editor"), callback_data=f"addedit:{chan_id}"))
    kb.add(InlineKeyboardButton("➖ " + ("Удалить редактора" if lang == "ru" else "Remove Editor"), callback_data=f"remedit:{chan_id}"))
    kb.add(InlineKeyboardButton("🗑️ " + ("Удалить канал" if lang == "ru" else "Delete Channel"), callback_data=f"delchan:{chan_id}"))
    kb.add(InlineKeyboardButton("⬅️ " + ("Назад" if lang == "ru" else "Back"), callback_data="back_to_manage"))
    try:
        await call.message.edit_text(TEXTS["manage_channel_title"][lang].format(title=title), reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, TEXTS["user_removed"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("delchan:"))
async def cb_delete_channel_confirm(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("title, owner_id").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ " + ("Удалить" if lang == "ru" else "Yes"), callback_data=f"confirm_del:{chan_id}"))
    kb.add(InlineKeyboardButton("❌ " + ("Отмена" if lang == "ru" else "Cancel"), callback_data=f"manage:{chan_id}"))
    await call.answer()
    try:
        await call.message.edit_text(TEXTS["confirm_delete_channel"][lang].format(title=title), reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, TEXTS["confirm_delete_channel"][lang].format(title=title), reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_del:"))
async def cb_delete_channel(call: types.CallbackQuery):
    tg_id = call.from_user.id
    lang = user_cache[tg_id]["lang"]
    chan_id = int(call.data.split(":")[1])
    user_id = user_cache[tg_id]["id"]
    res = supabase.table("channels").select("title, owner_id").eq("id", chan_id).execute()
    if not res.data or res.data[0]["owner_id"] != user_id:
        await call.answer(TEXTS["no_permission"][lang], show_alert=True)
        return
    title = res.data[0]["title"]
    supabase.table("posts").delete().eq("channel_id", chan_id).execute()
    supabase.table("channel_editors").delete().eq("channel_id", chan_id).execute()
    supabase.table("channels").delete().eq("id", chan_id).execute()
    await call.answer()
    res2 = supabase.table("channels").select("id, title").eq("owner_id", user_id).execute()
    channels_owned = res2.data or []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ " + ("Добавить канал" if lang == "ru" else "Add Channel"), callback_data="add_channel"))
    if channels_owned:
        for ch in channels_owned:
            kb.add(InlineKeyboardButton(ch["title"], callback_data=f"manage:{ch['id']}"))
    text = TEXTS["channel_removed"][lang].format(title=title) + "\n\n" + TEXTS["manage_intro"][lang]
    if not channels_owned:
        text += "\n" + TEXTS["manage_intro_none"][lang]
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except:
        await bot.send_message(call.from_user.id, text, reply_markup=kb)

# Settings (language)
@dp.message_handler(commands=['settings'], state='*')
@dp.message_handler(lambda m: m.text in [MENU_BUTTONS["settings"]["ru"], MENU_BUTTONS["settings"]["en"]], state='*')
async def open_settings(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        tg_id = message.from_user.id
        lang = user_cache[tg_id]["lang"]
        await message.reply(TEXTS["invalid_input"][lang])
        return
    tg_id = message.from_user.id
    lang = user_cache[tg_id]["lang"]
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Русский", callback_data="lang_ru"), InlineKeyboardButton("English", callback_data="lang_en"))
    await message.reply(TEXTS["language_prompt"][lang], reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data in ["lang_ru", "lang_en"])
async def cb_set_language(call: types.CallbackQuery):
    tg_id = call.from_user.id
    new_lang = "ru" if call.data == "lang_ru" else "en"
    user_id = user_cache[tg_id]["id"]
    supabase.table("users").update({"language": new_lang}).eq("id", user_id).execute()
    user_cache[tg_id]["lang"] = new_lang
    await call.answer()
    try:
        await call.message.edit_text(TEXTS["language_changed"][new_lang])
    except:
        pass
    await bot.send_message(call.from_user.id, TEXTS["language_changed"][new_lang], reply_markup=main_menu_keyboard(new_lang))

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
