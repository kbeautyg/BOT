import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz # For timezone handling

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, InputMediaVideo

from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Bot and Dispatcher
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable not set.")
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.MARKDOWN_V2)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL or SUPABASE_KEY environment variables not set.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger.info("Supabase client initialized.")

# --- FSM States ---

class AddChannelStates(StatesGroup):
    waiting_for_channel_id = State()

class RemoveChannelStates(StatesGroup):
    waiting_for_channel_selection = State()
    waiting_for_confirmation = State()

class AddProjectStates(StatesGroup):
    waiting_for_project_name = State()

class MoveChannelStates(StatesGroup):
    waiting_for_channel_to_move_selection = State()
    waiting_for_project_selection = State()

class PostCreationStates(StatesGroup):
    waiting_for_channel_selection = State()
    waiting_for_post_content = State()
    waiting_for_media = State() # This state is not explicitly used yet, but kept for future clarity
    waiting_for_buttons = State()
    waiting_for_poll_question = State()
    waiting_for_poll_options = State()
    waiting_for_poll_type = State()
    waiting_for_preview_action = State()
    waiting_for_schedule_time = State()
    waiting_for_publish_as = State() # For "publish as" feature

class SchedulePostStates(StatesGroup):
    waiting_for_date = State()
    waiting_for_time = State()

class RepostStates(StatesGroup):
    waiting_for_forwarded_message = State()
    waiting_for_repost_channel_selection = State()
    waiting_for_repost_options = State() # For adding text, buttons to reposted message

class EditPostStates(StatesGroup):
    waiting_for_post_to_edit_selection = State()
    waiting_for_edit_option = State()
    waiting_for_new_text = State()
    waiting_for_new_media = State()
    waiting_for_new_buttons = State()
    waiting_for_new_poll_question = State()
    waiting_for_new_poll_options = State()
    waiting_for_new_poll_type = State()
    waiting_for_new_schedule_date = State()
    waiting_for_new_schedule_time = State()

class DeletePostStates(StatesGroup):
    waiting_for_post_to_delete_selection = State()
    waiting_for_delete_confirmation = State()

# --- Helper Functions ---

def escape_markdown(text: str) -> str:
    """Helper function to escape markdown characters for MarkdownV2."""
    # List of characters to escape in MarkdownV2
    # _ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

async def send_post_content(chat_id: int, text: str = None, media: list = None, buttons: list = None, poll: dict = None, parse_mode: str = types.ParseMode.HTML):
    """
    Helper function to send post content (text, media, buttons, poll) to a chat.
    Handles different media types and albums.
    Returns the sent message object(s) or None on failure.
    """
    reply_markup = None
    if buttons:
        inline_keyboard = []
        for btn in buttons:
            if 'url' in btn:
                inline_keyboard.append(InlineKeyboardButton(text=btn['text'], url=btn['url']))
            elif 'callback_data' in btn:
                inline_keyboard.append(InlineKeyboardButton(text=btn['text'], callback_data=btn['callback_data']))
            # Add other button types if needed (e.g., switch_inline_query)
        reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    sent_message = None
    try:
        if poll:
            # Telegram polls cannot have media or buttons directly attached to them in the same message.
            # They are separate message types.
            # If media or buttons are present, we'll send them separately or prioritize poll.
            # For simplicity, if poll is present, we only send the poll.
            # A more robust solution would send media/text first, then the poll.
            if poll['type'] == 'regular':
                sent_message = await bot.send_poll(
                    chat_id=chat_id,
                    question=poll['question'],
                    options=poll['options'],
                    is_anonymous=True,
                    reply_markup=reply_markup # Buttons can be attached to polls
                )
            elif poll['type'] == 'quiz':
                sent_message = await bot.send_quiz(
                    chat_id=chat_id,
                    question=poll['question'],
                    options=poll['options'],
                    correct_option_id=poll['correct_option_id'],
                    is_anonymous=True,
                    reply_markup=reply_markup # Buttons can be attached to quizzes
                )
        elif media:
            if len(media) > 1: # Album
                media_group = []
                for i, m_item in enumerate(media):
                    if m_item['type'] == 'photo':
                        media_group.append(InputMediaPhoto(m_item['file_id'], caption=text if i == 0 else None, parse_mode=parse_mode))
                    elif m_item['type'] == 'video':
                        media_group.append(InputMediaVideo(m_item['file_id'], caption=text if i == 0 else None, parse_mode=parse_mode))
                    # Add other media types to album if supported by Telegram (e.g., document, audio are not in MediaGroup)
                
                if media_group:
                    sent_message = await bot.send_media_group(chat_id=chat_id, media=media_group)
                    # For media groups, buttons can only be attached to the first message if it's a photo/video
                    # or sent as a separate message. Aiogram's send_media_group doesn't directly support reply_markup.
                    # A workaround is to send buttons as a separate message after the album.
                    if reply_markup:
                        await bot.send_message(chat_id=chat_id, text=" ", reply_markup=reply_markup) # Send empty message with buttons
            else: # Single media
                m_item = media[0]
                if m_item['type'] == 'photo':
                    sent_message = await bot.send_photo(chat_id=chat_id, photo=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
                elif m_item['type'] == 'video':
                    sent_message = await bot.send_video(chat_id=chat_id, video=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
                elif m_item['type'] == 'animation':
                    sent_message = await bot.send_animation(chat_id=chat_id, animation=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
                elif m_item['type'] == 'voice':
                    sent_message = await bot.send_voice(chat_id=chat_id, voice=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
                elif m_item['type'] == 'audio':
                    sent_message = await bot.send_audio(chat_id=chat_id, audio=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
                elif m_item['type'] == 'document':
                    sent_message = await bot.send_document(chat_id=chat_id, document=m_item['file_id'], caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
        elif text:
            sent_message = await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            logger.warning(f"Attempted to send empty post to {chat_id}")
            return None
    except Exception as e:
        logger.error(f"Failed to send post to {chat_id}: {e}")
        return None
    return sent_message

async def show_post_preview(user_telegram_id: int, state: FSMContext):
    """
    Generates and sends a preview of the post.
    """
    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])
    post_buttons = data.get('post_buttons', [])
    post_poll = data.get('post_poll')
    
    preview_message = "*Предварительный просмотр поста:*\n\n"
    
    if post_text:
        preview_message += post_text + "\n\n" # Use raw HTML text from message.html_text
    
    if post_media:
        preview_message += f"\\(Медиа: {len(post_media)} файл\\(ов\\)\\)\n"

    if post_buttons:
        preview_message += "\n*Кнопки:*\n"
        for btn in post_buttons:
            if 'url' in btn:
                preview_message += f"• [{escape_markdown(btn['text'])}]({escape_markdown(btn['url'])})\n"
            else:
                preview_message += f"• {escape_markdown(btn['text'])} \\(callback: `{escape_markdown(btn['callback_data'])}`\\)\n"
    
    if post_poll:
        preview_message += "\n*Опрос:*\n"
        preview_message += f"Вопрос: {escape_markdown(post_poll['question'])}\n"
        preview_message += "Варианты:\n"
        for i, option in enumerate(post_poll['options']):
            prefix = "✅ " if post_poll.get('type') == 'quiz' and post_poll.get('correct_option_id') == i else ""
            preview_message += f"• {prefix}{escape_markdown(option)}\n"
        preview_message += f"Тип: {escape_markdown(post_poll['type'])}\n"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Отправить сейчас", callback_data="publish_now"),
        InlineKeyboardButton("Запланировать", callback_data="schedule_post"),
        InlineKeyboardButton("Сохранить в черновики", callback_data="save_draft")
    )
    
    # Add "Publish as" option if the channel allows anonymous posting
    try:
        channel_response = supabase.table('channels').select('telegram_channel_id').eq('id', selected_channel_uuid).execute()
        telegram_channel_id = channel_response.data[0]['telegram_channel_id']
        chat_member = await bot.get_chat_member(telegram_channel_id, bot.id)
        if chat_member.can_post_messages and chat_member.can_be_anonymous:
            keyboard.add(InlineKeyboardButton("Опубликовать от лица...", callback_data="choose_publish_as"))
    except Exception as e:
        logger.warning(f"Could not check 'can_be_anonymous' for channel {selected_channel_uuid}: {e}")
        # If error, assume cannot publish as channel or skip the option

    keyboard.add(
        InlineKeyboardButton("Редактировать", callback_data="edit_post_content"), # Placeholder for editing
        InlineKeyboardButton("Отмена", callback_data="cancel_post_creation")
    )

    await bot.send_message(
        user_telegram_id,
        preview_message,
        reply_markup=keyboard,
        parse_mode=types.ParseMode.MARKDOWN_V2
    )
    await PostCreationStates.waiting_for_preview_action.set()

# --- Handlers ---

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """
    Handles the /start command.
    Registers the user in the database if they don't exist.
    """
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    try:
        # Check if user exists in Supabase
        response = supabase.table('users').select('id').eq('telegram_id', user_id).execute()
        user_data = response.data

        if not user_data:
            # User does not exist, insert new user
            insert_data = {
                'telegram_id': user_id,
                'username': username,
                'first_name': first_name,
                'last_name': last_name
            }
            insert_response = supabase.table('users').insert(insert_data).execute()
            if insert_response.data:
                logger.info(f"New user registered: {user_id} ({username})")
                await message.reply(
                    "Привет\\! Я бот для управления публикациями в Telegram каналах\\.\n\n"
                    "Я помогу тебе планировать, редактировать и публиковать посты, "
                    "управлять каналами и проектами, а также предоставлять доступ другим пользователям\\.\n\n"
                    "Для начала, ты можешь добавить свой канал с помощью команды /add\_channel\\."
                )
            else:
                logger.error(f"Failed to register user {user_id}: {insert_response.json()}")
                await message.reply("Произошла ошибка при регистрации\\. Пожалуйста, попробуй еще раз позже\\.")
        else:
            # User already exists
            logger.info(f"Existing user {user_id} ({username}) started the bot.")
            await message.reply(
                "С возвращением\\! Я готов помочь тебе с управлением постами\\.\n\n"
                "Ты можешь использовать команды для планирования постов, управления каналами и проектами\\.\n"
                "Например, /add\_channel для добавления нового канала или /list\_channels для просмотра твоих каналов\\."
            )
    except Exception as e:
        logger.error(f"Error in /start command for user {user_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.message_handler(commands=['add_channel'])
async def cmd_add_channel(message: types.Message):
    """
    Initiates the process of adding a new channel.
    """
    await message.reply(
        "Чтобы добавить канал, выполни следующие шаги:\n\n"
        "1\\. Добавь меня в свой канал как администратора с правами на публикацию сообщений и редактирование сообщений других администраторов\\.\n"
        "2\\. Перешли мне любое сообщение из этого канала\\.\n\n"
        "Я буду ждать пересланное сообщение\\."
    )
    await AddChannelStates.waiting_for_channel_id.set()

@dp.message_handler(state=AddChannelStates.waiting_for_channel_id, content_types=types.ContentTypes.ANY)
async def process_channel_forward(message: types.Message, state: FSMContext):
    """
    Processes the forwarded message to get channel ID and title.
    """
    user_telegram_id = message.from_user.id
    chat_id = None
    chat_title = None

    if message.forward_from_chat:
        chat_id = message.forward_from_chat.id
        chat_title = message.forward_from_chat.title
    elif message.chat.type in ['channel', 'supergroup']: # If bot is already in channel and user sends message directly
        chat_id = message.chat.id
        chat_title = message.chat.title
    else:
        await message.reply("Пожалуйста, перешлите мне сообщение из канала, который вы хотите добавить, или отправьте сообщение напрямую из канала, если я уже там администратор\\.")
        return

    if not chat_id or not chat_title:
        await message.reply("Не удалось получить информацию о канале\\. Убедитесь, что это сообщение из канала\\.")
        return

    # Check if bot is admin in the channel
    try:
        chat_member = await bot.get_chat_member(chat_id, bot.id)
        if not chat_member.is_chat_admin():
            await message.reply(
                f"Я не являюсь администратором в канале *{escape_markdown(chat_title)}*\\.\n"
                "Пожалуйста, убедитесь, что вы добавили меня как администратора с необходимыми правами\\."
            )
            return
        # Check specific permissions (can_post_messages, can_edit_messages)
        if not chat_member.can_post_messages or not chat_member.can_edit_messages:
            await message.reply(
                f"У меня недостаточно прав администратора в канале *{escape_markdown(chat_title)}*\\.\n"
                "Пожалуйста, дайте мне права на *публикацию сообщений* и *редактирование сообщений других администраторов*\\."
            )
            return

    except Exception as e:
        logger.error(f"Error checking bot admin status in channel {chat_id}: {e}")
        await message.reply(
            f"Не удалось проверить статус администратора в канале *{escape_markdown(chat_title)}*\\.\n"
            "Возможно, я не добавлен в канал или произошла ошибка Telegram API\\."
        )
        return

    try:
        # Get user's internal ID from Supabase
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Check if channel already exists
        channel_response = supabase.table('channels').select('id').eq('telegram_channel_id', chat_id).execute()
        channel_data = channel_response.data

        if channel_data:
            # Channel already exists, check if current user is owner
            existing_channel_id = channel_data[0]['id']
            channel_owner_response = supabase.table('channels').select('owner_id').eq('id', existing_channel_id).execute()
            channel_owner_id = channel_owner_response.data[0]['owner_id']

            if channel_owner_id == user_uuid:
                await message.reply(f"Канал *{escape_markdown(chat_title)}* уже добавлен и вы являетесь его владельцем\\.")
            else:
                # Channel exists, but current user is not the owner. Check if user has access.
                channel_user_response = supabase.table('channel_users').select('role').eq('channel_id', existing_channel_id).eq('user_id', user_uuid).execute()
                if channel_user_response.data:
                    await message.reply(f"Канал *{escape_markdown(chat_title)}* уже добавлен, и у вас есть к нему доступ\\.")
                else:
                    await message.reply(
                        f"Канал *{escape_markdown(chat_title)}* уже добавлен другим пользователем\\.\n"
                        "Если вы хотите получить доступ к этому каналу, попросите его владельца добавить вас\\."
                    )
            await state.finish()
            return

        # Insert new channel
        insert_channel_data = {
            'telegram_channel_id': chat_id,
            'title': chat_title,
            'owner_id': user_uuid
        }
        insert_channel_response = supabase.table('channels').insert(insert_channel_data).execute()

        if insert_channel_response.data:
            new_channel_uuid = insert_channel_response.data[0]['id']
            logger.info(f"Channel added: {chat_title} ({chat_id}) by user {user_telegram_id}")

            # Assign owner role to the user in channel_users table
            insert_channel_user_data = {
                'channel_id': new_channel_uuid,
                'user_id': user_uuid,
                'role': 'owner'
            }
            supabase.table('channel_users').insert(insert_channel_user_data).execute()

            await message.reply(
                f"Канал *{escape_markdown(chat_title)}* успешно добавлен\\.\n"
                "Теперь вы можете начать планировать посты для него\\."
            )
        else:
            logger.error(f"Failed to add channel {chat_id}: {insert_channel_response.json()}")
            await message.reply("Произошла ошибка при добавлении канала\\. Пожалуйста, попробуй еще раз позже\\.")

    except Exception as e:
        logger.error(f"Error processing channel forward for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при обработке канала\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.message_handler(commands=['list_channels'])
async def cmd_list_channels(message: types.Message):
    """
    Lists all channels the user has access to.
    """
    user_telegram_id = message.from_user.id

    try:
        # Get user's internal ID from Supabase
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        # Get channels the user has access to
        # We need to join channel_users with channels to get channel titles
        response = supabase.table('channel_users').select('channel_id, channels(title, telegram_channel_id, owner_id)').eq('user_id', user_uuid).execute()
        accessible_channels = response.data

        if not accessible_channels:
            await message.reply("У вас пока нет доступа ни к одному каналу\\. Используйте команду /add\_channel, чтобы добавить новый канал\\.")
            return

        channels_list_message = "*Ваши каналы:*\n\n"
        for item in accessible_channels:
            channel_info = item['channels']
            channel_title = escape_markdown(channel_info['title'])
            channel_telegram_id = channel_info['telegram_channel_id']
            channel_owner_id = channel_info['owner_id']

            # Determine if the current user is the owner of this specific channel
            is_owner = " \\(Владелец\\)" if channel_owner_id == user_uuid else ""
            
            # Create a clickable link to the channel if it's a public channel (username starts with @)
            # Or just show the ID if it's a private channel (ID is negative)
            if str(channel_telegram_id).startswith('-100'): # Supergroup/Channel ID format
                # Try to get chat info to see if it has a username (public link)
                try:
                    chat_info = await bot.get_chat(channel_telegram_id)
                    if chat_info.username:
                        channel_link = f"https://t.me/{chat_info.username}"
                        channels_list_message += f"• [{channel_title}]({channel_link}){is_owner}\n"
                    else:
                        channels_list_message += f"• {channel_title} \\(ID: `{channel_telegram_id}`\\){is_owner}\n"
                except Exception:
                    channels_list_message += f"• {channel_title} \\(ID: `{channel_telegram_id}`\\){is_owner}\n"
            else:
                channels_list_message += f"• {channel_title} \\(ID: `{channel_telegram_id}`\\){is_owner}\n"

        await message.reply(channels_list_message)

    except Exception as e:
        logger.error(f"Error in /list_channels command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при получении списка каналов\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.message_handler(commands=['remove_channel'])
async def cmd_remove_channel(message: types.Message):
    """
    Initiates the process of removing a channel.
    Only allows owners to remove channels.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        # Get channels where the current user is the owner
        response = supabase.table('channels').select('id, title, telegram_channel_id').eq('owner_id', user_uuid).execute()
        owned_channels = response.data

        if not owned_channels:
            await message.reply("У вас нет каналов, которыми вы владеете и которые можно удалить\\.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for channel in owned_channels:
            keyboard.add(InlineKeyboardButton(
                text=f"{channel['title']} (ID: {channel['telegram_channel_id']})",
                callback_data=f"remove_channel_{channel['id']}"
            ))
        
        await message.reply("Выберите канал, который вы хотите удалить:", reply_markup=keyboard)
        await RemoveChannelStates.waiting_for_channel_selection.set()

    except Exception as e:
        logger.error(f"Error in /remove_channel command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при подготовке к удалению канала\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('remove_channel_'), state=RemoveChannelStates.waiting_for_channel_selection)
async def process_remove_channel_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a channel to remove.
    """
    channel_uuid = callback_query.data.split('_')[2]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user is the owner of the selected channel
        channel_response = supabase.table('channels').select('id, title').eq('id', channel_uuid).eq('owner_id', user_uuid).execute()
        channel_data = channel_response.data

        if not channel_data:
            await bot.send_message(user_telegram_id, "Вы не являетесь владельцем этого канала или канал не найден\\.")
            await state.finish()
            return

        channel_title = channel_data[0]['title']
        
        await state.update_data(channel_to_remove_uuid=channel_uuid, channel_title=channel_title)

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Да, удалить", callback_data="confirm_remove_channel_yes"),
            InlineKeyboardButton("Нет, отмена", callback_data="confirm_remove_channel_no")
        )

        await bot.send_message(
            user_telegram_id,
            f"Вы уверены, что хотите удалить канал *{escape_markdown(channel_title)}*?\n"
            "Это действие необратимо и удалит все связанные запланированные посты\\.",
            reply_markup=keyboard
        )
        await RemoveChannelStates.waiting_for_confirmation.set()

    except Exception as e:
        logger.error(f"Error processing remove channel selection for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data in ["confirm_remove_channel_yes", "confirm_remove_channel_no"], state=RemoveChannelStates.waiting_for_confirmation)
async def process_remove_channel_confirmation(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the confirmation for channel removal.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    channel_uuid = data.get('channel_to_remove_uuid')
    channel_title = data.get('channel_title')

    if callback_query.data == "confirm_remove_channel_yes":
        try:
            # Delete channel from Supabase
            delete_response = supabase.table('channels').delete().eq('id', channel_uuid).execute()
            
            if delete_response.data:
                logger.info(f"Channel {channel_title} ({channel_uuid}) removed by user {user_telegram_id}")
                await bot.send_message(user_telegram_id, f"Канал *{escape_markdown(channel_title)}* успешно удален\\.")
            else:
                logger.error(f"Failed to remove channel {channel_uuid}: {delete_response.json()}")
                await bot.send_message(user_telegram_id, "Произошла ошибка при удалении канала\\. Пожалуйста, попробуй еще раз позже\\.")
        except Exception as e:
            logger.error(f"Error deleting channel {channel_uuid} for user {user_telegram_id}: {e}")
            await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при удалении канала\\. Пожалуйста, попробуй еще раз позже\\.")
    else:
        await bot.send_message(user_telegram_id, "Удаление канала отменено\\.")
    
    await state.finish()

@dp.message_handler(commands=['add_project'])
async def cmd_add_project(message: types.Message):
    """
    Initiates the process of adding a new project.
    """
    await message.reply("Пожалуйста, введите название нового проекта\\.")
    await AddProjectStates.waiting_for_project_name.set()

@dp.message_handler(state=AddProjectStates.waiting_for_project_name, content_types=types.ContentTypes.TEXT)
async def process_project_name(message: types.Message, state: FSMContext):
    """
    Processes the project name and saves it to Supabase.
    """
    project_name = message.text.strip()
    user_telegram_id = message.from_user.id

    if not project_name:
        await message.reply("Название проекта не может быть пустым\\. Пожалуйста, введите название\\.")
        return

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Check if project with this name already exists for this user
        existing_project_response = supabase.table('projects').select('id').eq('owner_id', user_uuid).eq('name', project_name).execute()
        if existing_project_response.data:
            await message.reply(f"Проект с названием *{escape_markdown(project_name)}* уже существует\\.\nПожалуйста, выберите другое название или используйте существующий проект\\.")
            await state.finish()
            return

        insert_data = {
            'owner_id': user_uuid,
            'name': project_name
        }
        insert_response = supabase.table('projects').insert(insert_data).execute()

        if insert_response.data:
            logger.info(f"Project '{project_name}' added by user {user_telegram_id}")
            await message.reply(f"Проект *{escape_markdown(project_name)}* успешно создан\\.")
        else:
            logger.error(f"Failed to add project '{project_name}': {insert_response.json()}")
            await message.reply("Произошла ошибка при создании проекта\\. Пожалуйста, попробуй еще раз позже\\.")

    except Exception as e:
        logger.error(f"Error in process_project_name for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при создании проекта\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.message_handler(commands=['list_projects'])
async def cmd_list_projects(message: types.Message):
    """
    Lists all projects owned by the user.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        response = supabase.table('projects').select('name').eq('owner_id', user_uuid).execute()
        projects = response.data

        if not projects:
            await message.reply("У вас пока нет созданных проектов\\. Используйте команду /add\_project, чтобы создать новый\\.")
            return

        projects_list_message = "*Ваши проекты:*\n\n"
        for project in projects:
            projects_list_message += f"• {escape_markdown(project['name'])}\n"

        await message.reply(projects_list_message)

    except Exception as e:
        logger.error(f"Error in /list_projects command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при получении списка проектов\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.message_handler(commands=['move_channel_to_project'])
async def cmd_move_channel_to_project(message: types.Message):
    """
    Initiates the process of moving a channel to a project.
    Only allows owners to move channels.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        # Get channels where the current user is the owner
        response = supabase.table('channels').select('id, title, telegram_channel_id, projects(name)').eq('owner_id', user_uuid).execute()
        owned_channels = response.data

        if not owned_channels:
            await message.reply("У вас нет каналов, которыми вы владеете и которые можно переместить\\.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for channel in owned_channels:
            project_name = channel['projects']['name'] if channel['projects'] else "Без проекта"
            keyboard.add(InlineKeyboardButton(
                text=f"{channel['title']} (Текущий проект: {project_name})",
                callback_data=f"move_channel_select_{channel['id']}"
            ))
        
        await message.reply("Выберите канал, который вы хотите переместить:", reply_markup=keyboard)
        await MoveChannelStates.waiting_for_channel_to_move_selection.set()

    except Exception as e:
        logger.error(f"Error in /move_channel_to_project command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при подготовке к перемещению канала\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('move_channel_select_'), state=MoveChannelStates.waiting_for_channel_to_move_selection)
async def process_channel_to_move_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a channel to move.
    Then prompts for project selection.
    """
    channel_uuid = callback_query.data.split('_')[3]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user is the owner of the selected channel
        channel_response = supabase.table('channels').select('id, title').eq('id', channel_uuid).eq('owner_id', user_uuid).execute()
        channel_data = channel_response.data

        if not channel_data:
            await bot.send_message(user_telegram_id, "Вы не являетесь владельцем этого канала или канал не найден\\.")
            await state.finish()
            return

        channel_title = channel_data[0]['title']
        
        await state.update_data(channel_to_move_uuid=channel_uuid, channel_title=channel_title)

        # Get user's projects
        projects_response = supabase.table('projects').select('id, name').eq('owner_id', user_uuid).execute()
        projects = projects.data

        keyboard = InlineKeyboardMarkup(row_width=1)
        for project in projects:
            keyboard.add(InlineKeyboardButton(
                text=project['name'],
                callback_data=f"select_project_{project['id']}"
            ))
        keyboard.add(InlineKeyboardButton(
            text="Без проекта",
            callback_data="select_project_none"
        ))

        await bot.send_message(
            user_telegram_id,
            f"Выбран канал: *{escape_markdown(channel_title)}*\\.\n"
            "Теперь выберите проект, в который вы хотите его переместить, или 'Без проекта':",
            reply_markup=keyboard
        )
        await MoveChannelStates.waiting_for_project_selection.set()

    except Exception as e:
        logger.error(f"Error processing channel to move selection for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('select_project_'), state=MoveChannelStates.waiting_for_project_selection)
async def process_project_selection_for_channel_move(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a project for the channel.
    """
    project_id = callback_query.data.split('_')[2]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    channel_uuid = data.get('channel_to_move_uuid')
    channel_title = data.get('channel_title')

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        selected_project_name = "Без проекта"
        update_data = {}

        if project_id == "none":
            update_data['project_id'] = None
        else:
            # Verify project belongs to the user
            project_response = supabase.table('projects').select('name').eq('id', project_id).eq('owner_id', user_uuid).execute()
            project_data = project_response.data
            if not project_data:
                await bot.send_message(user_telegram_id, "Выбранный проект не найден или не принадлежит вам\\.")
                await state.finish()
                return
            selected_project_name = project_data[0]['name']
            update_data['project_id'] = project_id
        
        update_response = supabase.table('channels').update(update_data).eq('id', channel_uuid).eq('owner_id', user_uuid).execute()

        if update_response.data:
            logger.info(f"Channel '{channel_title}' moved to project '{selected_project_name}' by user {user_telegram_id}")
            await bot.send_message(
                user_telegram_id,
                f"Канал *{escape_markdown(channel_title)}* успешно перемещен в проект *{escape_markdown(selected_project_name)}*\\."
            )
        else:
            logger.error(f"Failed to move channel {channel_uuid} to project {project_id}: {update_response.json()}")
            await bot.send_message(user_telegram_id, "Произошла ошибка при перемещении канала\\. Пожалуйста, попробуй еще раз позже\\.")

    except Exception as e:
        logger.error(f"Error processing project selection for channel move for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.message_handler(commands=['new_post'])
async def cmd_new_post(message: types.Message, state: FSMContext):
    """
    Initiates the post creation process by asking the user to select a channel.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        # Get channels the user has access to (owner or editor)
        response = supabase.table('channel_users').select('channel_id, channels(id, title, telegram_channel_id)').eq('user_id', user_uuid).execute()
        accessible_channels = response.data

        if not accessible_channels:
            await message.reply("У вас нет доступа ни к одному каналу для создания постов\\. Используйте команду /add\_channel, чтобы добавить новый канал\\.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for item in accessible_channels:
            channel_info = item['channels']
            keyboard.add(InlineKeyboardButton(
                text=channel_info['title'],
                callback_data=f"select_channel_for_post_{channel_info['id']}"
            ))
        
        await message.reply("Выберите канал, в который вы хотите опубликовать пост:", reply_markup=keyboard)
        await PostCreationStates.waiting_for_channel_selection.set()

    except Exception as e:
        logger.error(f"Error in /new_post command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при начале создания поста\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('select_channel_for_post_'), state=PostCreationStates.waiting_for_channel_selection)
async def process_channel_selection_for_post(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a channel for post creation.
    Then asks for the post content.
    """
    channel_uuid = callback_query.data.split('_')[4]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user has access to the selected channel
        channel_user_response = supabase.table('channel_users').select('role').eq('channel_id', channel_uuid).eq('user_id', user_uuid).execute()
        if not channel_user_response.data:
            await bot.send_message(user_telegram_id, "У вас нет доступа к этому каналу\\.")
            await state.finish()
            return

        channel_response = supabase.table('channels').select('title').eq('id', channel_uuid).execute()
        channel_title = channel_response.data[0]['title']
        
        await state.update_data(selected_channel_uuid=channel_uuid, selected_channel_title=channel_title, post_text=None, post_media=[], post_buttons=[], post_poll=None)

        await bot.send_message(
            user_telegram_id,
            f"Выбран канал: *{escape_markdown(channel_title)}*\\.\n"
            "Теперь отправьте мне текст для вашего поста\\. Вы можете использовать форматирование MarkdownV2\\.\n"
            "Если пост будет только с медиа, отправьте медиафайл без текста, а затем нажмите 'Продолжить'\\.\n"
            "Для отмены введите /cancel\\."
        )
        await PostCreationStates.waiting_for_post_content.set()

    except Exception as e:
        logger.error(f"Error processing channel selection for post for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.message_handler(state=PostCreationStates.waiting_for_post_content, content_types=types.ContentTypes.ANY)
async def process_post_content(message: types.Message, state: FSMContext):
    """
    Processes the text and/or media content for the post.
    """
    data = await state.get_data()
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])

    # Handle text
    if message.text:
        if post_text: # If text was already set (e.g., from a previous message in an album)
            await message.reply("Вы уже отправили текст или подпись\\. Для альбомов текст должен быть в первом сообщении\\.")
            return
        post_text = message.html_text # Use html_text to preserve formatting for later MarkdownV2 conversion
        await state.update_data(post_text=post_text)
        await message.reply(
            "Текст поста сохранен\\. Теперь вы можете:\n"
            "• Отправить медиафайл (фото, видео, гиф, голосовое, аудио, документ)\n"
            "• Отправить несколько фото/видео для создания альбома\n"
            "• Нажать 'Продолжить' для перехода к добавлению кнопок или публикации\\.",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Продолжить", callback_data="continue_post_creation"))
        )
        return

    # Handle media
    media_info = None
    if message.photo:
        media_info = {'type': 'photo', 'file_id': message.photo[-1].file_id} # Get the largest photo
    elif message.video:
        media_info = {'type': 'video', 'file_id': message.video.file_id}
    elif message.animation:
        media_info = {'type': 'animation', 'file_id': message.animation.file_id}
    elif message.voice:
        media_info = {'type': 'voice', 'file_id': message.voice.file_id}
    elif message.audio:
        media_info = {'type': 'audio', 'file_id': message.audio.file_id}
    elif message.document:
        media_info = {'type': 'document', 'file_id': message.document.file_id}
    elif message.poll:
        await message.reply("Опросы будут обрабатываться на следующем шаге\\. Пожалуйста, сначала отправьте текст или медиа\\.")
        return
    else:
        await message.reply("Неподдерживаемый тип контента\\. Пожалуйста, отправьте текст, фото, видео, гиф, голосовое, аудио или документ\\.")
        return

    if media_info:
        # For albums, the caption is only in the first message.
        # For single media, caption can be present.
        if message.caption:
            if post_text: # If text was already set, it means user sent text then media with caption
                await message.reply("Вы уже отправили текст поста\\. Подпись к медиа будет проигнорирована, если текст поста уже установлен\\.")
            else:
                post_text = message.caption_html # Use html_text to preserve formatting
                await state.update_data(post_text=post_text)

        post_media.append(media_info)
        await state.update_data(post_media=post_media)

        await message.reply(
            "Медиафайл добавлен\\. Вы можете добавить еще медиа для альбома или:\n"
            "• Нажать 'Продолжить' для перехода к добавлению кнопок или публикации\\.",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Продолжить", callback_data="continue_post_creation"))
        )
    else:
        await message.reply("Пожалуйста, отправьте текст или медиафайл\\.")

@dp.callback_query_handler(lambda c: c.data == 'continue_post_creation', state=PostCreationStates.waiting_for_post_content)
async def continue_post_creation(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Allows user to continue to the next step after providing text/media.
    """
    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])

    if not post_text and not post_media:
        await bot.send_message(callback_query.from_user.id, "Пожалуйста, сначала отправьте текст или медиа для поста\\.")
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Добавить inline-кнопки", callback_data="add_inline_buttons"),
        InlineKeyboardButton("Добавить опрос", callback_data="add_poll"),
        InlineKeyboardButton("Продолжить без кнопок/опроса", callback_data="skip_buttons_poll")
    )
    await bot.send_message(
        callback_query.from_user.id,
        "Что вы хотите добавить к посту?",
        reply_markup=keyboard
    )
    await PostCreationStates.waiting_for_buttons.set() # Move to the next state for buttons/poll

@dp.callback_query_handler(lambda c: c.data == 'add_inline_buttons', state=PostCreationStates.waiting_for_buttons)
async def add_inline_buttons_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts the user to add inline buttons.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Отправьте мне кнопки в следующем формате (каждая кнопка с новой строки):\n"
        "`Текст кнопки - URL` (для URL-кнопок)\n"
        "`Текст кнопки - callback_data` (для callback-кнопок)\n\n"
        "Пример:\n"
        "`Посетить сайт - https://example.com`\n"
        "`Нажми меня - my_callback_data`\n\n"
        "Для завершения добавления кнопок или если кнопок нет, нажмите 'Продолжить'\\.",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Продолжить", callback_data="continue_after_buttons"))
    )
    # We stay in waiting_for_buttons state to capture text input for buttons
    # The next handler will process this text
    
@dp.message_handler(state=PostCreationStates.waiting_for_buttons, content_types=types.ContentTypes.TEXT)
async def process_inline_buttons(message: types.Message, state: FSMContext):
    """
    Processes the inline buttons text input.
    """
    data = await state.get_data()
    current_buttons = data.get('post_buttons', [])
    
    new_buttons_raw = message.text.strip().split('\n')
    parsed_buttons = []

    for btn_raw in new_buttons_raw:
        parts = btn_raw.split(' - ', 1)
        if len(parts) == 2:
            button_text = parts[0].strip()
            button_value = parts[1].strip()
            
            if button_value.startswith('http://') or button_value.startswith('https://'):
                parsed_buttons.append({'text': button_text, 'url': button_value})
            else:
                parsed_buttons.append({'text': button_text, 'callback_data': button_value})
        else:
            await message.reply(f"Неверный формат кнопки: `{escape_markdown(btn_raw)}`\\. Используйте 'Текст кнопки \\- Значение'\\.")
            return # Stop processing and ask user to correct

    current_buttons.extend(parsed_buttons)
    await state.update_data(post_buttons=current_buttons)

    await message.reply(
        "Кнопки добавлены\\. Вы можете добавить еще кнопки или:\n"
        "• Нажать 'Продолжить' для перехода к добавлению опроса или публикации\\.",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Продолжить", callback_data="continue_after_buttons"))
    )

@dp.callback_query_handler(lambda c: c.data == 'continue_after_buttons' or c.data == 'skip_buttons_poll', state=PostCreationStates.waiting_for_buttons)
async def continue_after_buttons_or_skip(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Continues to the next step after buttons/poll or skips them.
    """
    await bot.answer_callback_query(callback_query.id)
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Добавить опрос", callback_data="add_poll"),
        InlineKeyboardButton("Продолжить без опроса", callback_data="skip_poll")
    )
    await bot.send_message(
        callback_query.from_user.id,
        "Хотите добавить опрос к посту?",
        reply_markup=keyboard
    )
    await PostCreationStates.waiting_for_poll_question.set() # Move to the next state for poll

@dp.callback_query_handler(lambda c: c.data == 'add_poll', state=PostCreationStates.waiting_for_poll_question)
async def add_poll_question_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts the user to enter the poll question.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Отправьте мне вопрос для опроса\\.\n"
        "Для отмены добавления опроса введите /cancel\\."
    )
    await PostCreationStates.waiting_for_poll_question.set() # Stay in this state to capture text input

@dp.message_handler(state=PostCreationStates.waiting_for_poll_question, content_types=types.ContentTypes.TEXT)
async def process_poll_question(message: types.Message, state: FSMContext):
    """
    Processes the poll question and asks for options.
    """
    poll_question = message.text.strip()
    if not poll_question:
        await message.reply("Вопрос для опроса не может быть пустым\\. Пожалуйста, введите вопрос\\.")
        return
    
    await state.update_data(poll_question=poll_question)
    await message.reply(
        "Вопрос для опроса сохранен\\. Теперь отправьте мне варианты ответов, каждый с новой строки\\.\n"
        "Минимум 2, максимум 10 вариантов\\.\n\n"
        "Пример:\n"
        "`Вариант 1`\n"
        "`Вариант 2`\n"
        "`Вариант 3`"
    )
    await PostCreationStates.waiting_for_poll_options.set()

@dp.message_handler(state=PostCreationStates.waiting_for_poll_options, content_types=types.ContentTypes.TEXT)
async def process_poll_options(message: types.Message, state: FSMContext):
    """
    Processes the poll options and asks for poll type.
    """
    poll_options_raw = message.text.strip().split('\n')
    poll_options = [opt.strip() for opt in poll_options_raw if opt.strip()]

    if not (2 <= len(poll_options) <= 10):
        await message.reply("Количество вариантов должно быть от 2 до 10\\. Пожалуйста, введите варианты еще раз\\.")
        return
    
    await state.update_data(poll_options=poll_options)

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Обычный опрос", callback_data="poll_type_regular"),
        InlineKeyboardButton("Викторина", callback_data="poll_type_quiz")
    )
    await message.reply(
        "Варианты ответов сохранены\\. Теперь выберите тип опроса:",
        reply_markup=keyboard
    )
    await PostCreationStates.waiting_for_poll_type.set()

@dp.callback_query_handler(lambda c: c.data.startswith('poll_type_'), state=PostCreationStates.waiting_for_poll_type)
async def process_poll_type(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of poll type (regular or quiz).
    If quiz, asks for correct option.
    """
    poll_type = callback_query.data.split('_')[2]
    await bot.answer_callback_query(callback_query.id)
    
    data = await state.get_data()
    poll_question = data['poll_question']
    poll_options = data['poll_options']

    if poll_type == 'regular':
        await state.update_data(post_poll={'question': poll_question, 'options': poll_options, 'type': 'regular'})
        await bot.send_message(callback_query.from_user.id, "Опрос успешно добавлен\\.")
        # Move to preview state
        await show_post_preview(callback_query.from_user.id, state)
    elif poll_type == 'quiz':
        keyboard = InlineKeyboardMarkup(row_width=1)
        for i, option in enumerate(poll_options):
            keyboard.add(InlineKeyboardButton(text=option, callback_data=f"quiz_correct_option_{i}"))
        
        await bot.send_message(
            callback_query.from_user.id,
            "Выберите правильный вариант ответа для викторины:",
            reply_markup=keyboard
        )
        # We stay in waiting_for_poll_type state to capture correct option selection
        # The next callback handler will differentiate.

@dp.callback_query_handler(lambda c: c.data.startswith('quiz_correct_option_'), state=PostCreationStates.waiting_for_poll_type)
async def process_quiz_correct_option(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of the correct option for a quiz.
    """
    correct_option_index = int(callback_query.data.split('_')[3])
    await bot.answer_callback_query(callback_query.id)

    data = await state.get_data()
    poll_question = data['poll_question']
    poll_options = data['poll_options']

    if not (0 <= correct_option_index < len(poll_options)):
        await bot.send_message(callback_query.from_user.id, "Неверный индекс правильного ответа\\. Пожалуйста, попробуйте еще раз\\.")
        return

    await state.update_data(post_poll={
        'question': poll_question,
        'options': poll_options,
        'type': 'quiz',
        'correct_option_id': correct_option_index
    })
    await bot.send_message(callback_query.from_user.id, "Викторина успешно добавлена\\.")
    # Move to preview state
    await show_post_preview(callback_query.from_user.id, state)

@dp.callback_query_handler(lambda c: c.data == 'skip_poll', state=PostCreationStates.waiting_for_poll_question)
async def skip_poll_creation(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Skips poll creation and moves to preview.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Добавление опроса пропущено\\.")
    await show_post_preview(callback_query.from_user.id, state)

@dp.callback_query_handler(lambda c: c.data == 'publish_now', state=PostCreationStates.waiting_for_preview_action)
async def publish_post_now(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Publishes the post immediately.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id, text="Отправляю пост...")

    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])
    post_buttons = data.get('post_buttons', [])
    post_poll = data.get('post_poll')

    try:
        # Get channel Telegram ID
        channel_response = supabase.table('channels').select('telegram_channel_id').eq('id', selected_channel_uuid).execute()
        telegram_channel_id = channel_response.data[0]['telegram_channel_id']

        # Get user's internal ID
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_uuid = user_response.data[0]['id']

        # Send the post
        sent_message = await send_post_content(
            chat_id=telegram_channel_id,
            text=post_text,
            media=post_media,
            buttons=post_buttons,
            poll=post_poll,
            parse_mode=types.ParseMode.HTML # Use HTML parse mode as we stored html_text/caption_html
        )

        if sent_message:
            # Save post to Supabase
            insert_data = {
                'channel_id': selected_channel_uuid,
                'creator_id': user_uuid,
                'text': post_text,
                'media': post_media,
                'buttons': post_buttons,
                'poll': post_poll,
                'schedule_time': datetime.now(pytz.utc).isoformat(), # Set current time as published time
                'status': 'published',
                'telegram_message_id': sent_message.message_id if not isinstance(sent_message, list) else sent_message[0].message_id # For albums, take first message ID
            }
            supabase.table('posts').insert(insert_data).execute()
            await bot.send_message(user_telegram_id, "Пост успешно опубликован\\! 🎉")
        else:
            await bot.send_message(user_telegram_id, "Не удалось опубликовать пост\\. Произошла ошибка\\.")

    except Exception as e:
        logger.error(f"Error publishing post for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при публикации поста\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'schedule_post', state=PostCreationStates.waiting_for_preview_action)
async def schedule_post_prompt_date(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts the user to enter the date for scheduling.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Введите дату публикации в формате `ГГГГ-ММ-ДД` (например, `2025-12-31`)\\.\n"
        "Для отмены введите /cancel\\."
    )
    await SchedulePostStates.waiting_for_date.set()

@dp.message_handler(state=SchedulePostStates.waiting_for_date, content_types=types.ContentTypes.TEXT)
async def process_schedule_date(message: types.Message, state: FSMContext):
    """
    Processes the scheduled date and asks for time.
    """
    date_str = message.text.strip()
    try:
        schedule_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        if schedule_date < datetime.now().date():
            await message.reply("Дата не может быть в прошлом\\. Пожалуйста, введите будущую дату\\.")
            return
        await state.update_data(schedule_date=schedule_date)
        await message.reply(
            "Дата сохранена\\. Теперь введите время публикации в формате `ЧЧ:ММ` (например, `14:30`)\\.\n"
            "Время будет учтено по вашему текущему часовому поясу (UTC, если не настроено)\\."
        )
        await SchedulePostStates.waiting_for_time.set()
    except ValueError:
        await message.reply("Неверный формат даты\\. Пожалуйста, используйте `ГГГГ-ММ-ДД` (например, `2025-12-31`)\\.")

@dp.message_handler(state=SchedulePostStates.waiting_for_time, content_types=types.ContentTypes.TEXT)
async def process_schedule_time(message: types.Message, state: FSMContext):
    """
    Processes the scheduled time, combines with date, and saves the post/repost as scheduled.
    """
    time_str = message.text.strip()
    user_telegram_id = message.from_user.id

    try:
        schedule_time = datetime.strptime(time_str, '%H:%M').time()
        data = await state.get_data()
        schedule_date = data['schedule_date']
        
        # Combine date and time
        scheduled_datetime_local = datetime.combine(schedule_date, schedule_time)

        # Get user's timezone (default to UTC if not set)
        user_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
        user_timezone_str = user_response.data[0].get('timezone', 'UTC')
        user_timezone = pytz.timezone(user_timezone_str)

        # Localize the datetime and convert to UTC
        scheduled_datetime_local = user_timezone.localize(scheduled_datetime_local)
        scheduled_datetime_utc = scheduled_datetime_local.astimezone(pytz.utc)

        if scheduled_datetime_utc < datetime.now(pytz.utc):
            await message.reply("Время не может быть в прошлом\\. Пожалуйста, введите будущее время\\.")
            return

        # Determine if it's a regular post or a repost
        original_chat_id = data.get('original_chat_id')
        original_message_id = data.get('original_message_id')

        post_data = {
            'channel_id': data.get('selected_channel_uuid'),
            'creator_id': (await supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()).data[0]['id'],
            'schedule_time': scheduled_datetime_utc.isoformat(),
            'status': 'scheduled'
        }

        if original_chat_id and original_message_id: # It's a repost
            post_data['text'] = data.get('repost_text')
            post_data['buttons'] = data.get('repost_buttons', [])
            post_data['media'] = [{'type': 'repost', 'original_chat_id': original_chat_id, 'original_message_id': original_message_id}]
            post_type_msg = "репост"
        else: # It's a regular post
            post_data['text'] = data.get('post_text')
            post_data['media'] = data.get('post_media', [])
            post_data['buttons'] = data.get('post_buttons', [])
            post_data['poll'] = data.get('post_poll')
            post_type_msg = "пост"

        insert_response = supabase.table('posts').insert(post_data).execute()

        if insert_response.data:
            logger.info(f"{post_type_msg.capitalize()} scheduled for channel {data.get('selected_channel_title')} by user {user_telegram_id} at {scheduled_datetime_utc}")
            await message.reply(
                f"Ваш {post_type_msg} успешно запланирован на *{escape_markdown(scheduled_datetime_local.strftime('%Y-%m-%d %H:%M'))}* "
                f"\\(ваш часовой пояс: {escape_markdown(user_timezone_str)}\\)\\."
            )
        else:
            logger.error(f"Failed to schedule {post_type_msg}: {insert_response.json()}")
            await message.reply(f"Произошла ошибка при планировании {post_type_msg}\\. Пожалуйста, попробуй еще раз позже\\.")

    except ValueError:
        await message.reply("Неверный формат времени\\. Пожалуйста, используйте `ЧЧ:ММ` (например, `14:30`)\\.")
    except Exception as e:
        logger.error(f"Error scheduling {post_type_msg} for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при планировании {post_type_msg}\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'save_draft', state=PostCreationStates.waiting_for_preview_action)
async def save_post_as_draft(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Saves the post as a draft.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id, text="Сохраняю черновик...")

    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])
    post_buttons = data.get('post_buttons', [])
    post_poll = data.get('post_poll')

    try:
        # Get user's internal ID
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_uuid = user_response.data[0]['id']

        insert_data = {
            'channel_id': selected_channel_uuid,
            'creator_id': user_uuid,
            'text': post_text,
            'media': post_media,
            'buttons': post_buttons,
            'poll': post_poll,
            'status': 'draft'
        }
        insert_response = supabase.table('posts').insert(insert_data).execute()

        if insert_response.data:
            logger.info(f"Post saved as draft for channel {data.get('selected_channel_title')} by user {user_telegram_id}")
            await bot.send_message(user_telegram_id, "Пост успешно сохранен в черновики\\.")
        else:
            logger.error(f"Failed to save post as draft: {insert_response.json()}")
            await bot.send_message(user_telegram_id, "Произошла ошибка при сохранении черновика\\. Пожалуйста, попробуй еще раз позже\\.")

    except Exception as e:
        logger.error(f"Error saving post as draft for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при сохранении черновика\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'choose_publish_as', state=PostCreationStates.waiting_for_preview_action)
async def choose_publish_as(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts the user to choose who to publish as.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id)

    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("От имени канала", callback_data="publish_as_channel"))
    keyboard.add(InlineKeyboardButton("От имени бота", callback_data="publish_as_bot"))
    # Add option for "От имени подписчика" if you implement fetching sender_chat_id
    # keyboard.add(InlineKeyboardButton("От имени подписчика (нужен sender_chat_id)", callback_data="publish_as_user"))

    await bot.send_message(
        user_telegram_id,
        "Выберите, от чьего имени опубликовать пост:",
        reply_markup=keyboard
    )
    await PostCreationStates.waiting_for_publish_as.set()

@dp.callback_query_handler(lambda c: c.data.startswith('publish_as_'), state=PostCreationStates.waiting_for_publish_as)
async def process_publish_as_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of who to publish as and proceeds to publish.
    """
    publish_as_type = callback_query.data.split('_')[2]
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id, text=f"Публикую от имени {publish_as_type}...")

    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    post_text = data.get('post_text')
    post_media = data.get('post_media', [])
    post_buttons = data.get('post_buttons', [])
    post_poll = data.get('post_poll')

    try:
        channel_response = supabase.table('channels').select('telegram_channel_id').eq('id', selected_channel_uuid).execute()
        telegram_channel_id = channel_response.data[0]['telegram_channel_id']

        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_uuid = user_response.data[0]['id']

        # Determine sender_chat_id if publishing as channel
        # Note: For direct send_message/send_photo, etc., the 'sender_chat_id' is implicitly handled
        # by the bot's admin rights (can_be_anonymous). If can_be_anonymous is True, it posts as channel.
        # If False, it posts as bot. There's no direct 'sender_chat_id' parameter for these methods.
        # This logic is more relevant for `copy_message` or `forward_message` if you want to preserve original sender.
        # For now, we'll rely on bot's admin permissions.
        # If 'publish_as_channel' is selected, we assume bot has `can_be_anonymous` and will post as channel.
        # If 'publish_as_bot' is selected, we assume bot does NOT have `can_be_anonymous` or we explicitly
        # want to post as bot (which is default if not anonymous).
        # This part needs careful testing with actual bot permissions.

        sent_message = await send_post_content(
            chat_id=telegram_channel_id,
            text=post_text,
            media=post_media,
            buttons=post_buttons,
            poll=post_poll,
            parse_mode=types.ParseMode.HTML
        )

        if sent_message:
            insert_data = {
                'channel_id': selected_channel_uuid,
                'creator_id': user_uuid,
                'text': post_text,
                'media': post_media,
                'buttons': post_buttons,
                'poll': post_poll,
                'schedule_time': datetime.now(pytz.utc).isoformat(),
                'status': 'published',
                'telegram_message_id': sent_message.message_id if not isinstance(sent_message, list) else sent_message[0].message_id
            }
            supabase.table('posts').insert(insert_data).execute()
            await bot.send_message(user_telegram_id, f"Пост успешно опубликован от имени *{escape_markdown(publish_as_type)}*! 🎉")
        else:
            await bot.send_message(user_telegram_id, "Не удалось опубликовать пост\\. Произошла ошибка\\.")

    except Exception as e:
        logger.error(f"Error publishing post as {publish_as_type} for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при публикации поста\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.message_handler(commands=['repost'])
async def cmd_repost(message: types.Message):
    """
    Initiates the reposting process by asking the user to forward a message.
    """
    await message.reply(
        "Пожалуйста, перешлите мне сообщение из канала или чата, которое вы хотите репостнуть\\.\n"
        "Для отмены введите /cancel\\."
    )
    await RepostStates.waiting_for_forwarded_message.set()

@dp.message_handler(state=RepostStates.waiting_for_forwarded_message, content_types=types.ContentTypes.ANY)
async def process_repost_forwarded_message(message: types.Message, state: FSMContext):
    """
    Processes the forwarded message for reposting.
    """
    if not message.forward_from_chat and not message.forward_from:
        await message.reply("Пожалуйста, перешлите мне сообщение из канала или чата\\.")
        return

    original_chat_id = message.forward_from_chat.id if message.forward_from_chat else message.forward_from.id
    original_message_id = message.forward_from_message_id

    if not original_chat_id or not original_message_id:
        await message.reply("Не удалось получить информацию о пересланном сообщении\\. Убедитесь, что это пересланное сообщение\\.")
        return

    await state.update_data(
        original_chat_id=original_chat_id,
        original_message_id=original_message_id,
        repost_text=message.caption_html if message.caption else None, # Initial text for repost
        repost_buttons=[] # No buttons by default
    )

    user_telegram_id = message.from_user.id
    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Get channels the user has access to (for reposting)
        response = supabase.table('channel_users').select('channel_id, channels(id, title, telegram_channel_id)').eq('user_id', user_uuid).execute()
        accessible_channels = response.data

        if not accessible_channels:
            await message.reply("У вас нет доступа ни к одному каналу для репоста\\. Используйте команду /add\_channel, чтобы добавить новый канал\\.")
            await state.finish()
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for item in accessible_channels:
            channel_info = item['channels']
            keyboard.add(InlineKeyboardButton(
                text=channel_info['title'],
                callback_data=f"repost_channel_select_{channel_info['id']}"
            ))
        
        await message.reply("Выберите канал, куда вы хотите репостнуть сообщение:", reply_markup=keyboard)
        await RepostStates.waiting_for_repost_channel_selection.set()

    except Exception as e:
        logger.error(f"Error processing forwarded message for repost for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при обработке пересланного сообщения\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('repost_channel_select_'), state=RepostStates.waiting_for_repost_channel_selection)
async def process_repost_channel_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a channel for reposting.
    Then offers options to add text/buttons.
    """
    selected_channel_uuid = callback_query.data.split('_')[3]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user has access to the selected channel
        channel_user_response = supabase.table('channel_users').select('role').eq('channel_id', selected_channel_uuid).eq('user_id', user_uuid).execute()
        if not channel_user_response.data:
            await bot.send_message(user_telegram_id, "У вас нет доступа к этому каналу\\.")
            await state.finish()
            return

        channel_response = supabase.table('channels').select('title', 'telegram_channel_id').eq('id', selected_channel_uuid).execute()
        selected_channel_title = channel_response.data[0]['title']
        telegram_channel_id = channel_response.data[0]['telegram_channel_id']
        
        await state.update_data(selected_channel_uuid=selected_channel_uuid, selected_channel_title=selected_channel_title, telegram_channel_id=telegram_channel_id)

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("Добавить текст/подпись", callback_data="repost_add_text"),
            InlineKeyboardButton("Добавить inline-кнопки", callback_data="repost_add_buttons"),
            InlineKeyboardButton("Репостнуть сейчас", callback_data="repost_now"),
            InlineKeyboardButton("Запланировать репост", callback_data="repost_schedule"),
            InlineKeyboardButton("Отмена", callback_data="cancel_repost")
        )
        await bot.send_message(
            user_telegram_id,
            f"Выбран канал для репоста: *{escape_markdown(selected_channel_title)}*\\.\n"
            "Что вы хотите сделать с репостом?",
            reply_markup=keyboard
        )
        await RepostStates.waiting_for_repost_options.set()

@dp.callback_query_handler(lambda c: c.data == 'repost_add_text', state=RepostStates.waiting_for_repost_options)
async def repost_add_text_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts for additional text/caption for the reposted message.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Отправьте текст или подпись для репоста\\. Вы можете использовать форматирование MarkdownV2\\.\n"
        "Для отмены введите /cancel\\."
    )
    await RepostStates.waiting_for_repost_options.set() # Stay in this state to capture text input

@dp.message_handler(state=RepostStates.waiting_for_repost_options, content_types=types.ContentTypes.TEXT)
async def process_repost_text(message: types.Message, state: FSMContext):
    """
    Processes the text/caption for the reposted message.
    """
    await state.update_data(repost_text=message.html_text)
    await message.reply("Текст/подпись для репоста сохранены\\.")
    # Return to options
    # Re-send options by calling the handler that displays them
    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    dummy_callback_query = types.CallbackQuery(id='dummy', from_user=message.from_user, message=message, chat_instance='dummy', data=f"repost_channel_select_{selected_channel_uuid}")
    await process_repost_channel_selection(dummy_callback_query, state)


@dp.callback_query_handler(lambda c: c.data == 'repost_add_buttons', state=RepostStates.waiting_for_repost_options)
async def repost_add_buttons_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts for inline buttons for the reposted message.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Отправьте мне кнопки в следующем формате (каждая кнопка с новой строки):\n"
        "`Текст кнопки - URL` (для URL-кнопок)\n"
        "`Текст кнопки - callback_data` (для callback-кнопок)\n\n"
        "Для завершения добавления кнопок или если кнопок нет, нажмите 'Продолжить'\\.",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Продолжить", callback_data="repost_continue_after_buttons"))
    )
    await RepostStates.waiting_for_repost_options.set() # Stay in this state to capture text input

@dp.message_handler(state=RepostStates.waiting_for_repost_options, content_types=types.ContentTypes.TEXT)
async def process_repost_buttons(message: types.Message, state: FSMContext):
    """
    Processes the inline buttons text input for reposted message.
    """
    data = await state.get_data()
    current_buttons = data.get('repost_buttons', [])
    
    new_buttons_raw = message.text.strip().split('\n')
    parsed_buttons = []

    for btn_raw in new_buttons_raw:
        parts = btn_raw.split(' - ', 1)
        if len(parts) == 2:
            button_text = parts[0].strip()
            button_value = parts[1].strip()
            
            if button_value.startswith('http://') or button_value.startswith('https://'):
                parsed_buttons.append({'text': button_text, 'url': button_value})
            else:
                parsed_buttons.append({'text': button_text, 'callback_data': button_value})
        else:
            await message.reply(f"Неверный формат кнопки: `{escape_markdown(btn_raw)}`\\. Используйте 'Текст кнопки \\- Значение'\\.")
            return # Stop processing and ask user to correct

    current_buttons.extend(parsed_buttons)
    await state.update_data(repost_buttons=current_buttons)

    await message.reply("Кнопки добавлены\\.")
    # Return to options
    # Re-send options by calling the handler that displays them
    data = await state.get_data()
    selected_channel_uuid = data.get('selected_channel_uuid')
    dummy_callback_query = types.CallbackQuery(id='dummy', from_user=message.from_user, message=message, chat_instance='dummy', data=f"repost_channel_select_{selected_channel_uuid}")
    await process_repost_channel_selection(dummy_callback_query, state)


@dp.callback_query_handler(lambda c: c.data == 'repost_continue_after_buttons', state=RepostStates.waiting_for_repost_options)
async def repost_continue_after_buttons(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Continues after adding buttons for repost.
    """
    await bot.answer_callback_query(callback_query.id)
    # Re-send options by calling the handler that displays them
    await process_repost_channel_selection(callback_query, state)

@dp.callback_query_handler(lambda c: c.data == 'repost_now', state=RepostStates.waiting_for_repost_options)
async def repost_message_now(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Reposts the message immediately.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id, text="Репост сообщения...")

    data = await state.get_data()
    original_chat_id = data.get('original_chat_id')
    original_message_id = data.get('original_message_id')
    selected_channel_uuid = data.get('selected_channel_uuid')
    telegram_channel_id = data.get('telegram_channel_id')
    repost_text = data.get('repost_text')
    repost_buttons = data.get('repost_buttons', [])

    reply_markup = None
    if repost_buttons:
        inline_keyboard = []
        for btn in repost_buttons:
            if 'url' in btn:
                inline_keyboard.append(InlineKeyboardButton(text=btn['text'], url=btn['url']))
            elif 'callback_data' in btn:
                inline_keyboard.append(InlineKeyboardButton(text=btn['text'], callback_data=btn['callback_data']))
        reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    try:
        # Use copy_message for more control (caption, reply_markup)
        sent_message = await bot.copy_message(
            chat_id=telegram_channel_id,
            from_chat_id=original_chat_id,
            message_id=original_message_id,
            caption=repost_text,
            parse_mode=types.ParseMode.HTML,
            reply_markup=reply_markup
        )

        if sent_message:
            user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
            user_uuid = user_response.data[0]['id']

            insert_data = {
                'channel_id': selected_channel_uuid,
                'creator_id': user_uuid,
                'text': repost_text, # Store the new caption
                'media': [{'type': 'repost', 'original_chat_id': original_chat_id, 'original_message_id': original_message_id}], # Indicate it's a repost
                'buttons': repost_buttons,
                'schedule_time': datetime.now(pytz.utc).isoformat(),
                'status': 'published',
                'telegram_message_id': sent_message.message_id
            }
            supabase.table('posts').insert(insert_data).execute()
            await bot.send_message(user_telegram_id, "Сообщение успешно репостнуто\\! 🎉")
        else:
            await bot.send_message(user_telegram_id, "Не удалось репостнуть сообщение\\. Произошла ошибка\\.")

    except Exception as e:
        logger.error(f"Error reposting message for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при репосте сообщения\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'repost_schedule', state=RepostStates.waiting_for_repost_options)
async def repost_schedule_prompt_date(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Prompts the user to enter the date for scheduling a repost.
    """
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "Введите дату публикации репоста в формате `ГГГГ-ММ-ДД` (например, `2025-12-31`)\\.\n"
        "Для отмены введите /cancel\\."
    )
    await SchedulePostStates.waiting_for_date.set() # Reuse SchedulePostStates for date/time

@dp.callback_query_handler(lambda c: c.data == 'cancel_repost', state=RepostStates.waiting_for_repost_options)
async def cancel_repost_creation(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Cancels repost creation.
    """
    await bot.answer_callback_query(callback_query.id)
    await state.finish()
    await bot.send_message(callback_query.from_user.id, "Репост отменен\\.")

@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    """
    Allows user to cancel any ongoing FSM process.
    """
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Нет активных операций для отмены\\.")
        return

    await state.finish()
    await message.reply("Операция отменена\\.")

@dp.message_handler(commands=['list_scheduled_posts', 'drafts'])
async def cmd_list_posts(message: types.Message):
    """
    Lists scheduled and draft posts for the user.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        channel_access_response = supabase.table('channel_users').select('channel_id').eq('user_id', user_uuid).execute()
        accessible_channel_ids = [item['channel_id'] for item in channel_access_response.data]

        if not accessible_channel_ids:
            await message.reply("У вас нет доступа ни к одному каналу\\. Нет постов для отображения\\.")
            return

        response = supabase.table('posts').select('id, text, media, schedule_time, status, channels(title)').in_('channel_id', accessible_channel_ids).or_('status.eq.scheduled,status.eq.draft').order('schedule_time', desc=False).execute()
        posts = response.data

        if not posts:
            await message.reply("У вас пока нет запланированных постов или черновиков\\.")
            return

        posts_list_message = "*Ваши запланированные посты и черновики:*\n\n"
        for post in posts:
            channel_title = escape_markdown(post['channels']['title'])
            post_status = post['status']
            post_id = post['id']
            
            post_summary = ""
            if post['text']:
                post_summary = post['text'][:50].replace('\n', ' ') + "..." if len(post['text']) > 50 else post['text'].replace('\n', ' ')
            elif post['media']:
                post_summary = f"\\(Медиа: {post['media'][0]['type']}\\)"
            elif post['poll']:
                post_summary = f"\\(Опрос: {post['poll']['question'][:30]}...\\)" if len(post['poll']['question']) > 30 else f"\\(Опрос: {post['poll']['question']}\\)"

            schedule_info = ""
            if post_status == 'scheduled' and post['schedule_time']:
                # Convert UTC time to user's local timezone for display
                user_timezone_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
                user_timezone_str = user_timezone_response.data[0].get('timezone', 'UTC')
                user_timezone = pytz.timezone(user_timezone_str)
                
                utc_dt = datetime.fromisoformat(post['schedule_time'].replace('Z', '+00:00'))
                local_dt = utc_dt.astimezone(user_timezone)
                schedule_info = f" на *{escape_markdown(local_dt.strftime('%Y-%m-%d %H:%M'))}*"

            posts_list_message += (
                f"• ID: `{post_id[:8]}`\\.\\.\\. \\- Канал: *{channel_title}*\n"
                f"  Статус: *{post_status}*{schedule_info}\n"
                f"  Содержание: {escape_markdown(post_summary)}\n\n"
            )
        
        await message.reply(posts_list_message, parse_mode=types.ParseMode.MARKDOWN_V2)

    except Exception as e:
        logger.error(f"Error in /list_scheduled_posts command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при получении списка постов\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.message_handler(commands=['edit_post'])
async def cmd_edit_post(message: types.Message):
    """
    Initiates the process of editing a post.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        channel_access_response = supabase.table('channel_users').select('channel_id').eq('user_id', user_uuid).execute()
        accessible_channel_ids = [item['channel_id'] for item in channel_access_response.data]

        if not accessible_channel_ids:
            await message.reply("У вас нет доступа ни к одному каналу\\. Нет постов для редактирования\\.")
            return

        response = supabase.table('posts').select('id, text, media, schedule_time, status, channels(title)').in_('channel_id', accessible_channel_ids).or_('status.eq.scheduled,status.eq.draft').order('schedule_time', desc=False).execute()
        posts_to_edit = response.data

        if not posts_to_edit:
            await message.reply("У вас нет запланированных постов или черновиков для редактирования\\.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for post in posts_to_edit:
            channel_title = escape_markdown(post['channels']['title'])
            post_status = post['status']
            post_summary = ""
            if post['text']:
                post_summary = post['text'][:30].replace('\n', ' ') + "..." if len(post['text']) > 30 else post['text'].replace('\n', ' ')
            elif post['media']:
                post_summary = f"\\(Медиа: {post['media'][0]['type']}\\)"
            
            schedule_info = ""
            if post_status == 'scheduled' and post['schedule_time']:
                user_timezone_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
                user_timezone_str = user_timezone_response.data[0].get('timezone', 'UTC')
                user_timezone = pytz.timezone(user_timezone_str)
                utc_dt = datetime.fromisoformat(post['schedule_time'].replace('Z', '+00:00'))
                local_dt = utc_dt.astimezone(user_timezone)
                schedule_info = f" на {local_dt.strftime('%Y-%m-%d %H:%M')}"

            keyboard.add(InlineKeyboardButton(
                text=f"[{post_status.capitalize()}] {channel_title}: {post_summary}{schedule_info}",
                callback_data=f"edit_post_select_{post['id']}"
            ))
        
        await message.reply("Выберите пост для редактирования:", reply_markup=keyboard)
        await EditPostStates.waiting_for_post_to_edit_selection.set()

    except Exception as e:
        logger.error(f"Error in /edit_post command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при подготовке к редактированию поста\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('edit_post_select_'), state=EditPostStates.waiting_for_post_to_edit_selection)
async def process_post_to_edit_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a post to edit and offers editing options.
    """
    post_uuid = callback_query.data.split('_')[3]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user has access to this post (either creator or has channel access)
        post_response = supabase.table('posts').select('*, channels(telegram_channel_id, title)').eq('id', post_uuid).or_(f'creator_id.eq.{user_uuid},channels.channel_users.user_id.eq.{user_uuid}').execute()
        post_data = post_response.data
        
        if not post_data:
            await bot.send_message(user_telegram_id, "Пост не найден или у вас нет прав на его редактирование\\.")
            await state.finish()
            return
        
        post_info = post_data[0]
        await state.update_data(editing_post_uuid=post_uuid, current_post_data=post_info)

        # Show current post content
        preview_message = "*Текущее содержимое поста:*\n\n"
        if post_info['text']:
            preview_message += post_info['text'] + "\n\n"
        if post_info['media']:
            preview_message += f"\\(Медиа: {len(post_info['media'])} файл\\(ов\\)\\)\n"
        if post_info['buttons']:
            preview_message += "\n*Кнопки:*\n"
            for btn in post_info['buttons']:
                if 'url' in btn:
                    preview_message += f"• [{escape_markdown(btn['text'])}]({escape_markdown(btn['url'])})\n"
                else:
                    preview_message += f"• {escape_markdown(btn['text'])} \\(callback: `{escape_markdown(btn['callback_data'])}`\\)\n"
        if post_info['poll']:
            preview_message += "\n*Опрос:*\n"
            preview_message += f"Вопрос: {escape_markdown(post_info['poll']['question'])}\n"
            preview_message += f"Тип: {escape_markdown(post_info['poll']['type'])}\n"
        
        preview_message += f"\nСтатус: *{post_info['status']}*\n"
        if post_info['schedule_time']:
            user_timezone_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
            user_timezone_str = user_timezone_response.data[0].get('timezone', 'UTC')
            user_timezone = pytz.timezone(user_timezone_str)
            utc_dt = datetime.fromisoformat(post_info['schedule_time'].replace('Z', '+00:00'))
            local_dt = utc_dt.astimezone(user_timezone)
            preview_message += f"Запланировано на: *{escape_markdown(local_dt.strftime('%Y-%m-%d %H:%M'))}* \\(ваш часовой пояс: {escape_markdown(user_timezone_str)}\\)\n"

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Изменить текст", callback_data="edit_option_text"),
            InlineKeyboardButton("Изменить медиа", callback_data="edit_option_media"),
            InlineKeyboardButton("Изменить кнопки", callback_data="edit_option_buttons"),
            InlineKeyboardButton("Изменить опрос", callback_data="edit_option_poll"),
            InlineKeyboardButton("Изменить время", callback_data="edit_option_time"),
            InlineKeyboardButton("Завершить редактирование", callback_data="edit_option_done"),
            InlineKeyboardButton("Отмена", callback_data="cancel_edit_post")
        )

        await bot.send_message(
            user_telegram_id,
            preview_message,
            reply_markup=keyboard,
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
        await EditPostStates.waiting_for_edit_option.set()

    except Exception as e:
        logger.error(f"Error processing post to edit selection for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_option_'), state=EditPostStates.waiting_for_edit_option)
async def process_edit_option(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selected editing option.
    """
    edit_option = callback_query.data.split('_')[2]
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    post_uuid = data['editing_post_uuid']

    if edit_option == 'text':
        await bot.send_message(user_telegram_id, "Отправьте новый текст для поста\\. Для удаления текста отправьте `-`\\.")
        await EditPostStates.waiting_for_new_text.set()
    elif edit_option == 'media':
        await bot.send_message(user_telegram_id, "Отправьте новые медиафайлы (фото, видео, гиф, голосовое, аудио, документ)\\. Для удаления медиа отправьте `-`\\.")
        await state.update_data(new_post_media=[]) # Reset media for new input
        await EditPostStates.waiting_for_new_media.set()
    elif edit_option == 'buttons':
        await bot.send_message(user_telegram_id, "Отправьте новые кнопки в формате `Текст - Значение` (каждая с новой строки)\\. Для удаления кнопок отправьте `-`\\.")
        await state.update_data(new_post_buttons=[]) # Reset buttons for new input
        await EditPostStates.waiting_for_new_buttons.set()
    elif edit_option == 'poll':
        await bot.send_message(user_telegram_id, "Отправьте новый вопрос для опроса\\. Для удаления опроса отправьте `-`\\.")
        await state.update_data(new_post_poll=None) # Reset poll for new input
        await EditPostStates.waiting_for_new_poll_question.set()
    elif edit_option == 'time':
        await bot.send_message(user_telegram_id, "Введите новую дату публикации в формате `ГГГГ-ММ-ДД` (например, `2025-12-31`)\\.")
        await EditPostStates.waiting_for_new_schedule_date.set()
    elif edit_option == 'done':
        await bot.send_message(user_telegram_id, "Редактирование завершено\\.")
        await state.finish()
    elif edit_option == 'cancel':
        await bot.send_message(user_telegram_id, "Редактирование отменено\\.")
        await state.finish()

# Handlers for new content input during editing
@dp.message_handler(state=EditPostStates.waiting_for_new_text, content_types=types.ContentTypes.TEXT)
async def process_new_text(message: types.Message, state: FSMContext):
    new_text = message.text.strip()
    if new_text == '-':
        new_text = None
    else:
        new_text = message.html_text # Preserve formatting
    
    await state.update_data(post_text=new_text)
    await message.reply("Текст обновлен\\.")
    await show_post_preview(message.from_user.id, state) # Show preview and options again

@dp.message_handler(state=EditPostStates.waiting_for_new_media, content_types=types.ContentTypes.ANY)
async def process_new_media(message: types.Message, state: FSMContext):
    if message.text and message.text.strip() == '-':
        await state.update_data(post_media=[])
        await message.reply("Медиа удалены\\.")
        await show_post_preview(message.from_user.id, state)
        return

    media_info = None
    if message.photo:
        media_info = {'type': 'photo', 'file_id': message.photo[-1].file_id}
    elif message.video:
        media_info = {'type': 'video', 'file_id': message.video.file_id}
    elif message.animation:
        media_info = {'type': 'animation', 'file_id': message.animation.file_id}
    elif message.voice:
        media_info = {'type': 'voice', 'file_id': message.voice.file_id}
    elif message.audio:
        media_info = {'type': 'audio', 'file_id': message.audio.file_id}
    elif message.document:
        media_info = {'type': 'document', 'file_id': message.document.file_id}
    else:
        await message.reply("Неподдерживаемый тип контента\\. Пожалуйста, отправьте медиафайл или '-' для удаления\\.")
        return

    data = await state.get_data()
    new_post_media = data.get('new_post_media', [])
    new_post_media.append(media_info)
    await state.update_data(new_post_media=new_post_media)

    await message.reply(
        "Медиафайл добавлен\\. Вы можете добавить еще медиа для альбома или:\n"
        "• Нажать 'Завершить добавление медиа' для продолжения\\.",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Завершить добавление медиа", callback_data="done_adding_media"))
    )

@dp.callback_query_handler(lambda c: c.data == 'done_adding_media', state=EditPostStates.waiting_for_new_media)
async def done_adding_media_for_edit(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    await state.update_data(post_media=data.get('new_post_media', [])) # Transfer new_post_media to post_media
    await bot.send_message(callback_query.from_user.id, "Медиа обновлены\\.")
    await show_post_preview(callback_query.from_user.id, state)

@dp.message_handler(state=EditPostStates.waiting_for_new_buttons, content_types=types.ContentTypes.TEXT)
async def process_new_buttons(message: types.Message, state: FSMContext):
    new_buttons_raw = message.text.strip()
    if new_buttons_raw == '-':
        await state.update_data(post_buttons=[])
        await message.reply("Кнопки удалены\\.")
        await show_post_preview(message.from_user.id, state)
        return

    parsed_buttons = []
    for btn_raw in new_buttons_raw.split('\n'):
        parts = btn_raw.split(' - ', 1)
        if len(parts) == 2:
            button_text = parts[0].strip()
            button_value = parts[1].strip()
            if button_value.startswith('http://') or button_value.startswith('https://'):
                parsed_buttons.append({'text': button_text, 'url': button_value})
            else:
                parsed_buttons.append({'text': button_text, 'callback_data': button_value})
        else:
            await message.reply(f"Неверный формат кнопки: `{escape_markdown(btn_raw)}`\\. Используйте 'Текст кнопки \\- Значение'\\.")
            return
    
    await state.update_data(post_buttons=parsed_buttons)
    await message.reply("Кнопки обновлены\\.")
    await show_post_preview(message.from_user.id, state)

@dp.message_handler(state=EditPostStates.waiting_for_new_poll_question, content_types=types.ContentTypes.TEXT)
async def process_new_poll_question(message: types.Message, state: FSMContext):
    new_poll_question = message.text.strip()
    if new_poll_question == '-':
        await state.update_data(post_poll=None)
        await message.reply("Опрос удален\\.")
        await show_post_preview(message.from_user.id, state)
        return
    
    await state.update_data(new_poll_question=new_poll_question)
    await message.reply("Вопрос для опроса сохранен\\. Теперь отправьте варианты ответов, каждый с новой строки\\.")
    await EditPostStates.waiting_for_new_poll_options.set()

@dp.message_handler(state=EditPostStates.waiting_for_new_poll_options, content_types=types.ContentTypes.TEXT)
async def process_new_poll_options(message: types.Message, state: FSMContext):
    new_poll_options_raw = message.text.strip().split('\n')
    new_poll_options = [opt.strip() for opt in new_poll_options_raw if opt.strip()]

    if not (2 <= len(new_poll_options) <= 10):
        await message.reply("Количество вариантов должно быть от 2 до 10\\. Пожалуйста, введите варианты еще раз\\.")
        return
    
    await state.update_data(new_poll_options=new_poll_options)

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Обычный опрос", callback_data="new_poll_type_regular"),
        InlineKeyboardButton("Викторина", callback_data="new_poll_type_quiz")
    )
    await message.reply(
        "Варианты ответов сохранены\\. Теперь выберите тип опроса:",
        reply_markup=keyboard
    )
    await EditPostStates.waiting_for_new_poll_type.set()

@dp.callback_query_handler(lambda c: c.data.startswith('new_poll_type_'), state=EditPostStates.waiting_for_new_poll_type)
async def process_new_poll_type(callback_query: types.CallbackQuery, state: FSMContext):
    poll_type = callback_query.data.split('_')[3]
    await bot.answer_callback_query(callback_query.id)
    
    data = await state.get_data()
    new_poll_question = data['new_poll_question']
    new_poll_options = data['new_poll_options']

    if poll_type == 'regular':
        await state.update_data(post_poll={'question': new_poll_question, 'options': new_poll_options, 'type': 'regular'})
        await bot.send_message(callback_query.from_user.id, "Опрос обновлен\\.")
        await show_post_preview(callback_query.from_user.id, state)
    elif poll_type == 'quiz':
        keyboard = InlineKeyboardMarkup(row_width=1)
        for i, option in enumerate(new_poll_options):
            keyboard.add(InlineKeyboardButton(text=option, callback_data=f"new_quiz_correct_option_{i}"))
        
        await bot.send_message(
            callback_query.from_user.id,
            "Выберите правильный вариант ответа для викторины:",
            reply_markup=keyboard
        )
        # Stay in this state to capture correct option selection

@dp.callback_query_handler(lambda c: c.data.startswith('new_quiz_correct_option_'), state=EditPostStates.waiting_for_new_poll_type)
async def process_new_quiz_correct_option(callback_query: types.CallbackQuery, state: FSMContext):
    correct_option_index = int(callback_query.data.split('_')[4])
    await bot.answer_callback_query(callback_query.id)

    data = await state.get_data()
    new_poll_question = data['new_poll_question']
    new_poll_options = data['new_poll_options']

    if not (0 <= correct_option_index < len(new_poll_options)):
        await bot.send_message(callback_query.from_user.id, "Неверный индекс правильного ответа\\. Пожалуйста, попробуйте еще раз\\.")
        return

    await state.update_data(post_poll={
        'question': new_poll_question,
        'options': new_poll_options,
        'type': 'quiz',
        'correct_option_id': correct_option_index
    })
    await bot.send_message(callback_query.from_user.id, "Викторина обновлена\\.")
    await show_post_preview(callback_query.from_user.id, state)

@dp.message_handler(state=EditPostStates.waiting_for_new_schedule_date, content_types=types.ContentTypes.TEXT)
async def process_new_schedule_date(message: types.Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        schedule_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        if schedule_date < datetime.now().date():
            await message.reply("Дата не может быть в прошлом\\. Пожалуйста, введите будущую дату\\.")
            return
        await state.update_data(new_schedule_date=schedule_date)
        await message.reply(
            "Дата сохранена\\. Теперь введите новое время публикации в формате `ЧЧ:ММ` (например, `14:30`)\\."
        )
        await EditPostStates.waiting_for_new_schedule_time.set()
    except ValueError:
        await message.reply("Неверный формат даты\\. Пожалуйста, используйте `ГГГГ-ММ-ДД` (например, `2025-12-31`)\\.")

@dp.message_handler(state=EditPostStates.waiting_for_new_schedule_time, content_types=types.ContentTypes.TEXT)
async def process_new_schedule_time(message: types.Message, state: FSMContext):
    time_str = message.text.strip()
    user_telegram_id = message.from_user.id

    try:
        schedule_time = datetime.strptime(time_str, '%H:%M').time()
        data = await state.get_data()
        new_schedule_date = data['new_schedule_date']
        
        scheduled_datetime_local = datetime.combine(new_schedule_date, schedule_time)

        user_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
        user_timezone_str = user_response.data[0].get('timezone', 'UTC')
        user_timezone = pytz.timezone(user_timezone_str)

        scheduled_datetime_local = user_timezone.localize(scheduled_datetime_local)
        scheduled_datetime_utc = scheduled_datetime_local.astimezone(pytz.utc)

        if scheduled_datetime_utc < datetime.now(pytz.utc):
            await message.reply("Новое время не может быть в прошлом\\. Пожалуйста, введите будущее время\\.")
            return

        await state.update_data(schedule_time=scheduled_datetime_utc.isoformat(), status='scheduled')
        await message.reply(
            f"Время публикации обновлено на *{escape_markdown(scheduled_datetime_local.strftime('%Y-%m-%d %H:%M'))}* "
            f"\\(ваш часовой пояс: {escape_markdown(user_timezone_str)}\\)\\."
        )
        await show_post_preview(message.from_user.id, state)

    except ValueError:
        await message.reply("Неверный формат времени\\. Пожалуйста, используйте `ЧЧ:ММ` (например, `14:30`)\\.")
    except Exception as e:
        logger.error(f"Error processing new schedule time for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при обновлении времени\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data == 'cancel_edit_post', state=EditPostStates.waiting_for_edit_option)
async def cancel_edit_post(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Cancels post editing.
    """
    await bot.answer_callback_query(callback_query.id)
    await state.finish()
    await bot.send_message(callback_query.from_user.id, "Редактирование поста отменено\\.")

@dp.callback_query_handler(lambda c: c.data == 'edit_option_done', state=EditPostStates.waiting_for_edit_option)
async def finalize_post_edit(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Finalizes post editing and updates the database.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id, text="Сохраняю изменения...")

    data = await state.get_data()
    post_uuid = data['editing_post_uuid']
    
    update_data = {
        'text': data.get('post_text'),
        'media': data.get('post_media', []),
        'buttons': data.get('post_buttons', []),
        'poll': data.get('post_poll'),
        'schedule_time': data.get('schedule_time'),
        'status': data.get('status', 'draft'), # Ensure status is updated if time was set
        'updated_at': datetime.now(pytz.utc).isoformat()
    }

    try:
        update_response = supabase.table('posts').update(update_data).eq('id', post_uuid).execute()

        if update_response.data:
            logger.info(f"Post {post_uuid} updated by user {user_telegram_id}")
            await bot.send_message(user_telegram_id, "Пост успешно обновлен\\!")
        else:
            logger.error(f"Failed to update post {post_uuid}: {update_response.json()}")
            await bot.send_message(user_telegram_id, "Произошла ошибка при обновлении поста\\. Пожалуйста, попробуй еще раз позже\\.")

    except Exception as e:
        logger.error(f"Error finalizing post edit for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при обновлении поста\\. Пожалуйста, попробуй еще раз позже\\.")
    finally:
        await state.finish()

@dp.message_handler(commands=['delete_post'])
async def cmd_delete_post(message: types.Message):
    """
    Initiates the process of deleting a post.
    """
    user_telegram_id = message.from_user.id

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await message.reply("Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            return
        user_uuid = user_data[0]['id']

        channel_access_response = supabase.table('channel_users').select('channel_id').eq('user_id', user_uuid).execute()
        accessible_channel_ids = [item['channel_id'] for item in channel_access_response.data]

        if not accessible_channel_ids:
            await message.reply("У вас нет доступа ни к одному каналу\\. Нет постов для удаления\\.")
            return

        response = supabase.table('posts').select('id, text, media, schedule_time, status, channels(title)').in_('channel_id', accessible_channel_ids).or_('status.eq.scheduled,status.eq.draft').order('schedule_time', desc=False).execute()
        posts_to_delete = response.data

        if not posts_to_delete:
            await message.reply("У вас нет запланированных постов или черновиков для удаления\\.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for post in posts_to_delete:
            channel_title = escape_markdown(post['channels']['title'])
            post_status = post['status']
            post_summary = ""
            if post['text']:
                post_summary = post['text'][:30].replace('\n', ' ') + "..." if len(post['text']) > 30 else post['text'].replace('\n', ' ')
            elif post['media']:
                post_summary = f"\\(Медиа: {post['media'][0]['type']}\\)"
            
            schedule_info = ""
            if post_status == 'scheduled' and post['schedule_time']:
                user_timezone_response = supabase.table('users').select('timezone').eq('telegram_id', user_telegram_id).execute()
                user_timezone_str = user_timezone_response.data[0].get('timezone', 'UTC')
                user_timezone = pytz.timezone(user_timezone_str)
                utc_dt = datetime.fromisoformat(post['schedule_time'].replace('Z', '+00:00'))
                local_dt = utc_dt.astimezone(user_timezone)
                schedule_info = f" на {local_dt.strftime('%Y-%m-%d %H:%M')}"

            keyboard.add(InlineKeyboardButton(
                text=f"[{post_status.capitalize()}] {channel_title}: {post_summary}{schedule_info}",
                callback_data=f"delete_post_select_{post['id']}"
            ))
        
        await message.reply("Выберите пост для удаления:", reply_markup=keyboard)
        await DeletePostStates.waiting_for_post_to_delete_selection.set()

    except Exception as e:
        logger.error(f"Error in /delete_post command for user {user_telegram_id}: {e}")
        await message.reply("Произошла непредвиденная ошибка при подготовке к удалению поста\\. Пожалуйста, попробуй еще раз позже\\.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('delete_post_select_'), state=DeletePostStates.waiting_for_post_to_delete_selection)
async def process_post_to_delete_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the selection of a post to delete and asks for confirmation.
    """
    post_uuid = callback_query.data.split('_')[3]
    user_telegram_id = callback_query.from_user.id

    await bot.answer_callback_query(callback_query.id)

    try:
        user_response = supabase.table('users').select('id').eq('telegram_id', user_telegram_id).execute()
        user_data = user_response.data
        if not user_data:
            await bot.send_message(user_telegram_id, "Ваш пользовательский аккаунт не найден\\. Пожалуйста, начните с команды /start\\.")
            await state.finish()
            return
        user_uuid = user_data[0]['id']

        # Verify user has access to this post (either creator or has channel access)
        post_response = supabase.table('posts').select('id, text, channels(title)').eq('id', post_uuid).or_(f'creator_id.eq.{user_uuid},channels.channel_users.user_id.eq.{user_uuid}').execute()
        post_data = post_response.data
        
        if not post_data:
            await bot.send_message(user_telegram_id, "Пост не найден или у вас нет прав на его удаление\\.")
            await state.finish()
            return
        
        post_info = post_data[0]
        channel_title = escape_markdown(post_info['channels']['title'])
        post_summary = post_info['text'][:50].replace('\n', ' ') + "..." if post_info['text'] and len(post_info['text']) > 50 else (post_info['text'].replace('\n', ' ') if post_info['text'] else "Без текста")

        await state.update_data(deleting_post_uuid=post_uuid, deleting_post_summary=post_summary, deleting_channel_title=channel_title)

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Да, удалить", callback_data="confirm_delete_post_yes"),
            InlineKeyboardButton("Нет, отмена", callback_data="confirm_delete_post_no")
        )

        await bot.send_message(
            user_telegram_id,
            f"Вы уверены, что хотите удалить пост из канала *{channel_title}* с содержанием: \"{escape_markdown(post_summary)}\"?\n"
            "Это действие необратимо\\.",
            reply_markup=keyboard
        )
        await DeletePostStates.waiting_for_delete_confirmation.set()

    except Exception as e:
        logger.error(f"Error processing post to delete selection for user {user_telegram_id}: {e}")
        await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка\\. Пожалуйста, попробуй еще раз позже\\.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data in ["confirm_delete_post_yes", "confirm_delete_post_no"], state=DeletePostStates.waiting_for_delete_confirmation)
async def process_delete_post_confirmation(callback_query: types.CallbackQuery, state: FSMContext):
    """
    Handles the confirmation for post deletion.
    """
    user_telegram_id = callback_query.from_user.id
    await bot.answer_callback_query(callback_query.id)
    data = await state.get_data()
    post_uuid = data.get('deleting_post_uuid')
    post_summary = data.get('deleting_post_summary')
    channel_title = data.get('deleting_channel_title')

    if callback_query.data == "confirm_delete_post_yes":
        try:
            delete_response = supabase.table('posts').delete().eq('id', post_uuid).execute()
            
            if delete_response.data:
                logger.info(f"Post {post_uuid} deleted by user {user_telegram_id}")
                await bot.send_message(user_telegram_id, f"Пост из канала *{channel_title}* с содержанием \"{escape_markdown(post_summary)}\" успешно удален\\.")
            else:
                logger.error(f"Failed to delete post {post_uuid}: {delete_response.json()}")
                await bot.send_message(user_telegram_id, "Произошла ошибка при удалении поста\\. Пожалуйста, попробуй еще раз позже\\.")
        except Exception as e:
            logger.error(f"Error deleting post {post_uuid} for user {user_telegram_id}: {e}")
            await bot.send_message(user_telegram_id, "Произошла непредвиденная ошибка при удалении поста\\. Пожалуйста, попробуй еще раз позже\\.")
    else:
        await bot.send_message(user_telegram_id, "Удаление поста отменено\\.")
    
    await state.finish()

@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    """
    Allows user to cancel any ongoing FSM process.
    """
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Нет активных операций для отмены\\.")
        return

    await state.finish()
    await message.reply("Операция отменена\\.")


# --- Main execution ---

if __name__ == '__main__':
    logger.info("Starting bot...")
    executor.start_polling(dp, skip_updates=True)
