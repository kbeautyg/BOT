from aiogram import Router, types
from aiogram.filters import Command, Text
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery
from models import project, user

router = Router()

# State for adding channel
from aiogram.fsm.state import State, StatesGroup
class ChannelAddState(StatesGroup):
    WAITING_CHANNEL = State()

@router.message(Command('channels'))
async def cmd_channels(message: Message):
    telegram_user = message.from_user
    # Ensure user exists (should already from /start)
    db_user = await user.get_user_by_telegram(telegram_user.id)
    if not db_user:
        db_user = await user.create_user(telegram_user.id, telegram_user.full_name)
    user_id = db_user['id']
    projects = await project.get_projects_by_user(user_id)
    if not projects:
        await message.answer("У вас пока нет подключенных каналов.")
    else:
        # List channels with roles
        text_lines = ["Ваши проекты (каналы):"]
        for idx, proj in enumerate(projects, start=1):
            role = proj.get('role')
            text_lines.append(f"{idx}. {proj['name']} - роль: {role}")
        await message.answer("\n".join(text_lines))
    # Show Add Channel button
    kb = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Добавить канал", callback_data="add_channel"))
    await message.answer("Добавить новый канал:", reply_markup=kb)

@router.callback_query(Text('add_channel'))
async def on_add_channel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()  # acknowledge callback
    # Remove keyboard to prevent duplicate clicks
    await callback.message.edit_reply_markup()
    # Ask for channel info
    await callback.message.answer("Отправьте пересланное сообщение из канала или его @username для подключения:")
    await state.set_state(ChannelAddState.WAITING_CHANNEL)

@router.message(ChannelAddState.WAITING_CHANNEL)
async def process_channel_info(message: Message, state: FSMContext):
    telegram_user = message.from_user
    db_user = await user.get_user_by_telegram(telegram_user.id)
    user_id = db_user['id']
    chat = None
    # Determine if message is forwarded from a channel
    if message.forward_from_chat:
        chat = message.forward_from_chat
    else:
        # If text provided, try to interpret it as username or ID
        text = message.text.strip()
        if text.startswith('@'):
            # Remove '@' and get chat by username
            username = text
            try:
                chat = await message.bot.get_chat(username)
            except Exception as e:
                await message.answer("Не удалось получить информацию о канале. Убедитесь, что бот является администратором канала.")
                await state.clear()
                return
        else:
            # If numeric ID
            try:
                chat_id = int(text)
                chat = await message.bot.get_chat(chat_id)
            except Exception:
                await message.answer("Некорректное имя или ID канала. Попробуйте снова.")
                return
    # Validate chat is a channel and bot is admin
    if not chat or chat.type != types.ChatType.CHANNEL:
        await message.answer("Пожалуйста, укажите правильный канал (бот должен быть администратором).")
        await state.clear()
        return
    # Check if channel already exists in DB
    existing = await project.get_project_by_channel(chat.id)
    if existing:
        # Check membership
        member_role = await project.get_user_role(user_id, existing['id'])
        if member_role:
            await message.answer("Этот канал уже подключен в вашем списке.")
        else:
            await message.answer("Этот канал уже подключен другим пользователем. Вы не можете его добавить.")
        await state.clear()
        return
    # Create project and add user as owner
    new_proj = await project.create_project(user_id, chat.id, chat.title or chat.username or 'Channel')
    if not new_proj:
        await message.answer("Произошла ошибка при добавлении проекта. Попробуйте снова.")
    else:
        await message.answer(f"✅ Канал '{new_proj['name']}' успешно подключен.")
    # Clear state
    await state.clear()
