from aiogram import Router, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.utils.markdown import bold
from models import user, invite

router = Router()

@router.message(Command('start'))
async def cmd_start(message: types.Message, command: CommandObject):
    # Ensure user is in database
    telegram_user = message.from_user
    await user.ensure_user(telegram_user.id, telegram_user.full_name)
    # Check if start command has an invite code parameter
    args = command.args
    if args:
        # If there's an invite code, attempt to use it
        success, info = await invite.use_invite(args, telegram_user.id)
        await message.answer(info)
    else:
        # Send welcome message and main menu
        text = ("Привет, {0}! Я помогу с отложенными постами в Telegram каналах.\n"
                "Используйте команды или кнопки ниже для управления.").format(telegram_user.first_name)
        await message.answer(text, reply_markup=main_menu_keyboard())

def main_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text='📋 My Channels', callback_data='menu_channels'))
    keyboard.add(types.InlineKeyboardButton(text='➕ New Post', callback_data='menu_new_post'))
    keyboard.add(types.InlineKeyboardButton(text='🗓 Scheduled Posts', callback_data='menu_list_posts'))
    return keyboard
