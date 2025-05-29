from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import Message
from models import project, invite, user

router = Router()

@router.message(Command('invite'))
async def cmd_invite(message: Message):
    telegram_user = message.from_user
    db_user = await user.get_user_by_telegram(telegram_user.id)
    if not db_user:
        await message.answer("Пожалуйста, используйте /start для регистрации сначала.")
        return
    user_id = db_user['id']
    projects = await project.get_projects_by_user(user_id)
    if not projects:
        await message.answer("У вас нет проектов для приглашения пользователей.")
        return
    # If multiple projects, list them to choose
    if len(projects) > 1:
        text = "Выберите проект для приглашения пользователя:"
        kb = types.InlineKeyboardMarkup()
        for proj in projects:
            # Only allow if user is owner or admin
            if proj['role'] in ['owner', 'admin']:
                kb.add(types.InlineKeyboardButton(proj['name'], callback_data=f"inviteproj:{proj['id']}:{proj['name']}"))
        if not kb.inline_keyboard:
            await message.answer("У вас нет проектов, где вы можете приглашать пользователей.")
        else:
            await message.answer(text, reply_markup=kb)
    else:
        proj = projects[0]
        if proj['role'] not in ['owner', 'admin']:
            await message.answer("Только владелец или администратор проекта может приглашать пользователей.")
        else:
            code = await invite.create_invite(proj['id'])
            bot_username = (await message.bot.get_me()).username
            link = f"https://t.me/{bot_username}?start={code}"
            await message.answer(f"Пригласительная ссылка для проекта '{proj['name']}':\n{link}\nОтправьте ее пользователю, которого хотите пригласить.")

@router.callback_query(lambda c: c.data and c.data.startswith('inviteproj:'))
async def invite_project_select(callback: types.CallbackQuery):
    await callback.answer()
    try:
        _, pid, name = callback.data.split(':', 2)
        project_id = int(pid)
    except:
        return
    # Only generate invite if user is authorized (owner/admin)
    telegram_user = callback.from_user
    db_user = await user.get_user_by_telegram(telegram_user.id)
    user_id = db_user['id']
    role = await project.get_user_role(user_id, project_id)
    if role not in ['owner', 'admin']:
        await callback.message.answer("У вас нет прав пригласить в этот проект.")
        return
    code = await invite.create_invite(project_id)
    bot_username = (await callback.message.bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={code}"
    await callback.message.answer(f"Пригласительная ссылка для проекта '{name}':\n{link}")
