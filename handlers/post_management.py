# handlers/post_management.py

import logging
from typing import List, Dict, Any, Union, Optional, Set
import datetime
from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.utils.markdown import markdown_italic, markdown_bold # For formatting help text

# Импорты из проекта
# from services.db import get_user_posts, get_post_by_id
# from keyboards.inline_keyboards import (
#     get_post_management_keyboard, get_edit_section_keyboard, get_delete_confirmation_keyboard,
#     PostCallbackData, NavigationCallbackData, DeleteCallbackData, get_simple_back_keyboard
# )
# from keyboards.reply_keyboards import get_main_menu_keyboard
# from utils.datetime_utils import get_user_timezone, format_datetime
# from models.post import Post
# from .post_management_fsm_states import PostManagementStates
# from .post_creation_fsm_states import PostCreationStates

# Корректные импорты:
try:
    from services.db import get_user_posts, get_post_by_id
    from keyboards.inline_keyboards import (
        get_post_management_keyboard,
        get_edit_section_keyboard,
        get_delete_confirmation_keyboard,
        PostCallbackData,
        NavigationCallbackData,
        DeleteCallbackData,
        get_simple_back_keyboard
    )
    from keyboards.reply_keyboards import get_main_menu_keyboard
    from utils.datetime_utils import get_user_timezone, format_datetime
    from models.post import Post
    from .post_management_fsm_states import PostManagementStates
    from .post_creation_fsm_states import PostCreationStates
    # Import necessary creation keyboards for section editing entry points
    from keyboards.reply_keyboards import get_channel_selection_controls_keyboard
    from keyboards.inline_keyboards import get_dynamic_channel_selection_keyboard, get_schedule_type_keyboard, get_delete_options_keyboard
    from services.telegram_api import get_bot_channels_for_user # Required for channel editing entry point
except ImportError as e:
    logging.error(f"Failed to import dependencies in post_management.py: {e}")
    # Define mock components if imports fail - This is for basic structure validation,
    # real functionality will fail without actual imports.
    class PostManagementStates:
         showing_list = "PostManagementStates:showing_list"
         editing_section_selection = "PostManagementStates:editing_section_selection"
         confirming_post_deletion = "PostManagementStates:confirming_post_deletion"
    class PostCreationStates:
         waiting_for_text = "PostCreationStates:waiting_for_text"
         waiting_for_media_option = "PostCreationStates:waiting_for_media_option"
         waiting_for_media_files = "PostCreationStates:waiting_for_media_files"
         confirm_content_before_channels = "PostCreationStates:confirm_content_before_channels"
         waiting_for_channel_selection = "PostCreationStates:waiting_for_channel_selection"
         waiting_for_channel_selection_action = "PostCreationStates:waiting_for_channel_selection_action"
         waiting_for_schedule_type = "PostCreationStates:waiting_for_schedule_type"
         waiting_for_onetime_schedule_datetime = "PostCreationStates:waiting_for_onetime_schedule_datetime"
         waiting_for_recurring_type = "PostCreationStates:waiting_for_recurring_type"
         waiting_for_recurring_daily_time = "PostCreationStates:waiting_for_recurring_daily_time"
         waiting_for_recurring_weekly_days = "PostCreationStates:waiting_for_recurring_weekly_days"
         waiting_for_recurring_weekly_time = "PostCreationStates:waiting_for_recurring_weekly_time"
         waiting_for_recurring_monthly_day = "PostCreationStates:waiting_for_recurring_monthly_day"
         waiting_for_recurring_monthly_time = "PostCreationStates:waiting_for_recurring_monthly_time"
         waiting_for_recurring_yearly_date = "PostCreationStates:waiting_for_recurring_yearly_date"
         waiting_for_recurring_yearly_time = "PostCreationStates:waiting_for_recurring_yearly_time"
         waiting_for_deletion_option = "PostCreationStates:waiting_for_deletion_option"
         waiting_for_delete_hours = "PostCreationStates:waiting_for_delete_hours"
         waiting_for_delete_days = "PostCreationStates:waiting_for_delete_days"
         waiting_for_delete_datetime = "PostCreationStates:waiting_for_delete_datetime"
         preview_and_confirm = "PostCreationStates:preview_and_confirm"

    class MockPost:
        def __init__(self, **kwargs):
             for k, v in kwargs.items():
                  setattr(self, k, v)
        def __repr__(self):
             return f"<MockPost id={getattr(self, 'id', 'N/A')}>"

    async def get_user_posts(session, user_id, statuses=None): return []
    async def get_post_by_id(session, post_id): return None
    def get_post_management_keyboard(post_id): return None
    def get_edit_section_keyboard(draft_id=None): return None
    def get_delete_confirmation_keyboard(item_type, item_id, context_id=None): return None
    class PostCallbackData(CallbackData, prefix="post"): action: str; post_id: Optional[int] = None; value: Optional[str] = None
    class NavigationCallbackData(CallbackData, prefix="nav"): target: str; context_id: Optional[str] = None
    class DeleteCallbackData(CallbackData, prefix="delete"): action: str; item_type: str; item_id: str; context_id: Optional[str] = None
    def get_main_menu_keyboard(): return None
    def get_simple_back_keyboard(back_target_state, context_id=None, text="⬅️ Назад"): return None
    def get_user_timezone(user_id): return "UTC"
    def format_datetime(dt, tz): return str(dt) if dt else "N/A"
    def get_channel_selection_controls_keyboard(): return None
    def get_dynamic_channel_selection_keyboard(available_channels, selected_channel_ids=None, context_id=None): return None
    def get_schedule_type_keyboard(draft_id=None, back_target_state=None): return None
    def get_delete_options_keyboard(draft_id=None, back_target_state=None): return None
    async def get_bot_channels_for_user(bot, user_id): return []


# Setup logging
logger = logging.getLogger(__name__)

# Router instance
post_management_router = Router()

# Define allowed editing sections and their corresponding initial creation states
EDIT_SECTIONS_MAP = {
    "content": PostCreationStates.waiting_for_text,
    "channels": PostCreationStates.waiting_for_channel_selection_action, # State that handles interaction
    "schedule": PostCreationStates.waiting_for_schedule_type,
    "deletion": PostCreationStates.waiting_for_deletion_option,
}
# Mapping from section key to display name for user messages
EDIT_SECTIONS_NAMES = {
    "content": "Контент",
    "channels": "Каналы",
    "schedule": "Расписание",
    "deletion": "Удаление",
}


# --- Helper Functions ---

async def _format_post_for_display(post: Post, user_timezone: str) -> str:
    """
    Formats a Post object into a human-readable string for display to the user.
    Uses MarkdownV2 formatting.
    """
    status_map = {
        "scheduled": "✅ Запланирован",
        "sent": "🟢 Отправлен",
        "deleted": "🗑️ Удален",
        "error": "❌ Ошибка",
        "canceleduuid": "🆑 Отменен", # Example custom status
        "deletion_failed": "⚠️ Ошибка удаления"
        # Add other statuses as needed
    }
    status = status_map.get(post.status, post.status)

    text_summary = post.text[:150].replace('_', '\\_').replace('*', '\\*').replace('[', '\$$
').replace(']', '\
$$').replace('(', '\$').replace(')', '\$').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!') + '...' if post.text and len(post.text) > 150 else (post.text.replace('_', '\\_').replace('*', '\\*') if post.text else "Нет текста") # Basic MarkdownV2 escape
    media_summary = f"🖼️ Медиа: {len(post.media_paths or [])} файл(ов)" if post.media_paths else "🖼️ Медиа: Нет"

    schedule_summary = ""
    if post.schedule_type == 'one_time' and post.run_date:
        formatted_date = format_datetime(post.run_date, user_timezone) or 'Некорректное время'
        schedule_summary = f"⏰ Разово: {formatted_date}"
    elif post.schedule_type == 'recurring' and post.schedule_params:
        cron_type = post.schedule_params.get('type', 'Неизвестно')
        time_str = post.schedule_params.get('time', 'Не указано')
        if cron_type == 'daily':
            schedule_summary = f"⏰ Ежедневно в {time_str}"
        elif cron_type == 'weekly':
            days = post.schedule_params.get('days_of_week', [])
            day_names = {'mon': 'Пн', 'tue': 'Вт', 'wed': 'Ср', 'thu': 'Чт', 'fri': 'Пт', 'sat': 'Сб', 'sun': 'Вс'}
            formatted_days = ", ".join([day_names.get(d, d) for d in days])
            schedule_summary = f"⏰ Еженедельно по {formatted_days} в {time_str}"
        elif cron_type == 'monthly':
            day = post.schedule_params.get('day_of_month', 'Не указан')
            schedule_summary = f"⏰ Ежемесячно {day}-го числа в {time_str}"
        elif cron_type == 'yearly':
             month_day = post.schedule_params.get('month_day', 'Не указано')
             schedule_summary = f"⏰ Ежегодно {month_day} в {time_str}"
        else:
            schedule_summary = f"⏰ Циклически ({cron_type})"
    else:
        schedule_summary = "⏰ Расписание: Не настроено" # Should not happen for scheduled/sent posts

    deletion_summary = "🗑️ Автоудаление: Не настроено"
    if post.delete_after_seconds is not None and post.delete_after_seconds > 0:
        # Convert seconds back to readable format for display
        if post.delete_after_seconds % (24 * 3600) == 0:
             days = post.delete_after_seconds // (24 * 3600)
             deletion_summary = f"🗑️ Удалить через {days} дн."
        elif post.delete_after_seconds % 3600 == 0:
             hours = post.delete_after_seconds // 3600
             deletion_summary = f"🗑️ Удалить через {hours} ч."
        else:
             # Fallback to seconds if not whole days/hours
             deletion_summary = f"🗑️ Удалить через {post.delete_after_seconds} сек."


    # Escape MarkdownV2 special characters in all text fields
    def escape_markdown_v2(text: str) -> str:
        if not isinstance(text, str):
            return ""
        # List of characters to escape: _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return ''.join(['\\' + char if char in escape_chars else char for char in text])

    # Apply escaping *only* to user-provided text that isn't part of formatting
    # For this formatted string, we use MarkdownV2 directly, so we escape content *within* formatting.
    # Let's format it manually using bold/italic helpers for clarity instead of raw escapes.

    formatted_text = (
        f"{markdown_bold('Пост ID:')} {post.id}\n"
        f"Статус: {status}\n"
        f"{schedule_summary}\n"
        f"{deletion_summary}\n"
        f"{media_summary}\n"
        f"{markdown_bold('Текст:')} {markdown_italic(text_summary)}\n"
    )
    return formatted_text


async def _populate_fsm_for_editing(state: FSMContext, post: Post) -> None:
    """
    Loads relevant data from a Post object into FSM context for editing.
    Maps Post attributes to PostCreationStates FSM state keys.
    Adds an 'editing_post_id' flag.
    """
    # Clear current state data before populating for editing
    await state.clear()

    state_data = {}

    # Map Post attributes to expected keys in PostCreationStates FSM context
    state_data['editing_post_id'] = post.id # Flag to indicate editing mode
    state_data['text'] = post.text
    state_data['media_paths'] = post.media_paths or [] # Ensure it's a list, even if None. These might be file_ids or local paths.
    # For channels, Post stores a list of strings, FSM uses a set during selection
    state_data['selected_channel_ids'] = set(post.chat_ids) if post.chat_ids else set() # Convert list to set for editing flow
    state_data['schedule_type'] = post.schedule_type
    state_data['schedule_params'] = post.schedule_params if post.schedule_params is not None else {} # Ensure it's a dict
    state_data['run_date'] = post.run_date # Should be timezone-aware from DB
    state_data['delete_after_seconds'] = post.delete_after_seconds
    # Add a flag indicating that the FSM is pre-populated (for post_creation handlers)
    state_data['is_editing'] = True


    await state.update_data(**state_data)
    logger.info(f"FSM context populated for editing post ID: {post.id}")


# --- Command Handlers ---

# /myposts command handler is in handlers/commands.py and sets state to PostManagementStates.showing_list.
# The handler below is triggered when the state is already showing_list and the command is received.

@post_management_router.message(Command("editpost"))
async def handle_edit_post_command(
    message: Message,
    command: CommandObject,
    state: FSMContext,
    session: AsyncSession # Inject database session
) -> None:
    """
    Handles the /editpost <ID> <section> command.
    Directly transitions to editing a specific section of a post.
    """
    user_id = message.from_user.id
    args = command.args.split() if command.args else []

    if len(args) != 2:
        await message.answer(
            f"Некорректный формат команды\\. Используйте `{markdown_bold('/editpost')} <ID> <секция>`\\.\n"
            f"Доступные секции: {', '.join(EDIT_SECTIONS_NAMES.keys())}",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear() # Ensure state is cleared on invalid command format
        return

    try:
        post_id = int(args[0])
        section = args[1].lower()
    except ValueError:
        await message.answer(
            "Некорректный ID поста\\. ID должен быть числом\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    if section not in EDIT_SECTIONS_MAP:
        await message.answer(
            f"Некорректная секция `{escape_markdown_v2(section)}`\\. Доступные секции: {', '.join(EDIT_SECTIONS_NAMES.keys())}\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    logger.info(f"User {user_id} requested to edit post ID:{post_id}, section: {section} via command.")

    # Fetch the post from the database
    post = await get_post_by_id(session, post_id)

    # Check if post exists and belongs to the user
    if not post or post.user_id != user_id:
        await message.answer(
            f"Пост с ID `{post_id}` не найден или вы не имеете к нему доступа\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    # Populate FSM context with post data for editing
    await _populate_fsm_for_editing(state, post)

    # Transition directly to the specified creation state for the section
    target_state = EDIT_SECTIONS_MAP[section]
    await state.set_state(target_state)
    logger.info(f"Transitioned to state {target_state} for editing section '{section}' via command.")

    # Send the initial message and keyboard for the target state
    # This replicates the entry logic for specific states from post_creation.py, but adds a 'Back' button to editing_selection_state.
    user_timezone = get_user_timezone(user_id)

    if target_state == PostCreationStates.waiting_for_text:
         await message.answer(
            "Отредактируйте текст вашего поста\\. Отправьте новый текст\\.\n"
            "После ввода текста вы сможете отредактировать или добавить медиа\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_simple_back_keyboard(back_target_state="editing_selection_state", context_id=str(user_id))
         )
    elif target_state == PostCreationStates.waiting_for_channel_selection_action:
         # Re-fetch channels and show the selection keyboard
         try:
            available_channels_raw = await get_bot_channels_for_user(message.bot, user_id) # Need bot instance
            available_channels = [{'id': str(c['id']), 'name': c['name']} for c in available_channels_raw]

            if not available_channels:
                await message.answer(
                    "Не найдено доступных каналов или групп, где бот является администратором\\.",
                    parse_mode="MarkdownV2",
                    reply_markup=get_main_menu_keyboard() # Cannot proceed without channels
                )
                await state.clear() # End FSM
                # TODO: Cleanup temp files if any were in state before _populate_fsm_for_editing
                return

            state_data_after_populate = await state.get_data() # Get state data after populate
            current_selected_ids = set(state_data_after_populate.get('selected_channel_ids', [])) # Get from populated state

            await state.update_data(available_channels=available_channels) # Update available channels in state
            await message.answer(
                "Отредактируйте каналы для публикации\\. Нажмите \"Готово\" когда закончите\\.",
                parse_mode="MarkdownV2",
                reply_markup=get_dynamic_channel_selection_keyboard(
                    available_channels=available_channels,
                    selected_channel_ids=current_selected_ids,
                    context_id=str(user_id)
                )
            )
            # Send a ReplyKeyboard for flow control
            await message.answer(
                "Используйте кнопки ниже для завершения выбора или отмены\\.",
                parse_mode="MarkdownV2",
                reply_markup=get_channel_selection_controls_keyboard()
            )
         except Exception as e:
             logger.exception(f"Failed to prepare channel editing for user {user_id} via command: {e}")
             await message.answer("Произошла ошибка при переходе к редактированию каналов\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
             await state.clear()

    elif target_state == PostCreationStates.waiting_for_schedule_type:
         # Clear previous schedule data in state to force user to re-enter flow from the start
         await state.update_data(run_date=None, schedule_params=None)
         await message.answer(
            "Отредактируйте расписание\\. Выберите тип расписания для вашего поста\\:",
            parse_mode="MarkdownV2",
            reply_markup=get_schedule_type_keyboard(draft_id=str(user_id), back_target_state="editing_selection_state") # Back to section selection
         )
    elif target_state == PostCreationStates.waiting_for_deletion_option:
         # Clear previous deletion data in state to force user to re-enter flow from the start
         await state.update_data(delete_after_seconds=None, deletion_option_type=None, deletion_datetime=None)
         # Determine back target. When editing via command, back from deletion options goes to editing selection.
         await message.answer(
            "Отредактируйте настройки автоматического удаления поста\\:",
            parse_mode="MarkdownV2",
            reply_markup=get_delete_options_keyboard(draft_id=str(user_id), back_target_state="editing_selection_state") # Back to section selection
         )
    # Add handlers for other specific states like waiting_for_media_option if they are direct entry points for editing sections
    # Based on EDIT_SECTIONS_MAP, the entry points are the high-level configuration states.
    # If the editing flow for a section starts deeper, this needs adjustment.
    # For now, assuming editing starts at the main points.

    else:
         # Fallback for any other state targeted for editing (should not happen with current map)
         await message.answer(
            f"Переход к редактированию секции '{escape_markdown_v2(EDIT_SECTIONS_NAMES.get(section, section))}'\\."
            "Следуйте инструкциям или используйте кнопки\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_simple_back_keyboard(back_target_state="editing_selection_state", context_id=str(user_id))
         )


@post_management_router.message(Command("deletepost"))
async def handle_delete_post_command(
    message: Message,
    command: CommandObject,
    state: FSMContext,
    session: AsyncSession # Inject database session
) -> None:
    """
    Handles the /deletepost <ID> command.
    Initiates the post deletion confirmation process.
    """
    user_id = message.from_user.id
    args = command.args.split() if command.args else []

    if len(args) != 1:
        await message.answer(
            f"Некорректный формат команды\\. Используйте `{markdown_bold('/deletepost')} <ID>`\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear() # Ensure state is cleared on invalid command format
        return

    try:
        post_id = int(args[0])
    except ValueError:
        await message.answer(
            "Некорректный ID поста\\. ID должен быть числом\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    logger.info(f"User {user_id} requested to delete post ID:{post_id} via command.")

    # Fetch the post to check existence and ownership
    post = await get_post_by_id(session, post_id)

    # Check if post exists and belongs to the user
    # Also check if it's already deleted or in an unmanageable state?
    # For simplicity, allow requesting deletion of any non-deleted post owned by the user.
    if not post or post.user_id != user_id or post.status == 'deleted':
        status_info = f" (статус: {post.status})" if post else ""
        await message.answer(
            f"Пост с ID `{post_id}` не найден, вы не имеете к нему доступа или он уже удален{status_info}\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    # Clear any current state before setting confirmation state
    await state.clear()

    # Set state to await deletion confirmation
    await state.set_state(PostManagementStates.confirming_post_deletion)
    logger.info(f"Transitioned to state {PostManagementStates.confirming_post_deletion} for post ID:{post_id} via command.")

    # Send confirmation message with inline keyboard
    confirmation_text = f"Вы уверены, что хотите удалить пост ID:{post_id}?\\n"
    # Add a summary of the post being deleted
    user_timezone = get_user_timezone(user_id)
    confirmation_text += await _format_post_for_display(post, user_timezone)
    confirmation_text += "\n**Внимание**: Это действие необратимо\\." # Add emphasis

    await message.answer(
        confirmation_text,
        reply_markup=get_delete_confirmation_keyboard(item_type="post", item_id=post_id, context_id=str(user_id)), # Pass post_id as item_id (string)
        parse_mode="MarkdownV2"
    )


# --- State Handlers ---

# Handler for the initial message triggering the showing_list state
# This handler is triggered by the /myposts command itself, filtered by the state.
# This means the command handler in commands.py *only* needs to set the state.
@post_management_router.message(Command("myposts"), StateFilter(PostManagementStates.showing_list))
# Adding F.text filter here would mean it only triggers if the user sends /myposts while *already* in this state.
# Removing F.text means it triggers on the command message itself after the state is set by commands.py.
# This is the intended flow.
async def handle_show_user_posts(
    message: Message,
    state: FSMContext,
    session: AsyncSession # Inject database session
) -> None:
    """
    Handles displaying the user's posts when entering the showing_list state
    via the /myposts command.
    """
    user_id = message.from_user.id
    logger.info(f"User {user_id} is viewing their posts.")

    # Get user's timezone for formatting
    user_timezone = get_user_timezone(user_id)

    # Fetch user's scheduled and sent posts
    # Consider statuses that might need management: scheduled, sent, error, deletion_failed
    manageable_statuses = ["scheduled", "sent", "error", "deletion_failed"]
    posts = await get_user_posts(session, user_id, statuses=manageable_statuses)

    if not posts:
        await message.answer("У вас нет запланированных или отправленных постов для управления.", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state if no posts to manage
        return

    # Send posts with inline keyboards
    await message.answer(f"Найдено {len(posts)} постов:", reply_markup=None) # Initial message, remove ReplyKeyboard

    for post in posts:
        post_text = await _format_post_for_display(post, user_timezone)
        # Send each post with its management keyboard
        await message.answer(
            post_text,
            reply_markup=get_post_management_keyboard(post.id),
            parse_mode="MarkdownV2" # Use Markdown for formatted text
        )

    # Stay in showing_list state, waiting for inline button callbacks
    # Subsequent non-command messages in this state might need a handler
    # to prompt the user to use buttons or the command again.

@post_management_router.message(StateFilter(PostManagementStates.showing_list), ~Command("myposts", "cancel"))
async def handle_showing_list_invalid_input(message: Message) -> None:
    """Handles invalid input while showing the list of posts."""
    # Ignore callback queries - they have their own handlers
    if message.content_type != 'text':
         return # Ignore non-text messages

    await message.answer(
        "Вы просматриваете список постов\\. Используйте кнопки под постами для управления ими "
        "или введите `/myposts` снова для обновления списка\\.",
        parse_mode="MarkdownV2",
        reply_markup=None # Don't show reply keyboard here
    )


# Handler for inline 'Редактировать' button when viewing list
@post_management_router.callback_query(PostCallbackData.filter(F.action == "edit_published_post"), StateFilter(PostManagementStates.showing_list))
async def process_edit_published_post_callback(
    callback: CallbackQuery,
    callback_data: PostCallbackData,
    state: FSMContext,
    session: AsyncSession # Inject database session
) -> None:
    """
    Handles inline button click to edit a post from the list view.
    Transitions to editing section selection state.
    """
    post_id = callback_data.post_id
    user_id = callback.from_user.id

    if post_id is None:
        logger.error(f"Edit post callback received without post_id for user {user_id}.")
        await callback.answer("Ошибка: Не указан ID поста\\.", show_alert=True)
        return

    logger.info(f"User {user_id} requested to edit post ID:{post_id} via inline button.")

    # Fetch the post
    post = await get_post_by_id(session, post_id)

    if not post or post.user_id != user_id or post.status == 'deleted':
        status_info = f" (статус: {post.status})" if post else ""
        logger.warning(f"Edit requested for non-existent, unauthorized, or deleted post ID:{post_id} by user {user_id}.")
        await callback.answer(f"Пост не найден, вы не имеете к нему доступа или он уже удален{status_info}\\.", show_alert=True)
        # Attempt to remove the keyboard from the list item message
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
             logger.warning(f"Failed to remove inline keyboard for edited post {post_id} list item: {e}")
        return

    # Populate FSM context for editing
    await _populate_fsm_for_editing(state, post)

    # Transition to editing section selection state
    await state.set_state(PostManagementStates.editing_section_selection)
    logger.info(f"Transitioned to state {PostManagementStates.editing_section_selection} for editing post ID:{post_id}.")

    # Send editing section selection keyboard as a NEW message
    try:
        await callback.answer() # Answer the callback query first

        await callback.message.answer( # Use callback.message.answer to send message to the same chat
            f"{markdown_bold('Редактирование поста ID:')} {post_id}\\.\nВыберите, какую часть поста вы хотите отредактировать\\:",
            reply_markup=get_edit_section_keyboard(draft_id=str(user_id)), # Pass user_id as draft_id context for inline keyboard callbacks
            parse_mode="MarkdownV2"
        )
        # Note: We are NOT storing a specific message_id for this "Choose section" message yet.
        # If we need to edit THIS message later (e.g., when returning from editing a section),
        # we would need to store its ID in the state.

    except Exception as e:
        logger.exception(f"Failed to send editing section keyboard for post ID:{post_id} user {user_id}: {e}")
        await callback.message.answer("Произошла ошибка при переходе к редактированию\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state on error


# Handler for inline 'Удалить' button when viewing list
@post_management_router.callback_query(PostCallbackData.filter(F.action == "request_delete_post"), StateFilter(PostManagementStates.showing_list))
async def process_request_delete_post_callback(
    callback: CallbackQuery,
    callback_data: PostCallbackData,
    state: FSMContext,
    session: AsyncSession # Inject database session
) -> None:
    """
    Handles inline button click to request deletion of a post from the list view.
    Transitions to deletion confirmation state.
    """
    post_id = callback_data.post_id
    user_id = callback.from_user.id

    if post_id is None:
        logger.error(f"Delete post callback received without post_id for user {user_id}.")
        await callback.answer("Ошибка: Не указан ID поста\\.", show_alert=True)
        return

    logger.info(f"User {user_id} requested to delete post ID:{post_id} via inline button.")

    # Fetch the post to check existence and ownership
    if not post_id: # Should be caught by post_id is None check, but belt-and-suspenders
         await callback.answer("Ошибка: Некорректный ID поста\\.", show_alert=True)
         return

    post = await get_post_by_id(session, post_id)

    if not post or post.user_id != user_id or post.status == 'deleted':
        status_info = f" (статус: {post.status})" if post else ""
        logger.warning(f"Deletion requested for non-existent, unauthorized, or deleted post ID:{post_id} by user {user_id}.")
        await callback.answer(f"Пост не найден, вы не имеете к нему доступа или он уже удален{status_info}\\.", show_alert=True)
        # Attempt to remove the keyboard from the list item message
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
             logger.warning(f"Failed to remove inline keyboard for deleted post {post_id} list item: {e}")
        return

    # Clear any current state before setting confirmation state
    await state.clear()

    # Set state to await deletion confirmation
    await state.set_state(PostManagementStates.confirming_post_deletion)
    logger.info(f"Transitioned to state {PostManagementStates.confirming_post_deletion} for post ID:{post_id} via inline button.")

    # Send confirmation message with inline keyboard as a NEW message
    confirmation_text = f"Вы уверены, что хотите удалить пост ID:{post_id}?\\n"
    # Add a summary of the post being deleted
    user_timezone = get_user_timezone(user_id)
    confirmation_text += await _format_post_for_display(post, user_timezone)
    confirmation_text += "\n**Внимание**: Это действие необратимо\\." # Add emphasis

    try:
        await callback.answer() # Answer the callback query first

        await callback.message.answer(
             confirmation_text,
             reply_markup=get_delete_confirmation_keyboard(item_type="post", item_id=str(post_id), context_id=str(user_id)), # item_id needs to be string for CallbackData
             parse_mode="MarkdownV2"
        )
        # Keep the original post list item message as is.

    except Exception as e:
        logger.exception(f"Failed to send delete confirmation for post ID:{post_id} user {user_id}: {e}")
        await callback.message.answer("Произошла ошибка при запросе на удаление\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state on error (user might be stuck)


@post_management_router.message(StateFilter(PostManagementStates.editing_section_selection))
async def handle_editing_section_selection_invalid_input(message: Message) -> None:
    """Handles invalid input while in editing section selection state."""
    # Ignore callback queries, they have their own handler
    if message.content_type != 'text': # Allow commands like /cancel
         return # Ignore non-text messages

    await message.answer(
        "Выберите, какую часть поста вы хотите отредактировать, используя кнопки\\.",
        parse_mode="MarkdownV2",
        reply_markup=None # The inline keyboard is already shown in the previous message
    )

# Handler for inline 'edit_section' callback when in editing selection state
@post_management_router.callback_query(PostCallbackData.filter(F.action == "edit_section"), StateFilter(PostManagementStates.editing_section_selection))
async def process_edit_section_callback(
    callback: CallbackQuery,
    callback_data: PostCallbackData,
    state: FSMContext
) -> None:
    """
    Handles inline button click to select a section for editing.
    Transitions to the appropriate PostCreationStates state.
    """
    section_to_edit = callback_data.value # 'content', 'channels', 'schedule', 'deletion'
    user_id = callback.from_user.id
    state_data = await state.get_data()
    editing_post_id = state_data.get('editing_post_id') # Ensure we are editing a post

    if section_to_edit not in EDIT_SECTIONS_MAP:
        logger.error(f"Invalid edit section received for user {user_id}: {section_to_edit}")
        await callback.answer("Некорректная секция\\.", show_alert=True)
        return

    # Check if we are actually in an editing flow
    if editing_post_id is None:
         logger.error(f"Edit section callback received outside of post editing flow for user {user_id}.")
         await callback.answer("Ошибка FSM\\.", show_alert=True)
         await state.clear()
         await callback.message.answer("Произошла внутренняя ошибка\\. Пожалуйста, начните заново\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
         return

    logger.info(f"User {user_id} selected section '{section_to_edit}' for editing post ID:{editing_post_id}.")

    # Delete the editing section selection inline keyboard message
    try:
        await callback.message.delete()
        await callback.answer() # Answer the callback query
    except Exception as e:
        logger.warning(f"Failed to delete editing section message for user {user_id}: {e}")

    # Transition to the corresponding PostCreationStates state
    target_state = EDIT_SECTIONS_MAP[section_to_edit]
    await state.set_state(target_state)
    logger.info(f"Transitioned to state {target_state} for editing section '{section_to_edit}'.")

    # Send the initial message and keyboard for the target creation state
    # This replicates the entry logic for specific states from post_creation.py, but adds a 'Back' button to editing_selection_state.
    user_timezone = get_user_timezone(user_id)
    # Pass the editing_post_id in the context for callbacks within the editing flow
    editing_context_id = str(editing_post_id) # Using post ID as context ID for simplicity

    if target_state == PostCreationStates.waiting_for_text:
         # In editing content, we clear text and media paths in _populate_fsm_for_editing for re-input.
         # The initial message asks for new text.
         await callback.message.answer(
            "Отредактируйте текст вашего поста\\. Отправьте новый текст\\.\n"
            "После ввода текста вы сможете отредактировать или добавить медиа\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_simple_back_keyboard(back_target_state="editing_selection_state", context_id=editing_context_id)
         )
    elif target_state == PostCreationStates.waiting_for_channel_selection_action:
         # Re-fetch channels and show the selection keyboard
         try:
            available_channels_raw = await get_bot_channels_for_user(callback.bot, user_id)
            available_channels = [{'id': str(c['id']), 'name': c['name']} for c in available_channels_raw]

            if not available_channels:
                await callback.message.answer(
                    "Не найдено доступных каналов или групп, где бот является администратором\\.",
                    parse_mode="MarkdownV2",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                # TODO: Cleanup temp files?
                return

            # Get currently selected channels from state (populated by _populate_fsm_for_editing)
            state_data_after_populate = await state.get_data() # Get state data after populate
            current_selected_ids = set(state_data_after_populate.get('selected_channel_ids', []))

            await state.update_data(available_channels=available_channels) # Update available channels in state
            await callback.message.answer(
                "Отредактируйте каналы для публикации\\. Нажмите \"Готово\" когда закончите\\.",
                parse_mode="MarkdownV2",
                reply_markup=get_dynamic_channel_selection_keyboard(
                    available_channels=available_channels,
                    selected_channel_ids=current_selected_ids,
                    context_id=editing_context_id # Pass context_id
                )
            )
            # Send a ReplyKeyboard for flow control
            await callback.message.answer(
                "Используйте кнопки ниже для завершения выбора или отмены\\.",
                parse_mode="MarkdownV2",
                reply_markup=get_channel_selection_controls_keyboard()
            )
            # Note: Channel selection flow requires a back button on the ReplyKeyboard too,
            # or the inline keyboard should have a Back/Cancel button for the whole section editing.
            # get_channel_selection_controls_keyboard doesn't have 'Back'. Add a separate inline 'Back' button?
            # Let's add a simple inline 'Back' here.
            await callback.message.answer(
                 "\\.\\.\\.", # Placeholder message for the simple back button
                 reply_markup=get_simple_back_keyboard(back_target_state="editing_selection_state", context_id=editing_context_id)
            )


         except Exception as e:
             logger.exception(f"Failed to prepare channel editing for user {user_id} via edit section: {e}")
             await callback.message.answer("Произошла ошибка при переходе к редактированию каналов\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
             await state.clear()

    elif target_state == PostCreationStates.waiting_for_schedule_type:
         # Clear previous schedule data in state to force user to re-enter flow from the start
         await state.update_data(run_date=None, schedule_params=None)
         await callback.message.answer(
            "Отредактируйте расписание\\. Выберите тип расписания для вашего поста\\:",
            parse_mode="MarkdownV2",
            reply_markup=get_schedule_type_keyboard(draft_id=editing_context_id, back_target_state="editing_selection_state") # Back to section selection
         )
    elif target_state == PostCreationStates.waiting_for_deletion_option:
         # Clear previous deletion data in state to force user to re-enter flow from the start
         await state.update_data(delete_after_seconds=None, deletion_option_type=None, deletion_datetime=None)
         # Back from deletion options goes to editing selection when accessed via editing flow.
         await callback.message.answer(
            "Отредактируйте настройки автоматического удаления поста\\:",
            parse_mode="MarkdownV2",
            reply_markup=get_delete_options_keyboard(draft_id=editing_context_id, back_target_state="editing_selection_state") # Back to section selection
         )
    else:
         # Fallback for any other state targeted for editing (should not happen with current map)
         await callback.message.answer(
            f"Переход к редактированию секции '{escape_markdown_v2(EDIT_SECTIONS_NAMES.get(section_to_edit, section_to_edit))}'\\."
            "Следуйте инструкциям или используйте кнопки\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_simple_back_keyboard(back_target_state="editing_selection_state", context_id=editing_context_id)
         )


# Handler for 'Back' navigation from within PostCreationStates back to PostManagementStates.editing_section_selection
# This handler covers most states navigated to from editing_section_selection.
@post_management_router.callback_query(
    NavigationCallbackData.filter(F.target == "editing_selection_state"),
    StateFilter(
        PostCreationStates.waiting_for_text,
        PostCreationStates.waiting_for_media_option, # Might be reachable if text editing leads here
        PostCreationStates.waiting_for_media_files,
        PostCreationStates.waiting_for_channel_selection_action, # Back button from this state (inline)
        PostCreationStates.waiting_for_schedule_type,
        PostCreationStates.waiting_for_onetime_schedule_datetime,
        PostCreationStates.waiting_for_recurring_type,
        PostCreationStates.waiting_for_recurring_daily_time,
        PostCreationStates.waiting_for_recurring_weekly_days,
        PostCreationStates.waiting_for_recurring_weekly_time,
        PostCreationStates.waiting_for_recurring_monthly_day,
        PostCreationStates.waiting_for_recurring_monthly_time,
        PostCreationStates.waiting_for_recurring_yearly_date,
        PostCreationStates.waiting_for_recurring_yearly_time,
        PostCreationStates.waiting_for_deletion_option,
        PostCreationStates.waiting_for_delete_hours,
        PostCreationStates.waiting_for_delete_days,
        PostCreationStates.waiting_for_delete_datetime,
        PostCreationStates.preview_and_confirm # Back from preview after editing sections
    )
)
async def process_back_to_editing_selection(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Handles 'Back' navigation from within a PostCreationStates state (during editing)
    back to the editing section selection state.
    """
    user_id = callback.from_user.id
    current_state = await state.get_state()
    logger.info(f"User {user_id} went back from state {current_state} to editing selection.")

    # Check if we are in editing mode
    state_data = await state.get_data()
    editing_post_id = state_data.get('editing_post_id')
    if editing_post_id is None:
         logger.error(f"Back to editing selection callback received outside of editing flow for user {user_id}.")
         await callback.answer("Ошибка FSM\\.", show_alert=True)
         await state.clear()
         await callback.message.answer("Произошла внутренняя ошибка\\. Пожалуйста, начните заново\\.", parse_mode="MarkdownV2", reply_markup=get_main_menu_keyboard())
         return

    # Delete the message containing the 'Back' button's keyboard
    try:
        await callback.message.delete()
        await callback.answer() # Answer the callback query
    except Exception as e:
        logger.warning(f"Failed to delete message on back navigation for user {user_id}: {e}")

    # Transition to the editing section selection state
    await state.set_state(PostManagementStates.editing_section_selection)
    logger.info(f"Transitioned to state {PostManagementStates.editing_section_selection}.")

    # Send the editing section selection keyboard
    await callback.message.answer( # Use callback.message.answer to send message to the same chat
        f"{markdown_bold('Редактирование поста ID:')} {editing_post_id}\\.\nВыберите, какую часть поста вы хотите отредактировать\\:",
        reply_markup=get_edit_section_keyboard(draft_id=str(user_id)), # Pass user_id as draft_id context
        parse_mode="MarkdownV2"
    )

# Handler for text/invalid input while in deletion confirmation state
@post_management_router.message(StateFilter(PostManagementStates.confirming_post_deletion))
async def handle_confirming_post_deletion_invalid_input(message: Message) -> None:
    """Handles invalid input while in deletion confirmation state."""
    # Ignore callback queries, they have their own handlers (in inline_buttons.py)
    if message.content_type != 'text': # Allow commands like /cancel
         return # Ignore non-text messages

    await message.answer(
        "Пожалуйста, используйте кнопки 'Да, удалить' или 'Нет, отмена' для подтверждения или отмены удаления\\.",
        parse_mode="MarkdownV2",
        reply_markup=None # The inline keyboard is already shown
    )

# Generic handler for any other state in PostManagementStates that doesn't have a specific handler
@post_management_router.message(StateFilter(PostManagementStates), ~Command("cancel", "myposts"))
async def handle_unknown_post_management_input(message: Message, state: FSMContext) -> None:
    """Handles unexpected input in any PostManagementStates state without a specific handler."""
    current_state = await state.get_state()
    logger.warning(f"Received unexpected input in state {current_state} from user {message.from_user.id}: {message.text}")
    await message.answer(
        "Неизвестная команда или ввод для текущего действия\\. "
        "Пожалуйста, используйте кнопки или введите `/cancel` для отмены\\.",
        parse_mode="MarkdownV2",
        reply_markup=None # Assume inline keyboard is present, or user knows /cancel
    )

# Helper function for MarkdownV2 escaping
def escape_markdown_v2(text: str) -> str:
    """Escapes special characters for MarkdownV2."""
    if not isinstance(text, str):
        return ""
    # List of characters to escape: _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in text])
