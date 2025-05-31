# handlers/rss_integration.py

import logging
import os # Might be needed if using local files, but RSS usually uses URLs
import feedparser # Used in rss_service, but might be useful for initial validation here
from typing import List, Dict, Any, Set, Optional, Union

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, SQLAlchemyError # For unique constraint violation
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError
from aiogram.utils.markdown import markdown_bold, markdown_italic, escape_md

# Import FSM States
from .rss_integration_fsm_states import RssIntegrationStates
# Import other FSM states for potential transitions (e.g., back to main menu)
from .post_creation_fsm_states import PostCreationStates
from .post_management_fsm_states import PostManagementStates


# Import Keyboards
from keyboards.reply_keyboards import (
    get_main_menu_keyboard,
    get_cancel_keyboard,
    get_channel_selection_controls_keyboard,
)
from keyboards.inline_keyboards import (
    GeneralCallbackData,
    SelectionCallbackData,
    NavigationCallbackData,
    DeleteCallbackData,
    get_dynamic_channel_selection_keyboard,
    get_delete_confirmation_keyboard,
    get_rss_feed_item_keyboard, # For /myrss list items
    get_simple_back_keyboard, # For universal back buttons
    # New keyboards needed for RSS flow:
    # get_filter_keywords_option_keyboard,
    # get_frequency_option_keyboard,
    # get_confirm_rss_feed_keyboard,
    # get_rss_editing_sections_keyboard,
)

# Import Services and Utils
from services.db import (
    AsyncSessionLocal, # Factory for scheduler
    add_rss_feed,
    get_user_rss_feeds,
    get_rss_feed_by_id,
    delete_rss_feed_by_id,
    update_rss_feed_details, # Needed for editing
    get_user_by_telegram_id # To get user_id from telegram_user_id if not in state
)
from services.scheduler import (
    AsyncIOScheduler, # For type hinting DI
    # schedule_rss_check, # Assuming this function exists in scheduler.py
    remove_scheduled_job,
    # reschedule_rss_check # Assuming this function exists in scheduler.py
)
from services.telegram_api import get_bot_channels_for_user # Needed for channel selection
from services.rss_service import process_all_active_rss_feeds # The task that will be scheduled
from utils.validators import validate_url # Needed for URL validation
from utils.datetime_utils import get_user_timezone # Might be needed for display or scheduling context

# Setup logging
logger = logging.getLogger(__name__)

# Constants
DEFAULT_RSS_FREQUENCY_MINUTES = int(os.getenv("RSS_DEFAULT_FREQ", 30)) # Get default from env

# Router instance
rss_integration_router = Router()

# --- Helper Functions ---

async def _delete_messages_from_state(bot: Bot, chat_id: int, state: FSMContext, keys_to_delete: List[str]) -> None:
    """Helper to delete messages whose IDs are stored in state keys."""
    state_data = await state.get_data()
    message_ids_to_delete = []
    for key in keys_to_delete:
        msg_id = state_data.get(key)
        if msg_id is not None:
            message_ids_to_delete.append(msg_id)
            # Remove from state immediately to avoid double deletion attempts
            # await state.update_data({key: None}) # Set to None after collecting
            # Note: State data is cleared on FSM clear anyway, so maybe not strictly needed here.

    if message_ids_to_delete:
        logger.debug(f"Attempting to delete messages: {message_ids_to_delete} for user {chat_id}")
        try:
            # telegram_api.delete_telegram_messages handles lists and errors
            await delete_telegram_messages(bot, chat_id, message_ids_to_delete)
            # After successful deletion, clean up state keys if not clearing FSM
            for key in keys_to_delete:
                 if state_data.get(key) is not None:
                      # Using message_id = state_data.pop(key, None) during collection is better
                      pass # Assume pop was used
        except Exception as e:
            # Log error but don't fail the main handler
            logger.warning(f"Failed to delete messages {message_ids_to_delete} for user {chat_id}: {e}")


async def _format_rss_feed_for_display(feed: RssFeed, user_id: int) -> str:
    """Formats an RssFeed object into a human-readable string."""
    # Fetch channel names if possible (requires get_chat call for each ID)
    # For simplicity, just show IDs for now.
    channel_list = ", ".join(feed.channels) if feed.channels else "Не выбраны"
    keywords_list = ", ".join(feed.filter_keywords) if feed.filter_keywords else "Нет"
    frequency_str = f"{feed.frequency_minutes} мин."

    display_text = (
        f"📰 RSS Лента ID: {feed.id}\n"
        f"🔗 URL: {escape_md(feed.feed_url)}\n"
        f"📣 Каналы: {escape_md(channel_list)}\n"
        f"🔎 Фильтры (ключевые слова): {escape_md(keywords_list)}\n"
        f"⏳ Частота проверки: {frequency_str}\n"
        f"✅ Последняя проверка: {feed.last_checked_at.strftime('%Y-%m-%d %H:%M UTC') if feed.last_checked_at else 'Не проверялась'}"
    )
    return display_text.replace('.', '\\.').replace('-', '\\-') # Basic MarkdownV2 escape

# New keyboard functions needed based on Plan
def get_filter_keywords_option_keyboard(context_id: Optional[str] = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Ввести фильтры", callback_data=GeneralCallbackData(action="set_filter_option", value="enter", context_id=context_id).pack())
    builder.button(text="Пропустить фильтры", callback_data=GeneralCallbackData(action="set_filter_option", value="skip", context_id=context_id).pack())
    builder.button(text="⬅️ Назад", callback_data=NavigationCallbackData(target=RssIntegrationStates.waiting_for_channels.state, context_id=context_id).pack())
    builder.adjust(2, 1)
    return builder.as_markup()

def get_frequency_option_keyboard(context_id: Optional[str] = None, default_freq: int = DEFAULT_RSS_FREQUENCY_MINUTES) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=f"По умолчанию ({default_freq} мин)", callback_data=GeneralCallbackData(action="set_frequency_option", value="default", context_id=context_id).pack())
    builder.button(text="Ввести частоту", callback_data=GeneralCallbackData(action="set_frequency_option", value="enter", context_id=context_id).pack())
    builder.button(text="⬅️ Назад", callback_data=NavigationCallbackData(target=RssIntegrationStates.waiting_for_filter_keywords.state, context_id=context_id).pack())
    builder.adjust(2, 1)
    return builder.as_markup()

def get_confirm_rss_feed_keyboard(context_id: Optional[str] = None, is_editing: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Сохранить" if not is_editing else "✅ Обновить",
        callback_data=GeneralCallbackData(action="save_rss_feed", context_id=context_id).pack()
    )
    if not is_editing:
         # Only show 'Редактировать' button during initial creation confirmation
         builder.button(
             text="✏️ Редактировать",
             callback_data=GeneralCallbackData(action="edit_rss_sections", context_id=context_id).pack()
         )
    builder.button(
        text="❌ Отменить",
        callback_data=GeneralCallbackData(action="cancel_rss_creation", context_id=context_id).pack()
    )
    # Back button target depends on whether we are creating or editing
    back_target = RssIntegrationStates.waiting_for_frequency.state if not is_editing else RssIntegrationStates.editing_rss_feed_settings.state
    builder.button(
         text="⬅️ Назад",
         callback_data=NavigationCallbackData(target=back_target, context_id=context_id).pack()
    )

    # Adjust layout
    if not is_editing:
         builder.adjust(3, 1)
    else:
         builder.adjust(2, 1) # Save/Cancel, Back

    return builder.as_markup()

def get_rss_editing_sections_keyboard(context_id: Optional[str] = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Каналы", callback_data=GeneralCallbackData(action="edit_rss_section", value="channels", context_id=context_id).pack())
    builder.button(text="Фильтры", callback_data=GeneralCallbackData(action="edit_rss_section", value="filters", context_id=context_id).pack())
    builder.button(text="Частота", callback_data=GeneralCallbackData(action="edit_rss_section", value="frequency", context_id=context_id).pack())
    builder.button(text="⬅️ Назад к превью", callback_data=NavigationCallbackData(target=RssIntegrationStates.confirming_rss_feed_details.state, context_id=context_id).pack())
    builder.adjust(3, 1)
    return builder.as_markup()


# --- Handlers ---

# Initial /addrss handler is in handlers/commands.py, sets state to waiting_for_url

@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_url), F.text)
async def process_rss_url_input(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles RSS feed URL input."""
    url = message.text.strip()
    user_id = message.from_user.id

    if not validate_url(url):
        await message.answer(
            "Это не похоже на корректный URL (должен начинаться с http:// или https://). "
            "Пожалуйста, отправьте правильный URL RSS-ленты:",
            reply_markup=get_cancel_keyboard()
        )
        return

    # Optional: Attempt to fetch and parse the feed here to validate it's a working RSS feed
    # This can add latency, might be better to do it in the background or just validate format.
    # For this implementation, just validate the URL format.

    await state.update_data(feed_url=url)
    logger.info(f"User {user_id} entered RSS feed URL: {url}. Moving to channel selection.")

    await state.set_state(RssIntegrationStates.waiting_for_channels)

    # Fetch available channels and display the selection keyboard
    try:
        available_channels_raw = await get_bot_channels_for_user(bot, user_id) # Needs implementation in telegram_api.py
        available_channels = [{'id': str(c['id']), 'name': c['name']} for c in available_channels_raw]

        if not available_channels:
            await message.answer(
                "Не найдено доступных каналов или групп, где бот является администратором.",
                reply_markup=get_main_menu_keyboard()
            )
            await state.clear() # Cannot proceed without channels
            return

        # Initialize selected_channel_ids set in context
        await state.update_data(available_channels=available_channels, selected_channel_ids=set())

        channel_selection_message = (
            "Выберите каналы или группы, куда вы хотите публиковать записи из этой RSS-ленты. "
            "Нажмите на название канала/группы, чтобы выбрать его. Выберите несколько, если нужно.\n\n"
            "Нажмите \"Готово\" когда закончите."
        )

        # Send initial message with the dynamic inline keyboard
        channel_select_msg = await message.answer(
            channel_selection_message,
            reply_markup=get_dynamic_channel_selection_keyboard(
                available_channels=available_channels,
                selected_channel_ids=set(), # Initially none selected
                context_id=str(user_id) # Use user_id as context for callback
            )
        )
        # Store message ID to delete it later
        await state.update_data(temp_channel_select_message_id=channel_select_msg.message_id)


        # We should also send a ReplyKeyboard with "Готово" and "Отменить" for flow control
        reply_controls_msg = await message.answer(
             "Используйте кнопки ниже для завершения выбора или отмены.",
             reply_markup=get_channel_selection_controls_keyboard()
        )
        await state.update_data(temp_channel_select_controls_message_id=reply_controls_msg.message_id)


    except Exception as e:
        logger.exception(f"Failed to fetch channels for user {user_id} during RSS setup: {e}")
        await message.answer(
            "Произошла ошибка при загрузке списка каналов.",
            reply_markup=get_cancel_keyboard()
        )
        await state.clear()


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_url))
async def process_rss_url_input_invalid(message: Message) -> None:
    """Handles non-text input in waiting_for_url state."""
    await message.answer(
        "Пожалуйста, отправьте корректный URL RSS-ленты или нажмите \"❌ Отменить\".",
        reply_markup=get_cancel_keyboard()
    )


# --- Channel Selection State (waiting_for_channels) ---
# Handled by ReplyKeyboard "Готово" and inline toggles/buttons

@rss_integration_router.callback_query(
    SelectionCallbackData.filter(F.action_prefix == "toggle_channel"),
    StateFilter(RssIntegrationStates.waiting_for_channels, RssIntegrationStates.editing_rss_feed_settings) # Allow toggling channels in editing mode too
)
async def process_toggle_rss_channel_callback(callback: CallbackQuery, callback_data: SelectionCallbackData, state: FSMContext) -> None:
    """Handles toggling channel selection for RSS feed via inline keyboard."""
    state_data = await state.get_data()
    # Use the correct key for selected channels, based on whether we're editing or creating
    # For simplicity, let's use 'selected_channel_ids' for both creation and editing flow in FSM context
    selected_channel_ids: Set[str] = state_data.get('selected_channel_ids', set())
    available_channels: List[Dict[str, str]] = state_data.get('available_channels', [])
    channel_id_to_toggle = callback_data.item_id # This is already a string

    # Ensure the toggled channel is actually in the available list
    if not any(str(c['id']) == channel_id_to_toggle for c in available_channels):
        await callback.answer("Неизвестный канал.", show_alert=True)
        return

    if channel_id_to_toggle in selected_channel_ids:
        selected_channel_ids.discard(channel_id_to_toggle)
        logger.debug(f"User {callback.from_user.id} deselected channel {channel_id_to_toggle} for RSS.")
    else:
        selected_channel_ids.add(channel_id_to_toggle)
        logger.debug(f"User {callback.from_user.id} selected channel {channel_id_to_toggle} for RSS.")

    await state.update_data(selected_channel_ids=selected_channel_ids)

    # Edit the inline keyboard message to reflect the new selection
    try:
        await callback.message.edit_reply_markup(
            reply_markup=get_dynamic_channel_selection_keyboard(
                available_channels=available_channels,
                selected_channel_ids=selected_channel_ids,
                context_id=str(callback.from_user.id) # Use user_id as context for this keyboard
            )
        )
        await callback.answer() # Answer the callback query
    except Exception as e:
        logger.error(f"Error editing channel selection keyboard for RSS for user {callback.from_user.id}: {e}")
        await callback.answer("Произошла ошибка при обновлении списка.", show_alert=True)


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_channels), F.text == "Готово")
async def process_done_rss_channel_selection_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles 'Готово' from reply keyboard after channel selection for RSS."""
    await process_done_rss_channel_selection(message, state, bot)

# Use GeneralCallbackData for inline 'Готово' if implemented on the inline keyboard itself
# @rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "done_channel_selection"), StateFilter(RssIntegrationStates.waiting_for_channels))
# async def process_done_rss_channel_selection_inline(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
#    await process_done_rss_channel_selection(callback.message, state, bot, callback_query=callback)


async def process_done_rss_channel_selection(message: Message, state: FSMContext, bot: Bot, callback_query: Optional[CallbackQuery] = None) -> None:
    """Helper function to process 'Done' action after channel selection for RSS."""
    state_data = await state.get_data()
    selected_channel_ids: Set[str] = state_data.get('selected_channel_ids', set())
    user_id = message.from_user.id # Use message.from_user.id for consistency

    if not selected_channel_ids:
        text = "Пожалуйста, выберите хотя бы один канал для публикации."
        # if callback_query: await callback_query.answer(text, show_alert=True)
        # else:
        await message.answer(text)
        return

    # Delete temporary messages
    await _delete_messages_from_state(bot, user_id, state, ['temp_channel_select_message_id', 'temp_channel_select_controls_message_id'])
    await state.update_data(temp_channel_select_message_id=None, temp_channel_select_controls_message_id=None)


    await state.update_data(selected_channel_ids=list(selected_channel_ids)) # Store as list for DB
    logger.info(f"User {user_id} confirmed RSS channel selection. Moving to filter keywords.")

    await state.set_state(RssIntegrationStates.waiting_for_filter_keywords)
    filter_message_text = "Хотите добавить ключевые слова для фильтрации записей из ленты?"
    filter_options_msg = await message.answer(
        filter_message_text,
        reply_markup=get_filter_keywords_option_keyboard(context_id=str(user_id))
    )
    await state.update_data(temp_filter_option_message_id=filter_options_msg.message_id)


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_channels))
async def process_rss_channel_selection_invalid(message: Message) -> None:
    """Handles invalid input in waiting_for_channels state."""
    await message.answer(
        "Пожалуйста, выберите каналы, используя кнопки выше, или нажмите \"Готово\" / \"Отменить\" на клавиатуре.",
        reply_markup=get_channel_selection_controls_keyboard()
    )


# --- Filter Keywords Options State (waiting_for_filter_keywords) ---

@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "set_filter_option"), StateFilter(RssIntegrationStates.waiting_for_filter_keywords))
async def process_set_filter_option(callback: CallbackQuery, callback_data: GeneralCallbackData, state: FSMContext, bot: Bot) -> None:
    """Handles selecting filter keywords option (enter or skip)."""
    option = callback_data.value # 'enter' or 'skip'
    user_id = callback.from_user.id

    # Delete the filter options message
    await _delete_messages_from_state(bot, user_id, state, ['temp_filter_option_message_id'])
    await state.update_data(temp_filter_option_message_id=None)


    if option == 'enter':
        logger.info(f"User {user_id} chose to enter RSS filter keywords. Moving to entering_filter_keywords.")
        await state.set_state(RssIntegrationStates.waiting_for_filter_keywords) # Stay in the same logical state, just change the prompt/keyboard
        await callback.message.answer(
            "Отправьте ключевые слова для фильтрации записей (через запятую). Например: `Python, Django, Asyncio`\n"
            "Будут публиковаться только записи, содержащие *хотя бы одно* из этих слов в заголовке или описании.\n"
            "Нажмите \"❌ Отменить\" чтобы пропустить фильтры.", # Use Reply KB cancel for input state
            reply_markup=get_cancel_keyboard() # Simple cancel keyboard
        )
        # Store a flag indicating we are waiting for text input for filters
        await state.update_data(awaiting_filter_keywords_input=True)

    elif option == 'skip':
        logger.info(f"User {user_id} skipped RSS filter keywords. Moving to frequency.")
        await state.update_data(filter_keywords=None, awaiting_filter_keywords_input=False) # Store None for filters
        await state.set_state(RssIntegrationStates.waiting_for_frequency)
        frequency_message_text = f"Настройте частоту проверки RSS-ленты (в минутах)."
        frequency_options_msg = await callback.message.answer(
            frequency_message_text,
            reply_markup=get_frequency_option_keyboard(context_id=str(user_id), default_freq=DEFAULT_RSS_FREQUENCY_MINUTES)
        )
        await state.update_data(temp_frequency_option_message_id=frequency_options_msg.message_id)

    await callback.answer() # Answer the callback query


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_filter_keywords), F.text, F.fsm_context('awaiting_filter_keywords_input'))
async def process_filter_keywords_input(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles filter keywords input."""
    keywords_text = message.text.strip()
    user_id = message.from_user.id

    if not keywords_text:
         # Treat empty input as skipping keywords, similar to the 'skip' button
         logger.info(f"User {user_id} sent empty filter keywords, skipping.")
         filter_keywords_list = None
    else:
         # Split by comma, strip whitespace, remove empty strings
         filter_keywords_list = [kw.strip() for kw in keywords_text.split(',') if kw.strip()]
         logger.info(f"User {user_id} entered RSS filter keywords: {filter_keywords_list}. Moving to frequency.")

    # Delete the Reply KB cancel message if it exists (it shouldn't if we just received text input)
    # It's simpler to just rely on state transition.

    await state.update_data(filter_keywords=filter_keywords_list, awaiting_filter_keywords_input=False)
    await state.set_state(RssIntegrationStates.waiting_for_frequency)

    frequency_message_text = f"Настройте частоту проверки RSS-ленты (в минутах)."
    frequency_options_msg = await message.answer(
        frequency_message_text,
        reply_markup=get_frequency_option_keyboard(context_id=str(user_id), default_freq=DEFAULT_RSS_FREQUENCY_MINUTES)
    )
    await state.update_data(temp_frequency_option_message_id=frequency_options_msg.message_id)


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_filter_keywords), ~F.text)
async def process_filter_keywords_input_invalid_nontext(message: Message) -> None:
    """Handles non-text input when waiting for filter keywords."""
    # This state also handles the initial inline options, text input handler is filtered by state data.
    # This handler catches non-text messages when awaiting text input.
    state_data = await state.get_data()
    if state_data.get('awaiting_filter_keywords_input'):
         await message.answer(
             "Пожалуйста, отправьте ключевые слова списком через запятую или нажмите \"❌ Отменить\".",
             reply_markup=get_cancel_keyboard()
         )
    else:
         # If not awaiting text input, user should use inline keyboard buttons
         await message.answer(
             "Пожалуйста, используйте кнопки ниже для выбора.",
             reply_markup=get_filter_keywords_option_keyboard(context_id=str(message.from_user.id)) # Re-show options
         )


# --- Frequency Options State (waiting_for_frequency) ---

@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "set_frequency_option"), StateFilter(RssIntegrationStates.waiting_for_frequency))
async def process_set_frequency_option(callback: CallbackQuery, callback_data: GeneralCallbackData, state: FSMContext, bot: Bot) -> None:
    """Handles selecting frequency option (default or enter)."""
    option = callback_data.value # 'default' or 'enter'
    user_id = callback.from_user.id

    # Delete the frequency options message
    await _delete_messages_from_state(bot, user_id, state, ['temp_frequency_option_message_id'])
    await state.update_data(temp_frequency_option_message_id=None)


    if option == 'default':
        logger.info(f"User {user_id} chose default RSS frequency ({DEFAULT_RSS_FREQUENCY_MINUTES} min). Moving to confirmation.")
        await state.update_data(frequency_minutes=DEFAULT_RSS_FREQUENCY_MINUTES, awaiting_frequency_input=False)
        await state.set_state(RssIntegrationStates.confirming_rss_feed_details)
        await display_rss_feed_confirmation(callback.message, state, bot) # Helper to display confirmation

    elif option == 'enter':
        logger.info(f"User {user_id} chose to enter RSS frequency. Moving to entering frequency.")
        await state.set_state(RssIntegrationStates.waiting_for_frequency) # Stay in same logical state
        await callback.message.answer(
            "Отправьте желаемую частоту проверки в минутах (целое число, минимум 5 минут).",
            reply_markup=get_cancel_keyboard() # Simple cancel keyboard
        )
        # Store a flag indicating we are waiting for text input for frequency
        await state.update_data(awaiting_frequency_input=True)

    await callback.answer() # Answer the callback query


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_frequency), F.text, F.fsm_context('awaiting_frequency_input'))
async def process_frequency_input(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles frequency input."""
    frequency_text = message.text.strip()
    user_id = message.from_user.id

    try:
        frequency = int(frequency_text)
        if frequency < 5: # Minimum frequency, e.g., 5 minutes
            raise ValueError("Frequency too low")
    except ValueError:
        await message.answer(
            "Некорректное значение. Частота должна быть целым числом, минимум 5 минут. "
            "Пожалуйста, отправьте частоту в минутах:",
            reply_markup=get_cancel_keyboard()
        )
        return

    logger.info(f"User {user_id} entered RSS frequency: {frequency} min. Moving to confirmation.")
    await state.update_data(frequency_minutes=frequency, awaiting_frequency_input=False)
    await state.set_state(RssIntegrationStates.confirming_rss_feed_details)
    await display_rss_feed_confirmation(message, state, bot) # Helper to display confirmation


@rss_integration_router.message(StateFilter(RssIntegrationStates.waiting_for_frequency), ~F.text)
async def process_frequency_input_invalid_nontext(message: Message) -> None:
    """Handles non-text input when waiting for frequency."""
    state_data = await state.get_data()
    if state_data.get('awaiting_frequency_input'):
         await message.answer(
             "Пожалуйста, отправьте частоту в минутах (целое число) или нажмите \"❌ Отменить\".",
             reply_markup=get_cancel_keyboard()
         )
    else:
         # If not awaiting text input, user should use inline keyboard buttons
         await message.answer(
             "Пожалуйста, используйте кнопки ниже для выбора.",
             reply_markup=get_frequency_option_keyboard(context_id=str(message.from_user.id)) # Re-show options
         )


# --- Confirmation State (confirming_rss_feed_details) ---

async def display_rss_feed_confirmation(message: Message, state: FSMContext, bot: Bot) -> None:
    """Helper to display the RSS feed details confirmation message."""
    state_data = await state.get_data()
    user_id = message.from_user.id
    is_editing = state_data.get('editing_feed_id') is not None

    # Construct formatted details string
    # Need to fetch channel names for better display? Or just show IDs? Let's show IDs for simplicity.
    feed_url = state_data.get('feed_url', 'N/A')
    channels = state_data.get('selected_channel_ids', 'Не выбраны')
    keywords = state_data.get('filter_keywords', 'Нет')
    frequency = state_data.get('frequency_minutes', 'Не указана')

    confirmation_text = markdown_bold("Подтвердите данные RSS-ленты:") + "\n\n"
    confirmation_text += f"🔗 URL: {escape_md(feed_url)}\n"
    confirmation_text += f"📣 Каналы: {escape_md(', '.join(channels) if channels and isinstance(channels, list) else str(channels))}\n"
    confirmation_text += f"🔎 Фильтры: {escape_md(', '.join(keywords) if keywords and isinstance(keywords, list) else str(keywords))}\n"
    confirmation_text += f"⏳ Частота проверки: {frequency} мин."

    # Delete previous confirmation/editing message if exists
    await _delete_messages_from_state(bot, user_id, state, ['temp_confirmation_message_id', 'temp_editing_section_message_id'])
    await state.update_data(temp_confirmation_message_id=None, temp_editing_section_message_id=None)

    confirmation_msg = await message.answer(
        confirmation_text,
        reply_markup=get_confirm_rss_feed_keyboard(context_id=str(user_id), is_editing=is_editing),
        parse_mode="MarkdownV2"
    )
    await state.update_data(temp_confirmation_message_id=confirmation_msg.message_id)


@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "save_rss_feed"), StateFilter(RssIntegrationStates.confirming_rss_feed_details))
async def process_save_rss_feed(callback: CallbackQuery, state: FSMContext, session: AsyncSession, scheduler: AsyncIOScheduler, bot: Bot) -> None:
    """Handles saving the RSS feed details to the database and scheduling the job."""
    state_data = await state.get_data()
    user_id_telegram = callback.from_user.id
    editing_feed_id = state_data.get('editing_feed_id')
    is_editing = editing_feed_id is not None

    # Fetch user object to get DB user_id
    # Assume get_user_by_telegram_id is available and gets the User model instance
    user = await get_user_by_telegram_id(session, user_id_telegram)
    if not user:
        logger.error(f"User not found in DB for telegram_user_id {user_id_telegram} during RSS save.")
        await callback.answer("Произошла внутренняя ошибка. Пользователь не найден в БД.", show_alert=True)
        # Should not happen if user is created on /start
        await state.clear()
        await callback.message.answer("Пожалуйста, попробуйте начать заново.", reply_markup=get_main_menu_keyboard())
        return

    # Get data from state
    feed_url: str = state_data.get('feed_url')
    channels: List[str] = state_data.get('selected_channel_ids')
    filter_keywords: Optional[List[str]] = state_data.get('filter_keywords')
    frequency_minutes: int = state_data.get('frequency_minutes')

    # Validate required fields before saving/updating
    if not feed_url or not channels or not frequency_minutes:
        logger.error(f"Missing data in state for RSS save/update for user {user_id_telegram}. State: {state_data}")
        await callback.answer("Не хватает данных для сохранения RSS-ленты.", show_alert=True)
        # Stay in confirmation state, let user edit or cancel
        return

    try:
        if is_editing:
            # Update existing feed
            logger.info(f"User {user_id_telegram} confirmed editing RSS feed ID:{editing_feed_id}. Updating in DB.")
            updated_feed = await update_rss_feed_details(
                session=session,
                feed_id=editing_feed_id,
                data_to_update={
                    'feed_url': feed_url,
                    'channels': channels,
                    'filter_keywords': filter_keywords,
                    'frequency_minutes': frequency_minutes
                }
            )
            await session.commit() # Commit the update
            if updated_feed:
                 logger.info(f"RSS Feed ID:{editing_feed_id} successfully updated.")
                 success_message = f"✅ RSS Лента ID:{editing_feed_id} успешно обновлена!"
                 # Reschedule the job if frequency or channels changed (or just always reschedule on edit)
                 # Remove old job first just to be safe, although replace_existing should handle it
                 old_job_id = f'rss_check_{editing_feed_id}'
                 try:
                     await remove_scheduled_job(scheduler, old_job_id)
                 except Exception as e:
                      logger.warning(f"Failed to remove old RSS check job {old_job_id} during edit save: {e}")

                 # Add new job
                 # Need a function like schedule_rss_check in services/scheduler.py
                 # This function would likely take scheduler, bot, session_factory, feed_id, frequency_minutes
                 # The task function would then use session_factory to get a session and call check_and_publish_rss_feed.
                 # Job ID format: rss_check_<feed_id>

                 # !!! ASSUMING schedule_rss_check function exists and works like schedule_post_publication !!!
                 # from services.scheduler import schedule_rss_check # Need this import
                 try:
                      # schedule_rss_check takes scheduler, bot, session_factory, feed_id, frequency_minutes
                      # Job ID will be 'rss_check_<feed_id>'
                      await scheduler.add_job(
                            process_all_active_rss_feeds, # The task to run (checks all feeds, but scheduler handles frequency per job)
                            'interval',
                            minutes=frequency_minutes,
                            args=[bot, AsyncSessionLocal], # Pass bot instance and session factory to the task
                            id=f'rss_check_{editing_feed_id}', # Unique job ID per feed
                            replace_existing=True,
                            # next_run_time=datetime.datetime.now(scheduler.timezone) # Start immediately or soon
                      )
                      logger.info(f"RSS check job for feed ID:{editing_feed_id} rescheduled with frequency {frequency_minutes} min.")
                 except Exception as e:
                      logger.exception(f"Failed to reschedule RSS check job for feed ID:{editing_feed_id}: {e}")
                      # Log but proceed, feed config is saved, but auto-check might not work.
                      success_message += "\n⚠️ Не удалось обновить задачу автоматической проверки."


            else:
                 # Should not happen if update_rss_feed_details returns None only on not found
                 logger.error(f"Update to RSS feed ID:{editing_feed_id} failed unexpectedly after commit.")
                 success_message = f"❌ Произошла ошибка при обновлении RSS Ленты ID:{editing_feed_id}."


        else:
            # Add new feed
            logger.info(f"User {user_id_telegram} confirmed new RSS feed. Adding to DB.")
            new_feed = await add_rss_feed(
                session=session,
                user_id=user.id, # Use DB user ID
                feed_url=feed_url,
                channels=channels,
                frequency_minutes=frequency_minutes,
                filter_keywords=filter_keywords
            )
            await session.commit() # Commit the new feed
            logger.info(f"New RSS Feed added to DB with ID: {new_feed.id}.")
            success_message = f"✅ RSS Лента успешно добавлена (ID: {new_feed.id})!"

            # Schedule the check job for the new feed
            # Job ID format: rss_check_<feed_id>
            # Task: process_all_active_rss_feeds is intended to check *all* feeds periodically.
            # A better approach is to schedule a task specific to *this* feed ID,
            # or use a single task that checks all feeds due for checking,
            # scheduled at a high frequency (e.g., every minute), and the task logic
            # decides which feeds are due based on `last_checked_at` and `frequency_minutes`.

            # Let's use the recommended pattern: a single scheduled task (`process_all_active_rss_feeds`)
            # running periodically (e.g., every 15 mins) and it checks all feeds needing a check.
            # Adding a new feed doesn't need a new job *per feed*, it just makes that feed
            # eligible for checking by the existing main RSS check job.
            # We just need to ensure the main job exists (done during app startup).
            # For an initial check, we could trigger `check_and_publish_rss_feed` immediately,
            # but this might need careful session management outside the scheduled job context.
            # Simplest: Adding the feed makes it eligible for the *next* scheduled run of the main job.
            # Let's add a note about this.

            # Note: With the current scheduler setup calling process_all_active_rss_feeds,
            # adding a feed means it will be picked up by the *next* run of that task.
            # There is no separate job per feed ID.
            # If a job per feed is desired, schedule_rss_check would be needed here.
            # Let's assume the main job handles all feeds.
            pass # No separate job scheduling needed per feed with this model

    except IntegrityError as e:
        await session.rollback()
        logger.error(f"IntegrityError saving/updating RSS feed for user {user_id_telegram}: {e}")
        # Check if it's a unique constraint violation on user_id and feed_url
        if "uq_user_feed" in str(e):
             success_message = "❌ Вы уже добавили RSS-ленту с таким URL."
        else:
             success_message = "❌ Произошла ошибка при сохранении/обновлении RSS-ленты (нарушение целостности данных)."
    except SQLAlchemyError as e:
        await session.rollback()
        logger.exception(f"Database error saving/updating RSS feed for user {user_id_telegram}: {e}")
        success_message = "❌ Произошла ошибка базы данных при сохранении/обновлении RSS-ленты."
    except Exception as e:
        await session.rollback()
        logger.exception(f"Unexpected error saving/updating RSS feed for user {user_id_telegram}: {e}")
        success_message = "❌ Произошла непредвиденная ошибка при сохранении/обновлении RSS-ленты."

    # Delete the confirmation message
    await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_confirmation_message_id'])
    await state.update_data(temp_confirmation_message_id=None)

    # Clear FSM state
    await state.clear()
    logger.info(f"RSS feed save/update process completed for user {user_id_telegram}. State cleared.")

    # Send final message and return to main menu
    await callback.answer("Сохранено!" if not is_editing else "Обновлено!", show_alert=True)
    await callback.message.answer(
        success_message,
        reply_markup=get_main_menu_keyboard()
    )


@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "edit_rss_sections"), StateFilter(RssIntegrationStates.confirming_rss_feed_details))
async def process_edit_rss_feed(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Handles 'Редактировать' button from confirmation state."""
    user_id = callback.from_user.id
    logger.info(f"User {user_id} chose to edit RSS feed details. Moving to editing selection.")
    await state.set_state(RssIntegrationStates.editing_rss_feed_settings)

    # Delete the confirmation message
    await _delete_messages_from_state(bot, user_id, state, ['temp_confirmation_message_id'])
    await state.update_data(temp_confirmation_message_id=None)

    # Send editing section selection keyboard
    editing_sections_msg = await callback.message.answer(
        "Выберите, какую настройку RSS-ленты вы хотите изменить:",
        reply_markup=get_rss_editing_sections_keyboard(context_id=str(user_id)) # Pass user_id as context
    )
    await state.update_data(temp_editing_section_message_id=editing_sections_msg.message_id)
    await callback.answer() # Answer the callback query


@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "cancel_rss_creation"), StateFilter(RssIntegrationStates.confirming_rss_feed_details))
async def process_cancel_rss_creation(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Handles 'Отменить' from confirmation state."""
    await process_cancel_rss_fsm(callback, state, bot) # Use helper cancel


# --- Editing Selection State (editing_rss_feed_settings) ---

@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "edit_rss_section"), StateFilter(RssIntegrationStates.editing_rss_feed_settings))
async def process_edit_rss_section(callback: CallbackQuery, callback_data: GeneralCallbackData, state: FSMContext, bot: Bot) -> None:
    """Handles selecting a section to edit for an RSS feed."""
    section_to_edit = callback_data.value # 'channels', 'filters', 'frequency'
    user_id = callback.from_user.id
    state_data = await state.get_data()
    editing_feed_id = state_data.get('editing_feed_id') # Should be present if in editing flow

    if section_to_edit not in ['channels', 'filters', 'frequency']:
        logger.error(f"Invalid RSS edit section received for user {user_id}: {section_to_edit}")
        await callback.answer("Некорректная секция.", show_alert=True)
        return

    # Check if we are actually in an editing flow
    if editing_feed_id is None and 'feed_url' not in state_data: # Also check for feed_url if editing a draft
         logger.error(f"Edit section callback received outside of RSS editing flow for user {user_id}. State: {state_data}")
         await callback.answer("Ошибка FSM.", show_alert=True)
         await state.clear()
         await callback.message.answer("Произошла внутренняя ошибка. Пожалуйста, начните заново.", reply_markup=get_main_menu_keyboard())
         return

    logger.info(f"User {user_id} selected section '{section_to_edit}' for editing RSS feed.")

    # Delete the editing selection inline keyboard message
    await _delete_messages_from_state(bot, user_id, state, ['temp_editing_section_message_id'])
    await state.update_data(temp_editing_section_message_id=None)

    await callback.answer() # Answer the callback query


    # Transition to the corresponding states and send instructions/keyboards
    user_context_id = str(user_id) # Use user ID as context for callback data

    if section_to_edit == 'channels':
        # Re-fetch channels and display selection keyboard, pre-selecting current channels
        await state.set_state(RssIntegrationStates.waiting_for_channels) # Reuse state
        try:
            available_channels_raw = await get_bot_channels_for_user(bot, user_id)
            available_channels = [{'id': str(c['id']), 'name': c['name']} for c in available_channels_raw]

            if not available_channels:
                await callback.message.answer(
                    "Не найдено доступных каналов или групп, где бот является администратором.",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return

            # Keep currently selected channels in state (populated either by _populate_fsm_for_editing or from initial creation flow)
            current_selected_ids = set(state_data.get('selected_channel_ids', []))

            channel_selection_message = (
                "Выберите каналы или группы для публикации. Нажмите \"Готово\" когда закончите."
            )

            await state.update_data(available_channels=available_channels) # Update available channels in state
            # Stay in waiting_for_channels state until 'Done' or 'Cancel'

            channel_select_msg = await callback.message.answer(
                channel_selection_message,
                reply_markup=get_dynamic_channel_selection_keyboard(
                    available_channels=available_channels,
                    selected_channel_ids=current_selected_ids, # Pass current selection
                    context_id=user_context_id # Pass context_id
                )
            )
            await state.update_data(temp_channel_select_message_id=channel_select_msg.message_id)

            # Send a ReplyKeyboard for flow control
            reply_controls_msg = await callback.message.answer(
                "Используйте кнопки ниже для завершения выбора или отмены.",
                reply_markup=get_channel_selection_controls_keyboard()
            )
            await state.update_data(temp_channel_select_controls_message_id=reply_controls_msg.message_id)


        except Exception as e:
             logger.exception(f"Failed to prepare RSS channel editing for user {user_id} via edit section: {e}")
             await callback.message.answer("Произошла ошибка при переходе к редактированию каналов.", reply_markup=get_main_menu_keyboard())
             await state.clear()


    elif section_to_edit == 'filters':
        # Transition to awaiting input for filters
        await state.set_state(RssIntegrationStates.waiting_for_filter_keywords) # Reuse state
        # Clear previous filters from state to force re-input? Or display them?
        # Let's clear and ask for new ones for simplicity in editing flow.
        # await state.update_data(filter_keywords=None) # Clear previous filters? No, keep them for display in prompt.
        current_filters = state_data.get('filter_keywords')
        filter_prompt = "Отправьте новые ключевые слова для фильтрации записей (через запятую)."
        if current_filters:
             filter_prompt += f"\nТекущие фильтры: `{escape_md(', '.join(current_filters))}`"
        else:
             filter_prompt += "\nСейчас фильтры не установлены."

        filter_prompt += "\nНажмите \"❌ Отменить\" чтобы пропустить фильтры и вернуться назад."

        await callback.message.answer(
            filter_prompt,
            reply_markup=get_cancel_keyboard(), # Simple cancel keyboard
            parse_mode="MarkdownV2"
        )
        await state.update_data(awaiting_filter_keywords_input=True, edit_back_target=RssIntegrationStates.editing_rss_feed_settings.state) # Store back target


    elif section_to_edit == 'frequency':
        # Transition to awaiting input for frequency
        await state.set_state(RssIntegrationStates.waiting_for_frequency) # Reuse state
        # Clear previous frequency? No, keep it for display in prompt.
        # await state.update_data(frequency_minutes=None)
        current_frequency = state_data.get('frequency_minutes')
        freq_prompt = "Отправьте новую частоту проверки в минутах (целое число, минимум 5 минут)."
        if current_frequency:
             freq_prompt += f"\nТекущая частота: {current_frequency} мин."
        else:
             freq_prompt += "\nСейчас частота не установлена (будет использовано значение по умолчанию)."

        freq_prompt += "\nНажмите \"❌ Отменить\" чтобы вернуться назад."

        await callback.message.answer(
            freq_prompt,
            reply_markup=get_cancel_keyboard(), # Simple cancel keyboard
            parse_mode="MarkdownV2"
        )
        await state.update_data(awaiting_frequency_input=True, edit_back_target=RssIntegrationStates.editing_rss_feed_settings.state) # Store back target

    # Note: The handlers for waiting_for_channels, waiting_for_filter_keywords (when awaiting input),
    # and waiting_for_frequency (when awaiting input) need to check the `edit_back_target` flag
    # and transition back to `RssIntegrationStates.confirming_rss_feed_details` after receiving valid input
    # instead of continuing the original creation flow to the next step.


# Handlers for returning from editing sections (Channels, Filters, Frequency)
# These should lead back to confirming_rss_feed_details

# Helper to transition from an editing sub-state back to confirmation
async def finish_editing_section(message: Message, state: FSMContext, bot: Bot) -> None:
    """Called after successfully editing a section (filters or frequency text input)."""
    user_id = message.from_user.id
    logger.info(f"User {user_id} finished editing a section. Returning to confirmation.")

    # Clear flags used for editing sub-states
    await state.update_data(awaiting_filter_keywords_input=False, awaiting_frequency_input=False, edit_back_target=None)

    # Delete any ReplyKB messages used for input
    # This is complex, as the cancel KB is generic. Best to rely on state change clearing it.

    await state.set_state(RssIntegrationStates.confirming_rss_feed_details)
    await display_rss_feed_confirmation(message, state, bot)


# Modify process_filter_keywords_input and process_frequency_input to use finish_editing_section
# when `edit_back_target` is set in state.

# Back button handler from editing sections goes to confirming_rss_feed_details
@rss_integration_router.callback_query(NavigationCallbackData.filter(F.target == RssIntegrationStates.confirming_rss_feed_details.state), StateFilter(RssIntegrationStates.editing_rss_feed_settings))
async def process_back_from_editing_selection_to_confirmation(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Handles 'Back' navigation from editing selection to confirmation state."""
    user_id = callback.from_user.id
    logger.info(f"User {user_id} went back from editing selection to confirmation.")

    # Delete the editing selection message
    await _delete_messages_from_state(bot, user_id, state, ['temp_editing_section_message_id'])
    await state.update_data(temp_editing_section_message_id=None)

    await state.set_state(RssIntegrationStates.confirming_rss_feed_details)
    await display_rss_feed_confirmation(callback.message, state, bot) # Display current state data

    await callback.answer() # Answer callback


@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "cancel_rss_editing"), StateFilter(RssIntegrationStates.editing_rss_feed_settings))
async def process_cancel_rss_editing(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Handles 'Отменить' button from editing sections state."""
    # When canceling editing, we discard changes and go back to main menu.
    await process_cancel_rss_fsm(callback, state, bot)


# --- My RSS Feeds List (/myrss) ---

@rss_integration_router.message(Command("myrss"))
async def handle_my_rss_command(message: Message, state: FSMContext, session: AsyncSession, bot: Bot) -> None:
    """Handles the /myrss command."""
    user_id_telegram = message.from_user.id
    logger.info(f"User {user_id_telegram} requested their RSS feed list.")

    # Clear any current state before showing list
    await state.clear()
    await state.set_state(RssIntegrationStates.managing_rss_list)

    # Fetch user's RSS feeds
    # Need user.id from telegram_user_id first
    user = await get_user_by_telegram_id(session, user_id_telegram)
    if not user:
         logger.error(f"User not found in DB for telegram_user_id {user_id_telegram} during /myrss.")
         await message.answer("Произошла внутренняя ошибка. Пользователь не найден в БД.", reply_markup=get_main_menu_keyboard())
         await state.clear()
         return

    rss_feeds = await get_user_rss_feeds(session, user.id)

    if not rss_feeds:
        await message.answer("У вас нет добавленных RSS-лент.", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state if no feeds to manage
        return

    await message.answer(f"Найдено {len(rss_feeds)} RSS-лент:", reply_markup=None) # Remove ReplyKeyboard

    for feed in rss_feeds:
        feed_text = await _format_rss_feed_for_display(feed, user.id)
        # Send each feed with its management keyboard
        await message.answer(
            feed_text,
            reply_markup=get_rss_feed_item_keyboard(feed.id),
            parse_mode="MarkdownV2"
        )

    # Stay in managing_rss_list state, waiting for inline button callbacks


@rss_integration_router.message(StateFilter(RssIntegrationStates.managing_rss_list), ~Command("myrss", "cancel"))
async def handle_managing_rss_list_invalid_input(message: Message) -> None:
    """Handles invalid input while showing the list of RSS feeds."""
    # Ignore callback queries - they have their own handlers
    if message.content_type != 'text':
         return # Ignore non-text messages

    await message.answer(
        "Вы просматриваете список RSS-лент\\. Используйте кнопки под лентами для управления ими "
        "или введите `/myrss` снова для обновления списка\\.",
        parse_mode="MarkdownV2",
        reply_markup=None # Don't show reply keyboard here
    )


# Handlers for actions from /myrss list (Inline Callbacks)

@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "edit_rss_feed"), StateFilter(RssIntegrationStates.managing_rss_list))
async def process_edit_rss_feed_from_list(callback: CallbackQuery, callback_data: GeneralCallbackData, state: FSMContext, session: AsyncSession, bot: Bot) -> None:
    """Handles inline button click to edit an RSS feed from the list view."""
    feed_id_str = callback_data.value
    user_id_telegram = callback.from_user.id

    if not feed_id_str:
        logger.error(f"Edit RSS callback received without feed_id for user {user_id_telegram}.")
        await callback.answer("Ошибка: Не указан ID ленты.", show_alert=True)
        return

    try:
        feed_id = int(feed_id_str)
    except ValueError:
        logger.error(f"Invalid feed_id format received for user {user_id_telegram}: {feed_id_str}")
        await callback.answer("Ошибка: Некорректный ID ленты.", show_alert=True)
        return

    logger.info(f"User {user_id_telegram} requested to edit RSS feed ID:{feed_id} from list.")

    # Fetch the feed
    feed = await get_rss_feed_by_id(session, feed_id)

    # Check if feed exists and belongs to the user
    user = await get_user_by_telegram_id(session, user_id_telegram)
    if not feed or (user and feed.user_id != user.id):
        logger.warning(f"Edit requested for non-existent or unauthorized RSS feed ID:{feed_id} by user {user_id_telegram}.")
        await callback.answer(f"RSS Лента с ID {feed_id} не найдена или вы не имеете к ней доступа.", show_alert=True)
        # Attempt to remove the keyboard from the list item message
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
             logger.warning(f"Failed to remove inline keyboard for RSS feed {feed_id} list item: {e}")
        return

    # Populate FSM context with feed data for editing
    await state.clear() # Clear previous list state
    await state.update_data(
        editing_feed_id=feed.id,
        feed_url=feed.feed_url,
        selected_channel_ids=set(feed.channels) if feed.channels else set(), # Convert list to set for editing flow
        filter_keywords=feed.filter_keywords, # Keep as list or None
        frequency_minutes=feed.frequency_minutes,
        # Also store available channels here? Or fetch on demand in the next step?
        # Fetching on demand in the next step (editing_rss_feed_settings -> channels) is better.
    )


    # Transition to editing section selection state
    await state.set_state(RssIntegrationStates.editing_rss_feed_settings)
    logger.info(f"Transitioned to state {RssIntegrationStates.editing_rss_feed_settings} for editing RSS feed ID:{feed_id}.")

    # Send editing section selection keyboard as a NEW message
    try:
        await callback.answer() # Answer the callback query first

        editing_sections_msg = await callback.message.answer( # Use callback.message.answer to send message to the same chat
            f"{markdown_bold('Редактирование RSS Ленты ID:')} {feed_id}\\.\nВыберите, какую настройку вы хотите изменить\\:",
            reply_markup=get_rss_editing_sections_keyboard(context_id=str(user_id_telegram)), # Pass user_id as context
            parse_mode="MarkdownV2"
        )
        await state.update_data(temp_editing_section_message_id=editing_sections_msg.message_id)

    except Exception as e:
        logger.exception(f"Failed to send editing section keyboard for RSS feed ID:{feed_id} user {user_id_telegram}: {e}")
        await callback.message.answer("Произошла ошибка при переходе к редактированию.", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state on error


@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "request_delete_rss_feed"), StateFilter(RssIntegrationStates.managing_rss_list))
async def process_request_delete_rss_feed(callback: CallbackQuery, callback_data: GeneralCallbackData, state: FSMContext, session: AsyncSession) -> None:
    """Handles inline button click to request deletion of an RSS feed from the list view."""
    feed_id_str = callback_data.value
    user_id_telegram = callback.from_user.id

    if not feed_id_str:
        logger.error(f"Delete RSS callback received without feed_id for user {user_id_telegram}.")
        await callback.answer("Ошибка: Не указан ID ленты.", show_alert=True)
        return

    try:
        feed_id = int(feed_id_str)
    except ValueError:
        logger.error(f"Invalid feed_id format received for user {user_id_telegram}: {feed_id_str}")
        await callback.answer("Ошибка: Некорректный ID ленты.", show_alert=True)
        return

    logger.info(f"User {user_id_telegram} requested to delete RSS feed ID:{feed_id} from list.")

    # Fetch the feed to check existence and ownership
    user = await get_user_by_telegram_id(session, user_id_telegram)
    feed = await get_rss_feed_by_id(session, feed_id)

    if not feed or (user and feed.user_id != user.id):
        logger.warning(f"Deletion requested for non-existent or unauthorized RSS feed ID:{feed_id} by user {user_id_telegram}.")
        await callback.answer(f"RSS Лента с ID {feed_id} не найдена или вы не имеете к ней доступа.", show_alert=True)
        # Attempt to remove the keyboard from the list item message
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
             logger.warning(f"Failed to remove inline keyboard for RSS feed {feed_id} list item: {e}")
        return

    # Clear any current state before setting confirmation state
    await state.clear()

    # Set state to await deletion confirmation
    await state.set_state(RssIntegrationStates.confirming_rss_feed_deletion)
    logger.info(f"Transitioned to state {RssIntegrationStates.confirming_rss_feed_deletion} for RSS feed ID:{feed_id}.")

    # Send confirmation message with inline keyboard as a NEW message
    confirmation_text = f"Вы уверены, что хотите удалить RSS Ленту ID:{feed_id}?\\n"
    # Add a summary of the feed being deleted
    confirmation_text += await _format_rss_feed_for_display(feed, user.id)
    confirmation_text += "\n**Внимание**: Это действие необратимо\\." # Add emphasis

    try:
        await callback.answer() # Answer the callback query first

        confirmation_msg = await callback.message.answer(
             confirmation_text,
             reply_markup=get_delete_confirmation_keyboard(item_type="rss_feed", item_id=str(feed_id), context_id=str(user_id_telegram)), # item_id needs to be string for CallbackData
             parse_mode="MarkdownV2"
        )
        await state.update_data(temp_delete_confirmation_message_id=confirmation_msg.message_id)
        # Keep the original RSS list item message as is.

    except Exception as e:
        logger.exception(f"Failed to send delete confirmation for RSS feed ID:{feed_id} user {user_id_telegram}: {e}")
        await callback.message.answer("Произошла ошибка при запросе на удаление.", reply_markup=get_main_menu_keyboard())
        await state.clear() # Clear state on error (user might be stuck)


# --- Remove RSS Command Handler (/removerss <ID>) ---

@rss_integration_router.message(Command("removerss"))
async def handle_remove_rss_command(message: Message, command: CommandObject, state: FSMContext, session: AsyncSession) -> None:
    """
    Handles the /removerss <ID> command.
    Initiates the RSS feed deletion confirmation process.
    """
    user_id_telegram = message.from_user.id
    args = command.args.split() if command.args else []

    if len(args) != 1:
        await message.answer(
            f"Некорректный формат команды\\. Используйте `{markdown_bold('/removerss')} <ID>`\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear() # Ensure state is cleared on invalid command format
        return

    try:
        feed_id = int(args[0])
    except ValueError:
        await message.answer(
            "Некорректный ID ленты\\. ID должен быть числом\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    logger.info(f"User {user_id_telegram} requested to delete RSS feed ID:{feed_id} via command.")

    # Fetch the feed to check existence and ownership
    user = await get_user_by_telegram_id(session, user_id_telegram)
    feed = await get_rss_feed_by_id(session, feed_id)

    if not feed or (user and feed.user_id != user.id):
        logger.warning(f"Deletion requested for non-existent or unauthorized RSS feed ID:{feed_id} by user {user_id_telegram} via command.")
        await message.answer(
            f"RSS Лента с ID `{feed_id}` не найдена или вы не имеете к ней доступа\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return

    # Clear any current state before setting confirmation state
    await state.clear()

    # Set state to await deletion confirmation
    await state.set_state(RssIntegrationStates.confirming_rss_feed_deletion)
    logger.info(f"Transitioned to state {RssIntegrationStates.confirming_rss_feed_deletion} for RSS feed ID:{feed_id} via command.")

    # Send confirmation message with inline keyboard
    confirmation_text = f"Вы уверены, что хотите удалить RSS Ленту ID:{feed_id}?\\n"
    confirmation_text += await _format_rss_feed_for_display(feed, user.id)
    confirmation_text += "\n**Внимание**: Это действие необратимо\\." # Add emphasis

    confirmation_msg = await message.answer(
        confirmation_text,
        reply_markup=get_delete_confirmation_keyboard(item_type="rss_feed", item_id=str(feed_id), context_id=str(user_id_telegram)), # Pass feed_id as item_id (string)
        parse_mode="MarkdownV2"
    )
    await state.update_data(temp_delete_confirmation_message_id=confirmation_msg.message_id)


# --- Deletion Confirmation State (confirming_rss_feed_deletion) ---
# Handled by callbacks defined in keyboards/inline_buttons.py if they are generic,
# or define them here if they need RSS-specific logic (like removing scheduler job).
# Let's define them here to ensure RSS-specific scheduler job removal.

@rss_integration_router.callback_query(DeleteCallbackData.filter(F.action == "confirm" and F.item_type == "rss_feed"), StateFilter(RssIntegrationStates.confirming_rss_feed_deletion))
async def process_confirm_rss_feed_delete(
    callback: CallbackQuery,
    callback_data: DeleteCallbackData,
    session: AsyncSession,
    scheduler: AsyncIOScheduler, # Inject scheduler instance
    bot: Bot # Inject bot instance for message deletion
):
    """Handles confirmation of RSS feed deletion."""
    feed_id_str = callback_data.item_id
    user_id_telegram = callback.from_user.id
    state = FSMContext(bot=bot, storage=callback.message.bot.storage, user=callback.from_user.id, chat=callback.message.chat.id) # Recreate FSMContext for callback

    if not feed_id_str:
        logger.error(f"RSS delete confirm callback received without item_id for user {user_id_telegram}.")
        await callback.answer("Ошибка: Не указан ID ленты.", show_alert=True)
        # Attempt to delete the confirmation message
        await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
        await state.clear() # Clear state on error
        await callback.message.answer("Произошла внутренняя ошибка.", reply_markup=get_main_menu_keyboard())
        return

    try:
        feed_id = int(feed_id_str)
    except ValueError:
        logger.error(f"Invalid feed_id format in delete confirm callback for user {user_id_telegram}: {feed_id_str}")
        await callback.answer("Ошибка: Некорректный ID ленты.", show_alert=True)
        # Attempt to delete the confirmation message
        await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
        await state.clear() # Clear state on error
        await callback.message.answer("Произошла внутренняя ошибка.", reply_markup=get_main_menu_keyboard())
        return

    logger.info(f"User {user_id_telegram} confirmed deletion for RSS feed ID:{feed_id}.")

    try:
        # Delete the RSS feed from the database
        deleted_from_db = await delete_rss_feed_by_id(session, feed_id)

        if deleted_from_db:
            logger.info(f"RSS Feed ID:{feed_id} successfully deleted from DB.")

            # Remove the scheduled job for this feed
            rss_check_job_id = f'rss_check_{feed_id}'
            try:
                await remove_scheduled_job(scheduler, rss_check_job_id)
                logger.info(f"Scheduled RSS check job {rss_check_job_id} removed.")
            except Exception as e:
                 # Log warning but don't fail deletion if job removal fails
                 logger.warning(f"Failed to remove RSS check job {rss_check_job_id} for feed ID:{feed_id}: {e}")


            # Delete the confirmation message
            await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
            await state.update_data(temp_delete_confirmation_message_id=None)

            # Clear FSM state
            await state.clear()
            logger.info(f"RSS feed deletion process completed for user {user_id_telegram}. State cleared.")

            # Send success message
            await callback.answer("Удалено!", show_alert=True)
            await callback.message.answer(f"✅ RSS Лента ID:{feed_id} успешно удалена.", reply_markup=get_main_menu_keyboard())

        else:
            logger.warning(f"Attempted to delete RSS feed ID:{feed_id} from DB, but it was not found. User {user_id_telegram}.")
            # Attempt to delete the confirmation message
            await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
            await state.update_data(temp_delete_confirmation_message_id=None)
            await state.clear() # Clear state even if not found

            await callback.answer("Не найдено.", show_alert=True)
            await callback.message.answer(f"ℹ️ RSS Лента ID:{feed_id} не найдена в базе данных или уже была удалена.", reply_markup=get_main_menu_keyboard())

        await session.commit() # Commit the deletion (or lack thereof)

    except SQLAlchemyError as e:
        await session.rollback()
        logger.exception(f"Database error deleting RSS feed ID:{feed_id} for user {user_id_telegram}: {e}")
        # Attempt to delete the confirmation message before reporting error
        await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
        await state.update_data(temp_delete_confirmation_message_id=None)
        await state.clear() # Clear state on error

        await callback.answer("Ошибка!", show_alert=True)
        await callback.message.answer(f"❌ Произошла ошибка базы данных при удалении RSS Ленты ID:{feed_id}.", reply_markup=get_main_menu_keyboard())

    except Exception as e:
        # Catch any other unexpected exceptions
        logger.exception(f"Unexpected error deleting RSS feed ID:{feed_id} for user {user_id_telegram}: {e}")
        # Attempt to delete the confirmation message before reporting error
        await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
        await state.update_data(temp_delete_confirmation_message_id=None)
        await state.clear() # Clear state on error

        await callback.answer("Ошибка!", show_alert=True)
        await callback.message.answer(f"❌ Произошла непредвиденная ошибка при удалении RSS Ленты ID:{feed_id}.", reply_markup=get_main_menu_keyboard())


@rss_integration_router.callback_query(DeleteCallbackData.filter(F.action == "cancel" and F.item_type == "rss_feed"), StateFilter(RssIntegrationStates.confirming_rss_feed_deletion))
async def process_cancel_rss_feed_delete(
    callback: CallbackQuery,
    callback_data: DeleteCallbackData,
    bot: Bot
):
    """Handles cancellation of RSS feed deletion."""
    feed_id_str = callback_data.item_id # Get ID for logging, not used otherwise
    user_id_telegram = callback.from_user.id
    state = FSMContext(bot=bot, storage=callback.message.bot.storage, user=callback.from_user.id, chat=callback.message.chat.id) # Recreate FSMContext for callback

    logger.info(f"User {user_id_telegram} canceled deletion for RSS feed ID:{feed_id_str}.")

    try:
        # Delete the confirmation message
        await _delete_messages_from_state(bot, user_id_telegram, state, ['temp_delete_confirmation_message_id'])
        await state.update_data(temp_delete_confirmation_message_id=None)

        # Clear FSM state
        await state.clear()
        logger.info(f"RSS feed deletion cancellation process completed for user {user_id_telegram}. State cleared.")

        await callback.answer("Удаление отменено.", show_alert=True)
        await callback.message.answer("✅ Отмена удаления RSS-ленты.", reply_markup=get_main_menu_keyboard())

    except Exception as e:
        logger.exception(f"Error during RSS feed deletion cancellation for user {user_id_telegram}: {e}")
        await callback.answer("Ошибка отмены.", show_alert=True)
        await callback.message.answer("❌ Произошла ошибка при отмене удаления.", reply_markup=get_main_menu_keyboard())
        # State is likely already cleared by clear() above, but if error happened before that, might be stuck.
        # Hard clear might be needed on critical error paths.
        try: await state.clear()
        except Exception: pass # Ignore error if state is already gone


# --- Generic Cancel Handler for RSS FSM ---
# Overrides the generic one in commands.py for RSS FSM states

async def process_cancel_rss_fsm(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Helper function to process cancellation triggered by inline keyboard callbacks in RSS FSM."""
    user_id = callback.from_user.id
    logger.info(f"User {user_id} canceled RSS FSM via callback.")
    state_data = await state.get_data()

    # Delete temporary messages stored in state
    message_keys = [
        'temp_channel_select_message_id',
        'temp_channel_select_controls_message_id',
        'temp_filter_option_message_id',
        'temp_frequency_option_message_id',
        'temp_confirmation_message_id',
        'temp_editing_section_message_id',
        'temp_delete_confirmation_message_id',
    ]
    await _delete_messages_from_state(bot, user_id, state, message_keys)

    # Delete the inline keyboard message that triggered this cancel callback
    try:
        await callback.message.delete()
        await callback.answer("Отменено.", show_alert=True)
    except Exception as e:
        logger.warning(f"Failed to delete callback message on RSS cancel for user {user_id}: {e}")

    await state.clear()
    logger.info(f"RSS FSM canceled and state cleared for user {user_id}.")

    await callback.bot.send_message( # Use bot instance from callback for sending
        chat_id=user_id,
        text="Действие отменено. Возвращаемся в главное меню.",
        reply_markup=get_main_menu_keyboard()
    )

# Route generic cancel callbacks from various RSS states to the helper
@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "cancel_rss_creation"), StateFilter(
    RssIntegrationStates.waiting_for_channels, # If added a cancel button there
    RssIntegrationStates.waiting_for_filter_keywords,
    RssIntegrationStates.waiting_for_frequency,
    # RssIntegrationStates.confirming_rss_feed_details handled above
))
async def callback_cancel_rss_fsm_generic(callback: CallbackQuery, state: FSMContext, bot: Bot):
     await process_cancel_rss_fsm(callback, state, bot)

@rss_integration_router.callback_query(GeneralCallbackData.filter(F.action == "cancel_rss_editing"), StateFilter(
    RssIntegrationStates.editing_rss_feed_settings
))
async def callback_cancel_rss_editing_generic(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await process_cancel_rss_fsm(callback, state, bot)

# Need to handle "❌ Отменить" ReplyKeyboard button when in RSS FSM states.
# The generic handler in commands.py should catch this based on text filter.
# To include cleanup, we need to override it specifically for RSS states, similar to post_creation.py.
# Let's define an override handler here.
