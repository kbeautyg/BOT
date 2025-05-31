# handlers/post_creation.py

import logging
import os
import datetime
from typing import List, Dict, Any, Set, Optional, Union, Tuple

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from aiogram.fsm.context import FSMContext
from aiogram.utils.media_group import MediaGroupBuilder # Import if needed, but send_post_content handles it
from aiogram.exceptions import TelegramAPIError, AiogramError, MessageToDeleteNotFound, MessageCantBeDeleted
from aiogram.utils.markdown import escape_md, markdown_bold # Import MarkdownV2 helpers
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler # Import for dependency injection

# Import FSM States using absolute paths
from handlers.post_creation_fsm_states import PostCreationStates
from handlers.post_management_fsm_states import PostManagementStates # Needed for back navigation from editing

# Import Keyboards using absolute paths
from keyboards.reply_keyboards import (
    get_add_media_skip_cancel_keyboard,
    get_confirm_content_keyboard,
    get_channel_selection_controls_keyboard,
    get_cancel_keyboard,
    get_main_menu_keyboard # Import main menu keyboard for cancel
)
from keyboards.inline_keyboards import (
    PostCallbackData,
    SelectionCallbackData,
    NavigationCallbackData,
    GeneralCallbackData,
    get_confirm_draft_keyboard,
    get_edit_section_keyboard,
    get_schedule_type_keyboard,
    get_recurring_type_keyboard,
    get_days_of_week_keyboard,
    get_delete_options_keyboard,
    get_dynamic_channel_selection_keyboard,
    get_simple_back_keyboard
)

# Import Services and Utils using absolute paths
from services.db import AsyncSessionLocal, add_post # Assuming AsyncSessionLocal is exposed
from services.scheduler import schedule_post_publication, reschedule_post_publication # Assuming scheduler functions are available
from services.content_manager import validate_post_text, prepare_input_media_list, ensure_media_temp_dir_exists, MAX_POST_TEXT_LENGTH, MAX_MEDIA_GROUP_CAPTION_LENGTH # Import constants
from services.telegram_api import send_post_content, get_bot_channels_for_user, delete_telegram_messages
from utils.validators import validate_datetime, parse_time
from utils.datetime_utils import get_user_timezone # Assuming this util exists for timezone handling

# Setup logging
logger = logging.getLogger(__name__)

# Constants
TEMP_MEDIA_DIR = 'temp_media' # Directory to save temporary media files during creation
MAX_MEDIA_PER_POST = 10 # Telegram limit for media groups is 10
# POST_PREVIEW_CAPTION_LIMIT = 1024 # Caption limit, already imported

# Ensure temp media directory exists on startup (or application init)
# It's better to call this once during application startup, e.g., in bot.py
# ensure_media_temp_dir_exists(TEMP_MEDIA_DIR)


# Router instance
router = Router()


# --- Helper Functions ---

async def _delete_temp_media_files(media_paths: Optional[List[str]]) -> None:
    """Deletes temporary media files."""
    if not media_paths:
         return
    for path in media_paths:
        # Basic check to prevent deleting non-temp files accidentally
        if path and isinstance(path, str) and path.startswith(TEMP_MEDIA_DIR + os.sep) and os.path.exists(path):
            try:
                os.remove(path)
                logger.debug(f"Deleted temporary file: {path}")
            except OSError as e:
                logger.error(f"Error deleting temporary file {path}: {e}")
        elif path and isinstance(path, str) and not path.startswith(TEMP_MEDIA_DIR + os.sep):
             logger.warning(f"Skipping deletion of non-temp file path: {path}")
        else:
             logger.warning(f"Skipping deletion of invalid path type: {type(path).__name__} - {path}")


async def _delete_messages_from_state(bot: Bot, chat_id: int, state: FSMContext, keys_to_delete: List[str]) -> None:
    """Helper to delete messages whose IDs are stored in state keys."""
    state_data = await state.get_data()
    message_ids_to_delete = []
    # Collect IDs and remove keys from state data copy
    temp_state_data = state_data.copy()
    for key in keys_to_delete:
        msg_id = temp_state_data.pop(key, None)
        if msg_id is not None:
            # Ensure message_id is an integer before adding to list
            if isinstance(msg_id, int):
                message_ids_to_delete.append(msg_id)
            else:
                logger.warning(f"State key '{key}' contained non-integer value '{msg_id}'. Skipping deletion for this message.")


    if message_ids_to_delete:
        logger.debug(f"Attempting to delete messages: {message_ids_to_delete} for user {chat_id}")
        try:
            # delete_telegram_messages handles lists and errors
            await delete_telegram_messages(bot, chat_id, message_ids_to_delete)
            # Update state only after successful deletion attempt
            # (or if error is handled by delete_telegram_messages)
            await state.set_data(temp_state_data) # Save state data with keys removed
        except Exception as e:
            # Log error but don't fail the main handler
            logger.warning(f"Failed to delete messages {message_ids_to_delete} for user {chat_id}: {e}")
    else:
         logger.debug(f"No messages to delete for user {chat_id} from specified state keys: {keys_to_delete}")


async def _send_post_preview(bot: Bot, chat_id: int, state_data: Dict[str, Any]) -> Message:
    """Sends a preview of the post to the user."""
    text = state_data.get('text')
    media_paths = state_data.get('media_paths', [])
    selected_channel_ids = set(state_data.get('selected_channel_ids', [])) # Ensure it's a set for display
    schedule_type = state_data.get('schedule_type')
    run_date: Optional[datetime.datetime] = state_data.get('run_date')
    schedule_params = state_data.get('schedule_params')
    delete_after_seconds = state_data.get('delete_after_seconds')
    user_timezone = get_user_timezone(chat_id) # Assuming user_id is chat_id for direct chat

    preview_text_parts = [markdown_bold("📝 Предварительный просмотр поста:")]

    if text:
        # Escape text for MarkdownV2 preview display
        # Limit to a reasonable length for preview clarity
        truncated_text = text[:500] + '...' if len(text) > 500 else text
        safe_text = escape_md(truncated_text)
        preview_text_parts.append(f"📄 {markdown_bold('Текст:')}\n{safe_text}")
    else:
        preview_text_parts.append(f"📄 {markdown_bold('Текст:')} {markdown_italic('Отсутствует')}")

    preview_text_parts.append(f"🖼️ {markdown_bold('Медиа:')} {'Да' if media_paths else 'Нет'} ({len(media_paths)} файл(ов))")

    # Fetch channel names for display (requires bot to be admin in channels)
    # Using get_bot_channels_for_user which is a stub, so display might be basic
    channel_names = []
    try:
        all_user_channels = await get_bot_channels_for_user(bot, chat_id)
        available_channels_map = {str(c['id']): c['name'] for c in all_user_channels}
    except Exception as e:
        logger.warning(f"Failed to fetch channels for preview display for user {chat_id}: {e}. Displaying channel IDs.")
        available_channels_map = {} # Use empty map on failure

    if selected_channel_ids:
        channel_names = [
            available_channels_map.get(cid, f"Неизвестный канал \${cid}\$") # Escape ID in case it's not numeric
            for cid in selected_channel_ids
        ]
        preview_text_parts.append(f"📣 {markdown_bold('Каналы для публикации:')}\n" + "\\n".join([f"\\- {name}" for name in channel_names]))
    else:
        preview_text_parts.append(f"📣 {markdown_bold('Каналы для публикации:')} {markdown_italic('Не выбраны!')}") # Should not happen if validation works

    schedule_summary = f"⏰ {markdown_bold('Расписание:')} "
    if schedule_type == 'one_time' and run_date:
        # Format run_date using user timezone
        formatted_run_date = format_datetime(run_date, user_timezone) or markdown_italic('некорректное время')
        schedule_summary += f"Разово на {formatted_run_date}"
    elif schedule_type == 'recurring' and schedule_params:
        # Format recurring schedule details
        cron_type = schedule_params.get('type', 'Неизвестно')
        time_str = schedule_params.get('time', markdown_italic('Не указано'))
        if cron_type == 'daily':
            schedule_summary += f"Ежедневно в {escape_md(time_str)}"
        elif cron_type == 'weekly':
            days = schedule_params.get('days_of_week', [])
            day_names = {
                'mon': 'Пн', 'tue': 'Вт', 'wed': 'Ср', 'thu': 'Чт',
                'fri': 'Пт', 'sat': 'Сб', 'sun': 'Вс'
            }
            # Escape day names in case they contain markdown characters (unlikely for these names)
            formatted_days = ", ".join([escape_md(day_names.get(d, d)) for d in days])
            schedule_summary += f"Еженедельно по {formatted_days} в {escape_md(time_str)}"
        elif cron_type == 'monthly':
            day = schedule_params.get('day_of_month', markdown_italic('Не указан'))
            schedule_summary += f"Ежемесячно {day}\\-го числа в {escape_md(time_str)}"
        elif cron_type == 'yearly':
             month_day = schedule_params.get('month_day', markdown_italic('Не указано'))
             schedule_summary += f"Ежегодно {escape_md(month_day)} в {escape_md(time_str)}"
        else:
            schedule_summary += f"Циклически \${escape_md(str(cron_type))}\$ с параметрами: `{escape_md(str(schedule_params))}`"
    else:
        schedule_summary += markdown_italic("Не настроено!") # Should not happen if validation works

    preview_text_parts.append(schedule_summary)


    deletion_summary = f"🗑️ {markdown_bold('Автоудаление:')} "
    if delete_after_seconds is None:
        deletion_summary += markdown_italic("Не настроено")
    elif delete_after_seconds > 0:
        # Convert seconds back to a readable format (e.g., hours or days)
        if delete_after_seconds % (24 * 3600) == 0:
             days = delete_after_seconds // (24 * 3600)
             deletion_summary += f"Через {days} дн\\."
        elif delete_after_seconds % 3600 == 0:
             hours = delete_after_seconds // 3600
             deletion_summary += f"Через {hours} ч\\."
        else:
            # Fallback to seconds if not whole days/hours
            deletion_summary += f"Через {delete_after_seconds} сек\\."
    else:
         deletion_summary += markdown_italic("Некорректное время") # Should not happen if validation works

    preview_text_parts.append(deletion_summary)


    # Combine parts using double newline for paragraphs in MarkdownV2
    final_preview_text = "\n\n".join(preview_text_parts)


    # Prepare media for sending. send_post_content handles logic for media groups vs single media.
    # Note: prepare_input_media_list returns InputMedia objects, potentially using FSInputFile.
    # File handles for FSInputFile are managed by aiogram after passing them.
    input_media = prepare_input_media_list(media_paths)

    # send_post_content expects the main text for caption/message.
    # We pass the final_preview_text as the main text/caption for the preview message.
    # The actual post text goes into the FSM data for final saving.

    # Limit the preview text length for caption if media is present
    preview_caption = final_preview_text
    # If there's media and the text is too long for a caption, send it separately.
    # send_post_content handles this logic internally.

    # Send the preview content. send_post_content returns a list of sent messages.
    # The first message is usually the one we want to interact with (for editing/deleting preview).
    # We should store the sent message_ids in FSM context to delete the old preview message.
    sent_messages = await send_post_content(
         bot=bot,
         chat_id=chat_id,
         text=preview_caption, # Pass the formatted preview text
         media_items=input_media,
         parse_mode="MarkdownV2" # Use MarkdownV2 for preview text formatting
         # No reply_markup here for the preview message itself, the ReplyKB/InlineKB comes after/separately.
    )

    if not sent_messages:
        logger.error("Failed to send post preview.")
        # It might be better to return None or raise a specific exception here
        # for the caller to handle, rather than raising a generic TelegramAPIError.
        raise RuntimeError("Failed to send post preview.") # Raise custom error type


    # Note on file handles: Using FSInputFile means aiogram should handle closing.
    # Explicit manual closing here after send_post_content might interfere or be redundant.
    # If issues arise with file handles staying open, investigate aiogram's lifecycle or use manual closing with care.

    return sent_messages[0] # Return the first message object (usually the main one)


# --- State Handlers ---

# Initial step is triggered by /newpost or button in handlers/commands.py
# @router.message(Command("newpost") | F.text == "➕ Новый пост") -> handled in commands.py


@router.message(PostCreationStates.waiting_for_text, F.text)
async def process_text_input(message: Message, state: FSMContext) -> None:
    """Handles text input for the post."""
    text = message.text
    if not validate_post_text(text):
        await message.answer(
            f"Текст поста слишком длинный \${len(text)} символов\$\\. "
            f"Максимальная длина\\: {MAX_POST_TEXT_LENGTH}\\. " # Use imported constant
            "Пожалуйста, сократите текст и отправьте снова\\.",
            reply_markup=get_cancel_keyboard(),
            parse_mode="MarkdownV2"
        )
        return

    await state.update_data(text=text)
    logger.info(f"User {message.from_user.id} entered post text. Moving to media option.")

    await state.set_state(PostCreationStates.waiting_for_media_option)
    await message.answer(
        "Текст принят\\. Теперь вы можете добавить медиафайлы \$фото, видео, документы\$ "
        "к вашему посту, пропустить этот шаг или отменить создание поста\\.",
        reply_markup=get_add_media_skip_cancel_keyboard(),
        parse_mode="MarkdownV2"
    )

@router.message(PostCreationStates.waiting_for_text)
async def process_text_input_invalid(message: Message) -> None:
    """Handles non-text input in waiting_for_text state."""
    # Check for command /cancel explicitly if needed, but generic handler should catch it.
    await message.answer(
        "Пожалуйста, отправьте текст вашего поста или нажмите \"❌ Отменить\"\\.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="MarkdownV2"
    )

@router.message(PostCreationStates.waiting_for_media_option, F.text == "Добавить медиа")
async def process_add_media_option(message: Message, state: FSMContext) -> None:
    """Handles 'Добавить медиа' option."""
    await state.set_state(PostCreationStates.waiting_for_media_files)
    # Initialize media_paths list in context if not exists (e.g., if editing content)
    state_data = await state.get_data()
    if 'media_paths' not in state_data or state_data.get('media_paths') is None: # Ensure it's initialized as a list
        await state.update_data(media_paths=[])

    logger.info(f"User {message.from_user.id} chose to add media.")
    await message.answer(
        "Отправьте мне фото, видео или документы\\. Вы можете отправить несколько файлов\\.\\n"
        f"Лимит на медиагруппу\\: {MAX_MEDIA_PER_POST} файлов\\.\\n"
        "Когда закончите добавлять медиа, нажмите \"Пропустить\" \$или \"✅ Далее\"\$ для продолжения\\.", # Clarify button meaning
        reply_markup=get_add_media_skip_cancel_keyboard(), # Re-use the same keyboard, "Пропустить" now means "Done"
        parse_mode="MarkdownV2"
    )

@router.message(PostCreationStates.waiting_for_media_option, F.text == "Пропустить")
async def process_skip_media_option(message: Message, state: FSMContext) -> None:
    """Handles 'Пропустить' option in waiting_for_media_option state."""
    state_data = await state.get_data()
    await state.update_data(media_paths=[]) # Ensure media_paths is empty if skipping
    logger.info(f"User {message.from_user.id} skipped adding media. Moving to confirm content.")

    # Check if any text was added (media_paths is empty now)
    if not state_data.get('text'):
         # If no text and no media, post cannot be empty. Go back to text input.
         logger.warning(f"User {message.from_user.id} skipped media but had no text. Post is empty.")
         await message.answer(
             "Вы не добавили ни текст, ни медиа\\. Пост не может быть пустым\\. "
             "Пожалуйста, начните с текста\\.",
             reply_markup=get_cancel_keyboard(),
             parse_mode="MarkdownV2"
         )
         # Clear previous data (except potentially editing_post_id if in editing flow)
         editing_post_id = state_data.get('editing_post_id')
         await state.clear()
         if editing_post_id:
              # If editing, retain the editing flag for potential re-entry
              await state.update_data(editing_post_id=editing_post_id)
              await state.set_state(PostManagementStates.editing_section_selection) # Go back to editing section selection
              await message.answer(
                  f"{markdown_bold('Редактирование поста ID:')} {editing_post_id}. Выберите, что хотите изменить:",
                   reply_markup=get_edit_section_keyboard(draft_id=str(message.from_user.id)),
                   parse_mode="MarkdownV2"
              )
         else:
            await state.set_state(PostCreationStates.waiting_for_text) # Go back to text input for creation flow

         # No temp media to cleanup if it was skipped, but cleanup messages
         await _delete_messages_from_state(message.bot, message.chat.id, state, ['preview_message_id']) # Delete old preview if any
         return


    await state.set_state(PostCreationStates.confirm_content_before_channels)
    # Send content preview
    try:
        # Delete previous preview message if it exists
        await _delete_messages_from_state(message.bot, message.chat.id, state, ['preview_message_id'])

        # Re-fetch state data as it might have been updated
        state_data = await state.get_data()
        preview_message = await _send_post_preview(message.bot, message.chat.id, state_data)
        await state.update_data(preview_message_id=preview_message.message_id) # Store new message ID

        await message.answer(
            "Предварительный просмотр вашего поста\\:",
            reply_markup=get_confirm_content_keyboard(), # Use ReplyKB for flow control after preview
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logger.exception(f"Failed to send post preview for user {message.from_user.id} after skipping media: {e}")
        await message.answer(
            "Не удалось создать предварительный просмотр поста из\\-за ошибки\\.",
            reply_markup=get_cancel_keyboard(),
            parse_mode="MarkdownV2"
        )
        # Clear state and cleanup temp files if any were added before skipping
        state_data = await state.get_data() # Fetch data again before cleanup
        await state.clear()
        await _cleanup_temp_media(state_data.get('media_paths')) # Cleanup using paths from state data copy


@router.message(PostCreationStates.waiting_for_media_option)
async def process_media_option_invalid(message: Message) -> None:
    """Handles invalid input in waiting_for_media_option state."""
    # Check for command /cancel explicitly if needed.
    await message.answer(
        "Выберите действие с медиа, используя кнопки\\:",
        reply_markup=get_add_media_skip_cancel_keyboard(),
        parse_mode="MarkdownV2"
    )


@router.message(
    PostCreationStates.waiting_for_media_files,
    F.photo | F.video | F.document # Handle photo, video, or document messages
)
async def process_media_files(message: Message, state: FSMContext) -> None:
    """Handles receiving media files."""
    state_data = await state.get_data()
    media_paths: List[str] = state_data.get('media_paths', []) # Ensure it's a list

    if len(media_paths) >= MAX_MEDIA_PER_POST:
        await message.answer(
            f"Вы достигли лимита в {MAX_MEDIA_PER_POST} медиафайлов на пост\\.",
            parse_mode="MarkdownV2"
        )
        # Stay in the current state, user needs to click "Пропустить" to continue
        return

    # Determine file_id and file_ref based on media type
    file_id = None
    file_ref = None
    file_extension = None

    if message.photo:
        # Get the largest photo
        file_id = message.photo[-1].file_id
        file_ref = message.photo[-1].file_unique_id
        file_extension = 'jpg' # Default extension, Telegram handles various photo formats
    elif message.video:
        file_id = message.video.file_id
        file_ref = message.video.file_unique_id
        file_extension = 'mp4' # Default extension, Telegram handles various video formats
    elif message.document:
        file_id = message.document.file_id
        file_ref = message.document.file_unique_id
        # Use original file extension if available, otherwise guess from mime_type or default
        file_extension = message.document.file_name.split('.')[-1] if message.document.file_name else message.document.mime_type.split('/')[-1] if message.document.mime_type else 'bin'
    else:
        # Should not happen due to filter, but good practice
        await message.answer(
            "Пожалуйста, отправьте фото, видео или документ\\.",
            parse_mode="MarkdownV2"
        )
        return

    # Construct a temporary file path using user ID and unique file ID
    # ensure_media_temp_dir_exists should be called on app startup.
    # For robustness, could check here, but relies on startup setup.
    temp_file_name = f"{message.from_user.id}_{file_ref}.{file_extension}"
    temp_file_path = os.path.join(TEMP_MEDIA_DIR, temp_file_name)

    try:
        # Download the file
        file_info = await message.bot.get_file(file_id)
        # Check file size limit before downloading if file_size is available in file_info
        # Telegram API applies limits, but double-check if needed
        if file_info.file_size is not None and file_info.file_size > MAX_FILE_SIZE_BYTES:
             logger.warning(f"Attempted to download file {file_id} exceeding max size ({file_info.file_size} > {MAX_FILE_SIZE_BYTES}). Telegram might prevent download.")
             await message.answer(
                 f"Размер файла \${file_info.file_size} байт\$ превышает максимально допустимый \${MAX_FILE_SIZE_BYTES} байт, примерно {MAX_FILE_SIZE_BYTES / (1024*1024):.0f} MB\$\\. Пожалуйста, отправьте файл меньшего размера\\.",
                 parse_mode="MarkdownV2"
             )
             return # Do not proceed with download

        await message.bot.download_file(file_info.file_path, temp_file_path)

        # Validate downloaded file (size check implicitly done by Telegram before download limit)
        # MIME type check is done during prepare_input_media_list
        # Ensure the file exists and is not zero size after download
        if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
             logger.error(f"Downloaded file {temp_file_path} is empty or missing after download.")
             await message.answer("Произошла ошибка при загрузке файла. Файл пуст или не сохранился. Попробуйте еще раз.")
             # Attempt to clean up the potentially empty file
             if os.path.exists(temp_file_path):
                 try: os.remove(temp_file_path)
                 except: pass
             return

        # You might want to apply a watermark here for images
        # if message.photo:
        #     watermark_path = "path/to/watermark.png" # Configure this
        #     # Create output path, e.g., add _watermarked suffix
        #     output_path = temp_file_path.rsplit('.', 1)[0] + '_watermarked.' + temp_file_path.rsplit('.', 1)[-1]
        #     watermarked_path = apply_watermark_to_image(temp_file_path, watermark_path, output_path)
        #     if watermarked_path:
        #         # Delete original temp file and use watermarked one
        #         try: os.remove(temp_file_path)
        #         except Exception as e: logger.warning(f"Failed to remove original temp file after watermarking {temp_file_path}: {e}")
        #         temp_file_path = watermarked_path
        #     else:
        #         logger.warning(f"Failed to apply watermark to {temp_file_path}. Using original file.")


        media_paths.append(temp_file_path) # Store the path to the temporary file
        await state.update_data(media_paths=media_paths)
        logger.info(f"User {message.from_user.id} added media file: {temp_file_path}. Total: {len(media_paths)}")

        if len(media_paths) < MAX_MEDIA_PER_POST:
            await message.answer(
                f"Файл добавлен \${len(media_paths)}/{MAX_MEDIA_PER_POST}\$\\. "
                "Отправьте еще или нажмите \"Пропустить\" \$или \"✅ Далее\"\$ для продолжения\\.", # Clarify button meaning
                reply_markup=get_add_media_skip_cancel_keyboard(), # Re-use keyboard
                parse_mode="MarkdownV2"
            )
        else:
            await message.answer(
                f"Достигнут лимит в {MAX_MEDIA_PER_POST} медиафайлов\\. Нажмите \"Пропустить\" \$или \"✅ Далее\"\$ для продолжения\\.", # Clarify button meaning
                reply_markup=get_add_media_skip_cancel_keyboard(), # Re-use keyboard
                parse_mode="MarkdownV2"
            )

    except TelegramAPIError as e:
        logger.error(f"Telegram API error downloading file {file_id}: {e}")
        await message.answer(
            "Произошла ошибка при загрузке файла с серверов Telegram\\. Попробуйте еще раз\\.",
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logger.exception(f"Unexpected error processing media file {file_id}: {e}")
        await message.answer(
            "Произошла непредвиденная ошибка при обработке файла\\. Попробуйте еще раз\\.",
            parse_mode="MarkdownV2"
        )
        # Attempt to clean up the partially downloaded file
        if os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except: pass


@router.message(PostCreationStates.waiting_for_media_files, F.text == "Пропустить")
async def process_done_adding_media(message: Message, state: FSMContext) -> None:
    """Handles 'Пропустить' button in waiting_for_media_files state (meaning 'Done')."""
    state_data = await state.get_data()
    media_paths: List[str] = state_data.get('media_paths', []) # Ensure it's a list

    logger.info(f"User {message.from_user.id} finished adding media. Total files: {len(media_paths)}. Moving to confirm content.")

    # Check if any text or media was added
    if not state_data.get('text') and not media_paths:
         logger.warning(f"User {message.from_user.id} finished media step but post is empty.")
         await message.answer(
             "Вы не добавили ни текст, ни медиа\\. Пост не может быть пустым\\. "
             "Пожалуйста, начните с текста\\.",
             reply_markup=get_cancel_keyboard(), # Provide cancel keyboard
             parse_mode="MarkdownV2"
         )
         # Go back to text input state
         # Clear previous data (except potentially editing_post_id)
         editing_post_id = state_data.get('editing_post_id')
         await state.clear()
         # Cleanup any temp media that was added before this check
         await _cleanup_temp_media(media_paths) # Use media_paths from state_data *before* state.clear()

         if editing_post_id:
              # If editing, retain the editing flag for potential re-entry
              await state.update_data(editing_post_id=editing_post_id)
              await state.set_state(PostManagementStates.editing_section_selection) # Go back to editing section selection
              await message.answer(
                  f"{markdown_bold('Редактирование поста ID:')} {editing_post_id}. Выберите, что хотите изменить:",
                   reply_markup=get_edit_section_keyboard(draft_id=str(message.from_user.id)),
                   parse_mode="MarkdownV2"
              )
         else:
            await state.set_state(PostCreationStates.waiting_for_text) # Go back to text input for creation flow

         await _delete_messages_from_state(message.bot, message.chat.id, state_data, ['preview_message_id']) # Delete old preview if any
         return


    await state.set_state(PostCreationStates.confirm_content_before_channels)
    # Send content preview
    try:
        # Delete previous preview message if it exists
        await _delete_messages_from_state(message.bot, message.chat.id, state_data, ['preview_message_id'])

        # Re-fetch state data as it might have been updated
        state_data = await state.get_data()
        preview_message = await _send_post_preview(message.bot, message.chat.id, state_data)
        await state.update_data(preview_message_id=preview_message.message_id) # Store new message ID

        await message.answer(
            "Предварительный просмотр вашего поста\\:",
            reply_markup=get_confirm_content_keyboard(), # Use ReplyKB for flow control after preview
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logger.exception(f"Failed to send post preview for user {message.from_user.id} after adding media: {e}")
        await message.answer(
            "Не удалось создать предварительный просмотр поста из\\-за ошибки\\.",
            reply_markup=get_cancel_keyboard(),
            parse_mode="MarkdownV2"
        )
        # Clear state and cleanup temp files on critical error
        state_data = await state.get_data() # Fetch data again before cleanup
        await state.clear()
        await _cleanup_temp_media(state_data.get('media_paths'))
        await _delete_messages_from_state(message.bot, message.chat.id, state_data, ['preview_message_id'])


@router.message(PostCreationStates.waiting_for_media_files)
async def process_media_files_invalid(message: Message) -> None:
    """Handles non-media/non-button input in waiting_for_media_files state."""
    # Check for command /cancel explicitly if needed.
    await message.answer(
        "Пожалуйста, отправьте фото, видео, документ или нажмите \"Пропустить\" \$или \"✅ Далее\"\$ для продолжения\\.", # Clarify button meaning
        reply_markup=get_add_media_skip_cancel_keyboard(),
        parse_mode="MarkdownV2"
    )


# --- Content Confirmation State ---

# This state is reached after adding text and optionally media.
# User interacts via ReplyKeyboard.

@router.message(PostCreationStates.confirm_content_before_channels, F.text == "✅ Далее")
async def process_confirm_content_next(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles '✅ Далее' button from content confirmation."""
    logger.info(f"User {message.from_user.id} confirmed content. Moving to channel selection.")
    # Transition state will be handled after channel fetch

    # Fetch channels and display the selection keyboard
    user_id = message.from_user.id
    try:
        # Assume get_bot_channels_for_user exists and returns [{'id': int, 'name': str}]
        available_channels_raw = await get_bot_channels_for_user(bot, user_id)
        # Convert IDs to strings for CallbackData
        available_channels = [{'id': str(c['id']), 'name': c['name']} for c in available_channels_raw]

        if not available_channels:
            await message.answer(
                "Не найдено доступных каналов или групп, где бот является администратором\\.\\n"
                "Пожалуйста, добавьте бота в чат как администратора с необходимыми правами\\.",
                reply_markup=get_main_menu_keyboard(), # Cannot proceed without channels
                parse_mode="MarkdownV2"
            )
            # Clear state and cleanup temp files
            state_data = await state.get_data() # Fetch data again before cleanup
            await state.clear()
            await _cleanup_temp_media(state_data.get('media_paths'))
            await _delete_messages_from_state(bot, message.chat.id, state_data, ['preview_message_id'])
            return

        # Initialize selected_channel_ids set in context (or keep existing ones if editing)
        state_data = await state.get_data() # Re-fetch state data
        # If editing, selected_channel_ids might already be populated by _populate_fsm_for_editing
        # Ensure it's a set for selection logic
        current_selected_ids = set(state_data.get('selected_channel_ids', []))

        await state.update_data(available_channels=available_channels, selected_channel_ids=current_selected_ids)

        channel_selection_message = (
            "Выберите каналы или группы, куда вы хотите опубликовать пост\\. "
            "Нажмите на название канала/группы, чтобы выбрать его\\. Выберите несколько, если нужно\\.\\n\\n"
            "Нажмите \"Готово\" когда закончите\\."
        )

        # Send initial message with the dynamic inline keyboard
        channel_select_msg = await message.answer(
            channel_selection_message,
            reply_markup=get_dynamic_channel_selection_keyboard(
                available_channels=available_channels,
                selected_channel_ids=current_selected_ids, # Pass current selection for pre-selection
                context_id=str(user_id) # Use user_id as context for callback
            ),
            parse_mode="MarkdownV2"
        )
        # Store message ID to delete it later
        await state.update_data(temp_channel_select_message_id=channel_select_msg.message_id)


        # We should also send a ReplyKeyboard with "Добавить ещё", "Готово", "Отменить" for flow control
        # The 'Готово' button on this ReplyKB will trigger the transition to the next state.
        reply_controls_msg = await message.answer(
             "Используйте кнопки ниже для завершения выбора или отмены\\.",
             reply_markup=get_channel_selection_controls_keyboard(),
             parse_mode="MarkdownV2"
        )
        await state.update_data(temp_channel_select_controls_message_id=reply_controls_msg.message_id)


        # Delete the previous preview message
        await _delete_messages_from_state(bot, message.chat.id, state_data, ['preview_message_id'])

        # Transition to state waiting for callback queries (inline toggles) or Reply Keyboard actions ('Готово', 'Отменить')
        await state.set_state(PostCreationStates.waiting_for_channel_selection_action)


    except Exception as e:
        logger.exception(f"Failed to fetch channels for user {user_id}: {e}")
        await message.answer(
            "Произошла ошибка при загрузке списка каналов\\.",
            reply_markup=get_cancel_keyboard(), # Provide cancel keyboard on error
            parse_mode="MarkdownV2"
        )
        # Clear state and cleanup temp files
        state_data = await state.get_data() # Fetch data again before cleanup
        await state.clear()
        await _cleanup_temp_media(state_data.get('media_paths'))
        await _delete_messages_from_state(bot, message.chat.id, state_data, ['preview_message_id'])


@router.message(PostCreationStates.confirm_content_before_channels, F.text == "✏️ Редактировать контент")
async def process_edit_content_option(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles '✏️ Редактировать контент' button."""
    logger.info(f"User {message.from_user.id} chose to edit content. Returning to text input.")
    state_data = await state.get_data()

    # Clear previous text and media paths from context (user will re-enter)
    await state.update_data(text=None) # Clear text
    # Keep media_paths in state temporarily for cleanup
    media_paths_to_cleanup = state_data.get('media_paths', [])
    await state.update_data(media_paths=[]) # Clear media paths in state

    # Delete previous preview message
    await _delete_messages_from_state(bot, message.chat.id, state_data, ['preview_message_id'])

    # Cleanup temporary media files associated with the previous content
    await _cleanup_temp_media(media_paths_to_cleanup)

    await state.set_state(PostCreationStates.waiting_for_text)
    await message.answer(
        "Хорошо, давайте отредактируем контент\\. Отправьте новый текст поста\\.\\n"
        "После ввода текста вы сможете отредактировать или добавить медиа\\.",
        reply_markup=get_cancel_keyboard(), # Provide cancel keyboard
        parse_mode="MarkdownV2"
    )


# Note: '❌ Отменить' ReplyKB button is handled by handle_cancel_post_creation in this file

@router.message(PostCreationStates.confirm_content_before_channels)
async def process_confirm_content_invalid(message: Message) -> None:
    """Handles invalid input in confirm_content_before_channels state."""
    await message.answer(
        "Выберите действие с контентом, используя кнопки ниже\\:",
        reply_markup=get_confirm_content_keyboard(),
        parse_mode="MarkdownV2"
    )


# --- Channel Selection State (Action) ---

# This state handles callbacks from the inline keyboard and text from the reply keyboard.

@router.callback_query(SelectionCallbackData.filter(F.action_prefix == "toggle_channel"), PostCreationStates.waiting_for_channel_selection_action)
async def process_toggle_channel_callback(callback: CallbackQuery, callback_data: SelectionCallbackData, state: FSMContext) -> None:
    """Handles toggling channel selection via inline keyboard."""
    state_data = await state.get_data()
    selected_channel_ids: Set[str] = state_data.get('selected_channel_ids', set())
    available_channels: List[Dict[str, str]] = state_data.get('available_channels', [])
    channel_id_to_toggle = callback_data.item_id # This is already a string

    # Ensure the toggled channel is actually in the available list
    if not any(str(c['id']) == channel_id_to_toggle for c in available_channels):
        await callback.answer("Неизвестный канал.", show_alert=True)
        return

    if channel_id_to_toggle in selected_channel_ids:
        selected_channel_ids.discard(channel_id_to_toggle)
        logger.debug(f"User {callback.from_user.id} deselected channel {channel_id_to_toggle}")
    else:
        selected_channel_ids.add(channel_id_to_toggle)
        logger.debug(f"User {callback.from_user.id} selected channel {channel_id_to_toggle}")

    # Update state with the modified set
    await state.update_data(selected_channel_ids=list(selected_channel_ids)) # Store as list in state for consistency/serializability

    # Edit the inline keyboard message to reflect the new selection
    try:
        await callback.message.edit_reply_markup(
            reply_markup=get_dynamic_channel_selection_keyboard(
                available_channels=available_channels,
                selected_channel_ids=selected_channel_ids, # Pass set for keyboard generation
                context_id=str(callback.from_user.id)
            )
        )
        await callback.answer() # Answer the callback query
    except Exception as e:
        logger.error(f"Error editing channel selection keyboard for user {callback.from_user.id}: {e}")
        await callback.answer("Произошла ошибка при обновлении списка.", show_alert=True)


# Handle 'Готово' from ReplyKB
@router.message(PostCreationStates.waiting_for_channel_selection_action, F.text == "Готово")
async def process_done_channel_selection_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles 'Готово' from reply keyboard after channel selection."""
    await process_done_channel_selection(message, state, bot)


# Handle 'Готово' from InlineKB (if it exists on dynamic keyboard) - Example:
# @router.callback_query(GeneralCallbackData.filter(F.action == "done_channel_selection"), PostCreationStates.waiting_for_channel_selection_action)
# async def process_done_channel_selection_inline(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
#    await process_done_channel_selection(callback.message, state, bot, callback_query=callback)


async def process_done_channel_selection(message: Message, state: FSMContext, bot: Bot, callback_query: Optional[CallbackQuery] = None) -> None:
    """Helper function to process 'Done' action after channel selection."""
    state_data = await state.get_data()
    selected_channel_ids: List[str] = state_data.get('selected_channel_ids', []) # Should be list from state.update_data

    if not selected_channel_ids:
        text = "Пожалуйста, выберите хотя бы один канал для публикации."
        if callback_query: await callback_query.answer(text, show_alert=True)
        else: await message.answer(text)
        return

    # Delete temporary messages (inline channels keyboard and ReplyKB controls)
    await _delete_messages_from_state(bot, message.chat.id, state, ['temp_channel_select_message_id', 'temp_channel_select_controls_message_id'])

    # State data already updated with selected_channel_ids as list

    logger.info(f"User {message.from_user.id} confirmed channel selection ({len(selected_channel_ids)} channels). Moving to schedule type.")

    await state.set_state(PostCreationStates.waiting_for_schedule_type)
    schedule_message = "Выберите тип расписания для вашего поста\\:"
    # Back target for schedule type selection should be the state before channel selection (confirm_content_before_channels)
    # Or if editing, back to editing_selection_state
    editing_post_id = state_data.get('editing_post_id')
    back_target_state = PostCreationStates.confirm_content_before_channels.state if editing_post_id is None else PostManagementStates.editing_section_selection.state

    schedule_keyboard = get_schedule_type_keyboard(
        draft_id=str(message.from_user.id), # Use user_id as draft_id context
        back_target_state=back_target_state
    ) # Link back


    if callback_query:
        # If triggered by inline callback, the original message was the inline keyboard.
        # It's already deleted by _delete_messages_from_state.
        # Just send the next message.
        await callback_query.answer() # Answer the callback
        await message.answer(schedule_message, reply_markup=schedule_keyboard, parse_mode="MarkdownV2")
    else: # Handled via Reply Keyboard "Готово"
         # Send the next message
         await message.answer(schedule_message, reply_markup=schedule_keyboard, parse_mode="MarkdownV2")

    # Delete previous preview message - should have been deleted before channel selection
    # but clean up defensively if it wasn't.
    await _delete_messages_from_state(bot, message.chat.id, state_data, ['preview_message_id'])


# Handle 'Отменить' from ReplyKB
@router.message(PostCreationStates.waiting_for_channel_selection_action, F.text == "❌ Отменить")
async def process_cancel_channel_selection_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    """Handles 'Отменить' from reply keyboard during channel selection."""
    await process_cancel_creation(message, state, bot) # Use specific cancel handler


# Handle 'Отменить' from InlineKB (if it exists on dynamic keyboard) - Example:
# @router.callback_query(GeneralCallbackData.filter(F.action == "cancel_channel_selection"), PostCreationStates.waiting_for_channel_selection_action)
# async def process_cancel_channel_selection_inline(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
#    await process_cancel_creation(callback, state, bot) # Use specific cancel handler


@router.message(
