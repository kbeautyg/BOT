# services/content_manager.py

import logging
import os
import mimetypes
from typing import List, Optional, Tuple, BinaryIO

# Import constants and validation function from utils.validators
from utils.validators import validate_media_file, MAX_FILE_SIZE_BYTES, ALLOWED_MIME_TYPES

from aiogram.types import InputMedia, InputMediaPhoto, InputMediaVideo, InputMediaDocument
# Explicitly import FSInputFile if local files are used with InputMedia
from aiogram.types import FSInputFile # Used when media is a local file path

# Настройка логирования
logger = logging.getLogger(__name__)

# Максимальная длина текста поста (Telegram limit for standard message text)
# Although captions are limited to 1024, a standalone text message can be up to 4096
# characters. Since validate_post_text seems to validate the main text content,
# we use the larger limit.
MAX_POST_TEXT_LENGTH = 4096
# Limit for media group caption
MAX_MEDIA_GROUP_CAPTION_LENGTH = 1024


def validate_post_text(text: Optional[str]) -> bool:
    """
    Validates the post text length.

    Args:
        text: The text content of the post.

    Returns:
        True if the text is None or its length is within the allowed limit,
        False otherwise.
    """
    if text is None:
        return True
    # Check if it's a string and not empty, and within length limit
    return isinstance(text, str) and 0 < len(text) <= MAX_POST_TEXT_LENGTH

def _guess_mime_type(file_path: str) -> Optional[str]:
    """Guesses the MIME type of a file."""
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type

def ensure_media_temp_dir_exists(temp_dir: str = 'temp_media') -> None:
    """
    Ensures that a temporary directory for media exists.

    Args:
        temp_dir: The path to the temporary directory.
    """
    if not os.path.exists(temp_dir):
        try:
            os.makedirs(temp_dir)
            logger.info(f"Создана временная директория для медиа: {temp_dir}")
        except OSError as e:
            logger.error(f"Ошибка при создании временной директории {temp_dir}: {e}")
            # In a real application, this might need more robust error handling
            # (e.g., raise an exception or stop processing).

# Заглушка функции для применения водяного знака
def apply_watermark_to_image(image_path: str, watermark_path: str, output_path: str) -> Optional[str]:
    """
    Placeholder function to simulate applying a watermark to an image.
    In a real application, this would use a library like Pillow (PIL).

    Args:
        image_path: Path to the original image file.
        watermark_path: Path to the watermark image file.
        output_path: Path where the watermarked image will be saved.

    Returns:
        The path to the watermarked image file on success, None otherwise.
    """
    logger.warning(f"Функция apply_watermark_to_image является заглушкой. Водяной знак не применен к {image_path}. Файл просто копируется.")
    try:
        if not os.path.exists(image_path):
             logger.error(f"Исходный файл изображения не найден: {image_path}")
             return None
        # In the placeholder, just copy the file
        import shutil
        shutil.copy(image_path, output_path)
        # Check if the output file was actually created
        if os.path.exists(output_path):
             return output_path
        else:
             logger.error(f"Копирование файла {image_path} в {output_path} не удалось.")
             return None
    except FileNotFoundError:
        logger.error(f"Не найден файл для копирования (при заглушке водяного знака) - либо исходный, либо водяной знак: {image_path} or {watermark_path}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при копировании файла в заглушке водяного знака: {e}")
        return None


def prepare_input_media_list(media_files: List[str]) -> List[InputMedia]:
    """
    Prepares a list of aiogram InputMedia objects from local file paths.
    Validates file types and sizes. Does NOT set captions or reply_markup.

    Args:
        media_files: A list of local file paths to media files.

    Returns:
        A list of valid InputMedia objects (InputMediaPhoto, InputMediaVideo,
        InputMediaDocument). Returns an empty list if validation fails for all files
        or input is empty.
        Note: The file objects/paths created here are expected to be handled
        by the aiogram sending functions (send_media_group, etc.), which should
        manage the file lifecycle (reading, closing). Using FSInputFile is recommended.
    """
    input_media_list: List[InputMedia] = []

    if not media_files:
        logger.debug("Список media_files пуст. Нет медиа для подготовки.")
        return []

    for file_path in media_files:
        if not isinstance(file_path, str):
            logger.warning(f"Пропущен элемент списка медиа: не является строкой: {file_path}")
            continue

        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}. Пропускаем.")
            continue

        try:
            file_size = os.path.getsize(file_path)
            mime_type = _guess_mime_type(file_path)

            if not validate_media_file(file_size, mime_type):
                logger.error(f"Файл не прошел валидацию (размер или тип): {file_path}. Размер: {file_size}, MIME: {mime_type}. Пропускаем.")
                continue

            # Use FSInputFile for local files. Aiogram handles reading and closing.
            media_file = FSInputFile(path=file_path)

            # Determine type of InputMedia based on MIME
            media_item: Optional[InputMedia] = None
            if mime_type and mime_type.startswith('image/'):
                 media_item = InputMediaPhoto(media=media_file)
            elif mime_type and mime_type.startswith('video/'):
                 media_item = InputMediaVideo(media=media_file)
            elif mime_type and mime_type in ['application/pdf', 'audio/mpeg', 'audio/wav']:
                 # Add other document/audio types as needed that are allowed by validate_media_file
                 media_item = InputMediaDocument(media=media_file)
            else:
                 # This case should be rare if validate_media_file and ALLOWED_MIME_TYPES are consistent.
                 logger.warning(f"Неподдерживаемый MIME тип для InputMedia после валидации: {mime_type}. Файл: {file_path}. Пропускаем.")
                 continue # Don't add to list

            # !!! Важно: Не устанавливаем caption или reply_markup здесь.
            # Подписи и клавиатуры обрабатываются в services.telegram_api.py
            # при фактической отправке, т.к. они могут зависеть от контекста отправки (например, длинная подпись для медиагруппы).

            input_media_list.append(media_item)
            logger.debug(f"Файл {file_path} успешно подготовлен как {type(media_item).__name__}.")

        except FileNotFoundError:
             # This exception is already handled above by os.path.exists
             pass
        except OSError as e:
            logger.error(f"Ошибка файловой системы при обработке {file_path}: {e}. Пропускаем.")
        except Exception as e:
            logger.exception(f"Неожиданная ошибка при подготовке медиа файла {file_path}: {e}. Пропускаем.")

    if not input_media_list:
        logger.warning("Не удалось подготовить ни одного медиа файла из списка.")

    return input_media_list

# Example Usage (for demonstration/testing, commented out)
# async def example_usage():
#     # Ensure temp dir exists for dummy files
#     ensure_media_temp_dir_exists()
#     dummy_img_path = os.path.join(TEMP_MEDIA_DIR, "dummy_image.png")
#     dummy_video_path = os.path.join(TEMP_MEDIA_DIR, "dummy_video.mp4")
#     invalid_large_path = os.path.join(TEMP_MEDIA_DIR, "large_file.zip")
#
#     # Create dummy files for testing prepare_input_media_list
#     try:
#         # Create a small valid PNG file
#         if not os.path.exists(dummy_img_path):
#             with open(dummy_img_path, 'wb') as f:
#                 # A minimal valid PNG file (1x1 pixel, black)
#                 f.write(b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\xfc\xff\xff?\x03\x00\x08\xfb\x02\xfe\xaf\x50\xa0\x41\x00\x00\x00\x00IEND\xaeB`\x82')
#         # Create a dummy MP4 file (needs more complex data, maybe just a small pre-existing test file or skip)
#         # For simple test, let's just create an empty file and hope guess_mime_type works on extension
#         if not os.path.exists(dummy_video_path):
#              with open(dummy_video_path, 'wb') as f:
#                   f.write(b'\x00') # Minimal content
#         # Create an invalid large file
#         if not os.path.exists(invalid_large_path):
#             with open(invalid_large_path, 'wb') as f:
#                 f.write(os.urandom(MAX_FILE_SIZE_BYTES + 1024)) # Create a file slightly too big
#
#         media_paths = [dummy_img_path, dummy_video_path, "non_existent_file.jpg", invalid_large_path]
#         print("\n--- Подготовка InputMedia списка ---")
#         prepared_media = prepare_input_media_list(media_paths)
#         print(f"Подготовлено {len(prepared_media)} InputMedia объектов:")
#         for item in prepared_media:
#             # Accessing .path for FSInputFile
#             media_info = item.media.path if isinstance(item.media, FSInputFile) else '...'
#             print(f"  - Тип: {type(item).__name__}, Медиа (path): {media_info}")
#
#     finally:
#         # Clean up dummy files
#         for f in [dummy_img_path, dummy_video_path, invalid_large_path]:
#             if os.path.exists(f):
#                 try: os.remove(f)
#                 except Exception as e: print(f"Error removing dummy file {f}: {e}")
#
# if __name__ == "__main__":
#      # import asyncio
#      # asyncio.run(example_usage())
#     pass

