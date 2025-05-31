import logging
import os

# Определение пути к директории логов
LOGS_DIR = 'logs'
LOG_FILE = os.path.join(LOGS_DIR, 'bot.log')

# Определение формата логирования
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S,%mS'

def setup_logging(level=logging.INFO):
    """
    Настраивает стандартный логгер для бота.
    Логи выводятся в stdout и в файл 'logs/bot.log' с указанным уровнем.

    Args:
        level: Уровень логирования (например, logging.INFO, logging.DEBUG).
    """
    # Создаем директорию для логов, если она не существует
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Получаем корневой логгер
    logger = logging.getLogger()
    # Устанавливаем общий уровень логирования
    logger.setLevel(level)

    # Создаем форматтер
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    # Проверяем, были ли уже добавлены хэндлеры, чтобы избежать дублирования
    # Это важно, чтобы при повторном вызове setup_logging (например, в тестах) не плодить хэндлеры
    if not logger.handlers:
        # Создаем хэндлер для вывода в консоль (stdout)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(level) # Устанавливаем уровень для консоли
        logger.addHandler(stream_handler)

        # Создаем хэндлер для вывода в файл
        # Режим 'a' - append (дописывать в конец файла)
        file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level) # Устанавливаем уровень для файла
        logger.addHandler(file_handler)
    else:
        # Если хэндлеры уже есть, просто обновляем их уровень
        for handler in logger.handlers:
             handler.setLevel(level)
        logger.setLevel(level) # Обновляем уровень и у самого логгера


# Пример использования (можно удалить, если не нужно в самом модуле)
# if __name__ == '__main__':
#     setup_logging(logging.DEBUG) # Настраиваем на уровень DEBUG для примера
#     # Получаем логгер по имени или корневой
#     bot_logger = logging.getLogger(__name__)
#
#     bot_logger.info("Логирование настроено.")
#     bot_logger.info("Это информационное сообщение.")
#     bot_logger.warning("Это предупреждение.")
#     bot_logger.error("Это сообщение об ошибке.")
#     bot_logger.critical("Это критическое сообщение.")
#     bot_logger.debug("Это отладочное сообщение (должно появиться, т.к. уровень DEBUG).")
#
#     # Можно изменить уровень динамически
#     # setup_logging(logging.INFO)
#     # bot_logger.debug("Это отладочное сообщение после смены уровня (не должно появиться).")
#     # bot_logger.info("Это информационное сообщение после смены уровня (должно появиться).")


