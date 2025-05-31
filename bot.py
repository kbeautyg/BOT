# bot.py

import asyncio
import logging
import os
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.fsm.storage.memory import MemoryStorage # Or another storage like Redis

# Импорт собственных модулей и их компонентов
from utils.logger import setup_logging
from services.db import init_db, async_engine, AsyncSessionLocal
from services.scheduler import init_scheduler, restore_scheduled_jobs

# Импорт всех роутеров из обработчиков
# Убедитесь, что эти файлы и роутеры существуют
from handlers.commands import router as commands_router
from handlers.post_creation import router as post_creation_router
from handlers.post_management import post_management_router # Используем имя router из файла post_management
from handlers.rss_integration import rss_integration_router # Используем имя router из файла rss_integration
from handlers.inline_buttons import inline_buttons_router # Используем имя router из файла inline_buttons

# Настройка логирования (будет перенастроено setup_logging позже)
logger = logging.getLogger(__name__)


async def main():
    """
    Основная асинхронная функция для запуска Telegram бота.
    Инициализирует все компоненты, регистрирует обработчики и запускает поллинг.
    """
    # 1. Загрузка переменных окружения из файла .env
    load_dotenv()

    # 2. Настройка логирования на основе уровня из переменных окружения
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    setup_logging(log_level)
    logger.info("Логирование настроено.")

    # 3. Получение переменных окружения, необходимых для работы бота
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        logger.critical("BOT_TOKEN не установлен в переменных окружения! Бот не может быть запущен.")
        exit(1) # Выход из программы, если токен не установлен

    database_url = os.getenv('DATABASE_URL') # Используется сервисами, а не напрямую здесь
    if not database_url:
        logger.warning("DATABASE_URL не установлен в переменных окружения! База данных может не работать.")

    time_zone_str = os.getenv('TIME_ZONE', 'Europe/Berlin')
    logger.info(f"Используемая временная зона: {time_zone_str}")

    rss_default_freq_str = os.getenv('RSS_DEFAULT_FREQ', '30')
    # Преобразование в int может понадобиться, но для инициализации scheduler не нужно
    try:
        rss_default_freq = int(rss_default_freq_str)
    except ValueError:
        logger.warning(f"Некорректное значение RSS_DEFAULT_FREQ: {rss_default_freq_str}. Используется значение по умолчанию 30.")


    # 4. Инициализация базы данных
    # init_db создает таблицы, используя async_engine, который импортируется
    logger.info("Инициализация базы данных...")
    try:
        await init_db()
        logger.info("База данных успешно инициализирована (таблицы созданы, если не существовали).")
    except Exception as e:
        logger.critical(f"Ошибка инициализации базы данных: {e}", exc_info=True)
        # В зависимости от логики приложения, может потребоваться завершение работы, если БД недоступна
        # exit(1)


    # 5. Создание экземпляра Bot и Dispatcher
    # Используем MemoryStorage для FSM, для продакшена рекомендуется RedisStorage
    dp = Dispatcher(storage=MemoryStorage())
    bot = Bot(token=bot_token, parse_mode='HTML') # Используем HTML парсинг по умолчанию

    # 6. Инициализация планировщика задач
    # Передаем экземпляр бота и движок БД планировщику
    logger.info("Инициализация планировщика задач...")
    try:
        # init_scheduler запускает планировщик и возвращает его экземпляр
        scheduler = init_scheduler(async_engine, bot)
        logger.info("Планировщик задач инициализирован и запущен.")
    except Exception as e:
        logger.critical(f"Ошибка инициализации планировщика задач: {e}", exc_info=True)
        # Решение об остановке приложения зависит от критичности планировщика
        # exit(1)

    # 7. Передача зависимостей в workflow_data диспетчера для доступа в хэндлерах
    # Aiogram v3 инжектирует AsyncSession через session_factory в start_polling
    # Остальные зависимости можно передать через workflow_data или middlewares
    dp['scheduler'] = scheduler
    dp['session_factory'] = AsyncSessionLocal
    dp['bot_instance'] = bot # Передаем экземпляр бота


    # 8. Регистрация роутеров
    # Порядок регистрации может быть важен, более специфичные роутеры перед общими
    # Например, специфические FSM-хэндлеры перед общими командами /cancel
    logger.info("Регистрация роутеров...")
    dp.include_router(post_creation_router) # Обработчики создания постов (с отменой)
    dp.include_router(post_management_router) # Обработчики управления постами (с отменой)
    dp.include_router(rss_integration_router) # Обработчики RSS (с отменой)
    dp.include_router(inline_buttons_router) # Обработчики inline кнопок (включая подтверждение удаления)
    dp.include_router(commands_router) # Общие команды (/start, /help, общий /cancel если не переопределен)
    logger.info("Роутеры зарегистрированы.")

    # 9. Восстановление запланированных задач из базы данных
    # Передаем scheduler, bot и session_factory для доступа к БД внутри restore_scheduled_jobs
    logger.info("Восстановление запланированных задач...")
    try:
        await restore_scheduled_jobs(scheduler, bot, AsyncSessionLocal)
        logger.info("Восстановление запланированных задач завершено.")
    except Exception as e:
        logger.error(f"Ошибка при восстановлении запланированных задач: {e}", exc_info=True)
        # Приложение может продолжить работу, но некоторые задачи могут не быть восстановлены


    # 10. Пропуск необработанных обновлений (необязательно, но полезно при перезапусках)
    # await bot.delete_webhook(drop_pending_updates=True) # Если используется webhook
    # Для поллинга:
    logger.info("Запуск поллинга...")
    try:
        # Запуск поллинга. session_factory будет использоваться для инъекции AsyncSession в хэндлеры
        await dp.start_polling(bot, session_factory=AsyncSessionLocal)
    finally:
        # 11. Остановка планировщика и закрытие сессии бота при завершении поллинга
        logger.info("Остановка планировщика и бота...")
        scheduler.shutdown()
        await bot.session.close()
        logger.info("Приложение завершило работу.")


if __name__ == '__main__':
    try:
        # Запуск основной асинхронной функции
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        # Логирование корректного завершения при получении сигналов прерывания
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        # Логирование любых других необработанных исключений
        logger.critical(f"Бот завершил работу из-за непредвиденной ошибки: {e}", exc_info=True)

