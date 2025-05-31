# handlers/commands.py

import logging
from aiogram import Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession # Импорт для инъекции сессии
from aiogram.utils.markdown import escape_md, markdown_bold # Импорт для форматирования и экранирования

# Импорт FSM состояний из абсолютных путей
from handlers.post_creation_fsm_states import PostCreationStates
from handlers.post_management_fsm_states import PostManagementStates
from handlers.rss_integration_fsm_states import RssIntegrationStates

# Импорт зависимостей из абсолютных путей
from services.db import get_or_create_user
from keyboards.reply_keyboards import get_main_menu_keyboard, get_cancel_keyboard # Импорт get_cancel_keyboard


# --- Logging Setup ---
logger = logging.getLogger(__name__)

# --- Router Initialization ---
router = Router()

# --- Handlers ---

@router.message(CommandStart())
async def handle_start(
    message: Message,
    state: FSMContext,
    session: AsyncSession # Inject database session via middleware/DI
) -> None:
    """
    Handles the /start command. Welcomes the user, gets/creates user in DB,
    resets state, and shows the main menu keyboard.
    """
    user_id = message.from_user.id
    logger.info(f"User {user_id} started the bot.")
    await state.clear() # Clear any previous FSM state

    # Get or create user in the database
    # Pass potential defaults like username, first_name, last_name
    # telegram_user_id must be an integer
    user = await get_or_create_user(session, user_id, defaults={
        'telegram_user_id': user_id, # Redundant as it's the primary key argument, but good to include
        'username': message.from_user.username,
        'first_name': message.from_user.first_name,
        'last_name': message.from_user.last_name
        # Default preferred_mode and timezone are set in the User model
    })
    logger.info(f"User DB entry for Telegram ID {user.telegram_user_id} (DB ID: {user.id}) processed.")

    # Use escape_md for user's first name in case it contains MarkdownV2 special characters
    safe_first_name = escape_md(message.from_user.first_name or 'пользователь')

    welcome_message = (
        f"Привет, *{safe_first_name}*\\!\n\\n"
        "Я твой помощник для публикации постов в Telegram\\-каналы и группы, "
        "а также для интеграции RSS\\-лент\\.\\n"
        "Используй кнопки меню ниже для начала\\."
    )

    await message.answer(
        welcome_message,
        reply_markup=get_main_menu_keyboard(),
        parse_mode="MarkdownV2" # Use MarkdownV2 for welcome message
    )

@router.message(Command("help") | F.text == "❓ Помощь")
async def handle_help(
    message: Message,
    state: FSMContext
) -> None:
    """
    Handles the /help command or the '❓ Помощь' button.
    Provides detailed help information and resets state.
    """
    logger.info(f"User {message.from_user.id} requested help.")
    await state.clear() # Clear any previous FSM state

    help_text = (
        "*Справка по боту\\:*\n\\n"
        "Этот бот поможет вам планировать публикации в ваших каналах и группах, "
        "а также автоматически публиковать новости из RSS\\-лент\\.\n\\n"
        "*Основные команды и их текстовые альтернативы\\:*\n"
        f"\\- `{markdown_bold('/start')}`\\: Начать работу с ботом, показать главное меню\\.\n"
        f"\\- `{markdown_bold('/help')}` или кнопка \"❓ Помощь\": Показать эту справку\\.\n"
        f"\\- `{markdown_bold('/newpost')}` или кнопка \"➕ Новый пост\": Начать создание нового поста с текстом, медиа и планированием\\.\n"
        f"\\- `{markdown_bold('/myposts')}` или кнопка \"🗂 Мои посты\": Посмотреть список ваших запланированных постов и управлять ими \$редактировать, удалить, отменить\$.\\n"
        f"\\- `{markdown_bold('/addrss')}` или кнопка \"📰 Добавить RSS\": Начать процесс добавления новой RSS\\-ленты для автоматической публикации\\.\n\\n"
        "*В процессе создания поста или добавления RSS\\:*\n"
        "\\- Кнопка \"❌ Отменить\" или команда `/cancel`\\: Прерывает текущий процесс и возвращает в главное меню\\.\n\\n"
        "Для использования бота убедитесь, что он добавлен как администратор в каналах/группах, куда вы хотите публиковать, с необходимыми правами \$отправка сообщений, медиа, удаление сообщений и т\\.п\\.\$\\.\n\\n"
        "Выберите действие из главного меню ниже\\."
    )

    await message.answer(
        help_text,
        reply_markup=get_main_menu_keyboard(),
        parse_mode="MarkdownV2" # Use MarkdownV2 for help text
    )

# Note: /newpost, /myposts, /addrss commands primarily set the state and are handled
# in the respective state handlers (e.g., handlers/post_creation.py).
# The handlers below in commands.py are simplified entry points.

@router.message(Command("newpost") | F.text == "➕ Новый пост")
async def handle_new_post(
    message: Message,
    state: FSMContext
) -> None:
    """
    Handles the /newpost command or the '➕ Новый пост' button.
    Starts the post creation FSM.
    """
    logger.info(f"User {message.from_user.id} initiated new post creation.")
    # Clear previous state and set the new one
    await state.clear()
    await state.set_state(PostCreationStates.waiting_for_text)

    await message.answer(
        "Отправьте текст вашего поста\\. Можете использовать форматирование Telegram \$MarkdownV2 или HTML\$\\.\\n"
        "Или нажмите \"❌ Отменить\" для возврата в главное меню\\.",
        reply_markup=get_cancel_keyboard(), # Provide a cancel keyboard
        parse_mode="MarkdownV2"
    )

@router.message(Command("myposts") | F.text == "🗂 Мои посты")
async def handle_my_posts(
    message: Message,
    state: FSMContext
) -> None:
    """
    Handles the /myposts command or the '🗂 Мои посты' button.
    Starts the post management FSM.
    The listing logic is handled by the state entry handler in post_management.py.
    """
    logger.info(f"User {message.from_user.id} initiated post management.")
    # Clear previous state and set the new one
    await state.clear()
    await state.set_state(PostManagementStates.showing_list)

    # The state handler for showing_list in post_management.py will fetch and display posts.
    # Send an initial acknowledgment message.
    await message.answer(
        "Загружаю ваши посты\\.\\.\\.",
        reply_markup=None, # Remove reply keyboard while inline keyboards are shown
        parse_mode="MarkdownV2"
    )


@router.message(Command("addrss") | F.text == "📰 Добавить RSS")
async def handle_add_rss(
    message: Message,
    state: FSMContext
) -> None:
    """
    Handles the /addrss command or the '📰 Добавить RSS' button.
    Starts the RSS integration FSM.
    """
    logger.info(f"User {message.from_user.id} initiated RSS feed addition.")
    # Clear previous state and set the new one
    await state.clear()
    await state.set_state(RssIntegrationStates.waiting_for_url)

    await message.answer(
        "Отправьте URL RSS\\-ленты, которую вы хотите добавить\\.\\n"
        "Или нажмите \"❌ Отменить\" для возврата в главное меню\\.",
        reply_markup=get_cancel_keyboard(), # Provide a cancel keyboard
        parse_mode="MarkdownV2"
    )

# Generic cancel handler for any state.
# Specific cancel handlers in other modules might override this for cleanup.
@router.message(Command("cancel") | F.text == "❌ Отменить")
async def handle_cancel_generic(
    message: Message,
    state: FSMContext
) -> None:
    """
    Handles the /cancel command or the '❌ Отменить' button generically.
    Clears the current FSM state and returns to the main menu.
    Specific handlers might override this if cleanup is needed.
    """
    current_state = await state.get_state()
    if current_state is None:
        await message.answer(
            "Нет активного действия для отмены\\.",
            reply_markup=get_main_menu_keyboard(),
            parse_mode="MarkdownV2"
        )
        return

    logger.info(f"User {message.from_user.id} canceled state {current_state}.")
    await state.clear()

    await message.answer(
        "Действие отменено\\. Возвращаемся в главное меню\\.",
        reply_markup=get_main_menu_keyboard(),
        parse_mode="MarkdownV2"
    )

# --- End of Handlers ---

# This router instance will be included in the main dispatcher
# Example in your bot.py:
# from aiogram import Dispatcher, Bot
# from services.db import AsyncSessionLocal # Ensure this is available
# from .handlers import commands, post_creation, post_management, rss_integration, inline_buttons # Import routers

# async def main():
#     bot = Bot(token="YOUR_BOT_TOKEN")
#     dp = Dispatcher()

#     # Include routers - order matters for overriding handlers (e.g., cancel)
#     # Include more specific routers first
#     dp.include_router(post_creation.router) # Includes specific cancel handler
#     dp.include_router(post_management.post_management_router) # Includes specific cancel handler
#     dp.include_router(rss_integration.rss_integration_router) # Includes specific cancel handler
#     dp.include_router(inline_buttons.inline_buttons_router) # Handles inline button callbacks
#     dp.include_router(commands.router) # Includes generic commands and the generic cancel

#     # Register middleware or pass dependencies to handlers
#     # Example using a simple middleware to pass session and scheduler
#     # from bot import setup_middlewares # Assuming middleware setup function
#     # setup_middlewares(dp, AsyncSessionLocal, scheduler_instance) # Assuming scheduler_instance is initialized

#     await dp.start_polling(bot, session_factory=AsyncSessionLocal) # Pass sessionmaker for DI of AsyncSession

# if __name__ == "__main__":
#     # Ensure DB and Scheduler are initialized before running main
#     # import asyncio
#     # from services.db import init_db, async_engine
#     # from services.scheduler import init_scheduler, restore_scheduled_jobs
#     # async def startup():
#     #     await init_db()
#     #     bot_instance_for_scheduler = Bot(token="YOUR_BOT_TOKEN")
#     #     # Assume scheduler_instance is initialized globally or passed correctly
#     #     global scheduler_instance # Example for global access
#     #     scheduler_instance = init_scheduler(async_engine, bot_instance_for_scheduler)
#     #     # restore_scheduled_jobs needs bot, scheduler, session_factory
#     #     await restore_scheduled_jobs(scheduler_instance, bot_instance_for_scheduler, AsyncSessionLocal)
#     #     await bot_instance_for_scheduler.session.close() # Close session if bot was just for scheduler init
#     #
#     # asyncio.run(startup())
#     # asyncio.run(main())
#     pass # Keep the file as a clean module

