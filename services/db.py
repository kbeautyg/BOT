# services/db.py

import os
import datetime
import logging
from typing import List, Optional, Dict, Any, TypeVar, Type, Callable

from sqlalchemy import select, update, delete, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

# Import ORM models using absolute paths
from models.user import User
from models.post import Post
from models.rss_feed import RssFeed
from models.rss_item import RssItem

# Настройка логирования
logger = logging.getLogger(__name__)

# Get DATABASE_URL from environment variables, with a default for local development
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/dbname")

# Initialize async engine
# Add pool_recycle for connections that might be closed by the database (e.g., Supabase idle timeout)
async_engine = create_async_engine(DATABASE_URL, echo=False, pool_recycle=1800) # Recycle connections older than 30 minutes

# Initialize async session maker
AsyncSessionLocal = async_sessionmaker(bind=async_engine, expire_on_commit=False, class_=AsyncSession)

# Define the declarative base for ORM models
class Base(DeclarativeBase):
    """
    Base class for SQLAlchemy declarative models.
    All ORM models in the project should inherit from this class.
    """
    pass

# Ensure models inherit from this Base if they defined a local one for standalone purposes
# This part assumes the model files correctly import and use `Base` from `services.db`
# For instance, in models/user.py, the line 'from sqlalchemy.orm import DeclarativeBase'
# should be replaced with 'from services.db import Base'.
# We list the models here so Base.metadata.create_all knows about them.
# Imports above ensure models are known
# all_models = [User, Post, RssFeed, RssItem] # Not needed, Base.metadata knows through inheritance

async def init_db():
    """
    Initializes the database by creating all tables defined by the models.
    """
    logger.info("Attempting database initialization...")
    async with async_engine.begin() as conn:
        # Check if tables already exist might be complex. create_all is idempotent on existing tables.
        #await conn.run_sync(Base.metadata.drop_all) # Optional: drop all tables before creating
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialization complete.")


async def get_db_session() -> AsyncSession:
    """
    Async generator for dependency injection of database sessions.
    Handles session lifecycle (creation, commit, rollback, close).
    Yields:
        AsyncSession: The database session.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            # Session is committed automatically by the async context manager on successful exit
            # await session.commit() # Explicit commit here is usually not needed with async context manager pattern
        except Exception:
            await session.rollback()
            logger.exception("Database session rolled back due to exception.")
            raise
        # Session is closed automatically by the async context manager on exit


# Type variable for generic CRUD functions if needed, but not required by prompt
# T = TypeVar('T', bound=Base)

# --- User Functions ---

async def get_or_create_user(session: AsyncSession, telegram_user_id: int, defaults: Optional[dict] = None) -> User:
    """
    Retrieves a user by telegram_user_id or creates a new one if not found.

    Args:
        session: The SQLAlchemy async session.
        telegram_user_id: The Telegram user ID.
        defaults: Optional dictionary of default values for a new user.

    Returns:
        The existing or newly created User object.
    """
    stmt = select(User).where(User.telegram_user_id == telegram_user_id)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()

    if user is None:
        logger.info(f"User with telegram_user_id {telegram_user_id} not found. Creating new user.")
        if defaults is None:
            defaults = {}
        # Ensure only valid columns from User model are in defaults
        valid_user_defaults = {k:v for k,v in defaults.items() if hasattr(User, k)}
        user = User(telegram_user_id=telegram_user_id, **valid_user_defaults)
        session.add(user)
        await session.commit()
        await session.refresh(user) # Refresh to load default timestamps/ids
        logger.info(f"New user created with ID: {user.id}, Telegram ID: {user.telegram_user_id}")
    # else:
        # logger.debug(f"User found with ID: {user.id}, Telegram ID: {user.telegram_user_id}")
    return user

async def get_user_by_telegram_id(session: AsyncSession, telegram_user_id: int) -> Optional[User]:
    """
    Retrieves a user by their Telegram user ID.

    Args:
        session: The SQLAlchemy async session.
        telegram_user_id: The Telegram user ID.

    Returns:
        The User object if found, otherwise None.
    """
    stmt = select(User).where(User.telegram_user_id == telegram_user_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def update_user_preferred_mode(session: AsyncSession, telegram_user_id: int, mode: str) -> Optional[User]:
    """
    Updates the preferred mode for a user by Telegram user ID.

    Args:
        session: The SQLAlchemy async session.
        telegram_user_id: The Telegram user ID.
        mode: The new preferred mode string.

    Returns:
        The updated User object if found, otherwise None.
    """
    user = await get_user_by_telegram_id(session, telegram_user_id)
    if user:
        user.preferred_mode = mode
        await session.commit()
        await session.refresh(user)
        logger.info(f"Updated preferred mode for user {telegram_user_id} to {mode}.")
        return user
    logger.warning(f"User with telegram_user_id {telegram_user_id} not found for updating preferred mode.")
    return None

async def update_user_timezone(session: AsyncSession, telegram_user_id: int, timezone: str) -> Optional[User]:
    """
    Updates the timezone for a user by Telegram user ID.

    Args:
        session: The SQLAlchemy async session.
        telegram_user_id: The Telegram user ID.
        timezone: The new timezone string.

    Returns:
        The updated User object if found, otherwise None.
    """
    user = await get_user_by_telegram_id(session, telegram_user_id)
    if user:
        user.timezone = timezone
        await session.commit()
        await session.refresh(user)
        logger.info(f"Updated timezone for user {telegram_user_id} to {timezone}.")
        return user
    logger.warning(f"User with telegram_user_id {telegram_user_id} not found for updating timezone.")
    return None

# --- Post Functions ---

async def add_post(
    session: AsyncSession,
    user_id: int,
    chat_ids: List[str],
    schedule_type: str,
    text: Optional[str] = None,
    media_paths: Optional[List[str]] = None,
    schedule_params: Optional[dict] = None,
    run_date: Optional[datetime.datetime] = None,
    delete_after_seconds: Optional[int] = None,
    status: str = "scheduled"
) -> Post:
    """
    Adds a new post entry to the database.

    Args:
        session: The SQLAlchemy async session.
        user_id: The ID of the user creating the post.
        chat_ids: List of chat/channel IDs to post to.
        schedule_type: Type of schedule ('one_time', 'recurring').
        text: Optional text content of the post.
        media_paths: Optional list of media file paths or IDs.
        schedule_params: Optional dictionary of schedule parameters for recurring posts.
        run_date: Optional specific datetime for one-time posts.
        delete_after_seconds: Optional duration after which to delete the post.
        status: Initial status of the post (default: 'scheduled').

    Returns:
        The newly created Post object.
    """
    # Ensure chat_ids is a list of strings
    if not isinstance(chat_ids, list) or not all(isinstance(c, str) for c in chat_ids):
         logger.warning(f"add_post received invalid chat_ids type: {type(chat_ids).__name__}. Attempting conversion.")
         try:
              chat_ids = [str(c) for c in chat_ids] if chat_ids is not None else []
         except Exception:
              logger.error("Failed to convert chat_ids to list of strings.")
              chat_ids = [] # Default to empty list on failure

    # Ensure media_paths is a list of strings
    if not isinstance(media_paths, list) or not all(isinstance(m, str) for m in media_paths):
         logger.warning(f"add_post received invalid media_paths type: {type(media_paths).__name__}. Attempting conversion.")
         try:
              media_paths = [str(m) for m in media_paths] if media_paths is not None else []
         except Exception:
              logger.error("Failed to convert media_paths to list of strings.")
              media_paths = [] # Default to empty list on failure


    new_post = Post(
        user_id=user_id,
        chat_ids=chat_ids,
        schedule_type=schedule_type,
        text=text,
        media_paths=media_paths,
        schedule_params=schedule_params,
        run_date=run_date,
        delete_after_seconds=delete_after_seconds,
        status=status,
        sent_message_data={} # Initialize empty dict for sent data
    )
    session.add(new_post)
    # Await commit outside this function if part of a larger transaction
    # await session.commit()
    # await session.refresh(new_post) # Refresh happens after commit
    return new_post

async def get_post_by_id(session: AsyncSession, post_id: int) -> Optional[Post]:
    """
    Retrieves a post by its ID.

    Args:
        session: The SQLAlchemy async session.
        post_id: The ID of the post.

    Returns:
        The Post object if found, otherwise None.
    """
    stmt = select(Post).where(Post.id == post_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def get_user_posts(session: AsyncSession, user_id: int, statuses: Optional[List[str]] = None) -> List[Post]:
    """
    Retrieves all posts for a given user, optionally filtered by status.

    Args:
        session: The SQLAlchemy async session.
        user_id: The ID of the user.
        statuses: Optional list of statuses to filter by.

    Returns:
        A list of Post objects.
    """
    stmt = select(Post).where(Post.user_id == user_id)
    if statuses is not None:
        stmt = stmt.where(Post.status.in_(statuses))
    stmt = stmt.order_by(Post.created_at.desc()) # Add ordering
    result = await session.execute(stmt)
    return result.scalars().all()

async def get_all_posts_for_scheduling(session: AsyncSession, statuses: List[str] = ["scheduled", "pending_reschedule"]) -> List[Post]:
    """
    Retrieves all posts with specified statuses, typically for scheduling or processing.

    Args:
        session: The SQLAlchemy async session.
        statuses: List of statuses to include (default: "scheduled", "pending_reschedule").

    Returns:
        A list of Post objects.
    """
    stmt = select(Post).where(Post.status.in_(statuses))
    stmt = stmt.order_by(Post.run_date, Post.created_at) # Order by run_date first, then creation date
    result = await session.execute(stmt)
    return result.scalars().all()

async def update_post_details(session: AsyncSession, post_id: int, data_to_update: dict) -> Optional[Post]:
    """
    Updates specified fields for a post by ID.

    Args:
        session: The SQLAlchemy async session.
        post_id: The ID of the post.
        data_to_update: Dictionary of fields and new values to update.

    Returns:
        The updated Post object if found, otherwise None.
    """
    post = await get_post_by_id(session, post_id)
    if post:
        # Filter data_to_update to include only columns that exist on the model
        valid_updates = {k: v for k, v in data_to_update.items() if hasattr(Post, k)}
        for key, value in valid_updates.items():
            setattr(post, key, value)

        # Handle JSON fields that might need specific updates (e.g., appending to list)
        # Currently, we just replace the entire list/dict. If appending is needed, add specific logic.

        await session.commit()
        await session.refresh(post)
        logger.info(f"Updated details for post ID: {post_id}.")
        return post
    logger.warning(f"Post with ID {post_id} not found for updating details.")
    return None

async def update_post_status(session: AsyncSession, post_id: int, new_status: str) -> Optional[Post]:
    """
    Updates the status of a post by ID.

    Args:
        session: The SQLAlchemy async session.
        post_id: The ID of the post.
        new_status: The new status string.

    Returns:
        The updated Post object if found, otherwise None.
    """
    post = await get_post_by_id(session, post_id)
    if post:
        post.status = new_status
        # No commit here, allow calling function to manage transaction
        # await session.commit()
        # await session.refresh(post)
        logger.info(f"Updated status for post ID: {post_id} to {new_status}.")
        return post
    logger.warning(f"Post with ID {post_id} not found for updating status.")
    return None

async def delete_post_by_id(session: AsyncSession, post_id: int) -> bool:
    """
    Deletes a post by its ID.

    Args:
        session: The SQLAlchemy async session.
        post_id: The ID of the post.

    Returns:
        True if a post was deleted, False otherwise.
    """
    stmt = delete(Post).where(Post.id == post_id)
    result = await session.execute(stmt)
    if result.rowcount > 0:
        # No commit here, allow calling function to manage transaction
        # await session.commit()
        logger.info(f"Deleted post with ID: {post_id}.")
        return True
    logger.warning(f"Post with ID {post_id} not found for deletion.")
    return False

# --- RssFeed Functions ---

async def add_rss_feed(
    session: AsyncSession,
    user_id: int,
    feed_url: str,
    channels: List[str],
    frequency_minutes: int,
    filter_keywords: Optional[List[str]] = None,
    is_active: bool = True # Add is_active parameter
) -> RssFeed:
    """
    Adds a new RSS feed subscription to the database.

    Args:
        session: The SQLAlchemy async session.
        user_id: The ID of the user subscribing to the feed.
        feed_url: The URL of the RSS feed.
        channels: List of chat/channel IDs to post items to.
        frequency_minutes: How often to check the feed (in minutes).
        filter_keywords: Optional list of keywords to filter feed items.
        is_active: Initial status of the feed (default: True).

    Returns:
        The newly created RssFeed object.
    Raises:
        IntegrityError: If a feed with the same user_id and feed_url already exists.
    """
    # Ensure channels is a list of strings
    if not isinstance(channels, list) or not all(isinstance(c, str) for c in channels):
         logger.warning(f"add_rss_feed received invalid channels type: {type(channels).__name__}. Attempting conversion.")
         try:
              channels = [str(c) for c in channels] if channels is not None else []
         except Exception:
              logger.error("Failed to convert channels to list of strings.")
              channels = []

    # Ensure filter_keywords is a list of strings or None
    if filter_keywords is not None and (not isinstance(filter_keywords, list) or not all(isinstance(k, str) for k in filter_keywords)):
         logger.warning(f"add_rss_feed received invalid filter_keywords type: {type(filter_keywords).__name__}. Attempting conversion.")
         try:
              filter_keywords = [str(k) for k in filter_keywords] if filter_keywords is not None else None
         except Exception:
              logger.error("Failed to convert filter_keywords to list of strings or None.")
              filter_keywords = None


    new_feed = RssFeed(
        user_id=user_id,
        feed_url=feed_url,
        channels=channels,
        frequency_minutes=frequency_minutes,
        filter_keywords=filter_keywords,
        is_active=is_active # Set is_active
    )
    session.add(new_feed)
    # Await commit outside this function if part of a larger transaction
    # await session.commit()
    # await session.refresh(new_feed) # Refresh happens after commit
    return new_feed

async def get_rss_feed_by_id(session: AsyncSession, feed_id: int) -> Optional[RssFeed]:
    """
    Retrieves an RSS feed by its ID.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed.

    Returns:
        The RssFeed object if found, otherwise None.
    """
    stmt = select(RssFeed).where(RssFeed.id == feed_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def get_user_rss_feeds(session: AsyncSession, user_id: int) -> List[RssFeed]:
    """
    Retrieves all RSS feeds for a given user.

    Args:
        session: The SQLAlchemy async session.
        user_id: The ID of the user.

    Returns:
        A list of RssFeed objects.
    """
    stmt = select(RssFeed).where(RssFeed.user_id == user_id)
    stmt = stmt.order_by(RssFeed.created_at) # Add ordering
    result = await session.execute(stmt)
    return result.scalars().all()

async def get_all_active_rss_feeds(session: AsyncSession) -> List[RssFeed]:
    """
    Retrieves all active RSS feeds from the database.

    Args:
        session: The SQLAlchemy async session.

    Returns:
        A list of RssFeed objects.
    """
    # Filter by the new is_active column
    stmt = select(RssFeed).where(RssFeed.is_active == True)
    # Order by frequency or last_checked_at for better scheduling logic if needed
    # e.g., order by next expected check time
    result = await session.execute(stmt)
    return result.scalars().all()

async def update_rss_feed_details(session: AsyncSession, feed_id: int, data_to_update: dict) -> Optional[RssFeed]:
    """
    Updates specified fields for an RSS feed by ID.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed.
        data_to_update: Dictionary of fields and new values to update.

    Returns:
        The updated RssFeed object if found, otherwise None.
    """
    feed = await get_rss_feed_by_id(session, feed_id)
    if feed:
        # Filter data_to_update to include only columns that exist on the model
        valid_updates = {k: v for k, v in data_to_update.items() if hasattr(RssFeed, k)}
        for key, value in valid_updates.items():
            # Special handling for list fields if needed (e.g., ensuring list of strings)
            if key in ['channels', 'filter_keywords'] and isinstance(value, list):
                 try:
                      setattr(feed, key, [str(item) for item in value])
                 except Exception:
                      logger.warning(f"Failed to convert list for {key} on RSS feed {feed_id} update.")
                      # Keep original value or set to empty? Let's pass through original if conversion fails.
                      setattr(feed, key, value) # Or skip update for this key
            else:
                setattr(feed, key, value)


        # No commit here, allow calling function to manage transaction
        # await session.commit()
        # await session.refresh(feed)
        logger.info(f"Updated details for RSS feed ID: {feed_id}.")
        return feed
    logger.warning(f"RSS feed with ID {feed_id} not found for updating details.")
    return None

async def update_rss_feed_last_checked(session: AsyncSession, feed_id: int, last_checked_at: datetime.datetime) -> Optional[RssFeed]:
    """
    Updates the last checked timestamp for an RSS feed.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed.
        last_checked_at: The datetime the feed was last successfully checked. Must be timezone-aware (preferably UTC).

    Returns:
        The updated RssFeed object if found, otherwise None.
    """
    # Ensure last_checked_at is timezone-aware (UTC is recommended for storing)
    if last_checked_at.tzinfo is None:
         logger.warning("update_rss_feed_last_checked received naive datetime. Assuming UTC.")
         last_checked_at = last_checked_at.replace(tzinfo=datetime.timezone.utc)
    else:
         # Convert to UTC if not already
         last_checked_at = last_checked_at.astimezone(datetime.timezone.utc)


    feed = await get_rss_feed_by_id(session, feed_id)
    if feed:
        feed.last_checked_at = last_checked_at
        # No commit here, allow calling function to manage transaction
        # await session.commit()
        # await session.refresh(feed)
        logger.info(f"Updated last_checked_at for RSS feed ID: {feed_id}.")
        return feed
    logger.warning(f"RSS feed with ID {feed_id} not found for updating last_checked_at.")
    return None

async def delete_rss_feed_by_id(session: AsyncSession, feed_id: int) -> bool:
    """
    Deletes an RSS feed by its ID.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed.

    Returns:
        True if a feed was deleted, False otherwise.
    """
    # Optional: Delete associated RssItems first due to ForeignKey constraints
    # If cascade delete is not configured in the ORM relationship
    # await session.execute(delete(RssItem).where(RssItem.feed_id == feed_id))

    stmt = delete(RssFeed).where(RssFeed.id == feed_id)
    result = await session.execute(stmt)
    if result.rowcount > 0:
        # No commit here, allow calling function to manage transaction
        # await session.commit()
        logger.info(f"Deleted RSS feed with ID: {feed_id}.")
        return True
    logger.warning(f"RSS feed with ID {feed_id} not found for deletion.")
    return False

# --- RssItem Functions ---

async def add_rss_item(
    session: AsyncSession,
    feed_id: int,
    item_guid: str,
    title: Optional[str] = None,
    link: Optional[str] = None,
    description: Optional[str] = None,
    published_at_feed: Optional[datetime.datetime] = None
) -> RssItem:
    """
    Adds a new RSS item entry to the database.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed this item belongs to.
        item_guid: The unique identifier for the item from the feed.
        title: Optional title of the item.
        link: Optional link to the item content.
        description: Optional description or summary.
        published_at_feed: Optional publication datetime from the feed. Must be timezone-aware (preferably UTC).

    Returns:
        The newly created RssItem object.
    Raises:
        IntegrityError: If an item with the same feed_id and item_guid already exists.
    """
    # Ensure published_at_feed is timezone-aware (UTC is recommended for storing)
    if published_at_feed and published_at_feed.tzinfo is None:
         logger.warning("add_rss_item received naive published_at_feed. Assuming UTC.")
         published_at_feed = published_at_feed.replace(tzinfo=datetime.timezone.utc)
    elif published_at_feed:
         # Convert to UTC if not already
         published_at_feed = published_at_feed.astimezone(datetime.timezone.utc)


    new_item = RssItem(
        feed_id=feed_id,
        item_guid=item_guid,
        title=title,
        link=link,
        description=description,
        published_at_feed=published_at_feed,
        is_posted=False # Default to False
    )
    session.add(new_item)
    # Await commit outside this function if part of a larger transaction
    # await session.commit()
    # await session.refresh(new_item) # Refresh happens after commit
    return new_item


async def get_rss_item_by_guid(session: AsyncSession, feed_id: int, item_guid: str) -> Optional[RssItem]:
    """
    Retrieves an RSS item by its feed ID and item GUID.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the related RSS feed.
        item_guid: The unique identifier of the item.

    Returns:
        The RssItem object if found, otherwise None.
    """
    stmt = select(RssItem).where(RssItem.feed_id == feed_id, RssItem.item_guid == item_guid)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def get_posted_item_guids_for_feed(session: AsyncSession, feed_id: int) -> List[str]:
    """
    Retrieves a list of GUIDs for RSS items that have been marked as posted for a specific feed.

    Args:
        session: The SQLAlchemy async session.
        feed_id: The ID of the RSS feed.

    Returns:
        A list of item GUID strings.
    """
    stmt = select(RssItem.item_guid).where(RssItem.feed_id == feed_id, RssItem.is_posted == True)
    result = await session.execute(stmt)
    # Fetch all results and extract the scalar value (the GUID string)
    # Use result.scalars() to directly get scalar results
    return list(result.scalars().all())


async def mark_rss_item_as_posted(session: AsyncSession, item_id: int, is_posted_flag: bool = True) -> Optional[RssItem]:
    """
    Updates the 'is_posted' flag for an RSS item by ID.

    Args:
        session: The SQLAlchemy async session.
        item_id: The ID of the RSS item.
        is_posted_flag: The value to set the 'is_posted' flag to (default: True).

    Returns:
        The updated RssItem object if found, otherwise None.
    """
    # Using session.get is efficient for primary key lookup
    item = await session.get(RssItem, item_id)
    if item:
        item.is_posted = is_posted_flag
        # No commit here, allow calling function to manage transaction
        # await session.commit()
        # await session.refresh(item)
        logger.info(f"Marked RssItem ID:{item_id} as is_posted={is_posted_flag}.")
        return item
    logger.warning(f"RssItem with ID {item_id} not found for updating is_posted flag.")
    return None


