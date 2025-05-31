# services/rss_service.py

import logging
import feedparser
import datetime
import re
import asyncio
import json # Import json for parsing complex data if needed
from typing import List, Optional, Dict, Any, Callable, Set, Union

from aiogram import Bot
from aiogram.types import InputMediaPhoto
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError, IntegrityError # Import IntegrityError for handling unique constraints

# Assuming these imports are correctly structured based on REFERENCE
from services.db import (
    get_rss_feed_by_id,
    get_all_active_rss_feeds, # Used if implementing a master task, currently not scheduled
    get_posted_item_guids_for_feed,
    add_rss_item,
    update_rss_feed_last_checked,
    mark_rss_item_as_posted,
    get_rss_item_by_guid # Added for safety check
)
# Import Telegram API services
from services.telegram_api import send_post_content
# Import ORM models for type hinting
from models.rss_feed import RssFeed
from models.rss_item import RssItem
# Import utils for datetime formatting
from utils.datetime_utils import format_datetime # Import for formatting dates
from utils.validators import validate_url # Might be useful for feed_url validation during initial add (handled in handler)

# --- Configuration ---

# Set up logging for this module
logger = logging.getLogger(__name__)
# Logger level should be configured globally via setup_logging in bot.py
# logger.setLevel(logging.INFO)

# Maximum length for the description excerpt in the Telegram message caption/text
# Telegram caption limit is 1024 chars, but main text can be 4096.
# Our send_post_content handles long text by sending separately.
# The excerpt is mainly for the description summary.
# Keep it relatively short, around 300-400 characters, to not overwhelm the user.
RSS_ITEM_DESCRIPTION_EXCERPT_LENGTH = 400
# Add a placeholder for RSS feed parsing timeout
RSS_PARSE_TIMEOUT_SECONDS = 30 # Configure as needed, e.g., from env var


# --- Helper Functions ---

def _does_item_match_filter(entry_title: Optional[str], entry_summary: Optional[str], filter_keywords: Optional[List[str]]) -> bool:
    """
    Checks if an RSS entry's title or summary contains any of the filter keywords (case-insensitive).

    Args:
        entry_title: The title of the RSS entry.
        entry_summary: The summary/description of the RSS entry.
        filter_keywords: A list of keywords to filter by.

    Returns:
        True if filter_keywords is None or empty, or if any keyword is found
        in the title or summary (case-insensitive). False otherwise.
    """
    if not filter_keywords:
        # No keywords to filter by, all items match
        return True

    # Ensure filter_keywords is a list of non-empty strings
    valid_keywords = [kw.strip().lower() for kw in filter_keywords if isinstance(kw, str) and kw.strip()]
    if not valid_keywords:
         return True # If keyword list was provided but empty or contained only empty/non-string items

    search_string = f"{entry_title or ''} {entry_summary or ''}".lower()
    for keyword in valid_keywords:
        if keyword in search_string: # Check case-insensitively using lower()
            return True

    # No keywords matched
    return False

def _find_image_url(entry: feedparser.FeedParserDict) -> Optional[str]:
    """
    Attempts to find an image URL within an RSS entry.

    Checks common fields like 'media_content', 'enclosures', 'image', and 'summary'
    for image links.

    Args:
        entry: The feedparser entry dictionary.

    Returns:
        The URL of an image if found, otherwise None.
        Returns only the URL of the first image found.
    """
    # Check media_content (often used for multimedia, including images)
    if 'media_content' in entry:
        # media_content can be a list
        if isinstance(entry['media_content'], list):
             for media in entry['media_content']:
                 # Check if it's a dict, has a type starting with 'image/', and has a URL
                 if isinstance(media, dict) and media.get('type', '').startswith('image/') and media.get('url'):
                     logger.debug(f"Found image in media_content: {media['url']}")
                     return media['url']
        # Some feeds might have media_content as a single dict
        elif isinstance(entry['media_content'], dict):
             media = entry['media_content']
             if media.get('type', '').startswith('image/') and media.get('url'):
                 logger.debug(f"Found image in media_content (single dict): {media['url']}")
                 return media['url']


    # Check enclosures (often used for attachments, like podcasts or images)
    if 'enclosures' in entry:
        # enclosures is usually a list
        if isinstance(entry['enclosures'], list):
            for enclosure in entry['enclosures']:
                # Check if it's a dict, has a type starting with 'image/', and has a URL
                if isinstance(enclosure, dict) and enclosure.get('type', '').startswith('image/') and enclosure.get('url'):
                    logger.debug(f"Found image in enclosures: {enclosure['url']}")
                    return enclosure['url']
        # Should enclosures ever be a single dict? Feedparser typically makes it a list.


    # Check for a direct 'image' field (less common in standard RSS, more in Atom or extensions)
    # Check if it's a dict and has an 'href'
    if 'image' in entry and isinstance(entry['image'], dict) and entry['image'].get('href'):
         logger.debug(f"Found image in entry['image']: {entry['image']['href']}")
         return entry['image']['href']

    # Check the summary/description for an <img> tag
    # Prioritize 'summary_detail' or 'content' if they are marked as HTML
    # 'content' can be a list of dicts
    content_details = [entry.get('summary_detail')]
    if 'content' in entry and isinstance(entry['content'], list):
        content_details.extend(entry['content'])

    for content_detail in content_details:
        if content_detail and isinstance(content_detail, dict) and content_detail.get('type') == 'text/html':
            html_value = content_detail.get('value', '')
            if isinstance(html_value, str):
                 img_match = re.search(r'<img.*?src=["\'](.*?)["\'].*?>', html_value, re.IGNORECASE)
                 if img_match:
                     logger.debug(f"Found image in HTML content/summary_detail: {img_match.group(1)}")
                     return img_match.group(1)


    # Fallback to searching raw summary/content value if types are not explicitly html
    # Get summary first, then the value from the first item in content list if summary is empty
    summary_raw = entry.get('summary')
    if not summary_raw and 'content' in entry and isinstance(entry['content'], list) and entry['content']:
         first_content_item = entry['content'][0]
         if isinstance(first_content_item, dict):
              summary_raw = first_content_item.get('value')

    if isinstance(summary_raw, str):
         img_match = re.search(r'<img.*?src=["\'](.*?)["\'].*?>', summary_raw, re.IGNORECASE)
         if img_match:
              logger.debug(f"Found image in raw summary/content: {img_match.group(1)}")
              return img_match.group(1)

    logger.debug("No image URL found in entry.")
    return None


def _clean_html(raw_html: str) -> str:
    """
    Removes basic HTML tags and decodes common HTML entities from a string.
    More sophisticated cleaning might be needed for complex HTML.
    """
    if not isinstance(raw_html, str):
        return ""
    # Use regex to remove HTML tags
    clean_text = re.sub(r'<.*?>', '', raw_html)
    # Decode common HTML entities (consider using `html.unescape` for full coverage)
    # import html # Need to import html module if using html.unescape
    # clean_text = html.unescape(clean_text) # More complete decoding

    # Manual decoding for a few common ones
    clean_text = clean_text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    # Normalize whitespace and newlines
    clean_text = re.sub(r'\s+', ' ', clean_text).strip() # Replace multiple whitespace chars with single space
    clean_text = re.sub(r'(\r\n|\r|\n)+', '\n', clean_text) # Normalize newlines and reduce multiple newlines to one

    return clean_text


# --- Core Processing Function for a Single Feed Item ---

async def _process_single_feed_entry_logic(bot: Bot, db_session: AsyncSession, rss_feed: RssFeed, entry: feedparser.FeedParserDict, posted_guids: Set[str]) -> Optional[RssItem]:
    """
    Processes a single RSS feed entry, checks filters, formats content,
    and publishes to associated Telegram channels if it's new and matches filters.
    Adds the item to the database (does NOT commit).

    Args:
        bot: The Aiogram bot instance.
        db_session: The SQLAlchemy async session for this feed's processing.
        rss_feed: The RssFeed SQLAlchemy object.
        entry: The feedparser entry dictionary.
        posted_guids: A set of GUIDs for items already posted from this feed (pre-fetched).

    Returns:
        The RssItem object if successfully processed and added/marked in DB,
        otherwise None. Returns existing item if found in DB as a safeguard.
    """
    # Determine unique identifier
    # Use 'id' first, then 'link'. Ensure it's a string.
    # feedparser's entry.id might be None, check explicitly.
    guid = str(entry.get('id') or entry.get('link') or '').strip() # Ensure guid is string and strip whitespace
    if not guid:
        logger.warning(f"[{rss_feed.feed_url}] Entry missing id and link, skipping: {entry.get('title', 'No Title')}")
        return None

    # Check against pre-fetched set for performance
    if guid in posted_guids:
        logger.debug(f"[{rss_feed.feed_url}] Item with GUID {guid} already in pre-fetched set, skipping.")
        return None
    else:
         # As a fallback/safeguard, double check database for this specific GUID
         # This helps prevent reprocessing if the pre-fetched set was incomplete
         # due to a prior crash or concurrent process (though scheduler should prevent concurrent).
         # This check is less efficient than the set lookup, but safer.
         # Use db_session.get if GUID was the primary key, but it's a field with UniqueConstraint.
         existing_item = await get_rss_item_by_guid(db_session, rss_feed.id, guid)
         if existing_item:
              logger.warning(f"[{rss_feed.feed_url}] Item with GUID {guid} already found in DB despite not being in posted_guids set. Skipping.")
              return existing_item # Return existing item if found, don't re-process


    # Extract relevant fields
    # Ensure title and summary are treated as strings, even if None
    title = entry.get('title')
    link = entry.get('link')
    # Try summary, then content value
    summary_raw = entry.get('summary')
    if not summary_raw and 'content' in entry and isinstance(entry['content'], list) and entry['content']):
        first_content = entry['content'][0]
        if isinstance(first_content, dict):
            summary_raw = first_content.get('value')


    # Extract published date. feedparser gives a struct_time in UTC.
    # Use get() with a default of None to avoid KeyError if fields are missing
    published_parsed = entry.get('published_parsed') or entry.get('updated_parsed') or entry.get('created_parsed')
    published_at_feed = None
    if published_parsed:
        try:
            # Convert struct_time to timezone-aware datetime object (UTC)
            # struct_time is a tuple (year, month, day, hour, minute, second, weekday, yearday, isdst)
            # Use slice [0:6] to get (year, month, day, hour, minute, second)
            published_dt = datetime.datetime(*published_parsed[:6], tzinfo=datetime.timezone.utc)
            published_at_feed = published_dt
        except (ValueError, TypeError, IndexError) as e:
            logger.warning(f"[{rss_feed.feed_url}] Could not parse published date for {guid} ({entry.get('title', 'No Title')}): {published_parsed}. Error: {e}")
            published_at_feed = None # Keep as None if parsing fails
    else:
        # If no date is available, leave it None in the DB.
        # For processing order, we reverse the feedparser entries, which usually respects feed order (newest first).
        # Using current time is only for sorting/display purposes if no feed date exists, not for item identification.
        pass # Leave published_at_feed as None

    logger.info(f"[{rss_feed.feed_url}] Processing new item: {title or link or guid}")

    # Apply keyword filters
    if not _does_item_match_filter(title, summary_raw, rss_feed.filter_keywords):
        logger.debug(f"[{rss_feed.feed_url}] Item does not match filter keywords ({rss_feed.filter_keywords}), skipping: {title or guid}")
        # Optionally, save item as 'filtered_out' with is_posted=False to avoid re-checking but not posting.
        # For now, just skip this item entirely if filtered out.
        return None

    # Formating message content
    # Clean description and create an excerpt
    description_clean = _clean_html(summary_raw) if summary_raw else ""
    description_excerpt = description_clean[:RSS_ITEM_DESCRIPTION_EXCERPT_LENGTH].strip()
    # Add ellipsis if description was truncated
    if len(description_clean) > RSS_ITEM_DESCRIPTION_EXCERPT_LENGTH:
         # Try to break at the last word boundary within the limit
         last_space = description_excerpt.rfind(' ')
         if last_space > 0:
              description_excerpt = description_excerpt[:last_space]
         description_excerpt += "..."

    # Format Telegram message text (MarkdownV2 is preferred)
    # Needs careful escaping of MarkdownV2 special characters: _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
    # Use escape_md helper for user-provided content.

    message_text_parts = []
    if title:
        # Escape title before bolding
        safe_title = escape_md(title)
        message_text_parts.append(f"*{safe_title}*") # MarkdownV2 bold

    # Only add description if it's not empty after cleaning/excerpting
    if description_excerpt:
        # Escape excerpt before adding
        safe_excerpt = escape_md(description_excerpt)
        message_text_parts.append(safe_excerpt)

    # Add link after description, if any
    if link:
        # Link text (e.g., "Читать далее") doesn't need escaping unless it's dynamic user content.
        # The URL in the href itself might need URL encoding, but standard links usually work.
        # escape_md is for text content, not URL attributes.
        # Let's use MarkdownV2 link format: [Text](URL)
        message_text_parts.append(f"[Читать далее]({link})") # MarkdownV2 link

    message_text = "\n\n".join(message_text_parts)

    # If message is empty after formatting (e.g., item had only metadata, no text/link/summary)
    if not message_text.strip():
        logger.warning(f"[{rss_feed.feed_url}] Item has no content (title, summary, or link) to form a message, skipping: {guid}")
        # Optionally, save item as 'empty_content' with is_posted=False.
        return None


    # Find image URL and prepare media
    image_url = _find_image_url(entry)
    media_items: List[InputMedia] = []
    if image_url:
        # Create InputMediaPhoto from URL
        # Note: InputMediaPhoto takes media as file_id or URL.
        # Captions for media items within a group should only be set on the first item.
        # The main message_text will be used as the caption for a single media item,
        # or sent separately if too long for a media group caption.
        # The logic for caption handling is in send_post_content.
        # Here, we just create the InputMedia object with the URL.
        media_items.append(InputMediaPhoto(media=image_url))
        logger.debug(f"Prepared InputMediaPhoto from URL: {image_url}")
        # Note: send_media_group only supports photos and videos as InputMedia.
        # If _find_image_url finds other media types, they won't be handled correctly here.
        # The current _find_image_url focuses on image URLs, which is fine for InputMediaPhoto.


    # Publication to channels
    # Iterate through configured channels and attempt to send the post content.
    # We need to track if publication was successful to AT LEAST ONE channel.
    # Use send_post_content which handles text, single media, or media group.
    post_attempted_successfully = False
    has_configured_channels = bool(rss_feed.channels)

    if not has_configured_channels:
        logger.warning(f"[{rss_feed.feed_url}] No channels configured for this feed. Item will not be posted: {title or guid}")
        # If no channels, no attempt to post was made. Don't mark as posted in DB.
        return None # Skip item if no channels


    # Iterate through configured channels (which are stored as strings like '-1001234567890' or '@channelname')
    # send_post_content sends to a single chat_id. Loop through channels.
    successfully_sent_to_any_channel = False
    for channel_id_str in rss_feed.channels:
        logger.info(f"[{rss_feed.feed_url}] Attempting to post item '{title or guid}' to channel {channel_id_str}")
        try:
            # Call send_post_content for each channel. It returns a list of sent messages.
            # If the list is not empty, it means at least one message part was sent successfully to this channel.
            sent_messages = await send_post_content(
                bot=bot, # Pass the bot instance
                chat_id=channel_id_str, # Use string directly for chat_id
                text=message_text, # Pass the formatted MarkdownV2 text
                media_items=media_items, # Pass the prepared media items (list)
                parse_mode="MarkdownV2" # Use MarkdownV2 parse mode
                # reply_markup=... # Add markup if RSS items should have inline buttons
            )
            if sent_messages: # send_post_content returns [] on failure
                logger.info(f"[{rss_feed.feed_url}] Successfully sent item '{title or guid}' to channel {channel_id_str}. Message IDs: {[m.message_id for m in sent_messages]}")
                successfully_sent_to_any_channel = True # Mark success if sent to ANY channel
            else:
                 logger.error(f"[{rss_feed.feed_url}] Failed to send item '{title or guid}' to channel {channel_id_str}. send_post_content returned empty list.")
                 # Continue to next channel


        except Exception as send_error: # Catch any exception during sending to a specific channel
            logger.exception(f"[{rss_feed.feed_url}] Exception occurred while sending item '{title or guid}' to channel {channel_id_str}: {send_error}")
            # Continue to next channel even if one fails

    # Add item to the database and mark publication status
    # Add the item to the database if we processed it (passed filters, attempted sending).
    # Use is_posted=True ONLY IF successfully sent to AT LEAST ONE channel.
    # If it passed filters and had channels, but failed to send to all, still add item with is_posted=False
    # to avoid reprocessing it in the future.

    # Add the item to DB regardless of send success, but after filtering.
    # This ensures we don't process it again.
    try:
        # Re-check existence just before adding, in case of race conditions (unlikely with scheduler)
        existing_item_check = await get_rss_item_by_guid(db_session, rss_feed.id, guid)
        if existing_item_check:
            logger.warning(f"[{rss_feed.feed_url}] Item with GUID {guid} already exists in DB before add, skipping add. Could indicate concurrency issue.")
             # If it exists, and we successfully sent it now but it wasn't marked posted, update it.
            if successfully_sent_to_any_channel and not existing_item_check.is_posted:
                 await mark_rss_item_as_posted(db_session, existing_item_check.id, is_posted_flag=True)
                 logger.info(f"[{rss_feed.feed_url}] Marked existing item {guid} (ID: {existing_item_check.id}) as posted=True.")
            return existing_item_check # Return the existing item

        # Add new item to DB. add_rss_item defaults is_posted=False.
        new_rss_item = await add_rss_item(
            session=db_session,
            feed_id=rss_feed.id,
            item_guid=guid,
            title=title,
            link=link,
            description=description_clean, # Save full cleaned description
            published_at_feed=published_at_feed # Save parsed datetime (timezone-aware UTC)
        )

        if new_rss_item:
             # Mark as posted=True ONLY IF successfully sent to at least one channel
             if successfully_sent_to_any_channel:
                 updated_item = await mark_rss_item_as_posted(db_session, new_rss_item.id, is_posted_flag=True)
                 if updated_item:
                      logger.info(f"[{rss_feed.feed_url}] Added and marked RSS item {guid} as posted=True (ID: {updated_item.id}).")
                      return updated_item
                 else:
                      logger.error(f"[{rss_feed.feed_url}] Added RSS item {guid} (ID: {new_rss_item.id}) but failed to mark as posted=True!")
                      return new_rss_item # Return item even if update failed
             else:
                 # Item added, but not successfully posted to any channel. It remains is_posted=False.
                 logger.warning(f"[{rss_feed.feed_url}] Added RSS item {guid} (ID: {new_rss_item.id}) with is_posted=False (failed to send).")
                 return new_rss_item # Return the new item (with is_posted=False)

        else:
             # Should not happen if add_rss_item doesn't raise exception but returns None
             logger.error(f"[{rss_feed.feed_url}] add_rss_item returned None unexpectedly for GUID {guid}.")
             return None


    except IntegrityError as e:
        # This should be caught by the existing_item_check above, but handle defensively
        logger.warning(f"[{rss_feed.feed_url}] Integrity error (likely duplicate GUID) when adding item {guid}: {e}. Item likely already exists.")
        # Fetch and return the existing item if possible? Or just return None?
        # Let's fetch and return the existing item for consistency.
        try:
             existing_item_on_error = await get_rss_item_by_guid(db_session, rss_feed.id, guid)
             if existing_item_on_error:
                  # If we successfully sent it now but it wasn't marked posted, update it.
                 if successfully_sent_to_any_channel and not existing_item_on_error.is_posted:
                      await mark_rss_item_as_posted(db_session, existing_item_on_error.id, is_posted_flag=True)
                      logger.info(f"[{rss_feed.feed_url}] Marked item {guid} (ID: {existing_item_on_error.id}) as posted=True after IntegrityError.")
                 return existing_item_on_error
             else:
                  # Item not found even after IntegrityError? Weird state.
                  logger.error(f"[{rss_feed.feed_url}] Item with GUID {guid} not found after IntegrityError.")
                  return None
        except Exception as fetch_e:
             logger.error(f"[{rss_feed.feed_url}] Error fetching item after IntegrityError for GUID {guid}: {fetch_e}.")
             return None

    except SQLAlchemyError as e:
         logger.exception(f"[{rss_feed.feed_url}] Database error while adding/marking item {guid}: {e}")
         # Let the outer session handling deal with rollback/commit for this feed
         return None # Indicate DB failure for this item
    except Exception as e:
         logger.exception(f"[{rss_feed.feed_url}] Unexpected error while adding/marking item {guid}: {e}")
         return None # Indicate unexpected failure


# --- Main Function for Checking a Single Feed ---

# This function will be called by the scheduler task (_task_check_rss_feed)
async def check_and_publish_single_rss_feed(bot: Bot, session_factory: Callable[[], AsyncSession], feed_id: int) -> None:
    """
    Checks a single RSS feed for new items, processes them, and publishes to Telegram.
    Manages its own database session lifecycle for this specific feed check.

    Args:
        bot: The Aiogram bot instance.
        session_factory: A factory function (callable) that returns
                         an async context manager yielding an AsyncSession.
        feed_id: The ID of the RssFeed to check.
    """
    logger.info(f"Starting check for RSS feed ID: {feed_id}")

    feed = None # Initialize feed outside the initial session scope
    feed_url = None # Initialize feed_url outside for logging consistency

    # Use a separate session to get the feed initially
    try:
        async with session_factory() as session:
             # 1. Получение данных о ленте
             feed: Optional['RssFeed'] = await get_rss_feed_by_id(session, feed_id)
             # No commit needed for reading
        # The session will be closed on exiting the async with block

        if not feed:
            logger.error(f"RSS feed with ID {feed_id} not found in DB. Cannot check.")
            return # Feed not found, nothing to do

        feed_url = feed.feed_url
        logger.info(f"Checking RSS feed: {feed_url} (ID: {feed.id})")

        if not feed.is_active: # Check the is_active flag
             logger.warning(f"RSS feed {feed.id} (URL: {feed_url}) is not active. Skipping check.")
             return # Skip if not active

        # 2. Парсинг RSS-ленты
        parsed_feed = None
        try:
            # Parse the feed in a thread pool to avoid blocking the event loop
            # feedparser.parse is synchronous I/O and should not run directly
            # in the async event loop. Use run_in_executor.
            loop = asyncio.get_running_loop()
            # Using a timeout for parsing to prevent hanging on unresponsive feeds
            # RSS_PARSE_TIMEOUT_SECONDS = 30 # Defined as constant
            logger.debug(f"Attempting to parse feed URL: {feed_url} with timeout {RSS_PARSE_TIMEOUT_SECONDS}s.")
            parsed_feed = await asyncio.wait_for(
                loop.run_in_executor(None, feedparser.parse, feed_url),
                timeout=RSS_PARSE_TIMEOUT_SECONDS
            )

            if parsed_feed is None:
                logger.error(f"[{feed_url}] feedparser.parse returned None.")
                # Do not update last_checked_at on parsing failure
                # Optionally update feed status to 'parse_error' if RssFeed model had status
                return # Exit

            if parsed_feed.bozo:
                # bozo is 1 if the feed is not well-formed XML/RSS
                # parsed_feed.bozo_exception contains details
                logger.error(f"[{feed_url}] Error parsing feed (bozo={parsed_feed.bozo}): {parsed_feed.bozo_exception}")
                # Do not update last_checked_at on parsing failure
                # Optionally update feed status to 'parse_error' if RssFeed model had status
                return # Exit

            logger.info(f"[{feed_url}] Successfully parsed feed. Found {len(parsed_feed.entries)} entries.")

        except asyncio.TimeoutError:
             logger.error(f"[{feed_url}] Parsing timed out after {RSS_PARSE_TIMEOUT_SECONDS} seconds.")
             # Optionally update feed status to 'timeout_error'
             return # Exit on timeout
        except Exception as e:
            logger.exception(f"[{feed_url}] An error occurred during feed fetching or parsing: {e}")
            # Optionally update feed status to 'fetch_error' or 'parse_error'
            return # Exit function on parsing failure


        # If parsing was successful (no exception, not None, not bozo), proceed to process entries
        # Use a new session specifically for processing entries and updating last_checked_at
        # This session will be committed or rolled back together for THIS feed's processing run.
        async with session_factory() as session:
             try:
                # 3. Получение опубликованных записей (within the processing session)
                # Fetch all GUIDs that are marked as posted for THIS feed ID.
                posted_guids_list = await get_posted_item_guids_for_feed(session, feed.id)
                posted_guids_set = set(posted_guids_list)
                logger.debug(f"[{feed_url}] Found {len(posted_guids_set)} already posted GUIDs.")

                # 4. Итерация по записям ленты
                # Iterate in reverse to post older items first (if feed is newest first).
                # feedparser entries are usually in the order provided by the feed, often newest first.
                # Let's reverse the entries for chronological posting IF dates are available,
                # otherwise keep the original order.
                entries_to_process = list(parsed_feed.entries) # Convert to list

                # Check if entries have parseable dates and sort them
                def get_entry_date(entry):
                     parsed_date = entry.get('published_parsed') or entry.get('updated_parsed') or entry.get('created_parsed')
                     if parsed_date:
                          try:
                               # Return a sortable value (e.g., timestamp)
                               return datetime.datetime(*parsed_date[:6], tzinfo=datetime.timezone.utc).timestamp()
                          except (ValueError, TypeError, IndexError):
                               return None # Cannot parse date
                     return None # No date field found

                dated_entries = []
                undated_entries = []
                for entry in entries_to_process:
                    date_val = get_entry_date(entry)
                    if date_val is not None:
                         dated_entries.append((date_val, entry))
                    else:
                         undated_entries.append(entry)

                # Sort dated entries by timestamp (ascending for chronological order)
                dated_entries.sort(key=lambda x: x[0])

                # Combine - process dated entries chronologically, then undated in original feed order
                sorted_entries = [entry for date_val, entry in dated_entries] + undated_entries


                logger.info(f"[{feed_url}] Processing {len(sorted_entries)} entries ({len(dated_entries)} with dates, {len(undated_entries)} without).")

                new_items_count = 0
                for entry in sorted_entries:
                    # Process single entry - uses the session from the outer context
                    # _process_single_feed_entry_logic handles filtering, formatting, sending, and adding to DB (without commit)
                    try:
                         # _process_single_feed_entry_logic needs bot instance
                         processed_item = await _process_single_feed_entry_logic(
                             bot=bot, # Pass bot instance
                             db_session=session,
                             rss_feed=feed,
                             entry=entry,
                             posted_guids=posted_guids_set # Pass the set of already posted GUIDs
                         )
                         if processed_item:
                            new_items_count += 1
                         # Note: _process_single_feed_entry_logic adds/marks items in DB but does NOT commit.
                         # Commit happens below after processing all entries for this feed.
                    except Exception as entry_e:
                         # Catch errors processing a single entry, log, and continue to the next entry
                         entry_guid = entry.get('id') or entry.get('link', 'N/A')
                         entry_title = entry.get('title', 'No Title')
                         logger.exception(f"[{feed_url}] Error processing entry '{entry_title}' ({entry_guid}): {entry_e}")
                         # Do not increment new_items_count if processing failed for this entry


                logger.info(f"[{feed_url}] Finished processing entries. Attempted to post {new_items_count} new item(s) from {len(parsed_feed.entries)} total entries.")

                # 11. Обновление времени последней проверки
                # Update this only if the feed was successfully parsed and processed (entry loop completed).
                # Use timezone-aware datetime (UTC recommended for DB storage).
                await update_rss_feed_last_checked(session, feed.id, datetime.datetime.now(datetime.timezone.utc))
                logger.info(f"[{feed_url}] Updated last checked time.")

                # 10. Отметка об публикации (Commit)
                # The session context manager (`async with session_factory():`)
                # handles committing the session upon successful exit of the block.
                # All changes within this block (added items, marked items, updated last_checked_at)
                # will be committed together.
                logger.debug(f"[{feed_url}] Committing changes for feed ID {feed.id}")

             except SQLAlchemyError as e:
                 logger.exception(f"[{feed_url}] Database error during feed processing or committing: {e}")
                 # The `async with session_factory() in this block` context manager will handle rollback
                 # No need to re-raise, the outer handling will log the error and exit this function.
                 # Optionally update feed status to 'db_error' if model has status field
             except Exception as e:
                 logger.exception(f"[{feed_url}] Unexpected error during database operations for feed processing: {e}")
                 # The `async with session_factory() in this block` context manager will handle rollback
                 # No need to re-raise.
                 # Optionally update feed status to 'process_error'

        # End of async with session block, session is committed or rolled back


    except Exception as e:
        # Catch any remaining exceptions from the initial feed fetch or parsing (if not caught above)
        # This block will catch errors from getting the feed or the initial parsing block.
        # If feed_url is available, include it in the log.
        log_id = f"feed ID {feed_id}" + (f" (URL: {feed_url})" if feed_url else "")
        logger.exception(f"An error occurred during check_and_publish for {log_id}: {e}")
        # Optionally update feed status to 'critical_error' if model has status field


    logger.info(f"Check for RSS {feed_id} completed.")


# --- Optional: Function to process all active feeds (as mentioned in prompt) ---
# This function is NOT currently scheduled by APScheduler in scheduler.py based on the provided code structure.
# It's included as it was mentioned in the prompt and could be used for manual triggers or a different scheduling model.

async def process_all_active_rss_feeds(bot: 'Bot', db_session_factory: Callable[[], AsyncSession]):
    """
    (NOT CURRENTLY SCHEDULED) Processes all active RSS feeds that are due for checking.

    Retrieves all active feeds and calls check_and_publish_single_rss_feed for each
    that hasn't been checked recently enough based on its frequency.
    Each feed check gets its own session.

    Args:
        bot: The Aiogram bot instance.
        db_session_factory: A factory function (callable) that returns
                            an async context manager yielding an AsyncSession.
    """
    logger.info("Starting processing of all active RSS feeds (manual/recovery run)...")
    start_time = datetime.datetime.now()

    active_feeds: List[RssFeed] = []
    try:
        # Get list of active feeds using a temporary session just for fetching
        async with db_session_factory() as session:
            active_feeds = await get_all_active_rss_feeds(session) # This filters by is_active
            logger.info(f"Found {len(active_feeds)} active RSS feeds.")
    except SQLAlchemyError as e:
        logger.exception(f"Database error while fetching active feeds: {e}")
        logger.error("Failed to fetch active feeds. RSS processing aborted.")
        return # Abort if cannot even get the list of feeds
    except Exception as e:
        logger.exception(f"Unexpected error while fetching active feeds: {e}")
        logger.error("Failed to fetch active feeds. RSS processing aborted.")
        return

    if not active_feeds:
        logger.info("No active RSS feeds found to process.")
        return

    # Filter feeds that are due for checking
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    due_feeds = []
    for feed in active_feeds:
        # Calculate next check time: last_checked_at + frequency_minutes
        if feed.last_checked_at is None:
            # Never checked, so it's due immediately
            due_feeds.append(feed)
        else:
            # Ensure last_checked_at is timezone-aware for calculation
            last_checked_aware = feed.last_checked_at
            if last_checked_aware.tzinfo is None:
                 # Assume UTC if naive (as per DB storage recommendation)
                 last_checked_aware = last_checked_aware.replace(tzinfo=datetime.timezone.utc)

            time_since_last_check = now_utc - last_checked_aware
            if time_since_last_check >= datetime.timedelta(minutes=feed.frequency_minutes):
                due_feeds.append(feed)
            # else:
                # logger.debug(f"Feed {feed.id} not due yet. Next check after {last_checked_aware + datetime.timedelta(minutes=feed.frequency_minutes)}")


    logger.info(f"Found {len(due_feeds)} RSS feeds due for checking.")

    if not due_feeds:
        logger.info("No RSS feeds are currently due for checking.")
        return

    # Process due feeds sequentially. For concurrency, use asyncio.gather with a Semaphore
    # or limit the number of concurrent tasks. Sequential is simpler for now.
    failed_feeds_ids = []
    for feed in due_feeds:
        try:
            # Call the single feed processing function.
            # It manages its own session internally using the factory.
            await check_and_publish_single_rss_feed(bot, db_session_factory, feed.id)
        except Exception as e:
            # check_and_publish_single_rss_feed logs its own specific errors,
            # but catching here ensures the loop continues for other feeds.
            logger.error(f"Processing of feed ID {feed.id} (URL: {feed.feed_url}) failed with exception: {e}")
            failed_feeds_ids.append(feed.id)
            # Continue to the next feed

    end_time = datetime.datetime.now()
    duration = end_time - start_time

    if failed_feeds_ids:
         logger.warning(f"Finished processing all active RSS feeds in {duration}. Failed feeds IDs: {failed_feeds_ids}")
    else:
         logger.info(f"Finished processing all active RSS feeds successfully in {duration}.")


