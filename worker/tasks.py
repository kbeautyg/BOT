from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from models import post, project

# Initialize scheduler
scheduler = AsyncIOScheduler()

async def schedule_post(post_record: dict):
    """Schedule a post (job) for sending at specified time."""
    # Parse schedule time from post_record
    schedule_time = post_record['schedule_time']
    # Ensure schedule_time is a datetime
    if isinstance(schedule_time, str):
        try:
            schedule_time = datetime.fromisoformat(schedule_time)
        except Exception:
            schedule_time = datetime.now() + timedelta(seconds=5)
    # If time is in the past or very close, adjust to near future
    if schedule_time <= datetime.now():
        schedule_time = datetime.now() + timedelta(seconds=5)
        # Update in DB as well, if needed
        await post.update_post(post_record['id'], {'schedule_time': schedule_time})
        post_record['schedule_time'] = schedule_time.isoformat()
    # Add job to scheduler
    scheduler.add_job(send_scheduled_post, trigger='date', run_date=schedule_time, args=(post_record['id'],), id=f"post_{post_record['id']}")

async def send_scheduled_post(post_id: int):
    """Job: send the scheduled post to Telegram channel and handle repeating."""
    # Get post data fresh from DB
    p = await post.get_post(post_id)
    if not p or not p.get('active'):
        return  # Post was deleted or deactivated
    # Fetch channel info
    proj = await project.get_project(p['project_id'])
    if not proj:
        return
    channel_id = proj['channel_id']
    # Initialize bot (using global token from config)
    from config import BOT_TOKEN
    bot = Bot(token=BOT_TOKEN)
    # Prepare message components
    text = p.get('text') or ''
    media_type = p.get('media_type')
    media_id = p.get('media_id')
    buttons_data = p.get('buttons')
    reply_markup = None
    # Build inline keyboard if buttons present
    if buttons_data:
        import json
        try:
            buttons = json.loads(buttons_data)
        except:
            buttons = []
        if buttons:
            keyboard = []
            for btn in buttons:
                keyboard.append([{'text': btn['text'], 'url': btn['url']}])
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=btn['text'], url=btn['url']) for btn in row] 
                for row in keyboard
            ])
            reply_markup = markup
    # Send the message to the channel
    try:
        if media_type == 'photo':
            await bot.send_photo(chat_id=channel_id, photo=media_id, caption=text, reply_markup=reply_markup)
        elif media_type == 'video':
            await bot.send_video(chat_id=channel_id, video=media_id, caption=text, reply_markup=reply_markup)
        elif media_type == 'document':
            await bot.send_document(chat_id=channel_id, document=media_id, caption=text, reply_markup=reply_markup)
        else:
            await bot.send_message(chat_id=channel_id, text=text, reply_markup=reply_markup)
    except Exception as e:
        # Handle Telegram API errors (e.g., bot removed from channel)
        print(f"Failed to send post {post_id}: {e}")
    # Handle repeating schedule
    if p.get('repeat') == 'none':
        await post.update_post(post_id, {'active': False})
    else:
        # Calculate next occurrence
        repeat = p.get('repeat')
        next_time = datetime.now()
        if repeat == 'hourly':
            next_time = next_time + timedelta(hours=1)
        elif repeat == 'daily':
            next_time = next_time + timedelta(days=1)
        elif repeat == 'weekly':
            next_time = next_time + timedelta(weeks=1)
        # Update the post's schedule_time for next run
        await post.update_post(post_id, {'schedule_time': next_time})
        # Schedule the next occurrence
        scheduler.add_job(send_scheduled_post, trigger='date', run_date=next_time, args=(post_id,), id=f"post_{post_id}")
