import asyncio
import logging
from aiogram import Bot, Dispatcher
from config import BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY
from models import db
from worker import tasks
from bot.handlers import start, channels, posts, invite

logging.basicConfig(level=logging.INFO)

async def main():
    # Initialize database connection
    await db.connect_db(SUPABASE_URL, SUPABASE_KEY)
    # Initialize bot and dispatcher
    bot = Bot(token=BOT_TOKEN, parse_mode='HTML')
    dp = Dispatcher()
    # Register routers
    dp.include_router(start.router)
    dp.include_router(channels.router)
    dp.include_router(posts.router)
    dp.include_router(invite.router)
    # Start APScheduler
    tasks.scheduler.start()
    # Load existing scheduled posts from DB
    await load_existing_jobs()
    # Start polling
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

async def load_existing_jobs():
    """Load all active posts from DB and schedule them at startup."""
    active_posts = await db.supabase.table('posts').select('*').eq('active', True).execute()
    posts = active_posts.data if active_posts.data else []
    from datetime import datetime, timedelta
    for p in posts:
        # Parse schedule time
        try:
            st = datetime.fromisoformat(p['schedule_time']) if isinstance(p['schedule_time'], str) else p['schedule_time']
        except:
            st = None
        if st and st < datetime.now():
            # If missed schedule time
            if p.get('repeat') and p['repeat'] != 'none':
                # Calculate next future time based on repeat interval
                repeat = p['repeat']
                next_time = st
                while next_time <= datetime.now():
                    if repeat == 'hourly':
                        next_time += timedelta(hours=1)
                    elif repeat == 'daily':
                        next_time += timedelta(days=1)
                    elif repeat == 'weekly':
                        next_time += timedelta(weeks=1)
                    else:
                        break
                if next_time > datetime.now():
                    # Update next occurrence in DB and schedule it
                    await db.supabase.table('posts').update({'schedule_time': next_time.isoformat()}).eq('id', p['id']).execute()
                    p['schedule_time'] = next_time.isoformat()
                    await tasks.schedule_post(p)
                else:
                    # Could not compute next, mark inactive
                    await db.supabase.table('posts').update({'active': False}).eq('id', p['id']).execute()
            else:
                # One-time missed post, mark inactive
                await db.supabase.table('posts').update({'active': False}).eq('id', p['id']).execute()
        else:
            # Schedule future post normally
            await tasks.schedule_post(p)
    logging.info(f"Loaded {len(posts)} active posts.")

if __name__ == '__main__':
    asyncio.run(main())
