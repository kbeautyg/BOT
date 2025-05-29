from . import db

async def get_user_by_telegram(telegram_id: int):
    """Fetch a user by Telegram ID."""
    res = await db.supabase.table('users').select('*').eq('telegram_id', telegram_id).maybe_single().execute()
    return res.data

async def create_user(telegram_id: int, name: str):
    """Create a new user with given Telegram ID and name."""
    data = {'telegram_id': telegram_id, 'name': name}
    res = await db.supabase.table('users').insert(data).execute()
    # Return the created user record
    return res.data[0] if res.data else None

async def ensure_user(telegram_id: int, name: str):
    """Get user by telegram_id or create if not exists. Returns the user record."""
    user = await get_user_by_telegram(telegram_id)
    if user:
        return user
    # Create new user
    return await create_user(telegram_id, name)
