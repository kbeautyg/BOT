import json
from datetime import datetime
from . import db

async def create_post(project_id: int, text: str, media_type: str, media_id: str, buttons: list, schedule_time: datetime, repeat: str = 'none'):
    """Create a new scheduled post."""
    data = {
        'project_id': project_id,
        'text': text,
        'media_type': media_type,
        'media_id': media_id,
        'buttons': json.dumps(buttons) if buttons is not None else None,
        'schedule_time': schedule_time.isoformat(),
        'repeat': repeat,
        'active': True
    }
    res = await db.supabase.table('posts').insert(data).execute()
    return res.data[0] if res.data else None

async def update_post(post_id: int, updates: dict):
    """Update fields of a scheduled post."""
    # Convert datetime or list in updates if necessary
    if 'buttons' in updates and isinstance(updates['buttons'], list):
        updates['buttons'] = json.dumps(updates['buttons'])
    if 'schedule_time' in updates and isinstance(updates['schedule_time'], datetime):
        updates['schedule_time'] = updates['schedule_time'].isoformat()
    await db.supabase.table('posts').update(updates).eq('id', post_id).execute()

async def get_post(post_id: int):
    """Fetch a scheduled post by ID."""
    res = await db.supabase.table('posts').select('*').eq('id', post_id).maybe_single().execute()
    return res.data

async def get_active_posts_by_project(project_id: int):
    """List all active (upcoming) posts for a given project."""
    res = await db.supabase.table('posts').select('*').eq('project_id', project_id).eq('active', True).order('schedule_time', desc=False).execute()
    posts = res.data if res.data else []
    return posts

async def get_all_active_posts():
    """Retrieve all active posts across projects (for scheduling)."""
    res = await db.supabase.table('posts').select('*').eq('active', True).execute()
    return res.data if res.data else []
