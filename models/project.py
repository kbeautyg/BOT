from . import db

async def get_project(project_id: int):
    """Fetch a project by ID."""
    res = await db.supabase.table('projects').select('*').eq('id', project_id).maybe_single().execute()
    return res.data

async def get_project_by_channel(channel_id: int):
    """Fetch a project by Telegram channel ID."""
    res = await db.supabase.table('projects').select('*').eq('channel_id', channel_id).maybe_single().execute()
    return res.data

async def create_project(owner_user_id: int, channel_id: int, channel_name: str):
    """Create a new project (channel) and add the owner to project_members."""
    # Insert project
    project_data = {'channel_id': channel_id, 'name': channel_name}
    res = await db.supabase.table('projects').insert(project_data).execute()
    project = res.data[0] if res.data else None
    if project:
        project_id = project['id']
        # Add owner to membership table
        member_data = {'project_id': project_id, 'user_id': owner_user_id, 'role': 'owner'}
        await db.supabase.table('project_members').insert(member_data).execute()
    return project

async def get_projects_by_user(user_id: int):
    """Get all projects (channels) a user has access to, with their role in each."""
    # Fetch membership entries for user
    res_members = await db.supabase.table('project_members').select('project_id, role').eq('user_id', user_id).execute()
    memberships = res_members.data if res_members.data else []
    if not memberships:
        return []
    project_ids = [m['project_id'] for m in memberships]
    # Fetch project details
    res_projects = await db.supabase.table('projects').select('*').in_('id', project_ids).execute()
    projects = res_projects.data if res_projects.data else []
    # Create a mapping of project_id to role for the user
    role_map = {m['project_id']: m['role'] for m in memberships}
    # Combine project info with role
    for proj in projects:
        proj['role'] = role_map.get(proj['id'], None)
    # Optionally sort by project name
    projects.sort(key=lambda p: p.get('name', ''))
    return projects

async def add_user_to_project(user_id: int, project_id: int, role: str = 'editor'):
    """Add a user to a project with given role (used for inviting)."""
    # Check if membership already exists
    res_check = await db.supabase.table('project_members').select('*').eq('project_id', project_id).eq('user_id', user_id).maybe_single().execute()
    if res_check.data:
        # User already a member
        return False
    data = {'project_id': project_id, 'user_id': user_id, 'role': role}
    await db.supabase.table('project_members').insert(data).execute()
    return True

async def get_user_role(user_id: int, project_id: int):
    """Get the role of a user in a project, or None if not a member."""
    res = await db.supabase.table('project_members').select('role').eq('project_id', project_id).eq('user_id', user_id).maybe_single().execute()
    if not res.data:
        return None
    return res.data.get('role')
