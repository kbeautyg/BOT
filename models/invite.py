import secrets
from . import db, project

async def create_invite(project_id: int, role: str = 'editor'):
    """Create an invite code for a project and role. Returns the generated code."""
    # Generate a random 6-character code
    code = secrets.token_urlsafe(4)  # ~6-7 characters
    code = code.replace('-', '').replace('_', '')[:6]  # ensure no special chars, length 6
    data = {'code': code, 'project_id': project_id, 'role': role, 'used': False}
    try:
        await db.supabase.table('invites').insert(data).execute()
    except Exception:
        # If code collision, try again recursively
        return await create_invite(project_id, role)
    return code

async def use_invite(code: str, user_id: int):
    """Use an invite code to add a user to a project. Returns (success, message)."""
    # Lookup invite
    res = await db.supabase.table('invites').select('*').eq('code', code).maybe_single().execute()
    invite = res.data
    if not invite or invite.get('used'):
        return False, 'Invalid or expired invite link.'
    project_id = invite['project_id']
    invite_role = invite.get('role', 'editor')
    # Check if user already in project
    res_member = await db.supabase.table('project_members').select('*').eq('project_id', project_id).eq('user_id', user_id).maybe_single().execute()
    if res_member.data:
        # Mark invite as used anyway
        await db.supabase.table('invites').update({'used': True}).eq('code', code).execute()
        # Get project name for message
        proj = await project.get_project(project_id)
        proj_name = proj['name'] if proj else 'the project'
        return True, f'You are already a member of {proj_name}.'
    # Add user to project
    member_data = {'project_id': project_id, 'user_id': user_id, 'role': invite_role}
    await db.supabase.table('project_members').insert(member_data).execute()
    # Mark invite as used (one-time use)
    await db.supabase.table('invites').update({'used': True}).eq('code', code).execute()
    # Get project name for success message
    proj = await project.get_project(project_id)
    proj_name = proj['name'] if proj else 'the project'
    return True, f'Success! You have joined "{proj_name}" as {invite_role}.'
