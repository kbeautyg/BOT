from models import project

async def user_has_role(user_id: int, project_id: int, roles: list):
    """Check if user has one of the required roles in the project."""
    user_role = await project.get_user_role(user_id, project_id)
    if user_role is None:
        return False
    return user_role in roles
