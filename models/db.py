from supabase import AsyncClient, acreate_client

# Global Supabase client (initialized in connect_db)
supabase: AsyncClient = None

async def connect_db(url: str, key: str):
    """Initialize the global Supabase client."""
    global supabase
    supabase = await acreate_client(url, key)

