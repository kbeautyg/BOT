import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Telegram Bot token
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Supabase connection settings
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Ensure required variables are set
if not BOT_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError('Please set BOT_TOKEN, SUPABASE_URL, and SUPABASE_KEY in the environment or .env file')
