import os
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()

# Get from environment variables (works both locally and in GitHub Actions)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Your ticker list
RELEVANT_TICKERS = [
    "A", "AA", "TSLA", "GME", "AMC"
    # Add more tickers as needed
]

# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing required environment variables: SUPABASE_URL and/or SUPABASE_KEY")

FILE_NAME = "shrt20250715.csv"