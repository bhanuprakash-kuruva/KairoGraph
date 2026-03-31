import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/ragdb')

# Output configuration
OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR', './output'))
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Schema discovery configuration
EXCLUDE_SCHEMAS = ['information_schema', 'pg_catalog', 'pg_toast']
INCLUDE_TABLES = None  # Set to list of table names to filter, None for all