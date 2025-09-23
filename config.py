"""
Configuration module for RankAndReasoning Lambda
"""
import os
from pymongo import MongoClient
from typing import Any, Optional

# Load .env file for local development only (not needed in Lambda)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available in Lambda environment, which is fine
    pass


def get_env_var(var_name: str, required: bool = True) -> Optional[str]:
    """Get environment variable with optional requirement check"""
    value = os.getenv(var_name)
    if required and value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value


# MongoDB Configuration
# Accept both MONGODB_URI and MONGO_URI for consistency with other modules
MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
if not MONGODB_URI:
    raise ValueError("Required environment variable MONGODB_URI or MONGO_URI is not set")

DB_NAME = get_env_var("MONGODB_DB_NAME", required=False) or os.getenv("MONGODB_DB_NAME") or "brace"

# Initialize MongoDB client
mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client[DB_NAME]

# Admin API Key for authentication
ADMIN_API_KEY = get_env_var("ADMIN_API_KEY", required=False)