"""Configuration module for RankAndReasoning Lambda."""

import os
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


# External API configuration (used instead of MongoDB)
DATA_API_BASE_URL = get_env_var("BASE_URL")
DATA_API_KEY = get_env_var("ADMIN_KEY", required=False)
DATA_API_TIMEOUT = float(get_env_var("SEARCH_API_TIMEOUT", required=False) or 10)

