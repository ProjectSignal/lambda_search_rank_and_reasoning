from typing import Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Model configurations
MODEL_CONFIGS: Dict[str, Dict[str, Any]] = {
    "openai4o": {
        "model": "gpt-4o",
        "fallback_model": "gpt-4o-mini",
        "api_key": os.getenv("OPENAI_API_KEY",),
        "max_tokens": 4096,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "openai4o_mini": {
        "model": "gpt-4o-mini",
        "fallback_model": "gpt-4o",
        "api_key": os.getenv("OPENAI_API_KEY"),
        "max_tokens": 4096,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "anthropic_sonnet": {
        "model": "claude-3-5-sonnet-20240620",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("ANTHROPIC_API_KEY"),
        "max_tokens": 4096,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "anthropic_haiku": {
        "model": "claude-3-5-haiku-20241022",
        "fallback_model": "gemini/gemini-2.0-flash",
        "api_key": os.getenv("ANTHROPIC_API_KEY"),
        "max_tokens": 8192,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "gemini": {
        "model": "gemini/gemini-2.0-flash",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("GEMINI_API_KEY"),
        "max_tokens": 10000,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "groq_mixtral": {
        "model": "groq/mixtral-8x7b-32768",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("GROQ_API_KEY"),
        "max_tokens": 4000,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "groq_llama": {
        "model": "groq/llama-3.3-70b-versatile",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("GROQ_API_KEY"),
        "max_tokens": 4000,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "groq_gemma": {
        "model": "groq/gemma2-9b-it",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("GROQ_API_KEY"),
        "max_tokens": 4000,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "groq_deepseek_r": {
        "model": "groq/deepseek-r1-distill-llama-70b",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("GROQ_API_KEY"),
        "max_tokens": 4096,
        # "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "deepseek": {
        "model": "deepseek/deepseek-chat",
        "fallback_model": "together_ai/deepseek-ai/DeepSeek-V3",
        "api_key": os.getenv("DEEPSEEK_API_KEY"),
        "max_tokens": 4096,
        "temperature": 1,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "deepseek-r": {
        "model": "deepseek/deepseek-reasoner",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("DEEPSEEK_API_KEY"),
        "max_tokens": 6000,
        # "temperature": 1,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "together-deepseek": {
        "model": "together_ai/deepseek-ai/DeepSeek-V3",
        "fallback_model": "claude-3-5-haiku-20241022",
        "api_key": os.getenv("TOGETHERAI_API_KEY"),
        "max_tokens": 8000,
        "temperature": 1,
        "allowed_fails": 3,
        "cooldown_time": 60
    },
    "azure-gpt-4.1-mini": {
        "model": "azure/gpt-4.1-mini",
        "fallback_model": "gemini/gemini-2.0-flash",
        "max_tokens": 8000,
        "temperature": 0,
        "allowed_fails": 3,
        "cooldown_time": 60
    }
}
# Callback configurations with empty default
ENABLED_CALLBACKS = os.getenv("ENABLED_CALLBACKS", "").split(",")