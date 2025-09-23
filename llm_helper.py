import os
from typing import List, Dict, Optional, Any
from litellm import ModelResponse
import litellm
from openai import OpenAIError
from model_config import MODEL_CONFIGS
from callback import CustomCallback
import time
import logging
from logging_config import setup_logger

class LLMManager:   
    def __init__(self):
        # Use our centralized logging configuration
        self.logger = setup_logger(__name__)
        
        self.logger.info("Initializing LLMManager")
        self.callbacks = []
        self.custom_callback = CustomCallback()
        self.callbacks.append(self.custom_callback)
        litellm.callbacks = self.callbacks
        
        try:
            self._set_credentials()
        except Exception as e:
            self.logger.error(f"Failed to set credentials: {str(e)}")
            raise

    def _set_credentials(self):
        # Set standard API keys
        for provider, config in MODEL_CONFIGS.items():
            if config.get("api_key"):
                os.environ[f"{provider.upper()}_API_KEY"] = config["api_key"]
        
        # Set AWS credentials for Bedrock
        for provider, config in MODEL_CONFIGS.items():
            if provider == "anthropic_aws":
                if config.get("aws_access_key_id"):
                    os.environ["AWS_ACCESS_KEY_ID"] = config["aws_access_key_id"]
                if config.get("aws_secret_access_key"):
                    os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws_secret_access_key"]
                if config.get("aws_region_name"):
                    os.environ["AWS_REGION_NAME"] = config["aws_region_name"]
    
    async def get_completion(
        self,
        provider: str,
        messages: List[Dict[str, str]],
        fallback: bool = True,
        response_format: Optional[Dict[str, Any]] = None,
        stop: Optional[List[str]] = None,
        temperature: Optional[float] = None,
    ) -> ModelResponse:
        """Get completion from LLM provider with improved error handling and logging"""
        self.logger.info(f"Getting completion from provider: {provider}")
        
        try:
            config = MODEL_CONFIGS[provider]
        except KeyError:
            self.logger.error(f"Invalid provider: {provider}")
            raise ValueError(f"Provider {provider} not found in MODEL_CONFIGS")

        model = config["model"]
        self.logger.info(f"Using model: {model}")

        # Build model params with logging
        try:
            model_params = self._build_model_params(config, messages, stop, response_format, temperature)
        except Exception as e:
            self.logger.error(f"Error building model parameters: {str(e)}")
            raise

        # Primary model attempt
        try:
            self.logger.info("Sending request to primary model")
            response = await litellm.acompletion(**model_params)
            self.logger.info("Primary model request successful")
            return response
            
        except OpenAIError as e:
            self.logger.error(f"Error with primary model: {str(e)}")
            
            # Attempt fallback if enabled and available
            if fallback and "fallback_model" in config:
                return await self._try_fallback(config, model_params, e)
            raise

    async def _try_fallback(self, config: Dict, model_params: Dict, original_error: Exception) -> ModelResponse:
        """Helper method to handle fallback logic"""
        try:
            fallback_model = config["fallback_model"]
            self.logger.info(f"Attempting fallback to {fallback_model}")
            model_params["model"] = fallback_model
            response = await litellm.acompletion(**model_params)
            self.logger.info("Fallback request successful")
            return response
            
        except OpenAIError as e:
            self.logger.error(f"Fallback also failed: {str(e)}")
            # Re-raise original error to maintain error context
            raise original_error

    def _build_model_params(
        self, 
        config: Dict, 
        messages: List, 
        stop: Optional[List[str]], 
        response_format: Optional[Dict],
        temperature: Optional[float] = None,
    ) -> Dict:
        """Helper method to build model parameters"""
        # Filter out last assistant message for non-Anthropic models
        filtered_messages = messages
        model_name = config["model"].lower()
        if not ("anthropic" in model_name or "claude" in model_name) and messages:
            if messages[-1].get("role") == "assistant":
                filtered_messages = messages[:-1]
                self.logger.debug("Filtered out last assistant message for non-Anthropic model")

        model_params = {
            "model": config["model"],
            "messages": filtered_messages,
            "max_tokens": config.get("max_tokens"),
            "temperature": temperature if temperature is not None else config.get("temperature"),
        }

        if stop:
            model_params["stop"] = stop
            self.logger.debug(f"Using stop sequences: {stop}")

        if response_format:
            model_params["response_format"] = response_format
            self.logger.debug(f"Using response format: {response_format}")

        # Add AWS-specific parameters for Bedrock
        if config.get("aws_access_key_id"):
            model_params.update({
                "aws_access_key_id": config["aws_access_key_id"],
                "aws_secret_access_key": config.get("aws_secret_access_key"),
                "aws_region_name": config.get("aws_region_name")
            })

        return model_params