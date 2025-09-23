import functools
import asyncio
from typing import Any, Callable, Dict, List, TypeVar, Union
from logging_config import setup_logger

logger = setup_logger(__name__)

T = TypeVar('T', Dict[str, Any], List[Dict[str, Any]])

def retry_on_empty_result(max_retries: int = 3) -> Callable:
    """
    A decorator that retries the function if it returns an empty result.
    Works with both dict and list return types.
    First tries with original model for max_retries, then switches to anthropic_haiku for additional retries.
    
    Args:
        max_retries (int): Maximum number of retry attempts per model
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            # First try with original model
            for attempt in range(max_retries):
                try:
                    result = await func(*args, **kwargs)
                    
                    # Check if result is empty based on its type
                    is_empty = False
                    if isinstance(result, dict):
                        is_empty = not any(result.values())
                    elif isinstance(result, list):
                        is_empty = len(result) == 0
                    else:
                        logger.warning(f"Unexpected result type: {type(result)}")
                        return result
                    
                    if not is_empty:
                        return result
                    
                    if attempt < max_retries - 1:
                        logger.warning(f"Empty result on attempt {attempt + 1}, retrying...")
                        await asyncio.sleep(1)  # Add a small delay between retries
                    else:
                        logger.warning(f"All {max_retries} attempts with original model returned empty results, switching to anthropic_haiku")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Error on attempt {attempt + 1}: {str(e)}, retrying...")
                        await asyncio.sleep(1)
                    else:
                        logger.warning(f"All {max_retries} attempts with original model failed, switching to anthropic_haiku")

            # If we get here, try with anthropic_haiku model
            if hasattr(args[0], 'model_name'):  # Check if instance has model_name attribute
                original_model = args[0].model_name
                args[0].model_name = "anthropic_haiku"
                logger.info(f"Switched model from {original_model} to anthropic_haiku")
                
                for attempt in range(max_retries):
                    try:
                        result = await func(*args, **kwargs)
                        
                        # Check if result is empty
                        is_empty = False
                        if isinstance(result, dict):
                            is_empty = not any(result.values())
                        elif isinstance(result, list):
                            is_empty = len(result) == 0
                        
                        if not is_empty:
                            return result
                        
                        if attempt < max_retries - 1:
                            logger.warning(f"Empty result with anthropic_haiku on attempt {attempt + 1}, retrying...")
                            await asyncio.sleep(1)
                        else:
                            logger.error(f"All {max_retries} attempts with anthropic_haiku returned empty results")
                            # Reset model name before returning
                            args[0].model_name = original_model
                            return result
                            
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Error with anthropic_haiku on attempt {attempt + 1}: {str(e)}, retrying...")
                            await asyncio.sleep(1)
                        else:
                            logger.error(f"All {max_retries} attempts with anthropic_haiku failed with error: {str(e)}")
                            # Reset model name before returning
                            args[0].model_name = original_model
                            raise
                
                # Reset model name if we get here
                args[0].model_name = original_model
            
            return result  # Return the last result if all retries fail
        return wrapper
    return decorator 