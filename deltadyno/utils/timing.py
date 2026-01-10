"""
Timing decorator for the DeltaDyno trading system.

This module provides a decorator for measuring and logging function
execution times, useful for performance monitoring and optimization.
"""

import time
from functools import wraps
from typing import Callable, Any, Optional


def time_it(func: Callable) -> Callable:
    """
    Decorator to measure and log function execution time.

    Logs execution time at WARNING level if a logger is provided
    via the 'logger' keyword argument.

    Args:
        func: Function to wrap with timing measurement

    Returns:
        Wrapped function that logs execution time

    Example:
        @time_it
        def my_function(arg1, arg2, logger=None):
            # Function implementation
            pass

        # Call with logger to enable timing logs
        my_function("a", "b", logger=my_logger)
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Extract logger from kwargs if present
        logger: Optional[Any] = kwargs.get("logger")

        # Measure execution time
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time

        # Log timing if logger is available
        if logger:
            logger.warning(f"{func.__name__} executed in {elapsed_time:.3f} seconds")

        return result

    return wrapper

