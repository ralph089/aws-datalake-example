"""
Simple logging setup for AWS Glue jobs.
"""

import sys
from typing import Any

from loguru import logger


def setup_logging(job_name: str, log_level: str = "INFO") -> Any:
    """Configure structured logging for Glue jobs."""

    # Remove default handler
    logger.remove()

    # Validate log level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    level = log_level.upper() if log_level.upper() in valid_levels else "INFO"

    # Simple text output for all environments
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[job_name]: <20} | {message}",
        level=level,
    )

    # Return logger with bound context
    return logger.bind(job_name=job_name)
