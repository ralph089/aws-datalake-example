import structlog
import sys
from typing import Any, Dict
from aws_xray_sdk.core import xray_recorder

def setup_logging(job_name: str, environment: str = "local") -> structlog.BoundLogger:
    """Configure structured logging for Glue jobs"""
    
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if environment in ["dev", "staging", "prod"]:
        # JSON output for CloudWatch
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Human-readable for local development
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger(job_name)