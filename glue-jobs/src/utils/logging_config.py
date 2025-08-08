import json
import logging
import os
import sys
from typing import Any, Dict

import structlog

JSON_SERIALIZER = json.dumps


def _get_log_level(level_str: str) -> int:
    """Convert log level string to integer with validation"""
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARNING,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    level = level_map.get(level_str.upper())
    if level is None:
        # Try to parse as integer
        try:
            level = int(level_str)
            if level not in [10, 20, 30, 40, 50]:  # Valid logging levels
                raise ValueError
        except (ValueError, TypeError):
            # Fall back to INFO if invalid
            level = logging.INFO

    return level


def _add_exception_context(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add enhanced context to exception logs"""
    if 'exc_info' in event_dict and event_dict['exc_info']:
        exc_type, exc_value, exc_traceback = event_dict['exc_info']
        if exc_type and exc_value:
            event_dict['exception_type'] = exc_type.__name__
            event_dict['exception_message'] = str(exc_value)

            # Add correlation ID using existing job context if available
            if hasattr(logger, '_context') and logger._context:
                job_run_id = logger._context.get('job_run_id')
                if job_run_id:
                    event_dict['correlation_id'] = job_run_id

    return event_dict


def _truncate_long_values(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Truncate very long field values to keep logs readable"""
    max_length = 100

    # Handle specific fields that tend to be very verbose
    if 'results' in event_dict and isinstance(event_dict['results'], dict):
        results = event_dict['results']
        if 'quality_validation' in results and 'overall_status' in results['quality_validation']:
            status = results['quality_validation']['overall_status']
            passed = results['quality_validation'].get('passed_count', 0)
            failed = results['quality_validation'].get('failed_count', 0)
            event_dict['validation_summary'] = f"{status} ({passed} passed, {failed} failed)"
            del event_dict['results']

    if 'validation_results' in event_dict and isinstance(event_dict['validation_results'], dict):
        results = event_dict['validation_results']
        if 'quality_validation' in results and 'overall_status' in results['quality_validation']:
            status = results['quality_validation']['overall_status']
            passed = results['quality_validation'].get('passed_count', 0)
            failed = results['quality_validation'].get('failed_count', 0)
            event_dict['validation_summary'] = f"{status} ({passed} passed, {failed} failed)"
            del event_dict['validation_results']

    # Handle any remaining long dictionary values
    for key, value in list(event_dict.items()):
        if isinstance(value, dict) and len(str(value)) > max_length:
            if 'overall_status' in value:
                event_dict[key] = f"summary={value.get('overall_status', 'UNKNOWN')}"
            else:
                event_dict[key] = "<complex_data_type>"
        elif isinstance(value, str) and len(value) > max_length:
            event_dict[key] = f"{value[:max_length]}..."

    return event_dict


def setup_logging(job_name: str, environment: str = "local") -> Any:
    """Configure optimized structured logging for Glue jobs"""

    # Get log level from environment variable with validation
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = _get_log_level(log_level_str)

    # Respect NO_COLOR and FORCE_COLOR environment variables
    use_colors = bool(
        sys.stderr.isatty()
        and not os.environ.get("NO_COLOR")
        and (os.environ.get("FORCE_COLOR") or environment == "local")
    )

    # Base processors for all environments
    base_processors = [
        structlog.contextvars.merge_contextvars,  # Include context variables
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        _add_exception_context,  # Enhanced exception logging
        _truncate_long_values,  # Keep logs readable by truncating long values
    ]

    if environment in ["dev", "staging", "prod"]:
        # Production configuration with optimizations
        processors = base_processors + [
            structlog.processors.format_exc_info,  # Handle exceptions efficiently
            structlog.processors.TimeStamper(fmt="iso", utc=True),  # UTC timestamps
            structlog.processors.JSONRenderer(serializer=JSON_SERIALIZER),  # Fast JSON
        ]

        logger_factory = structlog.stdlib.LoggerFactory()

        # Enable filtering for better performance
        wrapper_class = structlog.make_filtering_bound_logger(log_level)

    else:
        # Local development configuration
        processors = base_processors + [
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            structlog.dev.set_exc_info,  # Pretty exception handling
            structlog.dev.ConsoleRenderer(
                colors=use_colors,
                sort_keys=False,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

        logger_factory = structlog.stdlib.LoggerFactory()
        wrapper_class = structlog.make_filtering_bound_logger(log_level)

    # Configure basic logging for stdlib integration
    # Always configure basic logging to ensure output appears in containers
    if environment != "local":
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=log_level,
        )
    else:
        # For local environment, also configure basic logging to ensure container output
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=log_level,
            force=True,  # Override any existing configuration
        )

    structlog.configure(
        processors=processors,  # type: ignore[arg-type]
        context_class=dict,
        logger_factory=logger_factory,
        wrapper_class=wrapper_class,
        cache_logger_on_first_use=True,  # Enable caching for better performance
    )

    return structlog.get_logger(job_name)
