"""
AWS Glue ETL Library - Simplified ETL framework for AWS Glue jobs.

This library provides essential components for building AWS Glue ETL jobs:
- BaseGlueJob: Core ETL pattern implementation
- Configuration: Pydantic-based job configuration
- Transformations: Common data transformation utilities
- Utils: AWS services integration (logging, secrets, notifications, API client)
"""

from glue_etl_lib.config import JobConfig, create_config_from_glue_args, create_local_config
from glue_etl_lib.base_job import BaseGlueJob
from glue_etl_lib.transformations import (
    add_processing_metadata,
    clean_email,
    standardize_name,
)

# Import version from config
from glue_etl_lib.config import __version__

# Utils are available but typically imported directly by jobs as needed
# from glue_etl_lib.utils import logging, secrets, notifications, api_client

__all__ = [
    "__version__",
    "BaseGlueJob",
    "JobConfig",
    "create_config_from_glue_args",
    "create_local_config",
    "add_processing_metadata",
    "clean_email",
    "standardize_name",
]