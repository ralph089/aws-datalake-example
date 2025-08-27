"""
Simple configuration for AWS Glue jobs.

Provides basic Pydantic configuration models with AWS Glue argument integration.
"""

__version__ = "1.0.0"

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class JobConfig(BaseModel):
    """Simple configuration for AWS Glue ETL jobs."""

    job_name: str = Field(..., description="Job identifier")
    job_run_id: str = Field(
        default_factory=lambda: f"local-{datetime.now().isoformat()}",
        description="Unique run identifier",
    )
    env: Literal["local", "dev", "staging", "prod"] = Field(
        default="local", description="Environment"
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", description="Logging level"
    )

    # S3 Event configuration
    bucket: str | None = Field(default=None, description="S3 bucket from event")
    object_key: str | None = Field(default=None, description="S3 object key from event")

    # Optional configuration
    enable_notifications: bool = Field(
        default=True, description="Enable SNS notifications"
    )

    @property
    def is_event_triggered(self) -> bool:
        """Check if job was triggered by S3 event."""
        return bool(self.bucket and self.object_key)

    @property
    def trigger_type(self) -> Literal["s3_event", "scheduled"]:
        """Get trigger type."""
        return "s3_event" if self.is_event_triggered else "scheduled"

    class Config:
        validate_assignment = True
        extra = "forbid"


def create_config_from_glue_args(sys_argv: list) -> JobConfig:
    """
    Create configuration from AWS Glue job arguments.

    Args:
        sys_argv: System arguments from sys.argv

    Returns:
        JobConfig instance
    """
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore[import-untyped]

        required_args = ["JOB_NAME"]
        optional_args = [
            "JOB_RUN_ID",
            "env",
            "bucket",
            "object_key",
            "LOG_LEVEL",
            "enable_notifications",
        ]

        # Get available optional arguments
        available_optional = [arg for arg in optional_args if f"--{arg}" in sys_argv]
        all_args = required_args + available_optional

        args = getResolvedOptions(sys_argv, all_args)

        # Map arguments to configuration
        config_data = {
            "job_name": args["JOB_NAME"],
            "job_run_id": args.get("JOB_RUN_ID", f"local-{datetime.now().isoformat()}"),
            "env": args.get("env", "local"),
            "log_level": args.get("LOG_LEVEL", "INFO"),
            "bucket": args.get("bucket"),
            "object_key": args.get("object_key"),
            "enable_notifications": args.get("enable_notifications", "true").lower()
            == "true",
        }

        return JobConfig(**config_data)

    except Exception as e:
        raise RuntimeError(f"Failed to create configuration: {e}") from e


def create_local_config(job_name: str, **overrides: Any) -> JobConfig:
    """
    Create a local development configuration.

    Args:
        job_name: Name of the job
        **overrides: Configuration overrides

    Returns:
        Local development configuration
    """
    config_data = {"job_name": job_name, "env": "local", **overrides}

    return JobConfig(**config_data)
