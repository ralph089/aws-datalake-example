"""
Configuration for the Data Lake to API Lambda function.

Provides Pydantic configuration models for Lambda event handling and runtime settings.
"""

import os
from typing import Any, Literal

from pydantic import BaseModel, Field


class LambdaConfig(BaseModel):
    """Configuration for Lambda function runtime."""

    env: Literal["local", "dev", "staging", "prod"] = Field(default="dev", description="Environment")
    athena_database: str = Field(default="default", description="Athena database name for queries")
    athena_workgroup: str = Field(default="primary", description="Athena workgroup for query execution")
    athena_output_bucket: str = Field(..., description="S3 bucket for Athena query results")
    api_secret_name: str = Field(..., description="AWS Secrets Manager secret name for API credentials")
    batch_size: int = Field(default=100, description="Number of records to send per API batch")
    max_records: int = Field(default=10000, description="Maximum number of records to process")

    class Config:
        validate_assignment = True
        extra = "forbid"


class ScheduledEventConfig(BaseModel):
    """Configuration for EventBridge scheduled events."""

    table_name: str = Field(..., description="Iceberg table name to query")
    date_filter: Literal["today", "yesterday", "last_week"] = Field(
        default="yesterday", description="Date filter for data query"
    )
    where_clause: str | None = Field(default=None, description="Optional WHERE clause for filtering")


class S3EventConfig(BaseModel):
    """Configuration for S3 event notifications."""

    bucket: str = Field(..., description="S3 bucket name")
    object_key: str = Field(..., description="S3 object key")


class APIGatewayEventConfig(BaseModel):
    """Configuration for API Gateway requests."""

    table_name: str = Field(..., description="Iceberg table name to query")
    limit: int = Field(default=100, description="Limit for query results")
    where_clause: str | None = Field(default=None, description="Optional WHERE clause for filtering")


def create_lambda_config() -> LambdaConfig:
    """
    Create Lambda configuration from environment variables.

    Returns:
        LambdaConfig instance
    """
    config_data = {
        "env": os.getenv("ENV", "dev"),
        "athena_database": os.getenv("ATHENA_DATABASE", "glue_catalog"),
        "athena_workgroup": os.getenv("ATHENA_WORKGROUP", "primary"),
        "athena_output_bucket": os.getenv("ATHENA_OUTPUT_BUCKET", f"athena-results-{os.getenv('ENV', 'dev')}"),
        "api_secret_name": os.getenv("API_SECRET_NAME", f"{os.getenv('ENV', 'dev')}/api/credentials"),
        "batch_size": int(os.getenv("BATCH_SIZE", "100")),
        "max_records": int(os.getenv("MAX_RECORDS", "10000")),
    }

    return LambdaConfig(**config_data)


def parse_scheduled_event(event: dict[str, Any]) -> ScheduledEventConfig:
    """Parse EventBridge scheduled event."""
    detail = event.get("detail", {})

    return ScheduledEventConfig(
        table_name=detail.get("table", "dev_customers_silver"),
        date_filter=detail.get("date_filter", "yesterday"),
        where_clause=detail.get("where_clause"),
    )


def parse_s3_event(event: dict[str, Any]) -> S3EventConfig:
    """Parse S3 event notification."""
    record = event["Records"][0]["s3"]

    return S3EventConfig(bucket=record["bucket"]["name"], object_key=record["object"]["key"])


def parse_api_gateway_event(event: dict[str, Any]) -> APIGatewayEventConfig:
    """Parse API Gateway request event."""
    params = event.get("queryStringParameters") or {}

    return APIGatewayEventConfig(
        table_name=params.get("table", "dev_customers_silver"),
        limit=int(params.get("limit", "100")),
        where_clause=params.get("where_clause"),
    )
