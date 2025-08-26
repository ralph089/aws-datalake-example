"""
Test fixtures for Lambda function tests.
"""

import json

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def lambda_config():
    """Sample Lambda configuration for testing."""
    from config import LambdaConfig

    return LambdaConfig(
        env="dev",
        athena_database="test_db",
        athena_workgroup="primary",
        athena_output_bucket="test-athena-results",
        api_secret_name="dev/api/credentials",
        batch_size=50,
        max_records=1000,
    )


@pytest.fixture
def sample_athena_records():
    """Sample records returned from Athena query."""
    return [
        {
            "customer_id": "1",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "processed_timestamp": "2024-01-15T10:00:00Z",
        },
        {
            "customer_id": "2",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane@example.com",
            "processed_timestamp": "2024-01-15T10:05:00Z",
        },
        {
            "customer_id": "3",
            "first_name": "Bob",
            "last_name": "Wilson",
            "email": "bob@example.com",
            "processed_timestamp": "2024-01-15T10:10:00Z",
        },
    ]


@pytest.fixture
def scheduled_event():
    """Sample EventBridge scheduled event."""
    return {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {"table": "dev_customers_silver", "date_filter": "yesterday"},
    }


@pytest.fixture
def s3_event():
    """Sample S3 event notification."""
    return {
        "Records": [{"s3": {"bucket": {"name": "data-lake"}, "object": {"key": "silver/customers/part-001.parquet"}}}]
    }


@pytest.fixture
def api_gateway_event():
    """Sample API Gateway request event."""
    return {
        "queryStringParameters": {
            "table": "dev_customers_silver",
            "limit": "100",
            "where_clause": "customer_id > '100'",
        }
    }


@pytest.fixture
def mock_context():
    """Mock Lambda context object."""

    class MockContext:
        aws_request_id = "test-request-123"
        function_name = "data-lake-to-api"

        def remaining_time_in_millis(self):
            return 300000

    return MockContext()


@pytest.fixture
def mock_secrets():
    """Mock AWS Secrets Manager with API credentials."""
    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")

        # Create test secret
        secret_value = {
            "api_base_url": "https://api.example.com",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "token_endpoint": "/oauth/token",
        }

        client.create_secret(Name="dev/api/credentials", SecretString=json.dumps(secret_value))

        yield client


@pytest.fixture
def mock_athena():
    """Mock AWS Athena client."""
    with mock_aws():
        client = boto3.client("athena", region_name="us-east-1")
        yield client


@pytest.fixture
def mock_s3():
    """Mock AWS S3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")

        # Create test bucket for Athena results
        client.create_bucket(Bucket="test-athena-results")

        yield client
