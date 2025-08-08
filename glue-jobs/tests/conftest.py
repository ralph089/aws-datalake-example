import json
import os

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    """Create mocked S3 client"""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        # Create test buckets
        client.create_bucket(Bucket="test-bronze")
        client.create_bucket(Bucket="test-silver")
        client.create_bucket(Bucket="test-gold")
        client.create_bucket(Bucket="data-lake-dev-bronze")
        client.create_bucket(Bucket="data-lake-dev-silver")
        client.create_bucket(Bucket="data-lake-dev-gold")
        yield client


@pytest.fixture
def secrets_client(aws_credentials):
    """Create mocked Secrets Manager client"""
    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        # Create test secret
        client.create_secret(
            Name="dev/api/credentials",
            SecretString=json.dumps({"base_url": "https://api.example.com", "bearer_token": "test-token-123"}),
        )
        yield client


@pytest.fixture
def sqs_client(aws_credentials):
    """Create mocked SQS client"""
    with mock_aws():
        client = boto3.client("sqs", region_name="us-east-1")
        # Create test DLQ
        client.create_queue(
            QueueName="dev-glue-dlq", Attributes={"MessageRetentionPeriod": "1209600", "VisibilityTimeout": "300"}
        )
        yield client


@pytest.fixture
def sns_client(aws_credentials):
    """Create mocked SNS client"""
    with mock_aws():
        client = boto3.client("sns", region_name="us-east-1")
        # Create test topic
        client.create_topic(Name="dev-job-notifications")
        yield client


# Note: Sample data fixtures can be replaced with direct factory imports
# Example usage in tests:
# from tests.factories import CustomerFactory
# customers = CustomerFactory.build_batch(3)


# Centralized Mock Management Fixtures


@pytest.fixture
def job_mocks():
    """
    Centralized mock configuration for BaseGlueJob dependencies.

    Eliminates the need for repetitive mock setup in every test.
    Returns configured mocks for all BaseGlueJob dependencies.
    """
    from .integration.test_job_runner import MockConfigFactory

    return MockConfigFactory.create_successful_job_flow_mocks()


@pytest.fixture
def failure_mocks():
    """Mock configuration optimized for failure scenario testing."""
    from .integration.test_job_runner import MockConfigFactory

    return MockConfigFactory.create_failure_scenario_mocks()


@pytest.fixture
def mock_job_context(job_mocks):
    """
    Context manager fixture for applying job mocks.

    Usage in tests:
        def test_something(mock_job_context):
            with mock_job_context as mocks:
                # Test code here - all BaseGlueJob deps are mocked
                job = CustomerImportJob("test", {"env": "local"})
    """
    from contextlib import ExitStack

    from .integration.test_job_runner import MockConfigFactory

    class MockJobContext:
        def __init__(self, mocks):
            self.mocks = mocks
            self.patches = None
            self.stack = None

        def __enter__(self):
            self.stack = ExitStack()
            self.patches = MockConfigFactory.apply_job_mocks(self.mocks)

            # Apply all patches using the stack
            for patch_obj in self.patches:
                self.stack.enter_context(patch_obj)

            return self.mocks

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.stack:
                self.stack.__exit__(exc_type, exc_val, exc_tb)

    return MockJobContext(job_mocks)


@pytest.fixture
def mock_failure_context(failure_mocks):
    """Context manager fixture for failure scenario testing."""
    from contextlib import ExitStack

    from .integration.test_job_runner import MockConfigFactory

    class MockJobContext:
        def __init__(self, mocks):
            self.mocks = mocks
            self.patches = None
            self.stack = None

        def __enter__(self):
            self.stack = ExitStack()
            self.patches = MockConfigFactory.apply_job_mocks(self.mocks)

            # Apply all patches using the stack
            for patch_obj in self.patches:
                self.stack.enter_context(patch_obj)

            return self.mocks

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.stack:
                self.stack.__exit__(exc_type, exc_val, exc_tb)

    return MockJobContext(failure_mocks)


# Note: No factory fixtures needed - import factories directly in tests
# from tests.factories import CustomerFactory, SalesTransactionFactory, etc.


# Job Factory Fixtures


@pytest.fixture
def job_factory():
    """
    Factory for creating pre-configured job instances.

    Usage:
        def test_something(job_factory):
            job = job_factory.customer_import("test_job_name")
            # Job is pre-configured with runner's Spark session
    """
    from jobs.api_data_fetch import APIDataFetchJob
    from jobs.customer_import import CustomerImportJob
    from jobs.inventory_sync import InventorySyncJob
    from jobs.sales_etl import SalesETLJob

    class JobFactory:
        def __init__(self, runner):
            self.runner = runner

        def customer_import(self, job_name="test_customer_job", run_id=None):
            run_id = run_id or f"test-{job_name}-123"
            job = CustomerImportJob(job_name, {"env": "local", "JOB_RUN_ID": run_id})
            job.spark = self.runner.spark
            return job

        def sales_etl(self, job_name="test_sales_job", run_id=None):
            run_id = run_id or f"test-{job_name}-123"
            job = SalesETLJob(job_name, {"env": "local", "JOB_RUN_ID": run_id})
            job.spark = self.runner.spark
            return job

        def inventory_sync(self, job_name="test_inventory_job", run_id=None):
            run_id = run_id or f"test-{job_name}-123"
            job = InventorySyncJob(job_name, {"env": "local", "JOB_RUN_ID": run_id})
            job.spark = self.runner.spark
            return job

        def api_data_fetch(self, job_name="test_api_job", run_id=None):
            run_id = run_id or f"test-{job_name}-123"
            job = APIDataFetchJob(job_name, {"env": "local", "JOB_RUN_ID": run_id})
            job.spark = self.runner.spark
            return job

        def create_job(self, job_class, job_name, run_id, args=None):
            """Generic job creation method"""
            args = args or {}
            full_args = {"env": "local", "JOB_RUN_ID": run_id, **args}
            job = job_class(job_name, full_args)
            job.spark = self.runner.spark
            return job

    from .integration.test_job_runner import IntegrationTestRunner
    runner = IntegrationTestRunner()
    runner.setup_mock_aws_services()
    runner.setup_spark_session()  # Initialize Spark session
    return JobFactory(runner)
