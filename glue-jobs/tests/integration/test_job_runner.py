"""
Base integration test framework for Glue jobs.

Provides utilities for running jobs in Docker environment and validating results.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
import structlog
from moto import mock_aws
from pyspark.sql import DataFrame, SparkSession


class IntegrationTestRunner:
    """Helper class for running integration tests on Glue jobs"""

    def __init__(self):
        self.temp_dirs = []
        self.spark = None
        self.s3_client = None
        self.test_buckets = ["test-bronze", "test-silver", "test-gold"]

    def setup_spark_session(self) -> SparkSession:
        """Create Spark session for integration testing"""
        if not self.spark:
            self.spark = (
                SparkSession.builder.appName("integration-test")
                .master("local[2]")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
        return self.spark

    def get_temp_dir(self) -> str:
        """Create temporary directory for test outputs"""
        temp_dir = tempfile.mkdtemp(prefix="integration_test_")
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def setup_mock_aws_services(self):
        """Setup mocked AWS services for testing"""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    def setup_s3_buckets_and_data(self):
        """Setup S3 buckets and upload test data"""
        with mock_aws():
            self.s3_client = boto3.client("s3", region_name="us-east-1")

            # Create test buckets
            for bucket in self.test_buckets:
                self.s3_client.create_bucket(Bucket=bucket)

            # Upload test data files
            test_data_dir = Path(__file__).parent.parent / "test_data"
            for csv_file in test_data_dir.glob("*.csv"):
                if csv_file.name.endswith(".csv"):
                    key = f"{csv_file.stem}/{csv_file.name}"
                    self.s3_client.upload_file(str(csv_file), "test-bronze", key)

            return self.s3_client

    def run_job_with_args(self, job_class, args: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a Glue job with given arguments and return execution results

        Args:
            job_class: The job class to instantiate and run
            args: Job arguments dictionary

        Returns:
            Dictionary with execution results and metrics
        """
        # Setup environment
        spark = self.setup_spark_session()

        # Mock the job arguments to include test environment
        test_args = {"env": "test", "JOB_RUN_ID": "test-run-123", **args}

        # Patch external dependencies for isolated testing
        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
            patch("jobs.base_job.get_secret"),
        ):
            # Setup mocks
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            # Instantiate and run job
            job = job_class(job_class.__name__, test_args)
            job.spark = spark  # Use our test spark session

            # Execute the job lifecycle
            try:
                # Extract
                raw_df = job.extract()
                raw_count = raw_df.count()

                # Transform
                transformed_df = job.transform(raw_df)
                transformed_count = transformed_df.count()

                # Validate
                validation_results = job.validate(transformed_df)

                # Load - returns list of output paths, not DataFrame
                output_paths = job.load(transformed_df)
                final_count = transformed_count  # Use transformed count since load returns paths

                return {
                    "success": True,
                    "raw_count": raw_count,
                    "transformed_count": transformed_count,
                    "final_count": final_count,
                    "validation_results": validation_results,
                    "columns": transformed_df.columns,
                    "sample_data": transformed_df.limit(5).collect() if transformed_df else [],
                }

            except Exception as e:
                return {"success": False, "error": str(e), "error_type": type(e).__name__}

    def validate_data_quality(self, df: DataFrame, expected_columns: list[str]) -> dict[str, Any]:
        """Validate data quality of the processed DataFrame"""
        results = {
            "column_check": set(df.columns) >= set(expected_columns),
            "row_count": df.count(),
            "null_checks": {},
            "duplicate_checks": {},
        }

        # Check for nulls in key columns
        for col in expected_columns:
            if col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                results["null_checks"][col] = null_count

        return results

    def validate_s3_output(self, bucket: str, prefix: str) -> dict[str, Any]:
        """Validate that data was written to S3 correctly"""
        if not self.s3_client:
            return {"success": False, "error": "S3 client not initialized"}

        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = response.get("Contents", [])

            return {
                "success": True,
                "file_count": len(files),
                "files": [obj["Key"] for obj in files],
                "total_size": sum(obj["Size"] for obj in files),
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def cleanup(self):
        """Clean up temporary directories and resources"""
        if self.spark:
            self.spark.stop()

        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


@pytest.fixture
def integration_runner():
    """Pytest fixture for integration test runner"""
    runner = IntegrationTestRunner()
    runner.setup_mock_aws_services()
    yield runner
    runner.cleanup()


@pytest.fixture
def s3_with_test_data(integration_runner):
    """Pytest fixture for S3 with test data loaded"""
    with mock_aws():
        return integration_runner.setup_s3_buckets_and_data()


# Performance Optimizations


@pytest.fixture(scope="session")
def shared_spark_session():
    """
    Session-scoped Spark session to improve test performance.

    Creates a single Spark session that can be reused across multiple tests
    to avoid the overhead of creating/stopping sessions for each test.
    """
    spark = (
        SparkSession.builder.appName("integration-test-session")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    # Set log level to reduce noise during tests
    spark.sparkContext.setLogLevel("WARN")

    yield spark
    spark.stop()


@pytest.fixture
def optimized_integration_runner(shared_spark_session):
    """
    Optimized integration test runner that reuses shared Spark session.

    This fixture provides better performance by avoiding Spark session
    creation/destruction overhead while maintaining test isolation.
    """
    runner = IntegrationTestRunner()
    runner.spark = shared_spark_session  # Use shared session
    runner.setup_mock_aws_services()
    yield runner
    runner.cleanup()


class MockConfigFactory:
    """Factory for creating reusable mock configurations to improve test performance."""

    @staticmethod
    def create_base_job_mocks():
        """Create standard mock configuration for BaseGlueJob dependencies."""
        mocks = {}

        # Setup logging mock
        logging_mock = MagicMock()
        logger_mock = MagicMock()
        logging_mock.return_value = logger_mock
        logger_mock.bind.return_value = logger_mock
        mocks["logging"] = logging_mock

        # Setup audit tracker mock
        audit_mock = MagicMock()
        mocks["audit"] = audit_mock

        # Setup DLQ handler mock
        dlq_mock = MagicMock()
        mocks["dlq"] = dlq_mock

        # Setup notification service mock
        notifications_mock = MagicMock()
        mocks["notifications"] = notifications_mock

        return mocks

    @staticmethod
    def apply_job_mocks(mock_dict):
        """
        Apply mock configuration using context managers.

        Returns a list of patch objects that can be used with ExitStack
        or individual context managers.
        """
        patches = [
            patch("jobs.base_job.setup_logging", mock_dict["logging"]),
            patch("jobs.base_job.AuditTracker", return_value=mock_dict["audit"]),
            patch("jobs.base_job.DLQHandler", return_value=mock_dict["dlq"]),
            patch("jobs.base_job.NotificationService", return_value=mock_dict["notifications"]),
        ]

        return patches

    @staticmethod
    def create_successful_job_flow_mocks():
        """Create mocks configured for successful job execution flows."""
        base_mocks = MockConfigFactory.create_base_job_mocks()

        # Configure mocks for successful scenarios
        base_mocks["audit"].start_job.return_value = None
        base_mocks["audit"].log_extract.return_value = None
        base_mocks["audit"].log_transform.return_value = None
        base_mocks["audit"].log_load.return_value = None
        base_mocks["audit"].complete_job.return_value = None

        base_mocks["dlq"].send_to_dlq.return_value = None
        base_mocks["notifications"].send_success_notification.return_value = None
        base_mocks["notifications"].send_failure_notification.return_value = None

        return base_mocks

    @staticmethod
    def create_failure_scenario_mocks():
        """Create mocks configured for job failure scenarios."""
        base_mocks = MockConfigFactory.create_base_job_mocks()

        # Configure mocks to expect failure scenarios
        base_mocks["audit"].complete_job.return_value = None  # Will be called with "FAILED" status
        base_mocks["dlq"].send_to_dlq.return_value = None  # Should be called on failure
        base_mocks["notifications"].send_failure_notification.return_value = None

        # Success notification should NOT be called in failure scenarios
        base_mocks["notifications"].send_success_notification.return_value = None

        return base_mocks


@pytest.fixture
def reusable_job_mocks():
    """Fixture providing reusable mock configurations for job tests."""
    return MockConfigFactory.create_successful_job_flow_mocks()


@pytest.fixture
def reusable_failure_mocks():
    """Fixture providing reusable mock configurations for failure scenario tests."""
    return MockConfigFactory.create_failure_scenario_mocks()
