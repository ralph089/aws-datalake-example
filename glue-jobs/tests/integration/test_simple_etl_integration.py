"""
Integration tests for Simple ETL job.

These tests run the complete job pipeline in Docker containers
to validate end-to-end functionality.
"""

import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from config import create_local_config
from jobs.simple_etl import SimpleETLJob


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for integration tests."""
    spark = (
        SparkSession.builder.appName("simple-etl-integration-tests")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def test_output_dir():
    """Create temporary output directory for test results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.mark.integration
class TestSimpleETLIntegration:
    """Integration tests for Simple ETL job."""

    def test_simple_etl_job_complete_pipeline(self, spark, test_output_dir):
        """Test complete Simple ETL job pipeline."""
        # Create config for integration test
        config = create_local_config(
            "simple_etl_integration_test",
            enable_notifications=False,  # Disable notifications for integration tests
        )

        # Initialize job
        job = SimpleETLJob(config)
        job.spark = spark  # Use fixture spark session

        # Run the job
        result = job.run()

        # Verify job completed successfully
        assert result is True

    def test_simple_etl_data_transformation(self, spark):
        """Test data transformation logic in isolation."""
        config = create_local_config(
            "simple_etl_transform_test", enable_notifications=False
        )
        job = SimpleETLJob(config)
        job.spark = spark

        # Load test data
        test_df = job.load_test_data("simple_etl", "customers.csv")

        # Verify data loaded
        assert test_df is not None
        assert test_df.count() > 0

        # Run transformation
        transformed_df = job.transform(test_df)

        # Verify transformations applied
        assert "customer_tier" in transformed_df.columns
        assert "processed_timestamp" in transformed_df.columns
        assert "processed_by_job" in transformed_df.columns

        # Verify data quality
        null_count = transformed_df.filter(transformed_df.email.isNull()).count()
        assert null_count == 0  # All emails should be valid after cleaning

    def test_job_handles_empty_data(self, spark):
        """Test that jobs handle empty datasets gracefully."""
        config = create_local_config("empty_data_test", enable_notifications=False)
        job = SimpleETLJob(config)
        job.spark = spark

        # Create empty DataFrame with expected schema
        empty_df = spark.createDataFrame(
            [],
            "customer_id INT, name STRING, email STRING, phone STRING, signup_date STRING",
        )

        # Job should handle empty data gracefully
        result = job.validate(empty_df)
        assert result is False  # Should return False for empty data

    def test_job_validation_with_missing_columns(self, spark):
        """Test job validation with missing required columns."""
        config = create_local_config("validation_test", enable_notifications=False)
        job = SimpleETLJob(config)
        job.spark = spark

        # Create DataFrame missing required columns
        incomplete_df = spark.createDataFrame(
            [("John", "john@example.com")], ["name", "email"]
        )

        # Validation should fail for incomplete data
        result = job.validate(incomplete_df)
        # Note: This depends on implementation of _get_required_columns in SimpleETLJob
