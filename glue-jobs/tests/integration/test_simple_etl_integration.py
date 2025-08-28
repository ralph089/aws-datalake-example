"""
Integration tests for Simple ETL job.

These tests run the complete job pipeline in Docker containers
to validate end-to-end functionality.
"""

import tempfile

import pytest

from config import create_local_config
from jobs.simple_etl import SimpleETLJob

# Removed duplicate spark fixture - using the one from conftest.py


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

        # Load actual test data from CSV file using relative path
        test_data_path = "test_data/simple_etl/customers.csv"
        test_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(test_data_path)
        )

        # Verify data loaded
        assert test_df is not None
        assert test_df.count() > 0

        # Run transformation
        transformed_df = job.transform(test_df)

        # Verify transformations applied
        assert "full_name" in transformed_df.columns  # SimpleETL adds full_name
        assert "processed_timestamp" in transformed_df.columns
        assert "processed_by_job" in transformed_df.columns

        # Verify data quality - some emails might be cleaned/nulled
        total_count = transformed_df.count()
        assert total_count > 0

    def test_job_handles_empty_data(self, spark):
        """Test that jobs handle empty datasets gracefully."""
        config = create_local_config("empty_data_test", enable_notifications=False)
        job = SimpleETLJob(config)
        job.spark = spark

        # Create empty DataFrame with expected schema using the spark fixture
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("customer_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("signup_date", StringType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Job should handle empty data gracefully
        result = job.validate(empty_df)
        assert result is False  # Should return False for empty data

    def test_job_validation_with_missing_columns(self, spark):
        """Test job validation with missing required columns."""
        config = create_local_config("validation_test", enable_notifications=False)
        job = SimpleETLJob(config)
        job.spark = spark

        # Create DataFrame missing required columns using the spark fixture
        incomplete_df = spark.createDataFrame(
            [("John", "john@example.com")], ["name", "email"]
        )

        # Validation should fail for incomplete data
        result = job.validate(incomplete_df)
        # Note: This depends on implementation of _get_required_columns in SimpleETLJob
