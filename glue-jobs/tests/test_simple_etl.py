"""
Tests for SimpleETLJob.
"""

from unittest.mock import patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from jobs.simple_etl import SimpleETLJob


class TestSimpleETLJob:
    """Test cases for SimpleETLJob."""

    @pytest.mark.unit
    def test_extract_local_environment(
        self, spark, sample_config, sample_customers_data
    ):
        """Test data extraction in local environment."""
        # Create DataFrame from sample data
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )

        test_df = spark.createDataFrame(sample_customers_data, schema)

        job = SimpleETLJob(sample_config)
        job.spark = spark

        # Mock load_test_data method
        with patch.object(job, "load_test_data", return_value=test_df):
            result = job.extract()

        assert result is not None
        assert result.count() == 3
        assert "customer_id" in result.columns

    @pytest.mark.unit
    def test_transform_cleans_data(self, spark, sample_config, sample_customers_data):
        """Test that transform method cleans and enriches data."""
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )

        test_df = spark.createDataFrame(sample_customers_data, schema)

        job = SimpleETLJob(sample_config)
        job.spark = spark

        result = job.transform(test_df)

        # Check that metadata was added
        assert "processed_timestamp" in result.columns
        assert "processed_by_job" in result.columns
        assert "job_run_id" in result.columns
        assert "full_name" in result.columns

        # Verify row count unchanged
        assert result.count() == 3

    @pytest.mark.unit
    def test_validate_passes_good_data(self, spark, sample_config):
        """Test validation passes for good quality data."""
        good_data = [
            {
                "customer_id": "1",
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
            },
            {
                "customer_id": "2",
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane@example.com",
            },
        ]

        test_df = spark.createDataFrame(good_data)

        job = SimpleETLJob(sample_config)
        result = job.validate(test_df)

        assert result is True

    @pytest.mark.unit
    def test_validate_fails_poor_quality_data(self, spark, sample_config):
        """Test validation fails for poor quality data."""
        # Most emails are invalid
        bad_data = [
            {
                "customer_id": "1",
                "first_name": "John",
                "last_name": "Doe",
                "email": "not_an_email",
            },
            {
                "customer_id": "2",
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "also_bad",
            },
            {
                "customer_id": "3",
                "first_name": "Bob",
                "last_name": "Wilson",
                "email": "good@example.com",
            },
        ]

        test_df = spark.createDataFrame(bad_data)

        job = SimpleETLJob(sample_config)
        job.spark = spark

        # Transform the data first, then validate
        transformed_df = job.transform(test_df)
        result = job.validate(transformed_df)

        assert result is False

    @pytest.mark.unit
    def test_load_local_shows_data(
        self, spark, sample_config, sample_customers_data
    ):
        """Test load method in local environment."""
        test_df = spark.createDataFrame(sample_customers_data)

        job = SimpleETLJob(sample_config)

        # Should not raise exception
        job.load(test_df)
        
        # Test passes if no exception is raised

    @pytest.mark.unit
    def test_required_columns_defined(self, sample_config):
        """Test that required columns are properly defined."""
        job = SimpleETLJob(sample_config)
        required = job._get_required_columns()

        expected = ["customer_id", "first_name", "email"]
        assert required == expected
