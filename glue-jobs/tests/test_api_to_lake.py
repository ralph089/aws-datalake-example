"""
Tests for APIToLakeJob.
"""

import json
from unittest.mock import Mock, mock_open, patch

import pytest

from jobs.api_to_lake import APIToLakeJob


class TestAPIToLakeJob:
    """Test cases for APIToLakeJob."""

    @pytest.mark.unit
    def test_extract_local_loads_mock_data(
        self, spark, sample_config, sample_products_data
    ):
        """Test extraction in local environment loads mock API data."""
        # Mock the test file reading
        api_response = {"data": sample_products_data}

        job = APIToLakeJob(sample_config)
        job.spark = spark

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=json.dumps(api_response))),
        ):
            result = job.extract()

        assert result is not None
        assert result.count() == 3
        assert "id" in result.columns
        assert "name" in result.columns

    @pytest.mark.unit
    def test_transform_adds_derived_fields(
        self, spark, sample_config, sample_products_data
    ):
        """Test transform adds price categories and metadata."""
        test_df = spark.createDataFrame(sample_products_data)

        job = APIToLakeJob(sample_config)
        job.spark = spark

        result = job.transform(test_df)

        # Check derived fields were added
        assert "price_category" in result.columns
        assert "name_length" in result.columns
        assert "processed_timestamp" in result.columns
        assert "processed_by_job" in result.columns

        # Check price categorization
        categories = [row.price_category for row in result.collect()]
        assert "premium" in categories  # $99.99 product
        assert "mid_range" in categories  # $19.99, $49.99 products

    @pytest.mark.unit
    def test_validate_passes_good_api_data(self, spark, sample_config):
        """Test validation passes for good API response data."""
        good_data = [
            {"id": i, "name": f"Product {i}", "category": "Electronics", "price": 99.99}
            for i in range(1, 11)  # Create 10 records to meet validation threshold
        ]

        test_df = spark.createDataFrame(good_data)

        job = APIToLakeJob(sample_config)
        result = job.validate(test_df)

        assert result is True

    @pytest.mark.unit
    def test_validate_fails_insufficient_data(self, spark, sample_config):
        """Test validation fails when API returns too few records."""
        # Only 5 records - below minimum threshold
        minimal_data = [
            {"id": i, "name": f"Product {i}", "price": 10.0} for i in range(5)
        ]

        test_df = spark.createDataFrame(minimal_data)

        job = APIToLakeJob(sample_config)
        result = job.validate(test_df)

        assert result is False

    @pytest.mark.unit
    def test_validate_fails_missing_required_fields(self, spark, sample_config):
        """Test validation fails when required fields are missing."""
        from pyspark.sql.types import IntegerType, StringType, DecimalType, StructField, StructType
        
        # Define schema with optional fields
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DecimalType(10, 2), True)
        ])
        
        # Missing names and prices
        bad_data = [
            {"id": 1, "name": None, "category": "Electronics", "price": None},
            {"id": 2, "name": None, "category": "Books", "price": None},
            {"id": 3, "name": "Product C", "category": "Clothing", "price": None},
        ] * 5  # Make it 15 records to pass count check

        test_df = spark.createDataFrame(bad_data, schema)

        job = APIToLakeJob(sample_config)
        result = job.validate(test_df)

        assert result is False

    @pytest.mark.unit
    @patch("jobs.base_job.APIClient")
    @patch("jobs.base_job.get_secret")
    def test_fetch_from_api_with_pagination(
        self, mock_get_secret, mock_api_client_class, spark, sample_config
    ):
        """Test API fetching with pagination."""
        # Mock secrets
        mock_get_secret.return_value = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "api_base_url": "https://api.example.com",
        }
        
        # Mock API client instance
        mock_client = Mock()
        mock_api_client_class.return_value = mock_client

        # Mock paginated responses
        page1_response = {
            "data": [
                {"id": i, "name": f"Product {i}", "price": 10.0} for i in range(100)
            ]
        }
        page2_response = {
            "data": [
                {"id": i, "name": f"Product {i}", "price": 10.0}
                for i in range(100, 150)
            ]
        }

        # First call returns full page, second returns partial page (stops pagination)
        mock_client.get.side_effect = [page1_response, page2_response]

        job = APIToLakeJob(sample_config)
        job.spark = spark

        result = job._fetch_from_api()

        assert result is not None
        # Should have called API twice (pagination)
        assert mock_client.get.call_count == 2

    @pytest.mark.unit
    def test_load_local_shows_data(
        self, spark, sample_config, sample_products_data
    ):
        """Test load method in local environment."""
        test_df = spark.createDataFrame(sample_products_data)

        job = APIToLakeJob(sample_config)

        # Should not raise exception
        job.load(test_df)
        
        # Test passes if no exception is raised

    @pytest.mark.unit
    def test_required_columns_defined(self, sample_config):
        """Test that required columns are properly defined."""
        job = APIToLakeJob(sample_config)
        required = job._get_required_columns()

        expected = ["id", "name", "price"]
        assert required == expected
