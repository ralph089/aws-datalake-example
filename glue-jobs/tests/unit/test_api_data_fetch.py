"""
Unit tests for APIDataFetchJob
Tests individual methods and components in isolation.
"""

import json
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from jobs.api_data_fetch import APIDataFetchJob


class TestAPIDataFetchJob:
    """Unit tests for API data fetch job"""

    @pytest.fixture
    def spark(self):
        """Create Spark session for testing"""
        return SparkSession.builder.appName("test_api_data_fetch").master("local[2]").getOrCreate()

    @pytest.fixture
    def job_args(self):
        """Standard job arguments for testing"""
        return {
            "JOB_NAME": "test_api_data_fetch",
            "env": "local",
            "api_endpoint": "/api/v1/test",
            "page_size": "10",
            "max_pages": "5",
        }

    @pytest.fixture
    def mock_job(self, job_args):
        """Create APIDataFetchJob with mocked dependencies"""
        with patch.object(APIDataFetchJob, "_create_spark_session"), \
             patch.object(APIDataFetchJob, "_setup_api_client"):
            job = APIDataFetchJob("test_api_data_fetch", job_args)
            job.spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
            job.api_client = Mock()
            return job

    def test_job_initialization(self, job_args):
        """Test job initialization with arguments"""
        with patch.object(APIDataFetchJob, "_create_spark_session"), \
             patch.object(APIDataFetchJob, "_setup_api_client"):
            job = APIDataFetchJob("test_job", job_args)

            assert job.job_name == "test_job"
            assert job.api_endpoint == "/api/v1/test"
            assert job.page_size == 10
            assert job.max_pages == 5

    def test_job_initialization_with_defaults(self):
        """Test job initialization with default values"""
        minimal_args = {"JOB_NAME": "test_job", "env": "local"}

        with patch.object(APIDataFetchJob, "_create_spark_session"), \
             patch.object(APIDataFetchJob, "_setup_api_client"):
            job = APIDataFetchJob("test_job", minimal_args)

            assert job.api_endpoint == "/api/v1/data"  # default
            assert job.page_size == 100  # default
            assert job.max_pages == 10  # default

    def test_create_empty_dataframe(self, mock_job):
        """Test creation of empty DataFrame with expected schema"""
        df = mock_job._create_empty_dataframe()

        assert df.count() == 0, "DataFrame should be empty"
        assert "id" in df.columns, "Should have id column"
        assert "name" in df.columns, "Should have name column"
        assert "value" in df.columns, "Should have value column"
        assert "created_at" in df.columns, "Should have created_at column"

    def test_create_empty_transformed_dataframe(self, mock_job):
        """Test creation of empty transformed DataFrame with full schema"""
        df = mock_job._create_empty_transformed_dataframe()

        assert df.count() == 0, "DataFrame should be empty"

        # Check for all expected columns after transformation
        expected_columns = [
            "id", "name", "value", "created_at", "has_valid_id",
            "api_endpoint", "extraction_timestamp", "processed_timestamp",
            "processed_by_job", "job_run_id"
        ]

        for col in expected_columns:
            assert col in df.columns, f"Should have {col} column"

    def test_extract_local_with_test_data(self, mock_job):
        """Test extract method in local environment with test data"""
        # Create test JSON file content
        test_data = [
            {"id": "test_001", "name": "Test Product", "value": "100.00"}
        ]

        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(test_data)

            # Mock spark.read to return our test data
            mock_df = Mock()
            mock_df.count.return_value = 1
            mock_job.spark.read.option.return_value.json.return_value = mock_df

            result = mock_job.extract()

            assert result == mock_df, "Should return mocked DataFrame"

    def test_extract_local_missing_test_data(self, mock_job):
        """Test extract method when test data file is missing"""
        # Mock spark.read to raise exception (file not found)
        mock_job.spark.read.option.return_value.json.side_effect = Exception("File not found")

        # Should return empty DataFrame when test data is missing
        result = mock_job.extract()
        assert result is not None, "Should return empty DataFrame when test data missing"

    def test_extract_with_api_client_success(self, mock_job):
        """Test extract method with successful API responses"""
        mock_job.environment = "dev"  # Non-local environment

        # Mock API responses for pagination
        api_responses = [
            {
                "data": [
                    {"id": "api_001", "name": "Product 1"},
                    {"id": "api_002", "name": "Product 2"}
                ],
                "pagination": {"page": 1, "total_pages": 2}
            },
            {
                "data": [
                    {"id": "api_003", "name": "Product 3"}
                ],
                "pagination": {"page": 2, "total_pages": 2}
            }
        ]

        mock_job.api_client.get.side_effect = api_responses

        # Mock spark.createDataFrame
        mock_df = Mock()
        mock_df.count.return_value = 3
        mock_job.spark.createDataFrame.return_value = mock_df

        result = mock_job.extract()

        assert result == mock_df, "Should return DataFrame from API data"
        assert mock_job.api_client.get.call_count == 2, "Should make 2 API calls for pagination"

    def test_extract_with_api_client_no_client(self, mock_job):
        """Test extract method when API client is not configured"""
        mock_job.environment = "dev"
        mock_job.api_client = None

        with pytest.raises(ValueError) as exc_info:
            mock_job.extract()

        assert "API client not configured" in str(exc_info.value)

    def test_extract_with_empty_api_response(self, mock_job):
        """Test extract method with empty API response"""
        mock_job.environment = "dev"
        mock_job.api_client.get.return_value = {"data": []}

        result = mock_job.extract()
        assert result is not None, "Should return empty DataFrame for empty API response"

    def test_flatten_json_columns(self, mock_job, spark):
        """Test JSON column flattening functionality"""
        # Create DataFrame with nested structure
        data = [
            {
                "id": "test_001",
                "metadata": {
                    "source": "api",
                    "priority": "high"
                }
            }
        ]

        df = spark.createDataFrame(data)
        result = mock_job._flatten_json_columns(df)

        # Check that nested columns were flattened
        assert "metadata_source" in result.columns, "Should flatten metadata.source"
        assert "metadata_priority" in result.columns, "Should flatten metadata.priority"
        assert "metadata" not in result.columns, "Should remove original nested column"

        # Verify values
        row = result.collect()[0]
        assert row["metadata_source"] == "api"
        assert row["metadata_priority"] == "high"

    def test_flatten_json_columns_no_structs(self, mock_job, spark):
        """Test JSON flattening with no struct columns"""
        # Create DataFrame with only simple columns
        data = [{"id": "test_001", "name": "Test Product"}]
        df = spark.createDataFrame(data)

        result = mock_job._flatten_json_columns(df)

        # Should return unchanged DataFrame
        assert result.columns == df.columns
        assert result.count() == df.count()

    def test_flatten_json_columns_error_handling(self, mock_job, spark):
        """Test JSON flattening error handling"""
        # Create DataFrame
        data = [{"id": "test_001"}]
        df = spark.createDataFrame(data)

        # Mock an error in the flattening process
        with patch.object(df, 'schema', side_effect=Exception("Schema error")):
            result = mock_job._flatten_json_columns(df)

            # Should return original DataFrame on error
            assert result.columns == df.columns

    def test_enrich_api_data(self, mock_job, spark):
        """Test API data enrichment functionality"""
        # Create test data
        data = [
            {"id": "test_001", "amount": 50},
            {"id": "test_002", "amount": 500},
            {"id": "test_003", "amount": 5000},
            {"id": None, "amount": 100}  # Test null ID
        ]

        df = spark.createDataFrame(data)
        result = mock_job._enrich_api_data(df)

        # Check enrichment columns were added
        assert "has_valid_id" in result.columns, "Should add has_valid_id column"
        assert "amount_category" in result.columns, "Should add amount_category column"

        # Verify enrichment logic
        rows = {row["id"]: row for row in result.collect() if row["id"] is not None}

        assert rows["test_001"]["has_valid_id"] is True
        assert rows["test_001"]["amount_category"] == "small"  # < 100

        assert rows["test_002"]["has_valid_id"] is True
        assert rows["test_002"]["amount_category"] == "medium"  # 100-1000

        assert rows["test_003"]["has_valid_id"] is True
        assert rows["test_003"]["amount_category"] == "large"  # > 1000

    def test_enrich_api_data_error_handling(self, mock_job, spark):
        """Test data enrichment error handling"""
        data = [{"id": "test_001"}]
        df = spark.createDataFrame(data)

        # Mock an error in enrichment
        with patch("pyspark.sql.functions.when", side_effect=Exception("Enrichment error")):
            result = mock_job._enrich_api_data(df)

            # Should return original DataFrame on error
            assert result.columns == df.columns

    def test_custom_validation_success(self, mock_job, spark):
        """Test successful custom validation"""
        # Create valid test data
        data = [
            {"id": "test_001", "name": "Product 1"},
            {"id": "test_002", "name": "Product 2"}
        ]

        df = spark.createDataFrame(data)
        result = mock_job.custom_validation(df)

        assert result is True, "Validation should pass for valid data"

    def test_custom_validation_empty_dataframe(self, mock_job, spark):
        """Test custom validation with empty DataFrame"""
        # Create empty DataFrame with correct schema
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)

        result = mock_job.custom_validation(df)

        assert result is False, "Validation should fail for empty DataFrame"

    def test_custom_validation_missing_required_columns(self, mock_job, spark):
        """Test custom validation with missing required columns"""
        # Create DataFrame without required 'id' column
        data = [{"name": "Product 1", "value": "100"}]
        df = spark.createDataFrame(data)

        result = mock_job.custom_validation(df)

        assert result is False, "Validation should fail for missing required columns"

    def test_custom_validation_duplicate_ids(self, mock_job, spark):
        """Test custom validation with duplicate IDs (should warn but not fail)"""
        # Create data with duplicate IDs
        data = [
            {"id": "test_001", "name": "Product 1"},
            {"id": "test_001", "name": "Product 1 Duplicate"}  # Duplicate ID
        ]

        df = spark.createDataFrame(data)
        result = mock_job.custom_validation(df)

        # Should pass validation but log warning
        assert result is True, "Validation should pass but warn about duplicates"

    def test_custom_validation_error_handling(self, mock_job, spark):
        """Test custom validation error handling"""
        data = [{"id": "test_001"}]
        df = spark.createDataFrame(data)

        # Mock an error in validation
        with patch.object(df, 'count', side_effect=Exception("Validation error")):
            result = mock_job.custom_validation(df)

            assert result is False, "Should return False on validation error"

    def test_transform_empty_dataframe(self, mock_job, spark):
        """Test transform method with empty DataFrame"""
        # Create empty DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)

        result = mock_job.transform(df)

        assert result.count() == 0, "Should return empty transformed DataFrame"
        # Should have transformed schema columns
        assert "processed_timestamp" in result.columns
        assert "api_endpoint" in result.columns

    def test_transform_with_data(self, mock_job, spark):
        """Test transform method with actual data"""
        # Create test data with nested structure
        data = [
            {
                "id": "test_001",
                "name": "Test Product",
                "value": "150",
                "metadata": {"source": "api", "priority": "high"}
            }
        ]

        df = spark.createDataFrame(data)
        result = mock_job.transform(df)

        assert result.count() == 1, "Should transform 1 record"

        # Check for flattened columns
        assert "metadata_source" in result.columns
        assert "metadata_priority" in result.columns

        # Check for enrichment columns
        assert "has_valid_id" in result.columns

        # Check for processing metadata
        assert "processed_timestamp" in result.columns
        assert "processed_by_job" in result.columns
        assert "api_endpoint" in result.columns

        # Verify values
        row = result.collect()[0]
        assert row["metadata_source"] == "api"
        assert row["has_valid_id"] is True
        assert row["api_endpoint"] == "/api/v1/test"

    def test_load_local_environment(self, mock_job, spark):
        """Test load method in local environment"""
        mock_job.environment = "local"

        # Create test DataFrame
        data = [{"id": "test_001", "name": "Test Product"}]
        df = spark.createDataFrame(data)

        # Mock DataFrame write operations
        mock_writer = Mock()
        df.coalesce = Mock(return_value=Mock(write=mock_writer))
        mock_writer.mode.return_value.option.return_value.csv = Mock()

        result = mock_job.load(df)

        assert len(result) == 1, "Should return one output path"
        assert "dist/local_output/api_data/" in result[0]

    def test_load_non_local_environment_iceberg_success(self, mock_job, spark):
        """Test load method with successful Iceberg write"""
        mock_job.environment = "dev"

        # Create test DataFrame
        data = [{"id": "test_001", "name": "Test Product"}]
        df = spark.createDataFrame(data)

        # Mock DataFrame Iceberg write operations
        mock_writer = Mock()
        df.writeTo = Mock(return_value=mock_writer)
        mock_writer.using.return_value.tableProperty.return_value.option.return_value.createOrReplace = Mock()

        result = mock_job.load(df)

        assert len(result) == 1, "Should return one output path"
        assert "s3://data-lake-dev-silver/api_data/" in result[0]

    def test_load_non_local_environment_iceberg_fallback(self, mock_job, spark):
        """Test load method with Iceberg failure and Parquet fallback"""
        mock_job.environment = "dev"

        # Create test DataFrame
        data = [{"id": "test_001", "name": "Test Product"}]
        df = spark.createDataFrame(data)

        # Mock Iceberg failure
        df.writeTo = Mock(side_effect=Exception("Iceberg write failed"))

        # Mock successful Parquet write
        mock_writer = Mock()
        df.write = mock_writer
        mock_writer.mode.return_value.option.return_value.parquet = Mock()

        result = mock_job.load(df)

        assert len(result) == 1, "Should return one output path"
        assert "s3://data-lake-dev-silver/api_data/" in result[0]
