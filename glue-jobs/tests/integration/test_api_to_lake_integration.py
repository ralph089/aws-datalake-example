"""
Integration tests for API to Lake job.

These tests run the complete job pipeline in Docker containers
to validate end-to-end functionality.
"""

import tempfile

import pytest
from pyspark.sql import SparkSession

from config import create_local_config
from jobs.api_to_lake import APIToLakeJob


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for integration tests."""
    spark = (
        SparkSession.builder.appName("api-to-lake-integration-tests")
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


class MockAPIClient:
    """Mock API client for integration tests."""

    def fetch_data(self, endpoint: str, **kwargs):
        """Return mock API data."""
        return [
            {
                "id": 1,
                "name": "Test Product 1",
                "price": 29.99,
                "category": "electronics",
            },
            {
                "id": 2,
                "name": "Test Product 2",
                "price": 149.99,
                "category": "electronics",
            },
            {"id": 3, "name": "Test Product 3", "price": 9.99, "category": "books"},
        ]


@pytest.mark.integration
class TestAPIToLakeIntegration:
    """Integration tests for API to Lake job."""

    @pytest.mark.skipif(
        False,  # Always run in Docker
        reason="Test disabled",
    )
    def test_api_to_lake_job_complete_pipeline(self, spark, test_output_dir):
        """Test complete API to Lake job pipeline."""
        # Create config for integration test
        config = create_local_config(
            "api_to_lake_integration_test", enable_notifications=False
        )

        # Initialize job
        job = APIToLakeJob(config)
        job.spark = spark

        # Mock API client for integration tests
        job.api_client = MockAPIClient()  # type: ignore[assignment]

        # Run the job
        result = job.run()

        # Verify job completed successfully
        assert result is True

    def test_api_data_processing(self, spark):
        """Test API data processing and transformation."""
        config = create_local_config("api_processing_test", enable_notifications=False)
        job = APIToLakeJob(config)
        job.spark = spark

        # Load mock API data from test file
        test_df = job.load_test_data("api_to_lake", "products_api.json")

        # Verify data loaded
        assert test_df is not None
        assert test_df.count() > 0

        # Run transformation
        transformed_df = job.transform(test_df)

        # Verify transformations
        assert "processed_timestamp" in transformed_df.columns
        assert "price_category" in transformed_df.columns

        # Verify all products have valid categories
        category_count = transformed_df.filter(
            transformed_df.price_category.isNotNull()  # type: ignore[arg-type]
        ).count()
        assert category_count == transformed_df.count()

    def test_api_job_handles_large_datasets(self, spark):
        """Test API job can handle large datasets efficiently."""
        config = create_local_config("large_dataset_test", enable_notifications=False)
        job = APIToLakeJob(config)
        job.spark = spark

        # Create a large dataset for testing
        large_data = [
            {
                "id": i,
                "name": f"Product {i}",
                "price": float(i % 100 + 10),
                "category": "test",
            }
            for i in range(1000)
        ]

        test_df = spark.createDataFrame(large_data)

        # Run transformation
        transformed_df = job.transform(test_df)

        # Verify it handles the large dataset
        assert transformed_df.count() == 1000
        assert "price_category" in transformed_df.columns

    def test_api_job_pagination_simulation(self, spark):
        """Test API job with simulated pagination."""
        config = create_local_config("pagination_test", enable_notifications=False)
        job = APIToLakeJob(config)
        job.spark = spark

        # Mock paginated API client
        class MockPaginatedAPIClient:
            def __init__(self):
                self.call_count = 0

            def get(self, endpoint, **kwargs):
                self.call_count += 1
                if self.call_count == 1:
                    return {
                        "data": [
                            {"id": i, "name": f"Product {i}", "price": 10.0}
                            for i in range(100)
                        ]
                    }
                elif self.call_count == 2:
                    return {
                        "data": [
                            {"id": i, "name": f"Product {i}", "price": 10.0}
                            for i in range(100, 150)
                        ]
                    }
                else:
                    return {"data": []}

        job.api_client = MockPaginatedAPIClient()  # type: ignore[assignment]

        # Test the pagination handling
        result = job._fetch_from_api()

        # Should have called API multiple times
        assert job.api_client.call_count >= 2  # type: ignore[attr-defined]
