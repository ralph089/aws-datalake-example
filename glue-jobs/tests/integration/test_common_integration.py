"""
Common integration tests for environment setup and shared functionality.

These tests validate that the Docker environment is set up correctly
and that basic functionality works across all jobs.
"""

import pytest
from pyspark.sql import SparkSession

from glue_etl_lib.config import create_local_config
from glue_etl_lib.transformations import clean_email, standardize_name
from glue_etl_lib.utils.logging import setup_logging
from glue_etl_lib.utils.notifications import NotificationService


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for integration tests."""
    spark = (
        SparkSession.builder.appName("common-integration-tests")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.mark.integration
class TestEnvironmentIntegration:
    """Integration tests for environment setup and basic functionality."""

    def test_pyspark_available(self, spark):
        """Test that PySpark is working in the integration environment."""
        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])

        # Verify it works
        assert df.count() == 3
        assert "name" in df.columns
        assert "age" in df.columns

    def test_spark_sql_operations(self, spark):
        """Test basic Spark SQL operations."""
        # Create test data
        data = [("product1", 100), ("product2", 200), ("product3", 150)]
        df = spark.createDataFrame(data, ["product", "price"])

        # Register as temp view
        df.createOrReplaceTempView("products")

        # Run SQL query
        result = spark.sql("SELECT product, price FROM products WHERE price > 120")
        result_data = result.collect()

        # Verify results
        assert len(result_data) == 2
        assert result_data[0]["product"] in ["product2", "product3"]

    def test_data_transformations(self, spark):
        """Test data transformation capabilities."""
        from pyspark.sql.functions import col, when

        # Create test data
        data = [("Alice", 25), ("Bob", 17), ("Charlie", 35), ("Diana", 16)]
        df = spark.createDataFrame(data, ["name", "age"])

        # Add age category
        df_with_category = df.withColumn(
            "category",
            when(col("age") >= 18, "adult").otherwise("minor"),  # type: ignore[operator]
        )

        # Verify transformation
        adults = df_with_category.filter(col("category") == "adult").count()
        minors = df_with_category.filter(col("category") == "minor").count()

        assert adults == 2
        assert minors == 2

    def test_imports_working(self):
        """Test that all essential imports work."""
        # Test core imports

        # Test that they can be instantiated/called
        config = create_local_config("test")
        logger = setup_logging("test", "INFO")
        notification_service = NotificationService("local")

        assert config is not None
        assert logger is not None
        assert notification_service is not None

    @pytest.mark.slow
    def test_large_dataframe_operations(self, spark):
        """Test operations on larger DataFrames."""
        # Create a larger dataset
        large_data = [(f"user_{i}", i % 100) for i in range(10000)]
        df = spark.createDataFrame(large_data, ["user", "score"])

        # Perform aggregation
        result = df.groupBy("score").count().collect()

        # Should have 100 different scores (0-99)
        assert len(result) == 100

    def test_transformation_functions_integration(self, spark):
        """Test that our custom transformation functions work in Spark."""
        # Test data with various edge cases
        data = [
            ("JOHN@EXAMPLE.COM", "  john  smith  "),
            ("invalid_email", "jane@#$%doe"),
            ("alice@test.org", "Bob O'Connor"),
        ]

        df = spark.createDataFrame(data, ["email", "name"])

        # Apply transformations
        from pyspark.sql.functions import col

        result = df.select(
            clean_email(col("email")).alias("clean_email"),
            standardize_name(col("name")).alias("clean_name"),
        ).collect()

        # Verify transformations
        assert result[0].clean_email == "john@example.com"
        assert result[0].clean_name == "john smith"
        assert result[1].clean_email is None  # Invalid email
        assert result[2].clean_name == "Bob O'Connor"
