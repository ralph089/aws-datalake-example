"""
Shared test configuration and fixtures.
"""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from config import create_local_config


@pytest.fixture
def spark():
    """Create a fresh Spark session for each test function.
    
    This ensures complete isolation between tests by creating
    a new session for each test.
    """
    import os
    
    # Check if we're running in Docker container (integration tests)
    is_docker = os.environ.get("PYTEST_INTEGRATION") == "1"
    
    if is_docker:
        # Docker configuration for AWS Glue container
        # Use a unique app name to avoid conflicts
        import time
        app_name = f"glue-integration-test-{int(time.time() * 1000)}"
        
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[2]")  # Use fewer cores to reduce resource conflicts
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "2")  # Reduce partitions for faster tests
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.hive.convertMetastoreParquet", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "localhost")
            # Disable Hive support to avoid AWS credentials issues
            .config("spark.sql.catalogImplementation", "in-memory")
            .getOrCreate()
        )
    else:
        # Local configuration for unit tests
        import time
        app_name = f"test-{int(time.time() * 1000)}"
        
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .getOrCreate()
        )

    yield spark
    
    # Clean shutdown
    try:
        spark.stop()
    except Exception:
        # Ignore shutdown errors
        pass


@pytest.fixture
def sample_config():
    """Create sample job configuration for testing."""
    return create_local_config("test_job", log_level="INFO", enable_notifications=False)


@pytest.fixture
def mock_notification_service():
    """Mock notification service."""
    with patch("utils.notifications.NotificationService") as mock:
        mock_instance = Mock()
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_secrets():
    """Mock secrets manager."""
    with patch("utils.secrets.get_secret") as mock:
        mock.return_value = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "api_base_url": "https://api.example.com",
        }
        yield mock


@pytest.fixture
def sample_customers_data():
    """Sample customer data for testing."""
    return [
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
        {
            "customer_id": "3",
            "first_name": "Bob",
            "last_name": "Johnson",
            "email": "invalid_email",
        },
    ]


@pytest.fixture
def sample_products_data():
    """Sample product API data for testing."""
    return [
        {"id": 1, "name": "Product A", "category": "Electronics", "price": 99.99},
        {"id": 2, "name": "Product B", "category": "Books", "price": 19.99},
        {"id": 3, "name": "Product C", "category": "Clothing", "price": 49.99},
    ]


# Removed job_with_spark factory - no longer needed with proper session isolation
