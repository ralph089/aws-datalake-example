import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from validators.data_quality import DataQualityChecker

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test_validators") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

def test_check_completeness(spark):
    """Test completeness validation"""
    data = [
        ("CUST001", "John", "john@example.com"),
        ("CUST002", "Jane", None),  # Missing email
        ("CUST003", None, "bob@example.com"),  # Missing name
        (None, "Alice", "alice@example.com")  # Missing customer_id
    ]
    df = spark.createDataFrame(data, ["customer_id", "name", "email"])
    
    checker = DataQualityChecker()
    results = checker.check_completeness(df, ["customer_id", "name", "email"])
    
    assert not results["passed"]  # Should fail due to nulls
    assert results["checks"]["customer_id"]["null_count"] == 1
    assert results["checks"]["name"]["null_count"] == 1
    assert results["checks"]["email"]["null_count"] == 1

def test_check_uniqueness(spark):
    """Test uniqueness validation"""
    data = [
        ("CUST001", "john@example.com"),
        ("CUST002", "jane@example.com"),
        ("CUST003", "john@example.com"),  # Duplicate email
        ("CUST001", "different@example.com")  # Duplicate customer_id
    ]
    df = spark.createDataFrame(data, ["customer_id", "email"])
    
    checker = DataQualityChecker()
    results = checker.check_uniqueness(df, ["customer_id", "email"])
    
    assert not results["passed"]  # Should fail due to duplicates
    assert results["checks"]["customer_id"]["duplicate_count"] == 1
    assert results["checks"]["email"]["duplicate_count"] == 1

def test_check_ranges(spark):
    """Test range validation"""
    data = [
        (25, 100.0),
        (30, 150.0),
        (-5, 50.0),    # Invalid age
        (35, -10.0)    # Invalid price
    ]
    df = spark.createDataFrame(data, ["age", "price"])
    
    checker = DataQualityChecker()
    range_checks = {
        "age": {"min": 0, "max": 120},
        "price": {"min": 0, "max": 10000}
    }
    results = checker.check_ranges(df, range_checks)
    
    assert not results["passed"]  # Should fail due to out-of-range values
    assert results["checks"]["age"]["out_of_range_count"] == 1  # Negative age
    assert results["checks"]["price"]["out_of_range_count"] == 1  # Negative price

def test_check_patterns(spark):
    """Test pattern validation"""
    data = [
        ("john@example.com",),
        ("jane.smith@company.org",),
        ("invalid-email",),
        ("another@invalid",)
    ]
    df = spark.createDataFrame(data, ["email"])
    
    checker = DataQualityChecker()
    pattern_checks = {
        "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    }
    results = checker.check_patterns(df, pattern_checks)
    
    assert not results["passed"]  # Should fail due to invalid emails
    assert results["checks"]["email"]["invalid_pattern_count"] == 2

def test_check_referential_integrity(spark):
    """Test referential integrity validation"""
    data = [
        ("active",),
        ("inactive",),
        ("suspended",),
        ("unknown",),    # Invalid status
        ("invalid",)     # Invalid status
    ]
    df = spark.createDataFrame(data, ["status"])
    
    checker = DataQualityChecker()
    reference_checks = {
        "status_check": {
            "column": "status",
            "valid_values": ["active", "inactive", "suspended"]
        }
    }
    results = checker.check_referential_integrity(df, reference_checks)
    
    assert not results["passed"]  # Should fail due to invalid statuses
    assert results["checks"]["status_check"]["invalid_count"] == 2

def test_all_checks_pass(spark):
    """Test scenario where all checks pass"""
    data = [
        ("CUST001", "John", "john@example.com", 25, 100.0, "active"),
        ("CUST002", "Jane", "jane@example.com", 30, 150.0, "inactive"),
        ("CUST003", "Bob", "bob@example.com", 35, 200.0, "suspended")
    ]
    df = spark.createDataFrame(data, ["customer_id", "name", "email", "age", "price", "status"])
    
    checker = DataQualityChecker()
    
    # Completeness check
    completeness = checker.check_completeness(df, ["customer_id", "name", "email"])
    assert completeness["passed"]
    
    # Uniqueness check
    uniqueness = checker.check_uniqueness(df, ["customer_id", "email"])
    assert uniqueness["passed"]
    
    # Range check
    range_checks = {
        "age": {"min": 0, "max": 120},
        "price": {"min": 0, "max": 10000}
    }
    ranges = checker.check_ranges(df, range_checks)
    assert ranges["passed"]
    
    # Pattern check
    pattern_checks = {
        "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    }
    patterns = checker.check_patterns(df, pattern_checks)
    assert patterns["passed"]
    
    # Referential integrity check
    reference_checks = {
        "status_check": {
            "column": "status",
            "valid_values": ["active", "inactive", "suspended"]
        }
    }
    referential = checker.check_referential_integrity(df, reference_checks)
    assert referential["passed"]