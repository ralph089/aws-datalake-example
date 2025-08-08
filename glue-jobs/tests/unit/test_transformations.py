import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from chispa import assert_df_equality, assert_column_equality

from transformations.common import (
    add_processing_metadata,
    categorize_amount,
    clean_email,
    standardize_name,
    standardize_phone,
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return (
        SparkSession.builder.appName("test")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def test_clean_email(spark):
    """Test email cleaning function using chispa assertions"""
    input_data = [("john.doe@example.com",), ("JANE.SMITH@EXAMPLE.COM",), ("invalid-email",), ("",), (None,)]
    input_df = spark.createDataFrame(input_data, ["email"])

    result_df = input_df.withColumn("clean_email", clean_email(col("email")))
    
    # Create expected results
    expected_data = [
        ("john.doe@example.com", "john.doe@example.com"),
        ("JANE.SMITH@EXAMPLE.COM", "jane.smith@example.com"), 
        ("invalid-email", None),  # Invalid email
        ("", None),  # Empty string
        (None, None)  # Null
    ]
    expected_df = spark.createDataFrame(expected_data, ["email", "clean_email"])
    
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_standardize_phone(spark):
    """Test phone number standardization using chispa assertions"""
    input_data = [("5551234567",), ("(555) 123-4567",), ("555.123.4567",), ("1234",), (None,)]
    input_df = spark.createDataFrame(input_data, ["phone"])

    result_df = input_df.withColumn("standard_phone", standardize_phone(col("phone")))
    
    # Create expected results
    expected_data = [
        ("5551234567", "(555) 123-4567"),
        ("(555) 123-4567", "(555) 123-4567"),
        ("555.123.4567", "(555) 123-4567"), 
        ("1234", "1234"),  # Too short, unchanged
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["phone", "standard_phone"])
    
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_standardize_name(spark):
    """Test name standardization using chispa assertions"""
    input_data = [("John Doe",), ("  jane   smith  ",), ("Bob-Johnson",), ("Mary O'Connor",), ("",), (None,)]
    input_df = spark.createDataFrame(input_data, ["name"])

    result_df = input_df.withColumn("standard_name", standardize_name(col("name")))
    
    # Create expected results
    expected_data = [
        ("John Doe", "John Doe"),
        ("  jane   smith  ", "jane smith"),
        ("Bob-Johnson", "Bob-Johnson"),
        ("Mary O'Connor", "Mary O'Connor"),
        ("", None),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "standard_name"])
    
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_categorize_amount(spark):
    """Test amount categorization using chispa assertions"""
    input_data = [(25.0,), (150.0,), (1500.0,), (0.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    result_df = input_df.withColumn("category", categorize_amount(col("amount")))
    
    # Create expected results
    expected_data = [
        (25.0, "small"),   # < 100
        (150.0, "medium"), # 100 <= x < 1000
        (1500.0, "large"), # >= 1000
        (0.0, "small")     # 0
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "category"])
    
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_add_processing_metadata(spark):
    """Test adding processing metadata using chispa for column verification"""
    input_data = [("CUST001", "John Doe"), ("CUST002", "Jane Smith")]
    input_df = spark.createDataFrame(input_data, ["customer_id", "name"])

    result_df = add_processing_metadata(input_df, "test_job", "run_123")

    # Check that metadata columns were added
    expected_columns = ["customer_id", "name", "processed_timestamp", "processed_by_job", "job_run_id"]
    assert set(result_df.columns) == set(expected_columns)

    # Use chispa to verify specific columns contain expected values
    job_name_df = result_df.select("processed_by_job").distinct()
    expected_job_df = spark.createDataFrame([("test_job",)], ["processed_by_job"])
    assert_df_equality(job_name_df, expected_job_df)
    
    run_id_df = result_df.select("job_run_id").distinct()
    expected_run_df = spark.createDataFrame([("run_123",)], ["job_run_id"])
    assert_df_equality(run_id_df, expected_run_df)
    
    # Verify timestamp column exists and is not null
    timestamp_count = result_df.filter(col("processed_timestamp").isNotNull()).count()
    assert timestamp_count == input_df.count()
