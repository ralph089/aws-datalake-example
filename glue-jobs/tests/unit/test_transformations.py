import pytest
from unittest.mock import Mock
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from transformations.common import (
    clean_email, standardize_phone, standardize_name, 
    categorize_amount, add_processing_metadata
)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

def test_clean_email(spark):
    """Test email cleaning function"""
    data = [
        ("john.doe@example.com",),
        ("JANE.SMITH@EXAMPLE.COM",),
        ("invalid-email",),
        ("",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["email"])
    
    result_df = df.withColumn("clean_email", clean_email(col("email")))
    results = result_df.collect()
    
    assert results[0]["clean_email"] == "john.doe@example.com"
    assert results[1]["clean_email"] == "jane.smith@example.com"
    assert results[2]["clean_email"] is None  # Invalid email
    assert results[3]["clean_email"] is None  # Empty string
    assert results[4]["clean_email"] is None  # Null

def test_standardize_phone(spark):
    """Test phone number standardization"""
    data = [
        ("5551234567",),
        ("(555) 123-4567",),
        ("555.123.4567",),
        ("1234",),  # Too short
        (None,)
    ]
    df = spark.createDataFrame(data, ["phone"])
    
    result_df = df.withColumn("standard_phone", standardize_phone(col("phone")))
    results = result_df.collect()
    
    assert results[0]["standard_phone"] == "(555) 123-4567"
    assert results[1]["standard_phone"] == "(555) 123-4567"
    assert results[2]["standard_phone"] == "(555) 123-4567"
    assert results[3]["standard_phone"] == "1234"  # Too short, unchanged
    assert results[4]["standard_phone"] is None

def test_standardize_name(spark):
    """Test name standardization"""
    data = [
        ("John Doe",),
        ("  jane   smith  ",),
        ("Bob-Johnson",),
        ("Mary O'Connor",),
        ("",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["name"])
    
    result_df = df.withColumn("standard_name", standardize_name(col("name")))
    results = result_df.collect()
    
    assert results[0]["standard_name"] == "John Doe"
    assert results[1]["standard_name"] == "jane smith"
    assert results[2]["standard_name"] == "Bob-Johnson"
    assert results[3]["standard_name"] == "Mary O'Connor"
    assert results[4]["standard_name"] is None
    assert results[5]["standard_name"] is None

def test_categorize_amount(spark):
    """Test amount categorization"""
    data = [
        (25.0,),
        (150.0,),
        (1500.0,),
        (0.0,)
    ]
    df = spark.createDataFrame(data, ["amount"])
    
    result_df = df.withColumn("category", categorize_amount(col("amount")))
    results = result_df.collect()
    
    assert results[0]["category"] == "small"   # < 100
    assert results[1]["category"] == "medium"  # 100 <= x < 1000
    assert results[2]["category"] == "large"   # >= 1000
    assert results[3]["category"] == "small"   # 0

def test_add_processing_metadata(spark):
    """Test adding processing metadata"""
    data = [
        ("CUST001", "John Doe"),
        ("CUST002", "Jane Smith")
    ]
    df = spark.createDataFrame(data, ["customer_id", "name"])
    
    result_df = add_processing_metadata(df, "test_job", "run_123")
    results = result_df.collect()
    
    # Check that metadata columns were added
    assert "processed_timestamp" in result_df.columns
    assert "processed_by_job" in result_df.columns
    assert "job_run_id" in result_df.columns
    
    # Check values
    assert results[0]["processed_by_job"] == "test_job"
    assert results[0]["job_run_id"] == "run_123"
    assert results[0]["processed_timestamp"] is not None