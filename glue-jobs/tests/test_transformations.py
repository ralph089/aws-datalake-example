"""
Tests for transformation functions.
"""

import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, StructField, StructType

from glue_etl_lib.transformations import (
    add_processing_metadata,
    categorize_amount,
    clean_email,
    standardize_name,
)


class TestTransformations:
    """Test cases for transformation functions."""

    @pytest.mark.unit
    def test_clean_email_valid_addresses(self, spark):
        """Test email cleaning with valid addresses."""
        data = [
            ("john@example.com",),
            ("JANE@DOMAIN.COM",),
            ("  bob@test.org  ",),
        ]

        df = spark.createDataFrame(data, ["email"])
        result = df.select(clean_email(col("email")).alias("clean_email")).collect()

        assert result[0].clean_email == "john@example.com"
        assert result[1].clean_email == "jane@domain.com"
        assert result[2].clean_email == "bob@test.org"

    @pytest.mark.unit
    def test_clean_email_invalid_addresses(self, spark):
        """Test email cleaning with invalid addresses."""
        data = [
            ("not_an_email",),
            ("@domain.com",),
            ("user@",),
            ("",),
        ]

        df = spark.createDataFrame(data, ["email"])
        result = df.select(clean_email(col("email")).alias("clean_email")).collect()

        # All should be None
        for row in result:
            assert row.clean_email is None

    @pytest.mark.unit
    def test_standardize_name_valid_names(self, spark):
        """Test name standardization with valid names."""
        data = [
            ("  John  Doe  ",),
            ("jane@#$%smith",),
            ("Bob   Wilson",),
            ("Mary-Jane O'Connor",),
        ]

        df = spark.createDataFrame(data, ["name"])
        result = df.select(standardize_name(col("name")).alias("clean_name")).collect()

        assert result[0].clean_name == "John Doe"
        assert result[1].clean_name == "janesmith"
        assert result[2].clean_name == "Bob Wilson"
        assert result[3].clean_name == "Mary-Jane O'Connor"

    @pytest.mark.unit
    def test_standardize_name_null_values(self, spark):
        """Test name standardization with null values."""
        data = [(None,), ("",)]

        df = spark.createDataFrame(data, ["name"])
        result = df.select(standardize_name(col("name")).alias("clean_name")).collect()

        for row in result:
            assert row.clean_name is None

    @pytest.mark.unit
    def test_categorize_amount_default_thresholds(self, spark):
        """Test amount categorization with default thresholds."""
        from decimal import Decimal

        data = [(Decimal("50.00"),), (Decimal("150.00"),), (Decimal("1500.00"),)]
        schema = StructType([StructField("amount", DecimalType(10, 2), True)])

        df = spark.createDataFrame(data, schema)
        result = df.select(categorize_amount(col("amount")).alias("category")).collect()

        assert result[0].category == "small"  # < 100
        assert result[1].category == "medium"  # 100-999
        assert result[2].category == "large"  # >= 1000

    @pytest.mark.unit
    def test_categorize_amount_custom_thresholds(self, spark):
        """Test amount categorization with custom thresholds."""
        from decimal import Decimal

        data = [(Decimal("25.00"),), (Decimal("75.00"),), (Decimal("150.00"),)]
        schema = StructType([StructField("amount", DecimalType(10, 2), True)])

        df = spark.createDataFrame(data, schema)
        result = df.select(
            categorize_amount(
                col("amount"), small_threshold=50, large_threshold=100
            ).alias("category")
        ).collect()

        assert result[0].category == "small"  # < 50
        assert result[1].category == "medium"  # 50-99
        assert result[2].category == "large"  # >= 100

    @pytest.mark.unit
    def test_add_processing_metadata(self, spark):
        """Test adding processing metadata to DataFrame."""
        data = [("John", "Doe"), ("Jane", "Smith")]
        df = spark.createDataFrame(data, ["first_name", "last_name"])

        result = add_processing_metadata(df, "test_job", "run_123")

        # Check new columns exist
        assert "processed_timestamp" in result.columns
        assert "processed_by_job" in result.columns
        assert "job_run_id" in result.columns

        # Check values
        first_row = result.collect()[0]
        assert first_row.processed_by_job == "test_job"
        assert first_row.job_run_id == "run_123"
        assert first_row.processed_timestamp is not None

        # Original data should be preserved
        assert first_row.first_name == "John"
        assert first_row.last_name == "Doe"
