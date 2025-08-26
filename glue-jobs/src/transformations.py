"""
Common data transformations for AWS Glue jobs.
"""

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    concat,
    current_timestamp,
    lit,
    lower,
    regexp_replace,
    trim,
    when,
)


def clean_email(email_col: Column) -> Column:
    """Clean and validate email addresses."""
    trimmed_email = trim(email_col)
    return when(
        trimmed_email.rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        lower(trimmed_email),
    ).otherwise(None)


def standardize_phone(phone_col: Column) -> Column:
    """Standardize phone number format to (XXX) XXX-XXXX."""
    cleaned = regexp_replace(phone_col, r"[^\d]", "")

    return when(
        cleaned.rlike(r"^\d{10}$"),
        concat(
            lit("("),
            cleaned.substr(1, 3),
            lit(") "),
            cleaned.substr(4, 3),
            lit("-"),
            cleaned.substr(7, 4),
        ),
    ).otherwise(phone_col)


def standardize_name(name_col: Column) -> Column:
    """Standardize name formatting."""
    cleaned = trim(regexp_replace(regexp_replace(name_col, r"\s+", " "), r"[^\w\s'-]", ""))
    return when(
        name_col.isNotNull() & (cleaned != ""),
        cleaned,
    ).otherwise(None)


def categorize_amount(amount_col: Column, small_threshold: int = 100, large_threshold: int = 1000) -> Column:
    """Categorize amounts into small, medium, large."""
    return (
        when(amount_col < small_threshold, "small")
        .when(amount_col < large_threshold, "medium")
        .otherwise("large")
    )


def add_processing_metadata(df: DataFrame, job_name: str, job_run_id: str) -> DataFrame:
    """Add standard processing metadata to DataFrame."""
    return (
        df.withColumn("processed_timestamp", current_timestamp())
        .withColumn("processed_by_job", lit(job_name))
        .withColumn("job_run_id", lit(job_run_id))
    )
