from pyspark.sql.functions import col, when, trim, lower, regexp_replace, coalesce, lit
from pyspark.sql import DataFrame
import re

def clean_email(email_col):
    """Clean and validate email addresses"""
    return when(
        email_col.rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        lower(trim(email_col))
    ).otherwise(None)

def standardize_phone(phone_col):
    """Standardize phone number format"""
    # Remove all non-digit characters
    cleaned = regexp_replace(phone_col, r"[^\d]", "")
    
    # Format as (XXX) XXX-XXXX for 10-digit numbers
    return when(
        cleaned.rlike(r"^\d{10}$"),
        concat(
            lit("("),
            cleaned.substr(1, 3),
            lit(") "),
            cleaned.substr(4, 3),
            lit("-"),
            cleaned.substr(7, 4)
        )
    ).otherwise(phone_col)

def standardize_name(name_col):
    """Standardize name formatting"""
    return when(
        name_col.isNotNull(),
        trim(
            regexp_replace(
                regexp_replace(name_col, r"\s+", " "),  # Multiple spaces to single space
                r"[^\w\s'-]", ""  # Remove special characters except apostrophes and hyphens
            )
        )
    ).otherwise(None)

def calculate_age_from_birthdate(birthdate_col, reference_date=None):
    """Calculate age from birth date"""
    from pyspark.sql.functions import current_date, datediff
    
    ref_date = reference_date if reference_date else current_date()
    return (datediff(ref_date, birthdate_col) / 365.25).cast("int")

def categorize_amount(amount_col, small_threshold=100, large_threshold=1000):
    """Categorize amounts into small, medium, large"""
    return when(amount_col < small_threshold, "small") \
           .when(amount_col < large_threshold, "medium") \
           .otherwise("large")

def add_processing_metadata(df: DataFrame, job_name: str, job_run_id: str) -> DataFrame:
    """Add standard processing metadata to DataFrame"""
    from pyspark.sql.functions import current_timestamp, lit
    
    return df.withColumn("processed_timestamp", current_timestamp()) \
             .withColumn("processed_by_job", lit(job_name)) \
             .withColumn("job_run_id", lit(job_run_id))