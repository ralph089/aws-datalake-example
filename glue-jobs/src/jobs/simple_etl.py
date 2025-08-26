"""
Simple ETL job example: CSV to Iceberg table transformation.

Demonstrates basic ETL patterns with data cleaning and validation.
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, when

from config import create_config_from_glue_args, create_local_config
from jobs.base_job import BaseGlueJob
from transformations import add_processing_metadata, clean_email, standardize_name


class SimpleETLJob(BaseGlueJob):
    """
    Example ETL job that processes customer CSV data.

    Pipeline:
    1. Extract: Load customer CSV data
    2. Transform: Clean email addresses and standardize names
    3. Validate: Check for required fields and data quality
    4. Load: Write to Iceberg table
    """

    def extract(self) -> DataFrame | None:
        """Extract customer data from CSV files."""
        if self.config.env == "local":
            # Load test data for local development
            return self.load_test_data("simple_etl", "customers.csv")
        else:
            # Load from S3 in AWS environments
            data_path = self._get_data_path(
                "s3://my-data-bucket/bronze/customers", "*.csv"
            )
            return self.load_data(data_path, "csv")

    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and transform customer data."""
        self.logger.info("Starting data transformations")

        # Clean and standardize data
        transformed_df = (
            df.withColumn("email", clean_email(col("email")))
            .withColumn("first_name", standardize_name(col("first_name")))
            .withColumn("last_name", standardize_name(col("last_name")))
            .withColumn(
                "full_name",
                when(
                    col("first_name").isNotNull() & col("last_name").isNotNull(),
                    concat(col("first_name"), lit(" "), col("last_name")),
                ).otherwise(col("first_name")),
            )
        )

        # Add processing metadata
        transformed_df = add_processing_metadata(
            transformed_df, self.job_name, self.job_run_id
        )

        self.logger.info(f"Transformed {transformed_df.count()} customer records")
        return transformed_df

    def validate(self, df: DataFrame) -> bool:
        """Validate transformed customer data."""
        if not super().validate(df):
            return False

        # Check data quality metrics
        total_count = df.count()
        valid_emails = df.filter(col("email").isNotNull()).count()
        valid_names = df.filter(col("first_name").isNotNull()).count()

        email_validity_rate = valid_emails / total_count if total_count > 0 else 0
        name_validity_rate = valid_names / total_count if total_count > 0 else 0

        self.logger.info("Data quality metrics:")
        self.logger.info(f"  Total records: {total_count}")
        self.logger.info(f"  Valid emails: {email_validity_rate:.1%}")
        self.logger.info(f"  Valid names: {name_validity_rate:.1%}")

        # Validation rules
        if email_validity_rate < 0.7:
            self.logger.error(f"Email validity rate too low: {email_validity_rate:.1%}")
            return False

        if name_validity_rate < 0.9:
            self.logger.error(f"Name validity rate too low: {name_validity_rate:.1%}")
            return False

        return True

    def load(self, df: DataFrame) -> None:
        """Load customer data to Iceberg table."""
        table_name = f"{self.config.env}_customers_silver"

        # In local environment, just show data
        if self.config.env == "local":
            self.logger.info("Local environment - showing sample data:")
            df.show(10)
            self.logger.info(f"Would write {df.count()} rows to table: {table_name}")
        else:
            # Write to Iceberg table in AWS
            self.write_to_iceberg(df, table_name)

    def _get_required_columns(self) -> list[str]:
        """Define required columns for customer data."""
        return ["customer_id", "first_name", "email"]


def main():
    """Main entry point for the ETL job."""
    if len(sys.argv) < 2:
        # Local development
        config = create_local_config("simple_etl")
    else:
        # AWS Glue environment
        config = create_config_from_glue_args(sys.argv)

    job = SimpleETLJob(config)
    success = job.run()

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
