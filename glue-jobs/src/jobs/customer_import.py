from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, split, when

from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata, clean_email, standardize_name, standardize_phone


class CustomerImportJob(BaseGlueJob):
    """
    Import and standardize customer data from CSV files.
    
    This job demonstrates a typical data cleaning and standardization pipeline:
    - Extracts customer data from bronze layer CSV files
    - Applies data quality transformations (email cleaning, phone standardization)
    - Enriches with external API data when available
    - Creates derived fields (full_name) for downstream analytics
    - Validates required fields and data quality rules
    - Outputs clean, standardized customer records to silver layer
    
    The job showcases common data engineering patterns for customer master data:
    - Input validation and cleaning using reusable transformation functions
    - Conditional API enrichment based on environment
    - Defensive programming with null handling and validation
    - Comprehensive audit trail for data lineage
    
    Args:
        test_data_path: Path to test CSV file for local development (default: tests/test_data/customers.csv)
    
    Local Development:
        Uses test CSV file with sample customer records. Modify tests/test_data/customers.csv
        to test different data scenarios including invalid emails, malformed phone numbers,
        and missing required fields.
    
    Input Schema:
        CSV with columns: customer_id, first_name, last_name, email, phone, address, city, state
    
    Output:
        Silver layer Iceberg table with standardized customer data and processing metadata
    """

    def extract(self) -> DataFrame:
        """Extract customer data from bronze layer"""
        # Load customer data using new streamlined approach
        fallback_path = self.args.get("test_data_path", "tests/test_data/customer_import/customers.csv")
        df = self.load_data("customers", fallback_path)

        self.logger.info("extracted_customer_data", row_count=df.count(), columns=df.columns)

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and enrich customer data"""

        # Basic cleaning and standardization
        df_cleaned = (
            df.withColumn("email", clean_email(col("email")))
            .withColumn("phone", standardize_phone(col("phone")))
            .withColumn("first_name", standardize_name(col("first_name")))
            .withColumn("last_name", standardize_name(col("last_name")))
            .filter(col("email").isNotNull())
        )  # Remove records without valid email

        # Enrich with API data if available
        # PERFORMANCE HINT: This step makes multiple API calls (one per unique customer)
        # For large datasets, consider batching API calls or using async processing
        if self.api_client and self.environment != "local":
            df_enriched = self._enrich_with_api_data(df_cleaned)
        else:
            df_enriched = df_cleaned

        # Add processing metadata
        df_final = add_processing_metadata(df_enriched, self.job_name, self.job_run_id)

        # Add derived fields
        df_final = df_final.withColumn(
            "full_name",
            when(
                col("first_name").isNotNull() & col("last_name").isNotNull(),
                concat(col("first_name"), lit(" "), col("last_name")),
            ).otherwise(col("first_name")),
        ).withColumn(
            "email_domain", when(col("email").isNotNull(), split(col("email"), "@").getItem(1)).otherwise(None)
        )

        self.logger.info("customer_data_transformed", input_rows=df.count(), output_rows=df_final.count())

        return df_final

    def _enrich_with_api_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich customer data with external API calls
        
        PERFORMANCE WARNING: This method makes multiple API calls in a loop!
        - One API call per unique customer email (up to 10 for demo)
        - In production, consider batching APIs or async processing
        - Monitor API rate limits and implement retry logic
        - Consider caching frequently accessed customer profiles
        """
        # Example: Enrich with customer profile data from external API
        # This is a simplified example - in practice you'd batch API calls

        try:
            # Collect email addresses for API enrichment (limit for demo)
            emails = [row["email"] for row in df.select("email").distinct().limit(10).collect()]

            enrichment_data = {}
            # MULTIPLE API CALLS: Each iteration makes one API call
            # TODO: Replace with batch API call if available: POST /customers/profiles with email list
            for email in emails:
                try:
                    if self.api_client is None:
                        continue
                    response = self.api_client.get("/customers/profile", params={"email": email})
                    if response and "profile" in response:
                        enrichment_data[email] = response["profile"]
                except Exception as e:
                    self.logger.warning("api_enrichment_failed", email=email, error=str(e))

            # Add enrichment data to DataFrame (simplified approach)
            if enrichment_data:
                self.logger.info("enrichment_completed", enriched_count=len(enrichment_data))
                # In practice, you'd use broadcast variables or joins for better performance

            return df  # Return original for now

        except Exception as e:
            self.logger.error("enrichment_error", error=str(e))
            return df

    def validate(self, df: DataFrame, validation_rules: dict | None = None) -> bool:
        """Override base validation to use customer-specific validation rules"""
        validation_rules = {
            "required_columns": ["customer_id", "email", "first_name", "last_name"],
            "unique_columns": ["customer_id", "email"],
            "string_patterns": {"email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
        }
        return super().validate(df, validation_rules=validation_rules)

    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for customer data using simple validation rules"""

        # Use simple data quality validation
        validation_rules = {
            "required_columns": ["customer_id", "email", "first_name", "last_name"],
            "unique_columns": ["customer_id", "email"],
            "string_patterns": {"email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
        }

        results = self.quality_checker.validate_dataframe(df, validation_rules)
        if results["overall_status"] != "PASSED":
            self.logger.warning("simple_validation_failed", results=results)
            return False

        # Additional business logic validation
        total_count = df.count()

        # Check for active customers (business rule)
        if "status" in df.columns:
            active_customers = df.filter(col("status") == "active").count()
            if active_customers == 0:
                self.logger.warning("no_active_customers_found", total_count=total_count, active_count=active_customers)
                # Warning only, don't fail validation

        # Check email domains for suspicious patterns (business rule)
        if "email_domain" in df.columns:
            suspicious_domains = ["tempmail.com", "10minutemail.com", "guerrillamail.com"]
            suspicious_count = df.filter(col("email_domain").isin(suspicious_domains)).count()
            if suspicious_count > 0:
                self.logger.warning("suspicious_email_domains_found", count=suspicious_count)
                # Warning only, don't fail validation

        self.logger.info("customer_validation_completed", total_records=total_count, validation_passed=True)

        return True

    def load(self, df: DataFrame) -> list[str]:
        """Write to silver layer as Iceberg table"""

        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/customers/"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            self.logger.info("loaded_to_local", path=output_path, row_count=df.count())
            return [output_path]

        # Write to Iceberg table in silver layer
        table_name = "glue_catalog.silver.customers"

        try:
            # Create or replace Iceberg table
            df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").option(
                "write.parquet.compression-codec", "snappy"
            ).createOrReplace()

            output_path = f"s3://data-lake-{self.environment}-silver/customers/"

            self.logger.info("loaded_to_silver_layer", table=table_name, path=output_path, row_count=df.count())

            return [output_path]

        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            output_path = f"s3://data-lake-{self.environment}-silver/customers/"

            df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

            self.logger.info("loaded_to_silver_parquet", path=output_path, row_count=df.count())

            return [output_path]


# Entry point for Glue
if __name__ == "__main__":
    import sys

    from awsglue.utils import getResolvedOptions

    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
    else:
        args = {"JOB_NAME": "customer_import", "env": "local"}

    # Add explicit print for visibility
    print(f"🚀 Starting CustomerImportJob with args: {args}")
    
    job = CustomerImportJob(args["JOB_NAME"], args)
    print("📊 Job instance created, starting execution...")
    job.run()
    print("✅ CustomerImportJob completed successfully!")
