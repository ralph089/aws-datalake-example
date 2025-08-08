from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata


class APIDataFetchJob(BaseGlueJob):
    """
    Fetch data from external API and load into data lake.
    
    This job demonstrates a common ETL pattern for ingesting data from REST APIs:
    - Handles pagination to fetch all available data
    - Flattens nested JSON structures into tabular format
    - Adds data quality checks and business logic enrichments
    - Includes full audit trail with processing metadata
    - Writes to Iceberg tables for ACID compliance
    
    The job is designed to be idempotent and can safely re-run using the job_run_id
    for deduplication. In local mode, it uses test data for development and testing.
    
    Args:
        api_endpoint: REST API endpoint to fetch data from (default: /api/v1/data)
        page_size: Number of records per API request (default: 100)
        max_pages: Maximum pages to fetch to prevent runaway jobs (default: 10)
        test_data_path: Path to test JSON file for local development (default: tests/test_data/api_response.json)
    
    Local Development:
        To test with different data, modify tests/test_data/api_response.json or provide
        a custom path via test_data_path argument. The test file should contain JSON
        with a "data" array of objects representing API response records.
    
    Output:
        Silver layer Iceberg table with enriched API data and processing metadata
    """

    def __init__(self, job_name: str, args: dict[str, Any]):
        super().__init__(job_name, args)
        self.api_endpoint = args.get("api_endpoint", "/api/v1/data")
        self.page_size = int(args.get("page_size", "100"))
        self.max_pages = int(args.get("max_pages", "10"))

    def extract(self) -> DataFrame:
        """Extract data from external API with pagination support"""
        if self.environment == "local":
            # Load from local data files using new streamlined approach
            try:
                return self.load_api_data("api_response")
            except Exception as e:
                self.logger.warning("local_test_data_not_found", error=str(e))
                # Create empty DataFrame with expected schema for local testing
                return self._create_empty_dataframe()

        if not self.api_client:
            raise ValueError("API client not configured. Check secrets configuration.")

        all_data = []
        page = 1

        self.logger.info("api_extraction_started", endpoint=self.api_endpoint, max_pages=self.max_pages)

        try:
            while page <= self.max_pages:
                self.logger.info("fetching_api_page", page=page, page_size=self.page_size)

                params = {"page": page, "limit": self.page_size}
                response = self.api_client.get(self.api_endpoint, params=params)

                if not response or "data" not in response:
                    self.logger.warning("empty_api_response", page=page)
                    break

                data = response["data"]
                if not data:
                    self.logger.info("no_more_data", page=page)
                    break

                all_data.extend(data)
                self.logger.info("fetched_api_page", page=page, records=len(data))

                # Check if we have more pages
                total_pages = response.get("pagination", {}).get("total_pages", page)
                if page >= total_pages:
                    self.logger.info("reached_last_page", page=page, total_pages=total_pages)
                    break

                page += 1

            if not all_data:
                self.logger.warning("no_api_data_fetched")
                return self._create_empty_dataframe()

            # Convert to Spark DataFrame
            df = self.spark.createDataFrame(all_data)
            self.logger.info("api_extraction_completed", total_records=len(all_data), pages_fetched=page - 1)

            return df

        except Exception as e:
            self.logger.error("api_extraction_failed", error=str(e), page=page)
            raise

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform API response data into structured format"""
        if df.count() == 0:
            self.logger.warning("no_data_to_transform")
            return self._create_empty_transformed_dataframe()

        self.logger.info("transformation_started", input_rows=df.count())

        try:
            # Flatten nested JSON structures if present
            df_flattened = self._flatten_json_columns(df)

            # Add standard data quality and enrichment
            df_enriched = self._enrich_api_data(df_flattened)

            # Add processing metadata
            df_final = add_processing_metadata(df_enriched, self.job_name, self.job_run_id)

            # Add extraction metadata
            df_final = df_final.withColumn("api_endpoint", lit(self.api_endpoint).cast(StringType())).withColumn(
                "extraction_timestamp", lit(datetime.now()).cast(TimestampType())
            )

            self.logger.info("transformation_completed", input_rows=df.count(), output_rows=df_final.count())

            return df_final

        except Exception as e:
            self.logger.error("transformation_failed", error=str(e))
            raise

    def _flatten_json_columns(self, df: DataFrame) -> DataFrame:
        """Flatten nested JSON columns into separate columns"""
        # This is a simplified flattening approach
        # In practice, you'd analyze the schema and flatten specific nested fields

        try:
            # Check if we have any struct columns that need flattening
            struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]

            if not struct_columns:
                return df

            df_flattened = df
            for col_name in struct_columns:
                # Flatten struct columns by creating new columns for each field
                data_type = df_flattened.schema[col_name].dataType
                if not isinstance(data_type, StructType):
                    continue
                struct_fields = data_type.fields
                for field in struct_fields:
                    new_col_name = f"{col_name}_{field.name}"
                    df_flattened = df_flattened.withColumn(new_col_name, col(f"{col_name}.{field.name}"))

                # Drop the original struct column
                df_flattened = df_flattened.drop(col_name)

            return df_flattened

        except Exception as e:
            self.logger.warning("json_flattening_failed", error=str(e))
            return df

    def _enrich_api_data(self, df: DataFrame) -> DataFrame:
        """Add data quality and business logic enrichments"""
        try:
            # Add data quality flags
            df_enriched = df

            # Example: Mark records with missing required fields
            if "id" in df.columns:
                df_enriched = df_enriched.withColumn("has_valid_id", when(col("id").isNotNull(), True).otherwise(False))

            # Example: Categorize data based on business rules
            if "amount" in df.columns:
                df_enriched = df_enriched.withColumn(
                    "amount_category",
                    when(col("amount") < 100, "small")
                    .when(col("amount") < 1000, "medium")
                    .otherwise("large"),
                )

            # Example: Add timestamp parsing if we have date strings
            for col_name in df.columns:
                if "date" in col_name.lower() or "time" in col_name.lower():
                    try:
                        # Attempt to parse timestamp strings
                        df_enriched = df_enriched.withColumn(
                            f"{col_name}_parsed", col(col_name).cast(TimestampType())
                        )
                    except Exception:
                        # Skip if parsing fails
                        pass

            return df_enriched

        except Exception as e:
            self.logger.warning("data_enrichment_failed", error=str(e))
            return df

    def validate(self, df: DataFrame, validation_rules: dict | None = None) -> bool:
        """Override base validation with API data-specific rules"""
        validation_rules = {
            "required_columns": ["id"],  # Adjust based on your API response structure
            "unique_columns": ["id"],
            "null_tolerance": {"id": 0.0},  # No nulls allowed in id column
        }
        return super().validate(df, validation_rules=validation_rules)

    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for API data"""
        try:
            total_count = df.count()

            if total_count == 0:
                self.logger.error("validation_failed", reason="No records after API extraction and transformation")
                return False

            # Validate required columns exist
            required_columns = ["id"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error("validation_failed", reason="Missing required columns", missing=missing_columns)
                return False

            # Check for duplicate IDs
            if "id" in df.columns:
                unique_ids = df.select("id").distinct().count()
                if unique_ids != total_count:
                    self.logger.warning("duplicate_ids_found", total=total_count, unique=unique_ids)
                    # Warning only, don't fail validation

            # Check data freshness (if we have timestamp fields)
            timestamp_columns = [col for col in df.columns if "timestamp" in col.lower() or "date" in col.lower()]
            if timestamp_columns:
                self.logger.info("timestamp_columns_found", columns=timestamp_columns)

            self.logger.info("api_data_validation_completed", total_records=total_count, validation_passed=True)
            return True

        except Exception as e:
            self.logger.error("validation_error", error=str(e))
            return False

    def load(self, df: DataFrame) -> list[str]:
        """Write API data to silver layer as Iceberg table"""
        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/api_data/"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            self.logger.info("loaded_to_local", path=output_path, row_count=df.count())
            return [output_path]

        # Write to Iceberg table in silver layer
        table_name = "glue_catalog.silver.api_data"

        try:
            # Create or replace Iceberg table
            df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").option(
                "write.parquet.compression-codec", "snappy"
            ).createOrReplace()

            output_path = f"s3://data-lake-{self.environment}-silver/api_data/"

            self.logger.info("loaded_to_silver_layer", table=table_name, path=output_path, row_count=df.count())

            return [output_path]

        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            output_path = f"s3://data-lake-{self.environment}-silver/api_data/"

            df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

            self.logger.info("loaded_to_silver_parquet", path=output_path, row_count=df.count())

            return [output_path]

    def _create_empty_dataframe(self) -> DataFrame:
        """Create empty DataFrame with expected schema for API responses"""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("created_at", StringType(), True),
        ])
        return self.spark.createDataFrame([], schema)

    def _create_empty_transformed_dataframe(self) -> DataFrame:
        """Create empty DataFrame with transformed schema"""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("has_valid_id", StringType(), True),
            StructField("api_endpoint", StringType(), True),
            StructField("extraction_timestamp", TimestampType(), True),
            StructField("processed_timestamp", TimestampType(), True),
            StructField("processed_by_job", StringType(), True),
            StructField("job_run_id", StringType(), True),
        ])
        return self.spark.createDataFrame([], schema)


# Entry point for Glue
if __name__ == "__main__":
    import sys

    from awsglue.utils import getResolvedOptions

    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        # Only require essential arguments, optional ones have defaults
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
        args.setdefault("api_endpoint", "/api/v1/data")
        args.setdefault("page_size", "100")
        args.setdefault("max_pages", "10")
    else:
        args = {
            "JOB_NAME": "api_data_fetch",
            "env": "local",
            "api_endpoint": "/api/v1/data",
            "page_size": "100",
            "max_pages": "10",
        }

    job = APIDataFetchJob(args["JOB_NAME"], args)
    job.run()
