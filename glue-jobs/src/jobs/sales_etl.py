from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, date_trunc, dayofmonth, lit, month, when, year
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum

from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata, categorize_amount


class SalesETLJob(BaseGlueJob):
    """
    Process and aggregate sales transaction data with product enrichment.
    
    This job demonstrates a comprehensive sales analytics ETL pipeline:
    - Extracts sales transactions from bronze layer and joins with product data
    - Creates multiple aggregation levels: daily, product, and customer summaries
    - Applies business logic for sales categorization and profit calculations
    - Generates time-series aggregations for trending and analytics
    - Implements the medallion architecture pattern with proper data lineage
    
    The job showcases advanced Spark SQL patterns including:
    - Complex joins between transaction and reference data
    - Window functions for time-based aggregations
    - Multiple output targets from single source (fan-out pattern)
    - Conditional logic for handling missing reference data
    - Performance optimizations with proper partitioning
    
    Key Business Logic:
    - Categorizes sales amounts (small/medium/large) for segmentation
    - Calculates profit margins when product cost data is available
    - Aggregates by multiple dimensions: time, product, customer
    - Tracks data quality metrics and processing statistics
    
    Args:
        aggregation_date: Specific date to process (default: current date)
        include_profit_analysis: Include profit calculations if product costs available
    
    Local Development:
        Uses test data from tests/test_data/sales.csv and tests/test_data/products.csv
        to simulate real transaction processing scenarios.
    
    Input Schema:
        - Sales: transaction_id, customer_id, product_id, quantity, amount, sale_date
        - Products: product_id, product_name, category, cost (for profit analysis)
    
    Output:
        Multiple silver layer tables: sales_daily_agg, sales_product_agg, sales_customer_agg
    """

    def extract(self) -> DataFrame:
        """Extract sales data from bronze layer"""

        if self.environment == "local":
            # For local testing, use test data
            sales_path = "tests/test_data/sales.csv"
            products_path = "tests/test_data/products.csv"
        else:
            sales_path = f"s3://data-lake-{self.environment}-bronze/sales/"
            products_path = f"s3://data-lake-{self.environment}-silver/products/"

        # Read sales data
        df_sales = self.spark.read.option("header", "true").option("inferSchema", "true").csv(sales_path)

        self.logger.info("extracted_sales_data", path=sales_path, row_count=df_sales.count())

        # Read product reference data
        try:
            df_products = self.spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)

            self.logger.info("extracted_products_data", path=products_path, row_count=df_products.count())
        except Exception as e:
            self.logger.warning("products_data_not_available", error=str(e))
            df_products = None

        # Join sales with products if available
        if df_products is not None:
            df_enriched = df_sales.join(
                df_products.select("product_id", "product_name", "category", "cost"), on="product_id", how="left"
            )
        else:
            df_enriched = df_sales

        return df_enriched

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform and aggregate sales data"""

        # Data cleaning and enrichment
        df_cleaned = (
            df.filter(col("quantity") > 0)
            .filter(col("unit_price") > 0)
            .filter(col("total_amount") > 0)
            .withColumn(
                "profit_margin",
                when(
                    col("cost").isNotNull() & (col("cost") > 0), (col("unit_price") - col("cost")) / col("unit_price")
                ).otherwise(None),
            )
            .withColumn("revenue", col("quantity") * col("unit_price"))
            .withColumn("transaction_size", categorize_amount(col("total_amount")))
        )

        # Add date dimensions
        df_with_dates = (
            df_cleaned.withColumn("transaction_year", year(col("transaction_date")))
            .withColumn("transaction_month", month(col("transaction_date")))
            .withColumn("transaction_day", dayofmonth(col("transaction_date")))
            .withColumn("transaction_date_trunc", date_trunc("day", col("transaction_date")))
        )

        # Create daily aggregations
        df_daily_agg = df_with_dates.groupBy(
            "transaction_date_trunc", "transaction_year", "transaction_month", "transaction_day"
        ).agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("daily_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            avg("total_amount").alias("avg_transaction_amount"),
            spark_max("total_amount").alias("max_transaction_amount"),
            spark_min("total_amount").alias("min_transaction_amount"),
            count("customer_id").alias("unique_customers"),
        )

        # Create product-level aggregations
        df_product_agg = df_with_dates.groupBy("product_id", "product_name", "category", "transaction_date_trunc").agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("product_revenue"),
            spark_sum("quantity").alias("quantity_sold"),
            avg("unit_price").alias("avg_unit_price"),
        )

        # Create customer-level aggregations
        df_customer_agg = df_with_dates.groupBy("customer_id", "transaction_date_trunc").agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("customer_revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_max("total_amount").alias("max_order_value"),
        )

        # Add processing metadata to all aggregations
        df_daily_final = add_processing_metadata(df_daily_agg, self.job_name, self.job_run_id).withColumn(
            "aggregation_type", lit("daily")
        )

        df_product_final = add_processing_metadata(df_product_agg, self.job_name, self.job_run_id).withColumn(
            "aggregation_type", lit("product_daily")
        )

        df_customer_final = add_processing_metadata(df_customer_agg, self.job_name, self.job_run_id).withColumn(
            "aggregation_type", lit("customer_daily")
        )

        # Single table pattern: combine all aggregations for ACID compliance

        # Union all aggregation types into one table
        df_comprehensive = (
            df_daily_final.select(
                lit("daily").alias("aggregation_type"),
                lit(None).cast("string").alias("product_id"),
                lit(None).cast("string").alias("product_name"),
                lit(None).cast("string").alias("category"),
                lit(None).cast("string").alias("customer_id"),
                col("transaction_date_trunc"),
                col("transaction_year"),
                col("transaction_month"),
                col("transaction_day"),
                col("transaction_count"),
                col("daily_revenue").alias("revenue_amount"),
                col("total_quantity").alias("quantity"),
                col("avg_transaction_amount").alias("avg_amount"),
                col("max_transaction_amount").alias("max_amount"),
                col("min_transaction_amount").alias("min_amount"),
                col("unique_customers").cast("long").alias("customer_count"),
                col("processed_timestamp"),
                col("job_run_id")
            )
            .union(
                df_product_final.select(
                    lit("product_daily").alias("aggregation_type"),
                    col("product_id"),
                    col("product_name"),
                    col("category"),
                    lit(None).cast("string").alias("customer_id"),
                    col("transaction_date_trunc"),
                    lit(None).cast("int").alias("transaction_year"),
                    lit(None).cast("int").alias("transaction_month"),
                    lit(None).cast("int").alias("transaction_day"),
                    col("transaction_count"),
                    col("product_revenue").alias("revenue_amount"),
                    col("quantity_sold").alias("quantity"),
                    col("avg_unit_price").alias("avg_amount"),
                    lit(None).cast("double").alias("max_amount"),
                    lit(None).cast("double").alias("min_amount"),
                    lit(None).cast("long").alias("customer_count"),
                    col("processed_timestamp"),
                    col("job_run_id")
                )
            )
            .union(
                df_customer_final.select(
                    lit("customer_daily").alias("aggregation_type"),
                    lit(None).cast("string").alias("product_id"),
                    lit(None).cast("string").alias("product_name"),
                    lit(None).cast("string").alias("category"),
                    col("customer_id"),
                    col("transaction_date_trunc"),
                    lit(None).cast("int").alias("transaction_year"),
                    lit(None).cast("int").alias("transaction_month"),
                    lit(None).cast("int").alias("transaction_day"),
                    col("transaction_count"),
                    col("customer_revenue").alias("revenue_amount"),
                    lit(None).cast("long").alias("quantity"),
                    col("avg_order_value").alias("avg_amount"),
                    col("max_order_value").alias("max_amount"),
                    lit(None).cast("double").alias("min_amount"),
                    lit(None).cast("long").alias("customer_count"),
                    col("processed_timestamp"),
                    col("job_run_id")
                )
            )
        )

        self.logger.info(
            "sales_data_transformed_to_single_table",
            input_rows=df.count(),
            comprehensive_output_rows=df_comprehensive.count(),
            daily_rows=df_daily_final.count(),
            product_rows=df_product_final.count(),
            customer_rows=df_customer_final.count()
        )

        # Return the comprehensive table for validation and loading
        return df_comprehensive

    def validate(self, df: DataFrame, validation_rules: dict | None = None) -> bool:
        """Override base validation to use sales-specific validation rules for comprehensive table"""
        validation_rules = {
            "required_columns": ["aggregation_type", "transaction_date_trunc", "revenue_amount", "transaction_count"],
            "numeric_ranges": {
                "revenue_amount": {"min": 0},
                "transaction_count": {"min": 0},
                "quantity": {"min": 0},
            },
        }
        return super().validate(df, validation_rules=validation_rules)

    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for comprehensive sales table using simple validation rules"""

        # Use simple data quality validation for the comprehensive table
        validation_rules = {
            "required_columns": ["aggregation_type", "transaction_date_trunc", "revenue_amount", "transaction_count"],
            "numeric_ranges": {
                "revenue_amount": {"min": 0, "max": 1000000},
                "transaction_count": {"min": 1, "max": 10000},
                "quantity": {"min": 0, "max": 100000},
            },
        }

        results = self.quality_checker.validate_dataframe(df, validation_rules)
        if results["overall_status"] != "PASSED":
            self.logger.warning("simple_validation_failed", results=results)
            return False

        # Business logic validation for comprehensive sales table
        total_records = df.count()

        # Validate each aggregation type separately
        agg_type_counts = df.groupBy("aggregation_type").count().collect()
        agg_counts = {row["aggregation_type"]: row["count"] for row in agg_type_counts}

        self.logger.info("comprehensive_table_validation", aggregation_type_counts=agg_counts)

        # Check for negative values in aggregations (should not happen after filtering)
        negative_revenue = df.filter(col("revenue_amount") < 0).count()
        if negative_revenue > 0:
            self.logger.warning("negative_revenue_found", count=negative_revenue)
            return False

        # Validate daily aggregations
        daily_records = df.filter(col("aggregation_type") == "daily")
        if daily_records.count() > 0:
            # Check for unreasonably high average amounts (business rule)
            if "avg_amount" in df.columns:
                high_avg_transactions = daily_records.filter(col("avg_amount") > 10000).count()
                if high_avg_transactions > 0:
                    self.logger.warning("high_average_transactions_detected", count=high_avg_transactions, threshold=10000)

            # Check for days with very low activity (business rule)
            low_activity_days = daily_records.filter(col("transaction_count") < 5).count()
            if low_activity_days > daily_records.count() * 0.5:
                self.logger.warning(
                    "many_low_activity_days",
                    low_activity_count=low_activity_days,
                    total_days=daily_records.count(),
                    threshold_percentage=50,
                )

            # Validate aggregation consistency for daily records (business rule)
            inconsistent_agg = daily_records.filter(
                (col("max_amount").isNotNull() & col("min_amount").isNotNull() & (col("max_amount") < col("min_amount")))
                | (col("avg_amount").isNotNull() & col("max_amount").isNotNull() & (col("avg_amount") > col("max_amount")))
                | (col("avg_amount").isNotNull() & col("min_amount").isNotNull() & (col("avg_amount") < col("min_amount")))
            ).count()

            if inconsistent_agg > 0:
                self.logger.error("inconsistent_aggregation_found", count=inconsistent_agg)
                return False

        self.logger.info("comprehensive_sales_validation_completed", total_records=total_records, validation_passed=True)

        return True

    def load(self, df: DataFrame) -> list[str]:
        """Write comprehensive sales data to single gold layer table (transactional)"""

        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/sales_comprehensive/"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            self.logger.info("loaded_to_local", path=output_path, row_count=df.count())
            return [output_path]

        # Write to single comprehensive Iceberg table in gold layer (ACID compliant)
        try:
            table_name = "glue_catalog.gold.sales_comprehensive"

            # Single atomic write operation ensures full transaction consistency
            df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").partitionedBy(
                col("aggregation_type"), col("transaction_date_trunc")
            ).option("write.parquet.compression-codec", "snappy").createOrReplace()

            output_path = f"s3://data-lake-{self.environment}-gold/sales_comprehensive/"

            self.logger.info(
                "loaded_to_gold_layer_transactional",
                table=table_name,
                path=output_path,
                row_count=df.count(),
                note="Single table write ensures ACID compliance"
            )

            return [output_path]

        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet (still single table)
            output_path = f"s3://data-lake-{self.environment}-gold/sales_comprehensive/"

            df.write.mode("overwrite").partitionBy("aggregation_type", "transaction_date_trunc").parquet(output_path)

            self.logger.info("loaded_to_gold_parquet", path=output_path, row_count=df.count())
            return [output_path]


# Entry point for Glue
if __name__ == "__main__":
    import sys

    from awsglue.utils import getResolvedOptions

    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
    else:
        args = {"JOB_NAME": "sales_etl", "env": "local"}

    job = SalesETLJob(args["JOB_NAME"], args)
    job.run()
