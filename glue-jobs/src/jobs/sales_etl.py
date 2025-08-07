from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum as spark_sum, count, avg, max as spark_max, min as spark_min, lit
from pyspark.sql.functions import current_timestamp, date_trunc, year, month, dayofmonth
from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata, categorize_amount
from validators.data_quality import DataQualityChecker
from typing import List

class SalesETLJob(BaseGlueJob):
    """Process and aggregate sales data"""
    
    def extract(self) -> DataFrame:
        """Extract sales data from bronze layer"""
        
        if self.environment == "local":
            # For local testing, use test data
            sales_path = "glue-jobs/tests/test_data/sales.csv"
            products_path = "glue-jobs/tests/test_data/products.csv"
        else:
            sales_path = f"s3://data-lake-{self.environment}-bronze/sales/"
            products_path = f"s3://data-lake-{self.environment}-silver/products/"
        
        # Read sales data
        df_sales = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(sales_path)
        
        self.logger.info("extracted_sales_data", 
                        path=sales_path,
                        row_count=df_sales.count())
        
        # Read product reference data
        try:
            df_products = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(products_path)
            
            self.logger.info("extracted_products_data", 
                           path=products_path,
                           row_count=df_products.count())
        except Exception as e:
            self.logger.warning("products_data_not_available", error=str(e))
            df_products = None
        
        # Join sales with products if available
        if df_products is not None:
            df_enriched = df_sales.join(
                df_products.select("product_id", "product_name", "category", "cost"),
                on="product_id",
                how="left"
            )
        else:
            df_enriched = df_sales
        
        return df_enriched
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform and aggregate sales data"""
        
        # Data cleaning and enrichment
        df_cleaned = df \
            .filter(col("quantity") > 0) \
            .filter(col("unit_price") > 0) \
            .filter(col("total_amount") > 0) \
            .withColumn("profit_margin", 
                       when(col("cost").isNotNull() & (col("cost") > 0),
                            (col("unit_price") - col("cost")) / col("unit_price"))
                       .otherwise(None)) \
            .withColumn("revenue", col("quantity") * col("unit_price")) \
            .withColumn("transaction_size", categorize_amount(col("total_amount")))
        
        # Add date dimensions
        df_with_dates = df_cleaned \
            .withColumn("transaction_year", year(col("transaction_date"))) \
            .withColumn("transaction_month", month(col("transaction_date"))) \
            .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
            .withColumn("transaction_date_trunc", date_trunc("day", col("transaction_date")))
        
        # Create daily aggregations
        df_daily_agg = df_with_dates.groupBy(
            "transaction_date_trunc",
            "transaction_year", 
            "transaction_month", 
            "transaction_day"
        ).agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("daily_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            avg("total_amount").alias("avg_transaction_amount"),
            spark_max("total_amount").alias("max_transaction_amount"),
            spark_min("total_amount").alias("min_transaction_amount"),
            count("customer_id").alias("unique_customers")
        )
        
        # Create product-level aggregations
        df_product_agg = df_with_dates.groupBy(
            "product_id",
            "product_name",
            "category",
            "transaction_date_trunc"
        ).agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("product_revenue"),
            spark_sum("quantity").alias("quantity_sold"),
            avg("unit_price").alias("avg_unit_price")
        )
        
        # Create customer-level aggregations
        df_customer_agg = df_with_dates.groupBy(
            "customer_id",
            "transaction_date_trunc"
        ).agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("total_amount").alias("customer_revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_max("total_amount").alias("max_order_value")
        )
        
        # Add processing metadata to all aggregations
        df_daily_final = add_processing_metadata(df_daily_agg, self.job_name, self.job_run_id) \
            .withColumn("aggregation_type", lit("daily"))
        
        df_product_final = add_processing_metadata(df_product_agg, self.job_name, self.job_run_id) \
            .withColumn("aggregation_type", lit("product_daily"))
        
        df_customer_final = add_processing_metadata(df_customer_agg, self.job_name, self.job_run_id) \
            .withColumn("aggregation_type", lit("customer_daily"))
        
        # Store aggregations for loading
        self.daily_agg = df_daily_final
        self.product_agg = df_product_final
        self.customer_agg = df_customer_final
        
        self.logger.info("sales_data_transformed", 
                        input_rows=df.count(),
                        daily_agg_rows=df_daily_final.count(),
                        product_agg_rows=df_product_final.count(),
                        customer_agg_rows=df_customer_final.count())
        
        # Return the main daily aggregation for validation
        return df_daily_final
    
    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for sales aggregations"""
        
        # Check for negative values in aggregations
        negative_revenue = df.filter(col("daily_revenue") < 0).count()
        if negative_revenue > 0:
            self.logger.warning("negative_revenue_found", count=negative_revenue)
            return False
        
        # Check for zero transaction counts
        zero_transactions = df.filter(col("transaction_count") == 0).count()
        if zero_transactions > 0:
            self.logger.warning("zero_transaction_count_found", count=zero_transactions)
            return False
        
        # Use data quality checker
        quality_checker = DataQualityChecker()
        
        # Check completeness of key fields
        completeness = quality_checker.check_completeness(
            df, 
            ["transaction_date_trunc", "daily_revenue", "transaction_count"]
        )
        if not completeness["passed"]:
            self.logger.warning("completeness_check_failed", results=completeness)
            return False
        
        # Check ranges for revenue
        range_checks = {
            "daily_revenue": {"min": 0, "max": 1000000},  # Max $1M per day
            "transaction_count": {"min": 1, "max": 10000}  # Max 10k transactions per day
        }
        ranges = quality_checker.check_ranges(df, range_checks)
        if not ranges["passed"]:
            self.logger.warning("range_check_failed", results=ranges)
            return False
        
        return True
    
    def load(self, df: DataFrame) -> List[str]:
        """Write aggregated data to gold layer"""
        output_paths = []
        
        if self.environment == "local":
            # For local testing, write to local directories
            daily_path = "dist/local_output/sales_daily_agg/"
            product_path = "dist/local_output/sales_product_agg/"
            customer_path = "dist/local_output/sales_customer_agg/"
            
            self.daily_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(daily_path)
            self.product_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(product_path)
            self.customer_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(customer_path)
            
            output_paths = [daily_path, product_path, customer_path]
            
            self.logger.info("loaded_to_local", paths=output_paths)
            return output_paths
        
        # Write to Iceberg tables in gold layer
        try:
            # Daily aggregations
            daily_table = "glue_catalog.gold.sales_daily_summary"
            self.daily_agg.writeTo(daily_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("transaction_year", "transaction_month") \
                .createOrReplace()
            
            # Product aggregations
            product_table = "glue_catalog.gold.sales_product_summary"
            self.product_agg.writeTo(product_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("transaction_date_trunc") \
                .createOrReplace()
            
            # Customer aggregations
            customer_table = "glue_catalog.gold.sales_customer_summary"
            self.customer_agg.writeTo(customer_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("transaction_date_trunc") \
                .createOrReplace()
            
            output_paths = [
                f"s3://data-lake-{self.environment}-gold/sales_daily_summary/",
                f"s3://data-lake-{self.environment}-gold/sales_product_summary/",
                f"s3://data-lake-{self.environment}-gold/sales_customer_summary/"
            ]
            
            self.logger.info("loaded_to_gold_layer", 
                           tables=[daily_table, product_table, customer_table],
                           paths=output_paths)
            
            return output_paths
            
        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            base_path = f"s3://data-lake-{self.environment}-gold/"
            
            daily_path = f"{base_path}sales_daily_summary/"
            product_path = f"{base_path}sales_product_summary/"
            customer_path = f"{base_path}sales_customer_summary/"
            
            self.daily_agg.write.mode("overwrite").partitionBy("transaction_year", "transaction_month").parquet(daily_path)
            self.product_agg.write.mode("overwrite").partitionBy("transaction_date_trunc").parquet(product_path)
            self.customer_agg.write.mode("overwrite").partitionBy("transaction_date_trunc").parquet(customer_path)
            
            output_paths = [daily_path, product_path, customer_path]
            
            self.logger.info("loaded_to_gold_parquet", paths=output_paths)
            return output_paths


# Entry point for Glue
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions
    
    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
    else:
        args = {
            'JOB_NAME': 'sales_etl',
            'env': 'local'
        }
    
    job = SalesETLJob(args['JOB_NAME'], args)
    job.run()