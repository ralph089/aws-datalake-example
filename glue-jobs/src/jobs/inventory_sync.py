from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, current_timestamp, lit, coalesce, concat
from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata
from validators.data_quality import DataQualityChecker
from typing import List, Dict, Any
import json

class InventorySyncJob(BaseGlueJob):
    """Sync inventory data from external API"""
    
    def extract(self) -> DataFrame:
        """Extract inventory data from API and existing silver layer"""
        
        # Get current inventory data from silver layer
        if self.environment == "local":
            current_inventory_path = "glue-jobs/tests/test_data/inventory.csv"
        else:
            current_inventory_path = f"s3://data-lake-{self.environment}-silver/inventory/"
        
        try:
            df_current = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(current_inventory_path)
            
            self.logger.info("extracted_current_inventory", 
                           path=current_inventory_path,
                           row_count=df_current.count())
        except Exception as e:
            self.logger.warning("no_existing_inventory_data", error=str(e))
            # Create empty DataFrame with expected schema
            df_current = self.spark.createDataFrame([], schema="product_id string, location_id string, quantity_on_hand int, last_updated timestamp")
        
        # Get fresh inventory data from API
        df_api = self._fetch_api_inventory()
        
        # Combine current and API data for comparison
        self.current_inventory = df_current
        self.api_inventory = df_api
        
        return df_api
    
    def _fetch_api_inventory(self) -> DataFrame:
        """Fetch inventory data from external API"""
        
        if self.environment == "local" or not self.api_client:
            # For local testing, simulate API data
            api_data = [
                {"product_id": "PROD001", "location_id": "LOC001", "quantity_on_hand": 150, "reorder_point": 50},
                {"product_id": "PROD002", "location_id": "LOC001", "quantity_on_hand": 75, "reorder_point": 25},
                {"product_id": "PROD003", "location_id": "LOC001", "quantity_on_hand": 200, "reorder_point": 100},
                {"product_id": "PROD001", "location_id": "LOC002", "quantity_on_hand": 90, "reorder_point": 50},
            ]
            
            df_api = self.spark.createDataFrame(api_data)
            self.logger.info("simulated_api_inventory", row_count=df_api.count())
            return df_api
        
        # Fetch from real API
        try:
            inventory_data = []
            page = 1
            page_size = 100
            
            while True:
                response = self.api_client.get("/inventory", params={
                    "page": page,
                    "page_size": page_size,
                    "include_locations": "true"
                })
                
                if not response or "items" not in response:
                    break
                
                items = response["items"]
                if not items:
                    break
                
                # Transform API response to expected format
                for item in items:
                    for location in item.get("locations", []):
                        inventory_data.append({
                            "product_id": item["product_id"],
                            "location_id": location["location_id"],
                            "quantity_on_hand": location["quantity_on_hand"],
                            "quantity_reserved": location.get("quantity_reserved", 0),
                            "reorder_point": location.get("reorder_point", 0),
                            "max_stock_level": location.get("max_stock_level", 1000),
                            "last_count_date": location.get("last_count_date"),
                            "cost_per_unit": item.get("cost_per_unit")
                        })
                
                page += 1
                if len(items) < page_size:
                    break
            
            if inventory_data:
                df_api = self.spark.createDataFrame(inventory_data)
                self.logger.info("fetched_api_inventory", 
                               row_count=df_api.count(),
                               pages_fetched=page-1)
                return df_api
            else:
                raise ValueError("No inventory data received from API")
                
        except Exception as e:
            self.logger.error("api_inventory_fetch_failed", error=str(e))
            raise
    
    def transform(self, df_api: DataFrame) -> DataFrame:
        """Compare API data with current data and identify changes"""
        
        # Add API fetch timestamp
        df_api_timestamped = df_api.withColumn("api_fetch_timestamp", current_timestamp())
        
        # Identify changes by comparing with current inventory
        if self.current_inventory.count() > 0:
            # Join API data with current data to find differences
            df_changes = df_api_timestamped.alias("api").join(
                self.current_inventory.alias("current"),
                on=["product_id", "location_id"],
                how="full_outer"
            ).select(
                coalesce(col("api.product_id"), col("current.product_id")).alias("product_id"),
                coalesce(col("api.location_id"), col("current.location_id")).alias("location_id"),
                col("api.quantity_on_hand").alias("new_quantity"),
                col("current.quantity_on_hand").alias("old_quantity"),
                col("api.quantity_reserved").alias("quantity_reserved"),
                col("api.reorder_point").alias("reorder_point"),
                col("api.max_stock_level").alias("max_stock_level"),
                col("api.last_count_date").alias("last_count_date"),
                col("api.cost_per_unit").alias("cost_per_unit"),
                col("api.api_fetch_timestamp").alias("api_fetch_timestamp"),
                when(col("current.product_id").isNull(), lit("new"))
                .when(col("api.product_id").isNull(), lit("deleted"))
                .when(col("api.quantity_on_hand") != col("current.quantity_on_hand"), lit("updated"))
                .otherwise(lit("unchanged")).alias("change_type")
            )
            
            # Filter to only changed records
            df_changed = df_changes.filter(col("change_type") != "unchanged")
            
            # Log change statistics
            change_stats = df_changes.groupBy("change_type").count().collect()
            change_dict = {row["change_type"]: row["count"] for row in change_stats}
            self.logger.info("inventory_changes_detected", changes=change_dict)
            
        else:
            # No existing data, treat all as new
            df_changed = df_api_timestamped.withColumn("change_type", lit("new"))
            self.logger.info("no_existing_inventory", treating_all_as_new=df_changed.count())
        
        # Add processing metadata
        df_final = add_processing_metadata(df_changed, self.job_name, self.job_run_id)
        
        # Add business logic flags
        df_final = df_final \
            .withColumn("needs_reorder", 
                       when(col("new_quantity").isNotNull() & col("reorder_point").isNotNull(),
                            col("new_quantity") <= col("reorder_point"))
                       .otherwise(False)) \
            .withColumn("overstocked",
                       when(col("new_quantity").isNotNull() & col("max_stock_level").isNotNull(),
                            col("new_quantity") > col("max_stock_level"))
                       .otherwise(False))
        
        return df_final
    
    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for inventory data"""
        
        # Check for negative quantities
        negative_qty = df.filter(col("new_quantity") < 0).count()
        if negative_qty > 0:
            self.logger.warning("negative_quantities_found", count=negative_qty)
            return False
        
        # Check for missing product/location combinations
        missing_keys = df.filter(col("product_id").isNull() | col("location_id").isNull()).count()
        if missing_keys > 0:
            self.logger.warning("missing_product_location_keys", count=missing_keys)
            return False
        
        # Use data quality checker
        quality_checker = DataQualityChecker()
        
        # Check completeness
        completeness = quality_checker.check_completeness(
            df, 
            ["product_id", "location_id", "change_type"]
        )
        if not completeness["passed"]:
            self.logger.warning("completeness_check_failed", results=completeness)
            return False
        
        # Check valid change types
        valid_change_types = ["new", "updated", "deleted", "unchanged"]
        referential = quality_checker.check_referential_integrity(df, {
            "change_type": {
                "column": "change_type",
                "valid_values": valid_change_types
            }
        })
        if not referential["passed"]:
            self.logger.warning("invalid_change_types_found", results=referential)
            return False
        
        return True
    
    def load(self, df: DataFrame) -> List[str]:
        """Write updated inventory to silver layer"""
        
        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/inventory_changes/"
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)
            
            self.logger.info("loaded_to_local", path=output_path, row_count=df.count())
            return [output_path]
        
        # Write to Iceberg table in silver layer
        try:
            # Write inventory changes log
            changes_table = "glue_catalog.silver.inventory_changes"
            df.writeTo(changes_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("change_type") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            # Create current inventory snapshot by merging changes
            current_table = "glue_catalog.silver.inventory_current"
            
            # Get only the latest state for each product-location
            df_current_state = df.filter(col("change_type") != "deleted") \
                .select(
                    "product_id", "location_id", "new_quantity", "quantity_reserved",
                    "reorder_point", "max_stock_level", "last_count_date", "cost_per_unit",
                    "needs_reorder", "overstocked", "processed_timestamp", "job_run_id"
                ).withColumnRenamed("new_quantity", "quantity_on_hand")
            
            df_current_state.writeTo(current_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("needs_reorder") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            output_paths = [
                f"s3://data-lake-{self.environment}-silver/inventory_changes/",
                f"s3://data-lake-{self.environment}-silver/inventory_current/"
            ]
            
            self.logger.info("loaded_to_silver_layer", 
                           tables=[changes_table, current_table],
                           paths=output_paths,
                           changes_count=df.count(),
                           current_count=df_current_state.count())
            
            return output_paths
            
        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            changes_path = f"s3://data-lake-{self.environment}-silver/inventory_changes/"
            current_path = f"s3://data-lake-{self.environment}-silver/inventory_current/"
            
            df.write.mode("overwrite").partitionBy("change_type").parquet(changes_path)
            
            # Create current state
            df_current_state = df.filter(col("change_type") != "deleted") \
                .select(
                    "product_id", "location_id", "new_quantity", "quantity_reserved",
                    "reorder_point", "max_stock_level", "last_count_date", "cost_per_unit",
                    "needs_reorder", "overstocked", "processed_timestamp", "job_run_id"
                ).withColumnRenamed("new_quantity", "quantity_on_hand")
            
            df_current_state.write.mode("overwrite").parquet(current_path)
            
            output_paths = [changes_path, current_path]
            self.logger.info("loaded_to_silver_parquet", paths=output_paths)
            
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
            'JOB_NAME': 'inventory_sync',
            'env': 'local'
        }
    
    job = InventorySyncJob(args['JOB_NAME'], args)
    job.run()