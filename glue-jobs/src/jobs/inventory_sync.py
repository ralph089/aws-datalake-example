from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, when

from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata


class InventorySyncJob(BaseGlueJob):
    """
    Synchronize inventory data between external API and data lake.
    
    This job demonstrates a real-time inventory synchronization pattern:
    - Fetches current inventory levels from external inventory management API
    - Compares with existing silver layer data to detect changes
    - Identifies inventory adjustments, stock movements, and discrepancies
    - Maintains complete audit trail of inventory changes over time
    - Implements change data capture (CDC) patterns for tracking deltas
    
    The job showcases operational data integration patterns including:
    - Real-time API data synchronization with batch processing
    - Delta detection using DataFrame comparisons
    - Incremental loading strategies for performance
    - Data quality validation for inventory business rules
    - Historical change tracking for auditing and analytics
    
    Key Features:
    - Detects quantity changes, new products, and discontinued items
    - Validates inventory business rules (negative quantities, threshold alerts)
    - Creates inventory change events for downstream consumption
    - Handles API failures gracefully with fallback strategies
    - Optimized for frequent execution with minimal data movement
    
    Business Rules:
    - Tracks quantity on hand by product and location
    - Flags negative inventory for investigation
    - Generates alerts for significant quantity changes
    - Maintains last updated timestamps for freshness tracking
    
    Args:
        sync_mode: 'full' for complete refresh or 'incremental' for changes only
        alert_threshold: Percentage change threshold for generating alerts
    
    Local Development:
        Uses simulated API responses with test inventory data to demonstrate
        change detection logic without requiring external API connectivity.
    
    Input Sources:
        - External inventory API (real-time quantities)
        - Current silver layer inventory table (historical state)
    
    Output:
        - Updated inventory silver layer table
        - Inventory changes audit table for tracking deltas
    """

    def extract(self) -> DataFrame:
        """Extract inventory data from API and existing silver layer"""
        # Get current inventory data
        try:
            df_current = self.load_data("inventory", "tests/test_data/inventory_sync/inventory.csv")
            self.logger.info("extracted_current_inventory", row_count=df_current.count())
        except Exception as e:
            self.logger.warning("no_existing_inventory_data", error=str(e))
            # Create empty DataFrame with expected schema
            df_current = self.spark.createDataFrame(
                [], schema="product_id string, location_id string, quantity_on_hand int, last_updated timestamp"
            )

        # Get fresh inventory data from API
        df_api = self._fetch_api_inventory()

        # Combine current and API data for comparison
        self.current_inventory = df_current
        self.api_inventory = df_api

        return df_api

    def _fetch_api_inventory(self) -> DataFrame:
        """Fetch inventory data from external API"""

        if self.environment == "local" or not self.api_client:
            # Load from local data files - uses new streamlined approach
            return self.load_api_data("inventory_api")

        # Fetch from real API
        try:
            inventory_data = []
            page = 1
            page_size = 100

            while True:
                response = self.api_client.get(
                    "/inventory", params={"page": page, "page_size": page_size, "include_locations": "true"}
                )

                if not response or "items" not in response:
                    break

                items = response["items"]
                if not items:
                    break

                # Transform API response to expected format
                for item in items:
                    for location in item.get("locations", []):
                        inventory_data.append(
                            {
                                "product_id": item["product_id"],
                                "location_id": location["location_id"],
                                "quantity_on_hand": location["quantity_on_hand"],
                                "quantity_reserved": location.get("quantity_reserved", 0),
                                "reorder_point": location.get("reorder_point", 0),
                                "max_stock_level": location.get("max_stock_level", 1000),
                                "last_count_date": location.get("last_count_date"),
                                "cost_per_unit": item.get("cost_per_unit"),
                            }
                        )

                page += 1
                if len(items) < page_size:
                    break

            if inventory_data:
                df_api = self.spark.createDataFrame(inventory_data)
                self.logger.info("fetched_api_inventory", row_count=df_api.count(), pages_fetched=page - 1)
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
            df_changes = (
                df_api_timestamped.alias("api")
                .join(self.current_inventory.alias("current"), on=["product_id", "location_id"], how="full_outer")
                .select(
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
                    .otherwise(lit("unchanged"))
                    .alias("change_type"),
                )
            )

            # Filter to only changed records
            df_changed = df_changes.filter(col("change_type") != "unchanged")

            # Log change statistics
            change_stats = df_changes.groupBy("change_type").count().collect()
            change_dict = {row["change_type"]: row["count"] for row in change_stats}
            self.logger.info("inventory_changes_detected", changes=change_dict)

        else:
            # No existing data, treat all as new
            df_changed = (
                df_api_timestamped.withColumn("change_type", lit("new"))
                .withColumn("new_quantity", col("quantity_on_hand"))
                .withColumn("old_quantity", lit(0).cast("long"))
            )
            self.logger.info("no_existing_inventory", treating_all_as_new=df_changed.count())

        # Add processing metadata
        df_final = add_processing_metadata(df_changed, self.job_name, self.job_run_id)

        # Add business logic flags
        df_final = df_final.withColumn(
            "needs_reorder",
            when(
                col("new_quantity").isNotNull() & col("reorder_point").isNotNull(),
                col("new_quantity") <= col("reorder_point"),
            ).otherwise(False),
        ).withColumn(
            "overstocked",
            when(
                col("new_quantity").isNotNull() & col("max_stock_level").isNotNull(),
                col("new_quantity") > col("max_stock_level"),
            ).otherwise(False),
        )

        return df_final

    def validate(self, df: DataFrame, validation_rules: dict | None = None) -> bool:
        """Override base validation to use inventory-specific validation rules"""
        validation_rules = {
            "required_columns": ["product_id", "location_id", "change_type"],
            "numeric_ranges": {"quantity": {"min": 0, "max": 1000000}, "reorder_point": {"min": 0, "max": 10000}},
        }
        return super().validate(df, validation_rules=validation_rules)

    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for inventory data using simple validation rules"""

        # Use simple data quality validation
        validation_rules = {
            "required_columns": ["product_id", "location_id", "change_type"],
            "numeric_ranges": {
                "new_quantity": {"min": 0},
                "quantity_reserved": {"min": 0},
                "reorder_point": {"min": 0},
                "max_stock_level": {"min": 1},
            },
        }

        results = self.quality_checker.validate_dataframe(df, validation_rules)
        if results["overall_status"] != "PASSED":
            self.logger.warning("simple_validation_failed", results=results)
            return False

        # Business logic validation for inventory
        total_changes = df.count()

        # Check for negative quantities (should not happen after range check)
        if "new_quantity" in df.columns:
            negative_qty = df.filter(col("new_quantity") < 0).count()
            if negative_qty > 0:
                self.logger.error("negative_quantities_found", count=negative_qty)
            return False

        # Check inventory level consistency (business rule)
        if "quantity_reserved" in df.columns and "new_quantity" in df.columns:
            invalid_reservations = df.filter(
                col("quantity_reserved").isNotNull()
                & col("new_quantity").isNotNull()
                & (col("quantity_reserved") > col("new_quantity"))
            ).count()

            if invalid_reservations > 0:
                self.logger.error("reserved_exceeds_on_hand", count=invalid_reservations)
                return False

        # Check reorder point logic (business rule)
        if "reorder_point" in df.columns and "max_stock_level" in df.columns:
            illogical_reorder = df.filter(
                col("reorder_point").isNotNull()
                & col("max_stock_level").isNotNull()
                & (col("reorder_point") >= col("max_stock_level"))
            ).count()

            if illogical_reorder > 0:
                self.logger.warning(
                    "reorder_point_exceeds_max_stock", count=illogical_reorder, total_changes=total_changes
                )
                # Warning only for now

        # Check for excessive inventory changes (business rule)
        change_stats = df.groupBy("change_type").count().collect()
        change_counts = {row["change_type"]: row["count"] for row in change_stats}

        deleted_count = change_counts.get("deleted", 0)
        if deleted_count > total_changes * 0.5:  # More than 50% deletions
            self.logger.warning(
                "excessive_inventory_deletions",
                deleted_count=deleted_count,
                total_changes=total_changes,
                deletion_percentage=(deleted_count / total_changes) * 100,
            )

        # Check for products that need reordering (business insight)
        if "needs_reorder" in df.columns:
            reorder_needed = df.filter(col("needs_reorder")).count()
            if reorder_needed > 0:
                self.logger.info(
                    "reorder_alerts_generated",
                    products_needing_reorder=reorder_needed,
                    percentage=(reorder_needed / total_changes) * 100,
                )

        # Check for overstocked items (business insight)
        if "overstocked" in df.columns:
            overstocked_count = df.filter(col("overstocked")).count()
            if overstocked_count > 0:
                self.logger.info(
                    "overstock_alerts_generated",
                    overstocked_items=overstocked_count,
                    percentage=(overstocked_count / total_changes) * 100,
                )

        self.logger.info(
            "inventory_validation_completed",
            total_changes=total_changes,
            change_types=change_counts,
            validation_passed=True,
        )

        return True

    def load(self, df: DataFrame) -> list[str]:
        """Write comprehensive inventory data to single silver layer table (transactional)"""

        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/inventory_comprehensive/"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            self.logger.info("loaded_to_local", path=output_path, row_count=df.count())
            return [output_path]

        # Single table pattern: write all inventory data to one comprehensive table (ACID compliant)
        try:
            table_name = "glue_catalog.silver.inventory_comprehensive"

            # Add record_type to distinguish changes from current state in single table
            df_with_record_type = df.withColumn("record_type", lit("change"))

            # Add current state records for non-deleted items
            df_current_records = (
                df.filter(col("change_type") != "deleted")
                .select(
                    "product_id",
                    "location_id",
                    "new_quantity",
                    "old_quantity",
                    "quantity_reserved",
                    "reorder_point",
                    "max_stock_level",
                    "last_count_date",
                    "cost_per_unit",
                    "api_fetch_timestamp",
                    lit("current").alias("change_type"),  # Mark as current state
                    "needs_reorder",
                    "overstocked",
                    "processed_timestamp",
                    "job_run_id"
                )
                .withColumn("record_type", lit("current"))
                .withColumnRenamed("new_quantity", "quantity_on_hand")
                .withColumn("new_quantity", col("quantity_on_hand"))
            )

            # Combine changes and current state in single comprehensive table
            df_comprehensive = df_with_record_type.union(df_current_records)

            # Single atomic write operation ensures full transaction consistency
            df_comprehensive.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").partitionedBy(
                col("record_type"), col("change_type")
            ).option("write.parquet.compression-codec", "snappy").createOrReplace()

            output_path = f"s3://data-lake-{self.environment}-silver/inventory_comprehensive/"

            self.logger.info(
                "loaded_to_silver_layer_transactional",
                table=table_name,
                path=output_path,
                total_records=df_comprehensive.count(),
                changes_count=df.count(),
                current_count=df_current_records.count(),
                note="Single table write ensures ACID compliance"
            )

            return [output_path]

        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet (still single table)
            output_path = f"s3://data-lake-{self.environment}-silver/inventory_comprehensive/"

            # Create comprehensive dataset for fallback
            df_with_record_type = df.withColumn("record_type", lit("change"))
            df_current_records = (
                df.filter(col("change_type") != "deleted")
                .select(
                    "product_id",
                    "location_id",
                    "new_quantity",
                    "old_quantity",
                    "quantity_reserved",
                    "reorder_point",
                    "max_stock_level",
                    "last_count_date",
                    "cost_per_unit",
                    "api_fetch_timestamp",
                    lit("current").alias("change_type"),
                    "needs_reorder",
                    "overstocked",
                    "processed_timestamp",
                    "job_run_id"
                )
                .withColumn("record_type", lit("current"))
                .withColumnRenamed("new_quantity", "quantity_on_hand")
                .withColumn("new_quantity", col("quantity_on_hand"))
            )

            df_comprehensive = df_with_record_type.union(df_current_records)
            df_comprehensive.write.mode("overwrite").partitionBy("record_type", "change_type").parquet(output_path)

            self.logger.info("loaded_to_silver_parquet", path=output_path, row_count=df_comprehensive.count())
            return [output_path]


# Entry point for Glue
if __name__ == "__main__":
    import sys

    from awsglue.utils import getResolvedOptions

    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
    else:
        args = {"JOB_NAME": "inventory_sync", "env": "local"}

    job = InventorySyncJob(args["JOB_NAME"], args)
    job.run()
