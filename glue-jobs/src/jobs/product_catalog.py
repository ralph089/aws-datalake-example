from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, coalesce, lit, current_timestamp, concat, row_number, count, avg, min, max
from pyspark.sql.window import Window
from jobs.base_job import BaseGlueJob
from transformations.common import add_processing_metadata, standardize_name
from validators.data_quality import DataQualityChecker
from typing import List, Dict

class ProductCatalogJob(BaseGlueJob):
    """Process and standardize product catalog data from multiple vendors"""
    
    def extract(self) -> DataFrame:
        """Extract product data from multiple vendor files"""
        
        if self.environment == "local":
            # For local testing, use test data
            vendor_paths = {
                "vendor_a": "glue-jobs/tests/test_data/products_vendor_a.csv",
                "vendor_b": "glue-jobs/tests/test_data/products_vendor_b.csv"
            }
        else:
            vendor_paths = {
                "vendor_a": f"s3://data-lake-{self.environment}-bronze/products/vendor_a/",
                "vendor_b": f"s3://data-lake-{self.environment}-bronze/products/vendor_b/",
                "vendor_c": f"s3://data-lake-{self.environment}-bronze/products/vendor_c/"
            }
        
        vendor_dfs = []
        
        for vendor, path in vendor_paths.items():
            try:
                df_vendor = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(path) \
                    .withColumn("source_vendor", lit(vendor))
                
                vendor_dfs.append(df_vendor)
                self.logger.info("extracted_vendor_data", 
                               vendor=vendor,
                               path=path,
                               row_count=df_vendor.count())
                               
            except Exception as e:
                self.logger.warning("vendor_data_not_available", 
                                  vendor=vendor, 
                                  path=path, 
                                  error=str(e))
        
        if not vendor_dfs:
            raise ValueError("No vendor data available")
        
        # Union all vendor data
        df_combined = vendor_dfs[0]
        for df in vendor_dfs[1:]:
            df_combined = df_combined.unionByName(df, allowMissingColumns=True)
        
        self.logger.info("combined_vendor_data", 
                        total_vendors=len(vendor_dfs),
                        total_rows=df_combined.count())
        
        return df_combined
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Standardize and normalize product data"""
        
        # Data standardization and cleaning
        df_standardized = df \
            .withColumn("product_name", standardize_name(col("product_name"))) \
            .withColumn("category", self._standardize_category(col("category"))) \
            .withColumn("brand", standardize_name(col("brand"))) \
            .withColumn("description", trim(col("description"))) \
            .withColumn("sku", upper(trim(col("sku")))) \
            .withColumn("upc", regexp_replace(col("upc"), r"[^\d]", "")) \
            .filter(col("product_name").isNotNull()) \
            .filter(col("price") > 0)
        
        # Generate standardized product IDs
        df_with_ids = df_standardized \
            .withColumn("standardized_product_id", 
                       concat(
                           col("source_vendor"),
                           lit("-"),
                           regexp_replace(upper(col("sku")), r"[^\w]", "")
                       ))
        
        # Create category mappings
        df_categorized = df_with_ids \
            .withColumn("category_level_1", self._extract_category_level_1(col("category"))) \
            .withColumn("category_level_2", self._extract_category_level_2(col("category"))) \
            .withColumn("category_level_3", col("category"))
        
        # Add business logic fields
        df_enriched = df_categorized \
            .withColumn("is_premium", 
                       when(col("price") > 100, True)
                       .otherwise(False)) \
            .withColumn("margin_category",
                       when(col("cost").isNotNull() & (col("price") > col("cost")),
                            when(((col("price") - col("cost")) / col("price")) > 0.5, "high")
                            .when(((col("price") - col("cost")) / col("price")) > 0.3, "medium")
                            .otherwise("low"))
                       .otherwise("unknown")) \
            .withColumn("availability_status",
                       when(col("in_stock") == True, "available")
                       .when(col("discontinued") == True, "discontinued")
                       .otherwise("out_of_stock"))
        
        # Deduplicate based on standardized product ID
        df_deduped = df_enriched \
            .withColumn("row_number", 
                       row_number().over(
                           Window.partitionBy("standardized_product_id")
                           .orderBy(col("last_updated").desc_nulls_last(), 
                                  col("source_vendor").asc())
                       )) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Add processing metadata
        df_final = add_processing_metadata(df_deduped, self.job_name, self.job_run_id)
        
        # Add quality scores
        df_final = df_final \
            .withColumn("data_quality_score", self._calculate_quality_score(df_final))
        
        self.logger.info("product_data_transformed", 
                        input_rows=df.count(),
                        output_rows=df_final.count(),
                        duplicates_removed=df.count() - df_deduped.count())
        
        return df_final
    
    def _standardize_category(self, category_col):
        """Standardize category names"""
        return when(category_col.isNotNull(),
                   trim(regexp_replace(
                       regexp_replace(category_col.lower(), r"[^\w\s]", " "),
                       r"\s+", " "
                   ))
               ).otherwise(None)
    
    def _extract_category_level_1(self, category_col):
        """Extract top-level category"""
        category_mapping = {
            "electronics": when(category_col.rlike(r"(?i)(electronic|computer|phone|tablet|tv)"), "Electronics"),
            "clothing": when(category_col.rlike(r"(?i)(clothing|apparel|shirt|pants|dress|shoes)"), "Clothing"),
            "home": when(category_col.rlike(r"(?i)(home|kitchen|furniture|decor)"), "Home & Garden"),
            "sports": when(category_col.rlike(r"(?i)(sport|fitness|outdoor|athletic)"), "Sports & Outdoors"),
            "books": when(category_col.rlike(r"(?i)(book|literature|education)"), "Books & Media")
        }
        
        result = lit("Other")
        for category, condition in category_mapping.items():
            result = condition.otherwise(result)
        
        return result
    
    def _extract_category_level_2(self, category_col):
        """Extract second-level category"""
        # Simplified subcategory extraction
        return when(category_col.rlike(r"(?i)men"), "Men's") \
               .when(category_col.rlike(r"(?i)women"), "Women's") \
               .when(category_col.rlike(r"(?i)children|kids"), "Children's") \
               .when(category_col.rlike(r"(?i)smartphone|phone"), "Mobile Phones") \
               .when(category_col.rlike(r"(?i)laptop|computer"), "Computers") \
               .otherwise(None)
    
    def _calculate_quality_score(self, df: DataFrame):
        """Calculate data quality score for each product"""
        # Simple quality scoring based on completeness
        score = lit(0)
        
        # Add points for each non-null field
        quality_fields = ["product_name", "category", "brand", "description", "price", "sku"]
        for field in quality_fields:
            if field in df.columns:
                score = score + when(col(field).isNotNull(), 1).otherwise(0)
        
        # Normalize to 0-100 scale
        return (score * 100 / len(quality_fields)).cast("int")
    
    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for product catalog data"""
        
        # Check for duplicate SKUs within same vendor
        duplicate_skus = df.groupBy("source_vendor", "sku").count() \
                          .filter(col("count") > 1).count()
        if duplicate_skus > 0:
            self.logger.warning("duplicate_skus_found", count=duplicate_skus)
            # Don't fail for duplicates, just log warning
        
        # Check for invalid prices
        invalid_prices = df.filter((col("price") <= 0) | col("price").isNull()).count()
        if invalid_prices > 0:
            self.logger.warning("invalid_prices_found", count=invalid_prices)
            return False
        
        # Use data quality checker
        quality_checker = DataQualityChecker()
        
        # Check completeness of critical fields
        completeness = quality_checker.check_completeness(
            df, 
            ["standardized_product_id", "product_name", "category_level_1", "price"]
        )
        if not completeness["passed"]:
            self.logger.warning("completeness_check_failed", results=completeness)
            return False
        
        # Check price ranges
        range_checks = {
            "price": {"min": 0.01, "max": 100000},  # $0.01 to $100k
            "data_quality_score": {"min": 0, "max": 100}
        }
        ranges = quality_checker.check_ranges(df, range_checks)
        if not ranges["passed"]:
            self.logger.warning("range_check_failed", results=ranges)
            return False
        
        # Check valid availability status
        valid_statuses = ["available", "out_of_stock", "discontinued"]
        referential = quality_checker.check_referential_integrity(df, {
            "availability_status": {
                "column": "availability_status",
                "valid_values": valid_statuses
            }
        })
        if not referential["passed"]:
            self.logger.warning("invalid_availability_status", results=referential)
            return False
        
        return True
    
    def load(self, df: DataFrame) -> List[str]:
        """Write standardized product catalog to silver layer"""
        
        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/products_standardized/"
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)
            
            self.logger.info("loaded_to_local", 
                           path=output_path,
                           row_count=df.count())
            return [output_path]
        
        # Write to Iceberg table in silver layer
        try:
            # Main product catalog table
            catalog_table = "glue_catalog.silver.product_catalog"
            df.writeTo(catalog_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("category_level_1", "source_vendor") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            # Create a dimension table for categories
            df_categories = df.select(
                "category_level_1", "category_level_2", "category_level_3"
            ).distinct()
            
            categories_table = "glue_catalog.silver.product_categories"
            df_categories.writeTo(categories_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            # Create a summary table for analytics
            df_summary = df.groupBy("category_level_1", "source_vendor", "availability_status") \
                .agg(
                    count("standardized_product_id").alias("product_count"),
                    avg("price").alias("avg_price"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    avg("data_quality_score").alias("avg_quality_score")
                )
            
            summary_table = "glue_catalog.gold.product_catalog_summary"
            df_summary.writeTo(summary_table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy("category_level_1") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            output_paths = [
                f"s3://data-lake-{self.environment}-silver/product_catalog/",
                f"s3://data-lake-{self.environment}-silver/product_categories/",
                f"s3://data-lake-{self.environment}-gold/product_catalog_summary/"
            ]
            
            self.logger.info("loaded_to_data_lake", 
                           tables=[catalog_table, categories_table, summary_table],
                           paths=output_paths,
                           catalog_count=df.count(),
                           categories_count=df_categories.count(),
                           summary_count=df_summary.count())
            
            return output_paths
            
        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            catalog_path = f"s3://data-lake-{self.environment}-silver/product_catalog/"
            categories_path = f"s3://data-lake-{self.environment}-silver/product_categories/"
            summary_path = f"s3://data-lake-{self.environment}-gold/product_catalog_summary/"
            
            df.write.mode("overwrite").partitionBy("category_level_1", "source_vendor").parquet(catalog_path)
            
            df_categories = df.select("category_level_1", "category_level_2", "category_level_3").distinct()
            df_categories.write.mode("overwrite").parquet(categories_path)
            
            df_summary = df.groupBy("category_level_1", "source_vendor", "availability_status") \
                .agg(
                    count("standardized_product_id").alias("product_count"),
                    avg("price").alias("avg_price"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price"),
                    avg("data_quality_score").alias("avg_quality_score")
                )
            df_summary.write.mode("overwrite").partitionBy("category_level_1").parquet(summary_path)
            
            output_paths = [catalog_path, categories_path, summary_path]
            self.logger.info("loaded_to_parquet", paths=output_paths)
            
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
            'JOB_NAME': 'product_catalog',
            'env': 'local'
        }
    
    job = ProductCatalogJob(args['JOB_NAME'], args)
    job.run()