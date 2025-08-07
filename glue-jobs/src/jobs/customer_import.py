from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lower, current_timestamp, lit, concat, split
from jobs.base_job import BaseGlueJob
from transformations.common import clean_email, standardize_phone, standardize_name, add_processing_metadata
from validators.data_quality import DataQualityChecker
from typing import List

class CustomerImportJob(BaseGlueJob):
    """Import and process customer data from CSV"""
    
    def extract(self) -> DataFrame:
        """Extract customer data from bronze layer"""
        input_path = f"s3://data-lake-{self.environment}-bronze/customers/"
        
        if self.environment == "local":
            # For local testing, use test data
            input_path = "glue-jobs/tests/test_data/customers.csv"
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)
        
        self.logger.info("extracted_customer_data", 
                        path=input_path, 
                        row_count=df.count(),
                        columns=df.columns)
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and enrich customer data"""
        
        # Basic cleaning and standardization
        df_cleaned = df \
            .withColumn("email", clean_email(col("email"))) \
            .withColumn("phone", standardize_phone(col("phone"))) \
            .withColumn("first_name", standardize_name(col("first_name"))) \
            .withColumn("last_name", standardize_name(col("last_name"))) \
            .filter(col("email").isNotNull())  # Remove records without valid email
        
        # Enrich with API data if available
        if self.api_client and self.environment != "local":
            df_enriched = self._enrich_with_api_data(df_cleaned)
        else:
            df_enriched = df_cleaned
        
        # Add processing metadata
        df_final = add_processing_metadata(df_enriched, self.job_name, self.job_run_id)
        
        # Add derived fields
        df_final = df_final \
            .withColumn("full_name", 
                       when(col("first_name").isNotNull() & col("last_name").isNotNull(),
                            concat(col("first_name"), lit(" "), col("last_name")))
                       .otherwise(col("first_name"))) \
            .withColumn("email_domain", 
                       when(col("email").isNotNull(),
                            split(col("email"), "@").getItem(1))
                       .otherwise(None))
        
        self.logger.info("customer_data_transformed", 
                        input_rows=df.count(),
                        output_rows=df_final.count())
        
        return df_final
    
    def _enrich_with_api_data(self, df: DataFrame) -> DataFrame:
        """Enrich customer data with external API calls"""
        # Example: Enrich with customer profile data from external API
        # This is a simplified example - in practice you'd batch API calls
        
        try:
            # Collect email addresses for API enrichment (limit for demo)
            emails = [row['email'] for row in df.select('email').distinct().limit(10).collect()]
            
            enrichment_data = {}
            for email in emails:
                try:
                    response = self.api_client.get(f"/customers/profile", params={"email": email})
                    if response and 'profile' in response:
                        enrichment_data[email] = response['profile']
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
    
    def custom_validation(self, df: DataFrame) -> bool:
        """Custom validation for customer data"""
        
        # Check email format
        invalid_emails = df.filter(~col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$")).count()
        if invalid_emails > 0:
            self.logger.warning("invalid_emails_found", count=invalid_emails)
            return False
        
        # Check for duplicates
        total_count = df.count()
        unique_count = df.select("email").distinct().count()
        if unique_count != total_count:
            self.logger.warning("duplicate_emails_found", 
                              total=total_count, 
                              unique=unique_count,
                              duplicates=total_count - unique_count)
            return False
        
        # Use data quality checker for additional validations
        quality_checker = DataQualityChecker()
        
        # Check completeness
        completeness = quality_checker.check_completeness(df, ["customer_id", "email", "first_name"])
        if not completeness["passed"]:
            self.logger.warning("completeness_check_failed", results=completeness)
            return False
        
        # Check patterns
        pattern_checks = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        }
        patterns = quality_checker.check_patterns(df, pattern_checks)
        if not patterns["passed"]:
            self.logger.warning("pattern_check_failed", results=patterns)
            return False
        
        return True
    
    def load(self, df: DataFrame) -> List[str]:
        """Write to silver layer as Iceberg table"""
        
        if self.environment == "local":
            # For local testing, write to local directory
            output_path = "dist/local_output/customers/"
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)
            
            self.logger.info("loaded_to_local", 
                           path=output_path,
                           row_count=df.count())
            return [output_path]
        
        # Write to Iceberg table in silver layer
        table_name = "glue_catalog.silver.customers"
        
        try:
            # Create or replace Iceberg table
            df.writeTo(table_name) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .option("write.parquet.compression-codec", "snappy") \
                .createOrReplace()
            
            output_path = f"s3://data-lake-{self.environment}-silver/customers/"
            
            self.logger.info("loaded_to_silver_layer", 
                           table=table_name,
                           path=output_path,
                           row_count=df.count())
            
            return [output_path]
            
        except Exception as e:
            self.logger.error("iceberg_write_failed", error=str(e))
            # Fallback to Parquet
            output_path = f"s3://data-lake-{self.environment}-silver/customers/"
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(output_path)
            
            self.logger.info("loaded_to_silver_parquet", 
                           path=output_path,
                           row_count=df.count())
            
            return [output_path]


# Entry point for Glue
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions
    
    # Handle both Glue and local execution
    if "--JOB_NAME" in sys.argv:
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
    else:
        args = {
            'JOB_NAME': 'customer_import',
            'env': 'local'
        }
    
    job = CustomerImportJob(args['JOB_NAME'], args)
    job.run()