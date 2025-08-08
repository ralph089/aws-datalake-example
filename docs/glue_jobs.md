# Glue Jobs ETL Architecture

This document explains the Extract, Transform, Load (ETL) architecture used in our AWS Glue jobs and how it integrates with the BaseGlueJob framework.

## What is ETL?

ETL (Extract, Transform, Load) is a data processing pattern that:

1. **Extract**: Pulls data from source systems (APIs, databases, files)
2. **Transform**: Cleans, validates, and enriches the data 
3. **Load**: Writes the processed data to target destinations (data lakes, warehouses)

Our implementation follows the **medallion architecture** pattern:
- **Bronze Layer**: Raw CSV data in S3 (as-is from source)
- **Silver Layer**: Cleaned/validated data as Iceberg tables
- **Gold Layer**: Business-ready aggregated data for analytics

## What is a Spark DataFrame?

A **Spark DataFrame** is a distributed collection of data organized into named columns, similar to a table in a database or an Excel spreadsheet. Key characteristics:

- **Distributed**: Data is split across multiple machines for parallel processing
- **Immutable**: Once created, DataFrames can't be modified (transformations create new DataFrames)
- **Lazy Evaluation**: Operations are planned but not executed until an action is triggered
- **Schema**: Each DataFrame has a defined structure with column names and data types
- **Resilient**: Automatically handles failures and retries operations

### DataFrame Example
```python
# Create a DataFrame from CSV
df = spark.read.option("header", "true").csv("customers.csv")

# Transform the data (lazy - not executed yet)
df_cleaned = df.filter(col("email").isNotNull()).withColumn("email", clean_email(col("email")))

# Action - triggers execution of all transformations
count = df_cleaned.count()  # This actually runs the operations
```

## BaseGlueJob Architecture

All our ETL jobs inherit from `BaseGlueJob`, which provides a standardized framework:

### Core ETL Lifecycle

```python
class CustomerImportJob(BaseGlueJob):
    def extract(self) -> DataFrame:
        """Pull raw data from source"""
        
    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and enrich the data"""
        
    def validate(self, df: DataFrame) -> bool:
        """Ensure data quality before loading"""
        
    def load(self, df: DataFrame) -> list[str]:
        """Write to target destination"""
```

### Built-in Infrastructure Services

BaseGlueJob automatically provides:

- **Structured Logging**: JSON-formatted logs with correlation IDs
- **Audit Trail**: Tracks data lineage and processing metrics
- **Dead Letter Queue (DLQ)**: Captures failed records for debugging
- **SNS Notifications**: Success/failure alerts
- **Data Quality Validation**: Built-in validation framework
- **API Client**: OAuth 2.0 enabled HTTP client for external APIs

### Transactional Execution Flow

The `run()` method orchestrates the ETL pipeline with transactional guarantees:

```python
def run(self):
    try:
        # 1. Extract Phase
        df = self.extract()
        
        # 2. Transform Phase  
        df_transformed = self.transform(df)
        
        # 3. Validate Phase (BEFORE any writes)
        if not self.validate(df_transformed):
            raise ValueError("Data validation failed - no data will be written")
            
        # 4. Load Phase (atomic operation)
        output_paths = self.load(df_transformed)
        
        # 5. Success handling
        self.audit_tracker.complete_job("SUCCESS")
        self.notification_service.send_success_notification(...)
        
    except Exception as e:
        # 6. Failure handling
        self.dlq_handler.send_to_dlq(...)
        self.notification_service.send_failure_notification(...)
        raise  # Ensures job fails in Glue
```

### Key Design Principles

#### 1. Fail Fast, Fail Safe
- Validate ALL data before writing ANY data
- Use Apache Iceberg's ACID transactions for consistency
- No partial writes if validation fails

#### 2. Environment-Aware Data Loading
The BaseGlueJob automatically handles different data sources:

```python
# Local development
df = self.load_data("customers")  # Loads from tests/test_data/customers.csv

# Remote environments  
df = self.load_data("customers")  # Loads from S3://data-lake-{env}-silver/customers/
```

#### 3. Smart Error Handling
- Failed records go to DLQ for manual review
- Structured logging enables easy debugging
- SNS notifications alert operations teams
- Audit trail provides complete data lineage

#### 4. Reusable Transformations
Common data transformations are centralized in `transformations/common.py`:

```python
from transformations.common import clean_email, standardize_phone, add_processing_metadata

df_cleaned = (df
    .withColumn("email", clean_email(col("email")))
    .withColumn("phone", standardize_phone(col("phone")))
)

df_final = add_processing_metadata(df_cleaned, self.job_name, self.job_run_id)
```

## Example: Customer Import Job

Here's how the concepts work together in practice:

```python
class CustomerImportJob(BaseGlueJob):
    def extract(self) -> DataFrame:
        # Environment-aware loading
        return self.load_data("customers", "tests/test_data/customers.csv")
    
    def transform(self, df: DataFrame) -> DataFrame:
        # Use reusable transformation functions
        df_cleaned = (df
            .withColumn("email", clean_email(col("email")))
            .withColumn("phone", standardize_phone(col("phone")))
            .filter(col("email").isNotNull())  # Remove invalid emails
        )
        
        # Add processing metadata for audit trail
        return add_processing_metadata(df_cleaned, self.job_name, self.job_run_id)
    
    def validate(self, df: DataFrame) -> bool:
        # Custom validation rules
        validation_rules = {
            "required_columns": ["customer_id", "email", "first_name", "last_name"],
            "unique_columns": ["customer_id", "email"],
            "string_patterns": {"email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}
        }
        return super().validate(df, validation_rules=validation_rules)
    
    def load(self, df: DataFrame) -> list[str]:
        if self.environment == "local":
            # Local testing - write CSV
            output_path = "dist/local_output/customers/"
            df.write.mode("overwrite").csv(output_path)
            return [output_path]
        else:
            # Production - write Iceberg table
            table_name = "glue_catalog.silver.customers"
            df.writeTo(table_name).using("iceberg").createOrReplace()
            return [f"s3://data-lake-{self.environment}-silver/customers/"]
```

## Benefits of This Architecture

1. **Consistency**: All jobs follow the same pattern
2. **Reliability**: Built-in error handling and validation
3. **Observability**: Comprehensive logging and audit trails
4. **Testability**: Environment-aware data loading enables local testing
5. **Maintainability**: Reusable components reduce duplication
6. **Scalability**: Spark DataFrame processing handles large datasets
7. **Data Quality**: Validation framework ensures clean data

## Development Workflow

1. **Local Testing**: Use test CSV files, validate transformations
2. **Integration Testing**: Run jobs in AWS Glue containers
3. **Staging Deployment**: Process real data in controlled environment
4. **Production Deployment**: Full-scale data processing with monitoring

This architecture enables reliable, scalable data processing while maintaining code quality and operational excellence.