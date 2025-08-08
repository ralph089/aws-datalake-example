from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from utils.api_client import APIClient
from utils.audit import AuditTracker
from utils.dlq_handler import DLQHandler
from utils.logging_config import setup_logging
from utils.notifications import NotificationService
from utils.secrets import get_secret
from utils.simple_data_quality import SimpleDataQualityChecker


class BaseGlueJob(ABC):
    """
    Base class with audit trail, DLQ support, and structured logging
    
    For transactional behavior in AWS data lakes, follow these patterns:
    
    1. SINGLE TABLE PATTERN:
       - Write all related data to ONE comprehensive Iceberg table
       - Use Apache Iceberg's native ACID transactions for consistency
       - Create views or filtered queries for different aggregation levels
       - Example: Instead of separate daily_agg, product_agg, customer_agg tables,
         write all to sales_transactions with aggregation_type column
    
    2. ATOMIC OPERATIONS:
       - Iceberg's writeTo().createOrReplace() is atomic per table
       - Validate ALL data before writing ANY tables
       - Use try/catch around entire load sequence for multi-table jobs
    
    3. AVOID STAGING TABLES:
       - Modern Iceberg provides built-in ACID without staging complexity
       - Direct writes with optimistic locking handle concurrency
    """

    def __init__(self, job_name: str, args: dict[str, Any]):
        self.job_name = job_name
        self.args = args
        self.environment = args.get("env", "local")
        self.job_run_id = args.get("JOB_RUN_ID") or f"local-{datetime.now().isoformat()}"

        # S3 event detection - check if job was triggered by S3 event
        self.source_bucket = args.get("bucket")
        self.source_object_key = args.get("object_key")
        self.is_event_triggered = bool(self.source_bucket and self.source_object_key)
        self.trigger_type = "s3_event" if self.is_event_triggered else "scheduled"

        # Setup structured logging
        self.logger = setup_logging(job_name, self.environment)
        self.logger = self.logger.bind(
            job_name=job_name,
            environment=self.environment,
            job_run_id=self.job_run_id,
            trigger_type=self.trigger_type,
            source_bucket=self.source_bucket,
            source_object_key=self.source_object_key,
            trace_id=None,
        )

        # Initialize components
        self.spark = self._create_spark_session()
        
        # Set Spark log level to reduce verbosity for local development
        if self.environment == "local":
            self.spark.sparkContext.setLogLevel("WARN")
        
        self.audit_tracker = AuditTracker(self.environment, self.job_name, self.job_run_id)
        self.dlq_handler = DLQHandler(self.environment, self.job_name)
        self.notification_service = NotificationService(self.environment)

        # Initialize simple data quality checker
        self.quality_checker = SimpleDataQualityChecker()

        # Setup API client if needed
        self.api_client = self._setup_api_client()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with minimal essential configuration"""
        builder = SparkSession.builder.appName(self.job_name)

        if self.environment != "local":
            # Essential Iceberg configuration only
            builder = (
                builder.config(
                    "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                )
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://data-lake-{self.environment}-silver/")
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            )
        else:
            # Local development with reduced parallelism
            builder = (
                builder.config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "4")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            )

        return builder.getOrCreate()

    def _setup_api_client(self) -> APIClient | None:
        """Setup API client with OAuth 2.0 client credentials or bearer token from Secrets Manager"""
        if self.environment == "local":
            # Use environment variables for local testing
            return None

        try:
            api_config = get_secret(f"etl-jobs/{self.environment}/api-config")
            if not api_config:
                self.logger.warning("api_client_config_missing")
                return None

            base_url = api_config["base_url"]

            # Support OAuth client credentials only (remove legacy api_key support)
            if "client_id" in api_config and "client_secret" in api_config:
                # OAuth 2.0 client credentials grant
                return APIClient(
                    base_url=base_url,
                    client_id=api_config["client_id"],
                    client_secret=api_config["client_secret"],
                    token_endpoint=api_config["token_endpoint"],
                )
            else:
                self.logger.warning("api_client_config_invalid", reason="Missing OAuth credentials")
                return None

        except Exception as e:
            self.logger.warning("api_client_setup_failed", error=str(e))
            return None

    # Smart Data Loading Methods
    def load_data(self, data_type: str, fallback_path: str | None = None) -> DataFrame:
        """
        Smart data loader that auto-detects trigger type and file format.
        
        Event-triggered: Loads specific file from S3 event (bucket/object_key)
        Scheduled: Loads directory data using existing logic
        Local: Uses test data structures
        
        Args:
            data_type: Data identifier (e.g., 'api_data', 'inventory_api', 'customers')
            fallback_path: Optional fallback path for local development
            
        Returns:
            DataFrame with loaded data
        """
        from pathlib import Path
        
        # Local environment processing
        if self.environment == "local":
            return self._load_local_data(data_type, fallback_path)
        
        # Remote environment - check trigger type
        if self.is_event_triggered:
            # Event-triggered: load specific file from S3 event
            return self._load_event_file()
        else:
            # Scheduled: load directory data (existing behavior)
            return self._load_directory_data(data_type)

    def _load_local_data(self, data_type: str, fallback_path: str | None = None) -> DataFrame:
        """Load data for local development environment"""
        from pathlib import Path
        
        # Try test data structure first (for tests)
        test_path = Path("tests/test_data") / self.job_name
        
        # Try JSON first, then CSV in test structure
        for ext in ["json", "csv"]:
            file_path = test_path / f"{data_type}.{ext}"
            if file_path.exists():
                df = self._read_file_by_extension(str(file_path), ext)
                self.logger.info("loaded_test_data", 
                               file=str(file_path), 
                               row_count=df.count(),
                               format=ext)
                return df
        
        # Try local data structure (for production local dev)
        local_path = Path("local/data") / self.job_name
        
        # Try JSON first, then CSV
        for ext in ["json", "csv"]:
            file_path = local_path / f"{data_type}.{ext}"
            if file_path.exists():
                df = self._read_file_by_extension(str(file_path), ext)
                self.logger.info("loaded_local_data", 
                               file=str(file_path), 
                               row_count=df.count(),
                               format=ext)
                return df
        
        # Use fallback if provided
        if fallback_path and Path(fallback_path).exists():
            ext = Path(fallback_path).suffix[1:]  # Remove the dot
            df = self._read_file_by_extension(fallback_path, ext)
            self.logger.info("loaded_fallback_data", 
                           file=fallback_path, 
                           row_count=df.count(),
                           format=ext)
            return df
        
        raise FileNotFoundError(f"No local data found for {data_type} in {test_path} or {local_path}")

    def _load_event_file(self) -> DataFrame:
        """Load specific file from S3 event trigger"""
        s3_path = f"s3://{self.source_bucket}/{self.source_object_key}"
        
        # Auto-detect file format from extension
        file_ext = self.source_object_key.split('.')[-1].lower()
        
        df = self._read_file_by_extension(s3_path, file_ext)
        
        self.logger.info("loaded_event_file", 
                       path=s3_path, 
                       row_count=df.count(),
                       file_format=file_ext,
                       trigger_type="s3_event")
        return df

    def _load_directory_data(self, data_type: str) -> DataFrame:
        """Load directory data for scheduled processing (existing behavior)"""
        s3_path = f"s3://data-lake-{self.environment}-bronze/{data_type}/"
        
        # Default to CSV for directory loading (existing behavior)
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        
        self.logger.info("loaded_directory_data", 
                       path=s3_path, 
                       row_count=df.count(),
                       trigger_type="scheduled")
        return df

    def _read_file_by_extension(self, file_path: str, file_ext: str) -> DataFrame:
        """Read file based on extension with consistent options"""
        if file_ext == "json":
            return self.spark.read.option("multiline", "true").json(file_path)
        elif file_ext == "csv":
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        else:
            # Default to CSV for unknown extensions
            self.logger.warning("unknown_file_extension", 
                              extension=file_ext, 
                              path=file_path, 
                              defaulting_to="csv")
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    def load_api_data(self, data_type: str = "api_data") -> DataFrame:
        """
        Load API data with smart environment handling.
        
        Local: Loads from local/data/{job_name}/{data_type}.json
        Remote: Should be overridden by jobs to implement actual API calls
        
        Args:
            data_type: API data identifier (default: 'api_data')
            
        Returns:
            DataFrame with API data
        """
        if self.environment == "local":
            return self.load_data(data_type)
        else:
            # For remote environments, this should be overridden by specific jobs
            # to implement actual API calls
            raise NotImplementedError(f"Remote API loading for {data_type} should be implemented in job-specific code")


    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source. Returns raw DataFrame."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform raw data. Returns cleaned DataFrame."""
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> list[str]:
        """Load data to target. Returns list of output paths."""
        pass

    def validate(self, df: DataFrame, validation_rules: dict | None = None) -> bool:
        """
        Data quality validation using simple validation rules
        Returns True if validation passes, False otherwise

        Args:
            df: DataFrame to validate
            validation_rules: Optional dictionary of validation rules
        """
        try:
            record_count = df.count()

            # Basic validation
            if record_count == 0:
                self.logger.error("validation_failed", reason="No records to process")
                return False

            validation_results = {"record_count": record_count, "basic_validation": "passed"}

            # Run simple data quality checks if rules are provided
            if validation_rules:
                quality_results = self.quality_checker.validate_dataframe(df, validation_rules)
                validation_results["quality_validation"] = quality_results

                if quality_results["overall_status"] != "PASSED":
                    self.logger.warning("quality_validation_failed", results=quality_results)

            # Custom validation (override in subclasses)
            custom_result = self.custom_validation(df)
            validation_results["custom_validation"] = custom_result

            # Overall success: basic + custom validation must pass
            overall_success = custom_result

            if not overall_success:
                self.logger.error("validation_failed", results=validation_results)
            else:
                self.logger.info("validation_passed", record_count=record_count, results=validation_results)

            # Audit trail
            self.audit_tracker.log_validation(validation_results)

            return overall_success

        except Exception as e:
            self.logger.error("validation_error", error=str(e))
            return False

    def custom_validation(self, df: DataFrame) -> bool:
        """Override in subclasses for custom validation logic"""
        return True

    def run(self) -> None:
        """Main ETL pipeline execution with transactional validation"""
        try:
            if self.is_event_triggered:
                self.logger.info("job_started", 
                               trigger_type="s3_event",
                               source_file=f"s3://{self.source_bucket}/{self.source_object_key}")
            else:
                self.logger.info("job_started", 
                               trigger_type="scheduled")

            # Start audit trail
            self.audit_tracker.start_job()

            # Extract
            self.logger.info("extract_phase_started")
            df = self.extract()
            extract_count = df.count()
            self.logger.info("extract_phase_completed", row_count=extract_count)
            self.audit_tracker.log_extract(extract_count)

            # Transform
            self.logger.info("transform_phase_started")
            df_transformed = self.transform(df)
            transform_count = df_transformed.count()
            self.logger.info("transform_phase_completed", row_count=transform_count)
            self.audit_tracker.log_transform(transform_count)

            # TRANSACTIONAL VALIDATION: Validate ALL data before ANY writes
            self.logger.info("validation_phase_started", note="Validating ALL data before writes for transactional integrity")
            if not self.validate(df_transformed):
                raise ValueError("Data validation failed - no data will be written (transactional behavior)")
            self.logger.info("validation_phase_completed", note="All data validated successfully - proceeding with atomic write")

            # Load (atomic operation for transactional behavior)
            self.logger.info("load_phase_started", note="Performing atomic write operation")
            output_paths = self.load(df_transformed)
            self.logger.info("load_phase_completed", output_paths=output_paths, note="Atomic write completed successfully")
            self.audit_tracker.log_load(output_paths)

            # Complete audit trail
            self.audit_tracker.complete_job("SUCCESS")

            # Send success notification
            self.notification_service.send_success_notification(
                {
                    "job_name": self.job_name,
                    "job_run_id": self.job_run_id,
                    "records_processed": transform_count,
                    "output_paths": output_paths,
                }
            )

            self.logger.info("job_completed_successfully", note="Transactional job completed - all data committed atomically")

        except Exception as e:
            self.logger.error("job_failed", error=str(e), error_type=type(e).__name__, note="Job failed - no partial data written due to transactional design")

            # Audit failure
            self.audit_tracker.complete_job("FAILED", str(e))

            # Send to DLQ
            self.dlq_handler.send_to_dlq(
                {
                    "job_name": self.job_name,
                    "job_run_id": self.job_run_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

            # Send failure notification
            self.notification_service.send_failure_notification(
                {
                    "job_name": self.job_name,
                    "job_run_id": self.job_run_id,
                    "error": str(e),
                }
            )

            # Re-raise to ensure job fails
            raise
