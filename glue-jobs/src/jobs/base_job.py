from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
import structlog
from typing import Dict, Any, Optional, List
import sys
from datetime import datetime
import boto3
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from utils.logging_config import setup_logging
from utils.audit import AuditTracker
from utils.dlq_handler import DLQHandler
from utils.notifications import NotificationService
from utils.secrets import get_secret
from utils.api_client import APIClient

# Patch boto3 for X-Ray tracing
patch_all()

class BaseGlueJob(ABC):
    """Base class with audit trail, DLQ support, and X-Ray tracing"""
    
    def __init__(self, job_name: str, args: Dict[str, Any]):
        self.job_name = job_name
        self.args = args
        self.environment = args.get("env", "local")
        self.job_run_id = args.get("JOB_RUN_ID", f"local-{datetime.now().isoformat()}")
        
        # Setup structured logging with X-Ray correlation
        self.logger = setup_logging(job_name, self.environment)
        self.logger = self.logger.bind(
            job_name=job_name,
            environment=self.environment,
            job_run_id=self.job_run_id,
            trace_id=xray_recorder.current_segment().trace_id if xray_recorder.current_segment() else None
        )
        
        # Initialize components
        self.spark = self._create_spark_session()
        self.audit_tracker = AuditTracker(self.environment, self.job_name, self.job_run_id)
        self.dlq_handler = DLQHandler(self.environment, self.job_name)
        self.notification_service = NotificationService(self.environment)
        
        # Setup API client if needed
        self.api_client = self._setup_api_client()
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg support"""
        builder = SparkSession.builder \
            .appName(self.job_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        if self.environment != "local":
            # Iceberg configurations
            builder = builder \
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://data-lake-{self.environment}-silver/") \
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        return builder.getOrCreate()
    
    def _setup_api_client(self) -> Optional[APIClient]:
        """Setup API client with credentials from Secrets Manager"""
        if self.environment == "local":
            # Use environment variables for local testing
            return None
        
        api_config = get_secret(f"{self.environment}/api/credentials")
        if api_config:
            return APIClient(
                base_url=api_config.get("base_url"),
                bearer_token=api_config.get("bearer_token")
            )
        return None
    
    @xray_recorder.capture('extract')
    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source with X-Ray tracing"""
        pass
    
    @xray_recorder.capture('transform')
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform data with X-Ray tracing"""
        pass
    
    @xray_recorder.capture('load')
    @abstractmethod
    def load(self, df: DataFrame) -> List[str]:
        """Load data to target. Returns list of output paths."""
        pass
    
    @xray_recorder.capture('validate')
    def validate(self, df: DataFrame) -> bool:
        """
        Data quality validation
        Returns True if validation passes, False otherwise
        """
        try:
            record_count = df.count()
            
            # Basic validation
            if record_count == 0:
                self.logger.error("validation_failed", reason="No records to process")
                return False
            
            # Column-level validation
            validation_results = {}
            for col in df.columns:
                null_count = df.filter(f"{col} IS NULL").count()
                null_percentage = (null_count / record_count) * 100
                validation_results[col] = {
                    "null_count": null_count,
                    "null_percentage": null_percentage
                }
            
            # Log validation results
            self.logger.info("validation_results", 
                           record_count=record_count,
                           column_stats=validation_results)
            
            # Audit trail
            self.audit_tracker.log_validation(validation_results)
            
            # Custom validation (override in subclasses)
            return self.custom_validation(df)
            
        except Exception as e:
            self.logger.error("validation_error", error=str(e))
            return False
    
    def custom_validation(self, df: DataFrame) -> bool:
        """Override in subclasses for custom validation logic"""
        return True
    
    @xray_recorder.capture('run')
    def run(self) -> None:
        """Main ETL pipeline execution"""
        try:
            self.logger.info("job_started")
            
            # Start audit trail
            self.audit_tracker.start_job()
            
            # Extract
            with xray_recorder.in_subsegment('extract_phase'):
                self.logger.info("extract_phase_started")
                df = self.extract()
                extract_count = df.count()
                self.logger.info("extract_phase_completed", row_count=extract_count)
                self.audit_tracker.log_extract(extract_count)
            
            # Transform
            with xray_recorder.in_subsegment('transform_phase'):
                self.logger.info("transform_phase_started")
                df_transformed = self.transform(df)
                transform_count = df_transformed.count()
                self.logger.info("transform_phase_completed", row_count=transform_count)
                self.audit_tracker.log_transform(transform_count)
            
            # Validate
            with xray_recorder.in_subsegment('validation_phase'):
                self.logger.info("validation_phase_started")
                if not self.validate(df_transformed):
                    raise ValueError("Data validation failed")
                self.logger.info("validation_phase_completed")
            
            # Load
            with xray_recorder.in_subsegment('load_phase'):
                self.logger.info("load_phase_started")
                output_paths = self.load(df_transformed)
                self.logger.info("load_phase_completed", output_paths=output_paths)
                self.audit_tracker.log_load(output_paths)
            
            # Complete audit trail
            self.audit_tracker.complete_job("SUCCESS")
            
            # Send success notification
            self.notification_service.send_success_notification({
                "job_name": self.job_name,
                "job_run_id": self.job_run_id,
                "records_processed": transform_count,
                "output_paths": output_paths
            })
            
            self.logger.info("job_completed_successfully")
            
        except Exception as e:
            self.logger.error("job_failed", 
                            error=str(e),
                            error_type=type(e).__name__)
            
            # Audit failure
            self.audit_tracker.complete_job("FAILED", str(e))
            
            # Send to DLQ
            self.dlq_handler.send_to_dlq({
                "job_name": self.job_name,
                "job_run_id": self.job_run_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            
            # Send failure notification
            self.notification_service.send_failure_notification({
                "job_name": self.job_name,
                "job_run_id": self.job_run_id,
                "error": str(e)
            })
            
            raise
            
        finally:
            if self.api_client:
                self.api_client.close()
            self.spark.stop()