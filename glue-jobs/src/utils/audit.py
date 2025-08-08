"""
Audit trail tracking utility for AWS Glue ETL jobs.

This module provides comprehensive audit logging capabilities for data processing jobs,
tracking each phase of the ETL pipeline (extract, transform, validate, load) with
structured logging and persistent storage in S3.

The audit trail includes:
- Job lifecycle events (start, completion)
- ETL phase metrics (record counts, source files, output paths)
- Validation results and data quality metrics
- Error tracking and failure analysis
- Timestamped records with job metadata

Audit records are stored in S3 with hierarchical partitioning by job name and date
for efficient querying and analysis.
"""

import json
from datetime import datetime
from typing import Any, List, Optional

import boto3
import structlog

logger = structlog.get_logger()


class AuditTracker:
    """
    Tracks comprehensive audit trail for ETL job execution.
    
    Provides structured logging and persistent S3 storage of job lifecycle events,
    data processing metrics, and validation results. Audit records are partitioned
    by job name and date for efficient querying.
    
    Attributes:
        environment: Deployment environment (dev/staging/prod/local)
        job_name: Name of the ETL job being tracked
        job_run_id: Unique identifier for this job execution
        logger: Structured logger with job context
        s3_client: boto3 S3 client for audit record storage (None for local)
        audit_bucket: S3 bucket for audit trail storage
        audit_prefix: S3 key prefix for audit records
    
    Example:
        >>> tracker = AuditTracker("dev", "customer_import", "job-123")
        >>> tracker.start_job()
        >>> tracker.log_extract(1000, ["s3://bucket/file.csv"])
        >>> tracker.log_transform(950)
        >>> tracker.log_validation({"passed": True, "failed_checks": 0})
        >>> tracker.log_load(["s3://bucket/output/"])
        >>> tracker.complete_job("success")
    """

    def __init__(self, environment: str, job_name: str, job_run_id: str) -> None:
        """
        Initialize audit tracker for a specific job run.
        
        Args:
            environment: Deployment environment (dev/staging/prod/local)
            job_name: Name of the ETL job to track
            job_run_id: Unique identifier for this job execution
        """
        self.environment = environment
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.logger = logger.bind(component="audit", job_name=job_name, job_run_id=job_run_id)

        # Initialize S3 client for audit logs
        if environment != "local":
            self.s3_client = boto3.client("s3")
            self.audit_bucket = f"data-lake-{environment}-gold"
            self.audit_prefix = "audit_trail/"
        else:
            self.s3_client = None

    def start_job(self) -> None:
        """
        Log job start event with initial metadata.
        
        Creates an audit record marking the beginning of job execution,
        including job identification and timestamp information.
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "environment": self.environment,
            "event_type": "job_started",
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.logger.info("job_started", **audit_record)
        self._write_audit_record(audit_record)

    def log_extract(self, record_count: int, source_files: Optional[List[str]] = None) -> None:
        """
        Log extract phase completion with data source metrics.
        
        Records the successful completion of the data extraction phase,
        including the number of records extracted and source file locations.
        
        Args:
            record_count: Number of records successfully extracted
            source_files: List of source file paths/URIs (optional)
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "extract_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "record_count": record_count,
            "source_files": source_files or [],
        }

        self.logger.info("extract_completed", **audit_record)
        self._write_audit_record(audit_record)

    def log_transform(self, record_count: int) -> None:
        """
        Log transform phase completion with processed data metrics.
        
        Records the successful completion of the data transformation phase,
        including the number of records that passed through transformation logic.
        
        Args:
            record_count: Number of records after transformation processing
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "transform_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "record_count": record_count,
        }

        self.logger.info("transform_completed", **audit_record)
        self._write_audit_record(audit_record)

    def log_validation(self, validation_results: dict[str, Any]) -> None:
        """
        Log validation phase completion with data quality results.
        
        Records the completion of data validation/quality checks, including
        detailed results from validation frameworks like Great Expectations.
        
        Args:
            validation_results: Dictionary containing validation outcomes,
                              typically including success/failure counts,
                              specific check results, and quality metrics
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "validation_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "validation_results": validation_results,
        }

        self.logger.info("validation_completed", **audit_record)
        self._write_audit_record(audit_record)

    def log_load(self, output_paths: List[str]) -> None:
        """
        Log load phase completion with output location details.
        
        Records the successful completion of the data loading phase,
        including all output paths where processed data was written.
        
        Args:
            output_paths: List of output file/directory paths where
                         processed data was written (S3 URIs, table locations, etc.)
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "load_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "output_paths": output_paths,
        }

        self.logger.info("load_completed", **audit_record)
        self._write_audit_record(audit_record)

    def complete_job(self, status: str, error_message: Optional[str] = None) -> None:
        """
        Log job completion with final status and optional error details.
        
        Records the final outcome of job execution, including success/failure
        status and any error information for failed jobs.
        
        Args:
            status: Final job status (e.g., "success", "failed", "cancelled")
            error_message: Optional error description for failed jobs
        """
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "job_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "status": status,
        }

        if error_message:
            audit_record["error_message"] = error_message

        self.logger.info("job_completed", **audit_record)
        self._write_audit_record(audit_record)

    def _write_audit_record(self, record: dict[str, Any]) -> None:
        """
        Write audit record to S3 with hierarchical partitioning.
        
        Persists audit records to S3 using a partitioned structure for efficient
        querying: audit_trail/job_name={job}/date={YYYY/MM/DD}/{run_id}-{event}.json
        
        Records are stored as JSON objects in the gold layer bucket with partition
        keys that enable efficient filtering by job name and date ranges.
        
        Args:
            record: Audit record dictionary to persist to S3
            
        Note:
            For local environments, records are only logged (not persisted to S3).
            Failures to write to S3 are logged but do not interrupt job execution.
        """
        if not self.s3_client:
            return

        try:
            # Write to S3 with partition by date and job
            date_partition = datetime.utcnow().strftime("%Y/%m/%d")
            key = f"{self.audit_prefix}job_name={self.job_name}/date={date_partition}/{self.job_run_id}-{record['event_type']}.json"

            self.s3_client.put_object(
                Bucket=self.audit_bucket, Key=key, Body=json.dumps(record), ContentType="application/json"
            )

            self.logger.debug("audit_record_written", s3_key=key)

        except Exception as e:
            self.logger.error("audit_write_failed", error=str(e), record=record)
