import boto3
import json
from datetime import datetime
from typing import Dict, Any, List
import structlog

logger = structlog.get_logger()

class AuditTracker:
    """Track audit trail for data processing jobs"""
    
    def __init__(self, environment: str, job_name: str, job_run_id: str):
        self.environment = environment
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.logger = logger.bind(
            component="audit",
            job_name=job_name,
            job_run_id=job_run_id
        )
        
        # Initialize S3 client for audit logs
        if environment != "local":
            self.s3_client = boto3.client('s3')
            self.audit_bucket = f"data-lake-{environment}-gold"
            self.audit_prefix = "audit_trail/"
        else:
            self.s3_client = None
    
    def start_job(self):
        """Log job start"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "environment": self.environment,
            "event_type": "job_started",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        self.logger.info("job_started", **audit_record)
        self._write_audit_record(audit_record)
    
    def log_extract(self, record_count: int, source_files: List[str] = None):
        """Log extract phase"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "extract_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "record_count": record_count,
            "source_files": source_files or []
        }
        
        self.logger.info("extract_completed", **audit_record)
        self._write_audit_record(audit_record)
    
    def log_transform(self, record_count: int):
        """Log transform phase"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "transform_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "record_count": record_count
        }
        
        self.logger.info("transform_completed", **audit_record)
        self._write_audit_record(audit_record)
    
    def log_validation(self, validation_results: Dict[str, Any]):
        """Log validation results"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "validation_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "validation_results": validation_results
        }
        
        self.logger.info("validation_completed", **audit_record)
        self._write_audit_record(audit_record)
    
    def log_load(self, output_paths: List[str]):
        """Log load phase"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "load_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "output_paths": output_paths
        }
        
        self.logger.info("load_completed", **audit_record)
        self._write_audit_record(audit_record)
    
    def complete_job(self, status: str, error_message: str = None):
        """Log job completion"""
        audit_record = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "event_type": "job_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "status": status
        }
        
        if error_message:
            audit_record["error_message"] = error_message
        
        self.logger.info("job_completed", **audit_record)
        self._write_audit_record(audit_record)
    
    def _write_audit_record(self, record: Dict[str, Any]):
        """Write audit record to S3"""
        if not self.s3_client:
            return
        
        try:
            # Write to S3 with partition by date and job
            date_partition = datetime.utcnow().strftime("%Y/%m/%d")
            key = f"{self.audit_prefix}job_name={self.job_name}/date={date_partition}/{self.job_run_id}-{record['event_type']}.json"
            
            self.s3_client.put_object(
                Bucket=self.audit_bucket,
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json"
            )
            
            self.logger.debug("audit_record_written", s3_key=key)
            
        except Exception as e:
            self.logger.error("audit_write_failed", error=str(e), record=record)