import json
from typing import Any, Optional

import boto3
from loguru import logger


class NotificationService:
    """Send notifications for job completion events"""

    def __init__(self, environment: str):
        self.environment = environment
        self.logger = logger.bind(component="notifications")

        self.topic_arn: Optional[str]
        if environment != "local":
            self.sns_client = boto3.client("sns")
            self.topic_arn = f"arn:aws:sns:us-east-1:{self._get_account_id()}:{environment}-job-notifications"
        else:
            self.sns_client = None
            self.topic_arn = None

    def send_success_notification(self, job_data: dict[str, Any]) -> None:
        """Send success notification"""
        if not self.sns_client:
            self.logger.info("success_notification", **job_data)
            return

        try:
            message = {
                "event_type": "job_success",
                "environment": self.environment,
                "job_name": job_data.get("job_name"),
                "job_run_id": job_data.get("job_run_id"),
                "records_processed": job_data.get("records_processed"),
                "output_paths": job_data.get("output_paths", []),
            }

            subject = (
                f"Glue Job Success: {job_data.get('job_name')} [{self.environment}]"
            )

            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=json.dumps(message, indent=2),
                Subject=subject,
                MessageAttributes={
                    "EventType": {"DataType": "String", "StringValue": "job_success"},
                    "Environment": {
                        "DataType": "String",
                        "StringValue": self.environment,
                    },
                },
            )

            self.logger.info(
                "success_notification_sent",
                message_id=response["MessageId"],
                job_name=job_data.get("job_name"),
            )

        except Exception as e:
            self.logger.error(
                "notification_send_failed", error=str(e), job_data=job_data
            )

    def send_failure_notification(self, job_data: dict[str, Any]) -> None:
        """Send failure notification"""
        if not self.sns_client:
            self.logger.error("failure_notification", **job_data)
            return

        try:
            message = {
                "event_type": "job_failure",
                "environment": self.environment,
                "job_name": job_data.get("job_name"),
                "job_run_id": job_data.get("job_run_id"),
                "error": job_data.get("error"),
            }

            subject = (
                f"🚨 Glue Job Failed: {job_data.get('job_name')} [{self.environment}]"
            )

            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=json.dumps(message, indent=2),
                Subject=subject,
                MessageAttributes={
                    "EventType": {"DataType": "String", "StringValue": "job_failure"},
                    "Environment": {
                        "DataType": "String",
                        "StringValue": self.environment,
                    },
                },
            )

            self.logger.info(
                "failure_notification_sent",
                message_id=response["MessageId"],
                job_name=job_data.get("job_name"),
            )

        except Exception as e:
            self.logger.error(
                "notification_send_failed", error=str(e), job_data=job_data
            )

    def _get_account_id(self) -> str:
        """Get AWS account ID"""
        try:
            sts_client = boto3.client("sts")
            account_id: str = sts_client.get_caller_identity()["Account"]
            return account_id
        except Exception as e:
            self.logger.error("failed_to_get_account_id", error=str(e))
            return "123456789012"  # Fallback for local testing
