import json
from typing import Any

import boto3
import structlog

logger = structlog.get_logger()


class DLQHandler:
    """Handle dead letter queue operations for failed jobs"""

    def __init__(self, environment: str, job_name: str):
        self.environment = environment
        self.job_name = job_name
        self.logger = logger.bind(component="dlq", job_name=job_name)

        if environment != "local":
            self.sqs_client = boto3.client("sqs")
            self.queue_url = f"https://sqs.us-east-1.amazonaws.com/{self._get_account_id()}/{environment}-glue-dlq"
        else:
            self.sqs_client = None
            self.queue_url = None

    def send_to_dlq(self, message_data: dict[str, Any]):
        """Send failed job information to dead letter queue"""
        if not self.sqs_client:
            self.logger.warning("dlq_not_configured", message=message_data)
            return

        try:
            message_body = {
                "job_name": self.job_name,
                "environment": self.environment,
                "timestamp": message_data.get("timestamp"),
                "error": message_data.get("error"),
                "job_run_id": message_data.get("job_run_id"),
                "details": message_data,
            }

            response = self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message_body),
                MessageAttributes={
                    "JobName": {"StringValue": self.job_name, "DataType": "String"},
                    "Environment": {"StringValue": self.environment, "DataType": "String"},
                },
            )

            self.logger.info("message_sent_to_dlq", message_id=response["MessageId"], job_name=self.job_name)

        except Exception as e:
            self.logger.error("dlq_send_failed", error=str(e), message=message_data)

    def _get_account_id(self) -> str:
        """Get AWS account ID"""
        try:
            sts_client = boto3.client("sts")
            return sts_client.get_caller_identity()["Account"]
        except Exception as e:
            self.logger.error("failed_to_get_account_id", error=str(e))
            return "123456789012"  # Fallback for local testing
