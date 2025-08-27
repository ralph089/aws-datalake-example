"""
AWS Lambda handler for processing SNS events and triggering AWS Glue jobs.

This Lambda function subscribes to an external SNS topic that publishes S3 events,
parses the S3 event details, and triggers the appropriate Glue job with the
correct bucket and object_key parameters.
"""

import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS client will be initialized on first use
glue_client = None


def _get_glue_client():
    """Get or initialize Glue client."""
    global glue_client
    if glue_client is None:
        glue_client = boto3.client("glue")
    return glue_client


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler function for SNS events.

    Args:
        event: SNS event containing S3 event details
        context: Lambda context object

    Returns:
        Response dictionary with processing results
    """
    try:
        logger.info(f"Received SNS event: {json.dumps(event, default=str)}")

        # Process all SNS records
        results = []
        for record in event.get("Records", []):
            if record.get("EventSource") == "aws:sns":
                result = _process_sns_record(record)
                results.append(result)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Processed {len(results)} SNS records", "results": results}),
        }

    except Exception as e:
        logger.error(f"Error processing SNS event: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def _process_sns_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Process a single SNS record and trigger appropriate Glue job.

    Args:
        record: SNS record from the event

    Returns:
        Processing result dictionary
    """
    try:
        # Extract SNS message
        sns_message = json.loads(record["Sns"]["Message"])
        logger.info(f"SNS message: {json.dumps(sns_message, default=str)}")

        # Parse S3 event details from SNS message
        s3_events = _extract_s3_events(sns_message)

        if not s3_events:
            logger.warning("No S3 events found in SNS message")
            return {"status": "skipped", "reason": "No S3 events found"}

        # Process each S3 event
        glue_job_results = []
        for s3_event in s3_events:
            bucket = s3_event["bucket"]
            object_key = s3_event["object_key"]
            event_name = s3_event.get("event_name", "ObjectCreated")

            logger.info(f"Processing S3 event: {event_name} for s3://{bucket}/{object_key}")

            # Determine which Glue job to trigger based on object key prefix
            glue_job_name = _determine_glue_job(object_key)

            if glue_job_name:
                job_run_id = _trigger_glue_job(glue_job_name, bucket, object_key)
                glue_job_results.append(
                    {"job_name": glue_job_name, "job_run_id": job_run_id, "bucket": bucket, "object_key": object_key}
                )
            else:
                logger.info(f"No matching Glue job for object key: {object_key}")
                glue_job_results.append(
                    {"status": "skipped", "reason": "No matching Glue job", "object_key": object_key}
                )

        return {
            "status": "success",
            "glue_jobs_triggered": len([r for r in glue_job_results if "job_run_id" in r]),
            "results": glue_job_results,
        }

    except Exception as e:
        logger.error(f"Error processing SNS record: {str(e)}")
        return {"status": "error", "error": str(e)}


def _extract_s3_events(sns_message: dict[str, Any]) -> list[dict[str, str]]:
    """
    Extract S3 event details from SNS message.

    SNS messages can contain S3 events in different formats:
    1. Direct S3 event notification
    2. EventBridge event with S3 details
    3. Custom format from external systems

    Args:
        sns_message: Parsed SNS message

    Returns:
        List of S3 event dictionaries with bucket, object_key, and event_name
    """
    s3_events = []

    try:
        # Format 1: Direct S3 event notification
        if "Records" in sns_message:
            for record in sns_message["Records"]:
                if record.get("eventSource") == "aws:s3":
                    s3_info = record.get("s3", {})
                    bucket_info = s3_info.get("bucket", {})
                    object_info = s3_info.get("object", {})

                    if bucket_info.get("name") and object_info.get("key"):
                        s3_events.append(
                            {
                                "bucket": bucket_info["name"],
                                "object_key": object_info["key"],
                                "event_name": record.get("eventName", "ObjectCreated"),
                            }
                        )

        # Format 2: EventBridge event with S3 details
        elif "detail" in sns_message and "bucket" in sns_message.get("detail", {}):
            detail = sns_message["detail"]
            if detail.get("bucket", {}).get("name") and detail.get("object", {}).get("key"):
                s3_events.append(
                    {
                        "bucket": detail["bucket"]["name"],
                        "object_key": detail["object"]["key"],
                        "event_name": sns_message.get("detail-type", "ObjectCreated"),
                    }
                )

        # Format 3: Simple key-value format
        elif "bucket" in sns_message and "object_key" in sns_message:
            s3_events.append(
                {
                    "bucket": sns_message["bucket"],
                    "object_key": sns_message["object_key"],
                    "event_name": sns_message.get("event_name", "ObjectCreated"),
                }
            )

        # Format 4: Try to find bucket and key anywhere in the message
        elif isinstance(sns_message, dict):
            bucket = None
            object_key = None

            # Common field names for bucket
            for bucket_field in ["bucketName", "bucket_name", "Bucket", "s3Bucket"]:
                if bucket_field in sns_message:
                    bucket = sns_message[bucket_field]
                    break

            # Common field names for object key
            for key_field in ["objectKey", "object_key", "key", "Key", "s3Key", "objectName"]:
                if key_field in sns_message:
                    object_key = sns_message[key_field]
                    break

            if bucket and object_key:
                s3_events.append(
                    {
                        "bucket": bucket,
                        "object_key": object_key,
                        "event_name": sns_message.get("eventName", "ObjectCreated"),
                    }
                )

        logger.info(f"Extracted {len(s3_events)} S3 events from SNS message")
        return s3_events

    except Exception as e:
        logger.error(f"Error extracting S3 events: {str(e)}")
        return []


def _determine_glue_job(object_key: str) -> str | None:
    """
    Determine which Glue job to trigger based on S3 object key prefix.

    Args:
        object_key: S3 object key

    Returns:
        Glue job name or None if no match
    """
    # Get environment
    env = os.getenv("ENVIRONMENT", "dev")

    # Get job mappings from environment variables
    # Format: PREFIX1:JOB1,PREFIX2:JOB2
    job_mappings_str = os.getenv("GLUE_JOB_MAPPINGS", "")

    if job_mappings_str:
        mappings = {}
        for mapping in job_mappings_str.split(","):
            if ":" in mapping:
                prefix, job = mapping.strip().split(":", 1)
                mappings[prefix.strip()] = job.strip()

        # Find matching prefix (longest match wins)
        matching_job = None
        longest_prefix = ""

        for prefix, job_name in mappings.items():
            if object_key.startswith(prefix) and len(prefix) > len(longest_prefix):
                longest_prefix = prefix
                matching_job = job_name

        if matching_job:
            logger.info(f"Matched prefix '{longest_prefix}' -> job '{matching_job}'")
            return matching_job

    # Default mappings if no environment variable set
    default_mappings = {
        "sales/": f"{env}-sales_etl",
        "customers/": f"{env}-customer_import",
        "inventory/": f"{env}-inventory_sync",
        "products/": f"{env}-product_catalog",
    }

    for prefix, job_name in default_mappings.items():
        if object_key.startswith(prefix):
            logger.info(f"Using default mapping: '{prefix}' -> '{job_name}'")
            return job_name

    logger.warning(f"No Glue job mapping found for object key: {object_key}")
    return None


def _trigger_glue_job(job_name: str, bucket: str, object_key: str) -> str:
    """
    Trigger AWS Glue job with S3 event parameters.

    Args:
        job_name: Name of the Glue job to trigger
        bucket: S3 bucket name
        object_key: S3 object key

    Returns:
        Glue job run ID

    Raises:
        ClientError: If Glue job fails to start
    """
    try:
        env = os.getenv("ENVIRONMENT", "dev")

        # Prepare job arguments
        arguments = {"--bucket": bucket, "--object_key": object_key, "--env": env, "--enable_metrics": "true"}

        logger.info(f"Starting Glue job '{job_name}' with arguments: {arguments}")

        # Start the Glue job
        response = _get_glue_client().start_job_run(JobName=job_name, Arguments=arguments)

        job_run_id = response["JobRunId"]
        logger.info(f"Started Glue job '{job_name}' with run ID: {job_run_id}")

        return job_run_id

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        logger.error(f"Failed to start Glue job '{job_name}': {error_code} - {error_message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error starting Glue job '{job_name}': {str(e)}")
        raise
