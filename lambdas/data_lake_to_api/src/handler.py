"""
AWS Lambda handler for querying Data Lake and sending data to REST API.

Supports multiple event types: EventBridge scheduled events, S3 notifications, and API Gateway requests.
"""

import json
import logging
from typing import Any

from api_client import APIClient
from config import (
    LambdaConfig,
    create_lambda_config,
    parse_api_gateway_event,
    parse_s3_event,
    parse_scheduled_event,
)
from data_connector import DataLakeConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler function.

    Args:
        event: Lambda event data
        context: Lambda context object

    Returns:
        Response dictionary with status and results
    """
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")

        # Initialize configuration
        config = create_lambda_config()
        logger.info(f"Lambda configuration loaded for environment: {config.env}")

        # Determine event type and parse configuration
        event_type, query_config = _parse_event(event)
        logger.info(f"Detected event type: {event_type}")

        # Initialize connectors
        data_connector = DataLakeConnector(config)

        # Build and execute query
        if event_type == "scheduled":
            query = data_connector.build_query(
                table_name=query_config.table_name,
                date_filter=query_config.date_filter,
                where_clause=query_config.where_clause,
                limit=config.max_records,
            )
        elif event_type == "s3":
            # For S3 events, query related table based on object key
            table_name = _infer_table_from_s3_key(query_config.object_key)
            query = data_connector.build_query(table_name=table_name, date_filter="today", limit=config.max_records)
        elif event_type == "api_gateway":
            query = data_connector.build_query(
                table_name=query_config.table_name, where_clause=query_config.where_clause, limit=query_config.limit
            )
        else:
            raise ValueError(f"Unsupported event type: {event_type}")

        logger.info(f"Executing query: {query}")

        # Query data from Data Lake
        records = data_connector.query_table(query)
        logger.info(f"Retrieved {len(records)} records from Data Lake")

        if not records:
            logger.info("No data found, returning success")
            return {
                "statusCode": 204,
                "body": json.dumps({"message": "No data found", "recordsProcessed": 0, "query": query}),
            }

        # Send data to API
        api_result = _send_to_api(config, records)

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Data processed successfully",
                    "recordsProcessed": len(records),
                    "apiResult": api_result,
                    "query": query,
                }
            ),
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)

        return {"statusCode": 500, "body": json.dumps({"error": str(e), "message": "Lambda execution failed"})}


def _parse_event(event: dict[str, Any]) -> tuple[str, Any]:
    """
    Parse Lambda event to determine type and extract configuration.

    Args:
        event: Lambda event data

    Returns:
        Tuple of (event_type, parsed_config)
    """
    # Check for EventBridge scheduled event
    if event.get("source") == "aws.events" and event.get("detail-type") == "Scheduled Event":
        return "scheduled", parse_scheduled_event(event)

    # Check for S3 event
    if "Records" in event and event["Records"] and "s3" in event["Records"][0]:
        return "s3", parse_s3_event(event)

    # Check for API Gateway event
    if "queryStringParameters" in event or "pathParameters" in event:
        return "api_gateway", parse_api_gateway_event(event)

    # Default to scheduled with basic configuration
    logger.warning("Unknown event type, defaulting to scheduled")
    return "scheduled", parse_scheduled_event({"detail": {}})


def _infer_table_from_s3_key(object_key: str) -> str:
    """
    Infer table name from S3 object key.

    Args:
        object_key: S3 object key

    Returns:
        Inferred table name
    """
    # Extract table name from path like "silver/customers/data.parquet"
    parts = object_key.split("/")

    if len(parts) >= 2:
        layer = parts[0]  # bronze, silver, gold
        table = parts[1]  # customers, products, etc.

        # Return table name with environment prefix
        env = create_lambda_config().env
        return f"{env}_{table}_{layer}"

    # Fallback to default table
    return "dev_customers_silver"


def _send_to_api(config: LambdaConfig, records: list) -> dict[str, Any]:
    """
    Send records to external REST API.

    Args:
        config: Lambda configuration
        records: List of records to send

    Returns:
        API send result summary
    """
    try:
        with APIClient.from_secrets_manager(config.api_secret_name) as api_client:
            # Send data in batches
            result = api_client.send_batch(endpoint="/api/data/ingest", records=records, batch_size=config.batch_size)

            logger.info(f"API send completed: {result}")
            return result

    except Exception as e:
        logger.error(f"Failed to send data to API: {e}")
        raise


# For local testing
if __name__ == "__main__":
    import os

    # Set up local environment
    os.environ["ENV"] = "local"
    os.environ["ATHENA_DATABASE"] = "default"
    os.environ["ATHENA_OUTPUT_BUCKET"] = "test-athena-results"
    os.environ["API_SECRET_NAME"] = "local/api/credentials"

    # Test with scheduled event
    test_event = {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {"table": "dev_customers_silver", "date_filter": "yesterday"},
    }

    class MockContext:
        aws_request_id = "test-request-id"
        function_name = "data-lake-to-api"

        def remaining_time_in_millis(self):
            return 300000

    try:
        result = lambda_handler(test_event, MockContext())
        print(f"Lambda result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"Local test failed: {e}")
