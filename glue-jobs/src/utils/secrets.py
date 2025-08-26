import json
from typing import Any

import boto3
from loguru import logger


def get_secret(secret_name: str, region: str = "us-east-1") -> dict[str, Any] | None:
    """Retrieve secret from AWS Secrets Manager"""

    try:
        client = boto3.client("secretsmanager", region_name=region)

        logger.info("retrieving_secret", secret_name=secret_name)

        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"]

        # Try to parse as JSON, fallback to plain string
        try:
            secret_data = json.loads(secret_string)
            logger.info(
                "secret_retrieved",
                secret_name=secret_name,
                keys=list(secret_data.keys()),
            )
            return secret_data
        except json.JSONDecodeError:
            logger.info("secret_retrieved", secret_name=secret_name, type="string")
            return {"value": secret_string}

    except Exception as e:
        logger.error("secret_retrieval_failed", secret_name=secret_name, error=str(e))
        return None
