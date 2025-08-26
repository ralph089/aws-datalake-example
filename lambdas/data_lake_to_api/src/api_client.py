"""
HTTP client adapted for Lambda functions with OAuth 2.0 and structured logging.

Optimized for AWS Lambda with proper resource management and timeout handling.
"""

import json
import logging
import time
from typing import Any

import boto3
import httpx
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging for Lambda
logger = logging.getLogger(__name__)


class APIClient:
    """HTTP client with OAuth 2.0 client credentials grant optimized for Lambda functions."""

    def __init__(
        self,
        base_url: str,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_endpoint: str | None = None,
        bearer_token: str | None = None,
    ):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint
        self.access_token = bearer_token
        self.token_expires_at = 0

        # Configure HTTP client with Lambda-appropriate timeouts
        self.client = httpx.Client(
            base_url=base_url,
            timeout=30.0,  # Lambda timeout consideration
        )

    @classmethod
    def from_secrets_manager(cls, secret_name: str) -> "APIClient":
        """Create APIClient from AWS Secrets Manager credentials."""
        try:
            secrets_client = boto3.client("secretsmanager")
            response = secrets_client.get_secret_value(SecretId=secret_name)
            credentials = json.loads(response["SecretString"])

            return cls(
                base_url=credentials["api_base_url"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
                token_endpoint=credentials.get("token_endpoint", "/oauth/token"),
            )
        except ClientError as e:
            logger.error(f"Failed to retrieve credentials from {secret_name}: {e}")
            raise
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Invalid credentials format in {secret_name}: {e}")
            raise

    def _get_access_token(self) -> str:
        """Get access token using client credentials grant."""
        if not self.client_id or not self.client_secret or not self.token_endpoint:
            if self.access_token:
                return self.access_token
            raise ValueError("Either provide client_id/client_secret/token_endpoint for OAuth or bearer_token")

        # Check if current token is still valid (with 60 second buffer)
        if self.access_token and time.time() < (self.token_expires_at - 60):
            return self.access_token

        logger.info(f"Requesting access token from {self.token_endpoint}")

        try:
            response = self.client.post(
                self.token_endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = time.time() + expires_in

            logger.info(f"Access token obtained, expires in {expires_in} seconds")
            return self.access_token

        except httpx.HTTPError as e:
            logger.error(f"Token request failed: {e}")
            raise

    def _get_auth_headers(self) -> dict[str, str]:
        """Get authorization headers with current access token."""
        token = self._get_access_token()
        return {"Authorization": f"Bearer {token}"}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET request with retry logic and OAuth authentication."""
        logger.info(f"API GET request to {endpoint}")

        try:
            headers = self._get_auth_headers()
            response = self.client.get(endpoint, params=params, headers=headers)
            response.raise_for_status()

            logger.info(f"API response: {response.status_code}")
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"API GET error for {endpoint}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def post(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        """POST request with retry logic and OAuth authentication."""
        logger.info(f"API POST request to {endpoint}")

        try:
            headers = self._get_auth_headers()
            response = self.client.post(endpoint, json=json, headers=headers)
            response.raise_for_status()

            logger.info(f"API response: {response.status_code}")
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"API POST error for {endpoint}: {e}")
            raise

    def send_batch(self, endpoint: str, records: list[dict[str, Any]], batch_size: int = 100) -> dict[str, Any]:
        """
        Send records in batches to the API.

        Args:
            endpoint: API endpoint
            records: List of records to send
            batch_size: Number of records per batch

        Returns:
            Summary of batch operations
        """
        total_records = len(records)
        successful_batches = 0
        failed_batches = 0

        logger.info(f"Sending {total_records} records in batches of {batch_size}")

        for i in range(0, total_records, batch_size):
            batch = records[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                payload = {
                    "batch_number": batch_num,
                    "total_batches": (total_records + batch_size - 1) // batch_size,
                    "records": batch,
                }

                self.post(endpoint, payload)
                successful_batches += 1
                logger.info(f"Successfully sent batch {batch_num} ({len(batch)} records)")

            except Exception as e:
                failed_batches += 1
                logger.error(f"Failed to send batch {batch_num}: {e}")
                # Continue with next batch instead of failing completely

        return {
            "total_records": total_records,
            "total_batches": (total_records + batch_size - 1) // batch_size,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "success_rate": successful_batches / ((successful_batches + failed_batches) or 1),
        }

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self) -> "APIClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
