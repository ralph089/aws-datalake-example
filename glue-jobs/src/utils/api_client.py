import time
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


class APIClient:
    """HTTP client with OAuth 2.0 client credentials grant and structured logging"""

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

        self.client = httpx.Client(
            base_url=base_url,
            timeout=30.0,
        )
        self.logger = logger.bind(component="api_client", base_url=base_url)

    def _get_access_token(self) -> str:
        """Get access token using client credentials grant"""
        if not self.client_id or not self.client_secret or not self.token_endpoint:
            if self.access_token:
                return self.access_token
            raise ValueError("Either provide client_id/client_secret/token_endpoint for OAuth or bearer_token")

        # Check if current token is still valid (with 60 second buffer)
        if self.access_token and time.time() < (self.token_expires_at - 60):
            return self.access_token

        self.logger.info("requesting_access_token", token_endpoint=self.token_endpoint)

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

            self.logger.info("access_token_obtained", expires_in=expires_in)
            return self.access_token

        except httpx.HTTPError as e:
            self.logger.error("token_request_failed", error=str(e))
            raise

    def _get_auth_headers(self) -> dict[str, str]:
        """Get authorization headers with current access token"""
        token = self._get_access_token()
        return {"Authorization": f"Bearer {token}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET request with retry logic and OAuth authentication"""
        self.logger.info("api_request", method="GET", endpoint=endpoint, params=params)

        try:
            headers = self._get_auth_headers()
            response = self.client.get(endpoint, params=params, headers=headers)
            response.raise_for_status()

            self.logger.info("api_response", status_code=response.status_code)
            return dict(response.json())
        except httpx.HTTPError as e:
            self.logger.error("api_error", error=str(e), endpoint=endpoint)
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    def post(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        """POST request with retry logic and OAuth authentication"""
        self.logger.info("api_request", method="POST", endpoint=endpoint)

        try:
            headers = self._get_auth_headers()
            response = self.client.post(endpoint, json=json, headers=headers)
            response.raise_for_status()

            self.logger.info("api_response", status_code=response.status_code)
            return dict(response.json())
        except httpx.HTTPError as e:
            self.logger.error("api_error", error=str(e), endpoint=endpoint)
            raise

    def close(self) -> None:
        """Close the HTTP client"""
        self.client.close()
