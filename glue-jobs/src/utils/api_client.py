import httpx
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

logger = structlog.get_logger()

class APIClient:
    """HTTP client with retry logic and structured logging"""
    
    def __init__(self, base_url: str, bearer_token: Optional[str] = None):
        self.base_url = base_url
        self.headers = {}
        if bearer_token:
            self.headers["Authorization"] = f"Bearer {bearer_token}"
        
        self.client = httpx.Client(
            base_url=base_url,
            headers=self.headers,
            timeout=30.0,
        )
        self.logger = logger.bind(component="api_client", base_url=base_url)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET request with retry logic"""
        self.logger.info("api_request", method="GET", endpoint=endpoint, params=params)
        
        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()
            
            self.logger.info("api_response", status_code=response.status_code)
            return response.json()
        except httpx.HTTPError as e:
            self.logger.error("api_error", error=str(e), endpoint=endpoint)
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def post(self, endpoint: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """POST request with retry logic"""
        self.logger.info("api_request", method="POST", endpoint=endpoint)
        
        try:
            response = self.client.post(endpoint, json=json)
            response.raise_for_status()
            
            self.logger.info("api_response", status_code=response.status_code)
            return response.json()
        except httpx.HTTPError as e:
            self.logger.error("api_error", error=str(e), endpoint=endpoint)
            raise
    
    def close(self):
        """Close the HTTP client"""
        self.client.close()