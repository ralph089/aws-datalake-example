import json

import boto3
import pytest
from moto import mock_aws

from utils.api_client import APIClient
from utils.audit import AuditTracker
from utils.dlq_handler import DLQHandler
from utils.notifications import NotificationService
from utils.secrets import get_secret


class TestSecrets:
    @mock_aws
    def test_get_secret_success(self):
        """Test successful secret retrieval"""
        # Setup
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret_data = {"api_key": "test-key-123", "base_url": "https://api.test.com"}

        client.create_secret(Name="test/secret", SecretString=json.dumps(secret_data))

        # Test
        result = get_secret("test/secret")

        # Verify
        assert result == secret_data

    @mock_aws
    def test_get_secret_not_found(self):
        """Test secret not found"""
        result = get_secret("nonexistent/secret")
        assert result is None

    @mock_aws
    def test_get_secret_string_value(self):
        """Test secret with string value (not JSON)"""
        client = boto3.client("secretsmanager", region_name="us-east-1")

        client.create_secret(Name="test/string-secret", SecretString="plain-string-value")

        result = get_secret("test/string-secret")
        assert result == {"value": "plain-string-value"}


class TestDLQHandler:
    @mock_aws
    def test_send_to_dlq_success(self, aws_credentials, mocker):
        """Test successful DLQ message sending using pytest-mock"""
        # Setup
        sqs = boto3.client("sqs", region_name="us-east-1")
        queue = sqs.create_queue(QueueName="dev-glue-dlq")
        queue_url = queue["QueueUrl"]

        # Use pytest-mock instead of patch context manager
        mocker.patch("utils.dlq_handler.DLQHandler._get_account_id", return_value="123456789012")
        
        handler = DLQHandler("dev", "test_job")

        message_data = {
            "job_run_id": "test_run_123",
            "error": "Test error message",
            "timestamp": "2023-01-01T12:00:00",
        }

        # Test
        handler.send_to_dlq(message_data)

        # Verify message was sent
        messages = sqs.receive_message(QueueUrl=queue_url)
        assert "Messages" in messages

        message_body = json.loads(messages["Messages"][0]["Body"])
        assert message_body["job_name"] == "test_job"
        assert message_body["error"] == "Test error message"

    def test_local_environment_no_sqs(self):
        """Test DLQ handler in local environment"""
        handler = DLQHandler("local", "test_job")

        # Should not fail, just log warning
        handler.send_to_dlq({"error": "test error"})


class TestNotificationService:
    @mock_aws
    def test_send_success_notification(self, aws_credentials, mocker):
        """Test successful notification sending using pytest-mock"""
        # Setup
        sns = boto3.client("sns", region_name="us-east-1")
        topic = sns.create_topic(Name="dev-job-notifications")
        topic["TopicArn"]

        # Use pytest-mock
        mocker.patch("utils.notifications.NotificationService._get_account_id", return_value="123456789012")
        
        service = NotificationService("dev")

        job_data = {
            "job_name": "test_job",
            "job_run_id": "run_123",
            "records_processed": 100,
            "output_paths": ["s3://bucket/path/"],
        }

        # Test
        service.send_success_notification(job_data)

        # In real implementation, would verify SNS message was sent

    @mock_aws
    def test_send_failure_notification(self, aws_credentials, mocker):
        """Test failure notification sending using pytest-mock"""
        # Setup
        sns = boto3.client("sns", region_name="us-east-1")
        sns.create_topic(Name="dev-job-notifications")

        # Use pytest-mock
        mocker.patch("utils.notifications.NotificationService._get_account_id", return_value="123456789012")
        
        service = NotificationService("dev")

        job_data = {"job_name": "test_job", "job_run_id": "run_123", "error": "Job failed due to validation error"}

        # Test
        service.send_failure_notification(job_data)


class TestAuditTracker:
    @mock_aws
    def test_audit_tracking(self, aws_credentials):
        """Test audit trail tracking"""
        # Setup
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="data-lake-dev-gold")

        tracker = AuditTracker("dev", "test_job", "run_123")

        # Test various audit events
        tracker.start_job()
        tracker.log_extract(100, ["file1.csv", "file2.csv"])
        tracker.log_transform(95)
        tracker.log_validation({"email": {"null_count": 0}})
        tracker.log_load(["s3://output/path/"])
        tracker.complete_job("SUCCESS")

        # Verify files were written to S3
        objects = s3.list_objects_v2(Bucket="data-lake-dev-gold", Prefix="audit_trail/")
        assert "Contents" in objects
        assert len(objects["Contents"]) > 0

    def test_local_environment_no_s3(self):
        """Test audit tracker in local environment"""
        tracker = AuditTracker("local", "test_job", "run_123")

        # Should not fail, just skip S3 writes
        tracker.start_job()
        tracker.complete_job("SUCCESS")


class TestAPIClient:
    def test_api_client_initialization_with_bearer_token(self):
        """Test API client initialization with bearer token"""
        client = APIClient("https://api.test.com", bearer_token="test-token")

        assert client.base_url == "https://api.test.com"
        assert client.access_token == "test-token"

    def test_api_client_initialization_with_oauth_credentials(self):
        """Test API client initialization with OAuth credentials"""
        client = APIClient(
            "https://api.test.com",
            client_id="test-id",
            client_secret="test-secret",
            token_endpoint="https://auth.test.com/token",
        )

        assert client.base_url == "https://api.test.com"
        assert client.client_id == "test-id"
        assert client.client_secret == "test-secret"
        assert client.token_endpoint == "https://auth.test.com/token"

    def test_successful_get_request_with_bearer_token(self, mocker):
        """Test successful GET request with bearer token using pytest-mock"""
        # Setup mock using pytest-mock
        mock_client = mocker.patch("utils.api_client.httpx.Client")
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.status_code = 200
        mock_client.return_value.get.return_value = mock_response

        # Test
        client = APIClient("https://api.test.com", bearer_token="test-token")
        result = client.get("/endpoint", params={"key": "value"})

        # Verify
        assert result == {"data": "test"}
        mock_client.return_value.get.assert_called_once_with(
            "/endpoint", params={"key": "value"}, headers={"Authorization": "Bearer test-token"}
        )

    def test_successful_post_request_with_bearer_token(self, mocker):
        """Test successful POST request with bearer token using pytest-mock"""
        # Setup mock using pytest-mock
        mock_client = mocker.patch("utils.api_client.httpx.Client")
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.status_code = 200
        mock_client.return_value.post.return_value = mock_response

        # Test
        client = APIClient("https://api.test.com", bearer_token="test-token")
        result = client.post("/endpoint", json={"data": "test"})

        # Verify
        assert result == {"success": True}
        mock_client.return_value.post.assert_called_once_with(
            "/endpoint", json={"data": "test"}, headers={"Authorization": "Bearer test-token"}
        )

    def test_oauth_client_credentials_flow(self, mocker):
        """Test OAuth 2.0 client credentials grant flow using pytest-mock"""
        # Setup mocks using pytest-mock
        mock_time = mocker.patch("utils.api_client.time.time")
        mock_client = mocker.patch("utils.api_client.httpx.Client")
        mock_time.return_value = 1000

        # Mock token response
        mock_token_response = mocker.MagicMock()
        mock_token_response.json.return_value = {"access_token": "oauth-token", "expires_in": 3600}
        mock_token_response.status_code = 200

        # Mock API response
        mock_api_response = mocker.MagicMock()
        mock_api_response.json.return_value = {"data": "test"}
        mock_api_response.status_code = 200

        # Configure mock client to return different responses for different calls
        mock_client_instance = mock_client.return_value
        mock_client_instance.post.side_effect = [mock_token_response, mock_api_response]

        # Test
        client = APIClient(
            "https://api.test.com",
            client_id="test-id",
            client_secret="test-secret",
            token_endpoint="https://auth.test.com/token",
        )
        result = client.post("/endpoint", json={"data": "test"})

        # Verify token request was made
        assert mock_client_instance.post.call_count == 2
        token_call = mock_client_instance.post.call_args_list[0]
        assert token_call[0][0] == "https://auth.test.com/token"
        assert token_call[1]["data"] == {
            "grant_type": "client_credentials",
            "client_id": "test-id",
            "client_secret": "test-secret",
        }

        # Verify API request was made with bearer token
        api_call = mock_client_instance.post.call_args_list[1]
        assert api_call[0][0] == "/endpoint"
        assert api_call[1]["headers"] == {"Authorization": "Bearer oauth-token"}

        assert result == {"data": "test"}

    def test_client_close(self):
        """Test client close"""
        client = APIClient("https://api.test.com")
        # Should not raise exception
        client.close()
