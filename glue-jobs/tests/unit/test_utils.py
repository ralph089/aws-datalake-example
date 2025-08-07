import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from moto import mock_secretsmanager, mock_sqs, mock_sns, mock_s3
import boto3

from utils.secrets import get_secret
from utils.dlq_handler import DLQHandler
from utils.notifications import NotificationService
from utils.audit import AuditTracker
from utils.api_client import APIClient

class TestSecrets:
    
    @mock_secretsmanager
    def test_get_secret_success(self):
        """Test successful secret retrieval"""
        # Setup
        client = boto3.client('secretsmanager', region_name='us-east-1')
        secret_data = {"api_key": "test-key-123", "base_url": "https://api.test.com"}
        
        client.create_secret(
            Name="test/secret",
            SecretString=json.dumps(secret_data)
        )
        
        # Test
        result = get_secret("test/secret")
        
        # Verify
        assert result == secret_data
    
    @mock_secretsmanager
    def test_get_secret_not_found(self):
        """Test secret not found"""
        result = get_secret("nonexistent/secret")
        assert result is None
    
    @mock_secretsmanager
    def test_get_secret_string_value(self):
        """Test secret with string value (not JSON)"""
        client = boto3.client('secretsmanager', region_name='us-east-1')
        
        client.create_secret(
            Name="test/string-secret",
            SecretString="plain-string-value"
        )
        
        result = get_secret("test/string-secret")
        assert result == {"value": "plain-string-value"}


class TestDLQHandler:
    
    @mock_sqs
    def test_send_to_dlq_success(self, aws_credentials):
        """Test successful DLQ message sending"""
        # Setup
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue = sqs.create_queue(QueueName='dev-glue-dlq')
        queue_url = queue['QueueUrl']
        
        with patch('utils.dlq_handler.DLQHandler._get_account_id', return_value='123456789012'):
            handler = DLQHandler("dev", "test_job")
            
            message_data = {
                "job_run_id": "test_run_123",
                "error": "Test error message",
                "timestamp": "2023-01-01T12:00:00"
            }
            
            # Test
            handler.send_to_dlq(message_data)
            
            # Verify message was sent
            messages = sqs.receive_message(QueueUrl=queue_url)
            assert 'Messages' in messages
            
            message_body = json.loads(messages['Messages'][0]['Body'])
            assert message_body['job_name'] == 'test_job'
            assert message_body['error'] == 'Test error message'
    
    def test_local_environment_no_sqs(self):
        """Test DLQ handler in local environment"""
        handler = DLQHandler("local", "test_job")
        
        # Should not fail, just log warning
        handler.send_to_dlq({"error": "test error"})


class TestNotificationService:
    
    @mock_sns
    def test_send_success_notification(self, aws_credentials):
        """Test successful notification sending"""
        # Setup
        sns = boto3.client('sns', region_name='us-east-1')
        topic = sns.create_topic(Name='dev-job-notifications')
        topic_arn = topic['TopicArn']
        
        with patch('utils.notifications.NotificationService._get_account_id', return_value='123456789012'):
            service = NotificationService("dev")
            
            job_data = {
                "job_name": "test_job",
                "job_run_id": "run_123",
                "records_processed": 100,
                "output_paths": ["s3://bucket/path/"]
            }
            
            # Test
            service.send_success_notification(job_data)
            
            # In real implementation, would verify SNS message was sent
    
    @mock_sns
    def test_send_failure_notification(self, aws_credentials):
        """Test failure notification sending"""
        # Setup
        sns = boto3.client('sns', region_name='us-east-1')
        topic = sns.create_topic(Name='dev-job-notifications')
        
        with patch('utils.notifications.NotificationService._get_account_id', return_value='123456789012'):
            service = NotificationService("dev")
            
            job_data = {
                "job_name": "test_job",
                "job_run_id": "run_123",
                "error": "Job failed due to validation error"
            }
            
            # Test
            service.send_failure_notification(job_data)


class TestAuditTracker:
    
    @mock_s3
    def test_audit_tracking(self, aws_credentials):
        """Test audit trail tracking"""
        # Setup
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='data-lake-dev-gold')
        
        tracker = AuditTracker("dev", "test_job", "run_123")
        
        # Test various audit events
        tracker.start_job()
        tracker.log_extract(100, ["file1.csv", "file2.csv"])
        tracker.log_transform(95)
        tracker.log_validation({"email": {"null_count": 0}})
        tracker.log_load(["s3://output/path/"])
        tracker.complete_job("SUCCESS")
        
        # Verify files were written to S3
        objects = s3.list_objects_v2(Bucket='data-lake-dev-gold', Prefix='audit_trail/')
        assert 'Contents' in objects
        assert len(objects['Contents']) > 0
    
    def test_local_environment_no_s3(self):
        """Test audit tracker in local environment"""
        tracker = AuditTracker("local", "test_job", "run_123")
        
        # Should not fail, just skip S3 writes
        tracker.start_job()
        tracker.complete_job("SUCCESS")


class TestAPIClient:
    
    def test_api_client_initialization(self):
        """Test API client initialization"""
        client = APIClient("https://api.test.com", "test-token")
        
        assert client.base_url == "https://api.test.com"
        assert client.headers["Authorization"] == "Bearer test-token"
    
    @patch('utils.api_client.httpx.Client')
    def test_successful_get_request(self, mock_client):
        """Test successful GET request"""
        # Setup mock
        mock_response = Mock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.status_code = 200
        mock_client.return_value.get.return_value = mock_response
        
        # Test
        client = APIClient("https://api.test.com", "test-token")
        result = client.get("/endpoint", params={"key": "value"})
        
        # Verify
        assert result == {"data": "test"}
        mock_client.return_value.get.assert_called_once_with("/endpoint", params={"key": "value"})
    
    @patch('utils.api_client.httpx.Client')
    def test_successful_post_request(self, mock_client):
        """Test successful POST request"""
        # Setup mock
        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_response.status_code = 200
        mock_client.return_value.post.return_value = mock_response
        
        # Test
        client = APIClient("https://api.test.com", "test-token")
        result = client.post("/endpoint", json={"data": "test"})
        
        # Verify
        assert result == {"success": True}
        mock_client.return_value.post.assert_called_once_with("/endpoint", json={"data": "test"})
    
    def test_client_close(self):
        """Test client close"""
        client = APIClient("https://api.test.com")
        # Should not raise exception
        client.close()