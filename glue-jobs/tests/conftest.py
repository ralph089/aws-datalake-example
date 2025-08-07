import pytest
import boto3
from moto import mock_s3, mock_secretsmanager, mock_sqs, mock_sns
import json
import os

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture
def s3_client(aws_credentials):
    """Create mocked S3 client"""
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        # Create test buckets
        client.create_bucket(Bucket="test-bronze")
        client.create_bucket(Bucket="test-silver")
        client.create_bucket(Bucket="test-gold")
        client.create_bucket(Bucket="data-lake-dev-bronze")
        client.create_bucket(Bucket="data-lake-dev-silver")
        client.create_bucket(Bucket="data-lake-dev-gold")
        yield client

@pytest.fixture
def secrets_client(aws_credentials):
    """Create mocked Secrets Manager client"""
    with mock_secretsmanager():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        # Create test secret
        client.create_secret(
            Name="dev/api/credentials",
            SecretString=json.dumps({
                "base_url": "https://api.example.com",
                "bearer_token": "test-token-123"
            })
        )
        yield client

@pytest.fixture
def sqs_client(aws_credentials):
    """Create mocked SQS client"""
    with mock_sqs():
        client = boto3.client("sqs", region_name="us-east-1")
        # Create test DLQ
        response = client.create_queue(
            QueueName="dev-glue-dlq",
            Attributes={
                'MessageRetentionPeriod': '1209600',
                'VisibilityTimeout': '300'
            }
        )
        yield client

@pytest.fixture
def sns_client(aws_credentials):
    """Create mocked SNS client"""
    with mock_sns():
        client = boto3.client("sns", region_name="us-east-1")
        # Create test topic
        response = client.create_topic(Name="dev-job-notifications")
        yield client

@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing"""
    return [
        {
            "customer_id": "CUST001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "phone": "5551234567",
            "registration_date": "2023-01-15"
        },
        {
            "customer_id": "CUST002",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@example.com",
            "phone": "(555) 987-6543",
            "registration_date": "2023-02-20"
        },
        {
            "customer_id": "CUST003",
            "first_name": "Bob",
            "last_name": "Johnson",
            "email": "invalid-email",  # Invalid email for testing
            "phone": "555-555-5555",
            "registration_date": "2023-03-10"
        }
    ]

@pytest.fixture
def sample_sales_data():
    """Sample sales data for testing"""
    return [
        {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 2,
            "unit_price": 29.99,
            "total_amount": 59.98,
            "transaction_date": "2023-01-15"
        },
        {
            "transaction_id": "TXN002",
            "customer_id": "CUST002",
            "product_id": "PROD002",
            "quantity": 1,
            "unit_price": 149.99,
            "total_amount": 149.99,
            "transaction_date": "2023-01-16"
        },
        {
            "transaction_id": "TXN003",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 0,  # Invalid quantity for testing
            "unit_price": 29.99,
            "total_amount": 0,
            "transaction_date": "2023-01-17"
        }
    ]

@pytest.fixture
def sample_product_data():
    """Sample product data for testing"""
    return [
        {
            "product_id": "PROD001",
            "product_name": "Wireless Headphones",
            "category": "Electronics > Audio",
            "brand": "TechBrand",
            "price": 29.99,
            "cost": 15.00,
            "sku": "WH-001",
            "in_stock": True
        },
        {
            "product_id": "PROD002",
            "product_name": "Smart Watch",
            "category": "Electronics > Wearables",
            "brand": "SmartTech",
            "price": 149.99,
            "cost": 75.00,
            "sku": "SW-002",
            "in_stock": True
        },
        {
            "product_id": "PROD003",
            "product_name": "Invalid Product",
            "category": "Unknown",
            "brand": "",
            "price": -10.00,  # Invalid price for testing
            "cost": 5.00,
            "sku": "",
            "in_stock": False
        }
    ]

@pytest.fixture
def sample_inventory_data():
    """Sample inventory data for testing"""
    return [
        {
            "product_id": "PROD001",
            "location_id": "LOC001",
            "quantity_on_hand": 150,
            "reorder_point": 50,
            "max_stock_level": 500,
            "last_updated": "2023-01-15"
        },
        {
            "product_id": "PROD002",
            "location_id": "LOC001",
            "quantity_on_hand": 25,  # Below reorder point
            "reorder_point": 30,
            "max_stock_level": 200,
            "last_updated": "2023-01-15"
        }
    ]