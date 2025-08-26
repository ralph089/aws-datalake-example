# Data Lake to API Lambda Function

Lambda function that queries AWS Data Lake (Iceberg tables) via Amazon Athena and sends data to external REST APIs. Supports multiple event triggers and includes comprehensive error handling and retry logic.

## Features

- **Multi-Event Support**: EventBridge schedules, S3 notifications, API Gateway requests
- **Athena Integration**: Query Iceberg tables with automatic result handling
- **OAuth Authentication**: Secure API communication via AWS Secrets Manager
- **Batch Processing**: Configurable batch sizes for large datasets
- **Error Handling**: Comprehensive logging and graceful failure handling
- **Simple Configuration**: Environment-based settings with Pydantic validation

## Quick Start

### Prerequisites

- Python 3.11+
- UV package manager
- AWS CLI configured
- AWS Lambda execution role with appropriate permissions

### Setup

```bash
# Navigate to Lambda project
cd lambdas/data_lake_to_api

# Install dependencies
uv sync --dev

# Run tests
uv run pytest

# Package for deployment
./scripts/package.sh
```

## Configuration

The Lambda function uses environment variables for configuration:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENV` | Environment (local/dev/staging/prod) | `dev` |
| `ATHENA_DATABASE` | Athena database name | `glue_catalog` |
| `ATHENA_WORKGROUP` | Athena workgroup | `primary` |
| `ATHENA_OUTPUT_BUCKET` | S3 bucket for query results | `athena-results-{env}` |
| `API_SECRET_NAME` | Secrets Manager secret name | `{env}/api/credentials` |
| `BATCH_SIZE` | Records per API batch | `100` |
| `MAX_RECORDS` | Maximum records to process | `10000` |

## Event Types

### 1. EventBridge Scheduled Event

Execute on a schedule (daily, hourly, etc.)

```json
{
    "source": "aws.events",
    "detail-type": "Scheduled Event",
    "detail": {
        "table": "dev_customers_silver",
        "date_filter": "yesterday",
        "where_clause": "region = 'US'"
    }
}
```

### 2. S3 Event Notification

Trigger when new data arrives in the Data Lake

```json
{
    "Records": [{
        "s3": {
            "bucket": {"name": "data-lake"},
            "object": {"key": "silver/customers/part-001.parquet"}
        }
    }]
}
```

### 3. API Gateway Request

On-demand data export via REST API

```json
{
    "queryStringParameters": {
        "table": "dev_customers_silver",
        "limit": "100",
        "where_clause": "processed_date >= '2024-01-01'"
    }
}
```

## API Integration

### Secrets Manager Configuration

Store API credentials in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name "dev/api/credentials" \
  --secret-string '{
    "api_base_url": "https://api.example.com",
    "client_id": "your_client_id",
    "client_secret": "your_client_secret",
    "token_endpoint": "/oauth/token"
  }'
```

### API Payload Format

The Lambda sends data in batches with this structure:

```json
{
    "batch_number": 1,
    "total_batches": 3,
    "records": [
        {
            "customer_id": "123",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "processed_timestamp": "2024-01-15T10:00:00Z"
        }
    ]
}
```

## Development Commands

```bash
# Setup development environment
uv sync --dev

# Run linting and formatting
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type checking
uv run mypy src/

# Run tests
uv run pytest
uv run pytest -v --cov=src

# Local testing
uv run python src/handler.py

# Package for deployment
./scripts/package.sh
```

## Example Usage

### Daily Customer Export

```python
# EventBridge rule configured to trigger daily at 9 AM
{
    "source": "aws.events",
    "detail-type": "Scheduled Event", 
    "detail": {
        "table": "prod_customers_silver",
        "date_filter": "yesterday"
    }
}
```

### Real-time Data Sync

```python
# S3 event notification when new data arrives
{
    "Records": [{
        "s3": {
            "bucket": {"name": "production-data-lake"},
            "object": {"key": "silver/orders/2024/01/15/orders.parquet"}
        }
    }]
}
```

### On-demand Export

```bash
# API Gateway request
curl "https://api.yourdomain.com/export?table=dev_products_silver&limit=500"
```

## Testing

### Unit Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test
uv run pytest tests/test_handler.py::TestLambdaHandler::test_scheduled_event_success
```

### Local Testing

```bash
# Test with sample event
uv run python src/handler.py
```

### Mock Services

Tests use `moto` to mock AWS services:
- Amazon Athena
- AWS Secrets Manager
- Amazon S3

## Deployment

### Manual Deployment

```bash
# Package Lambda
./scripts/package.sh

# Upload to S3
aws s3 cp lambda-deployment.zip s3://lambda-artifacts-dev/

# Update Lambda function
aws lambda update-function-code \
  --function-name data-lake-to-api-dev \
  --s3-bucket lambda-artifacts-dev \
  --s3-key lambda-deployment.zip
```

### GitHub Actions

The CI/CD pipeline automatically:
1. Tests code on every push/PR
2. Packages Lambda function
3. Deploys to appropriate environment
4. Comments on PRs with deployment status

## IAM Permissions

Lambda execution role needs:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow", 
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::athena-results-*/*",
                "arn:aws:s3:::data-lake/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:*/api/credentials*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:GetDatabase"
            ],
            "Resource": "*"
        }
    ]
}
```

## Troubleshooting

### Common Issues

**Query Timeout**
- Increase Lambda timeout (max 15 minutes)
- Optimize Athena queries with appropriate partitioning
- Use LIMIT clauses for large datasets

**Authentication Errors**
- Verify Secrets Manager secret format
- Check API credentials are valid
- Ensure proper IAM permissions

**Data Format Issues**
- Verify Iceberg table schema
- Check for schema evolution in tables
- Handle null values in query results

### Monitoring

- **CloudWatch Logs**: Lambda execution logs
- **CloudWatch Metrics**: Lambda duration, errors, invocations
- **X-Ray Tracing**: Enable for detailed request tracking

### Performance Optimization

- Use Athena query result caching
- Implement proper table partitioning
- Batch API calls appropriately
- Consider async processing for large datasets

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EventBridge   │    │   S3 Events     │    │  API Gateway    │
│   Schedules     │    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼──────────────┐
                    │                            │
                    │    Lambda Function         │
                    │  (data-lake-to-api)        │
                    │                            │
                    └─────────────┬──────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │        Amazon Athena       │
                    │     (Query Iceberg         │
                    │       Tables)              │
                    └─────────────┬──────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │      External REST API     │
                    │     (OAuth Protected)      │
                    └────────────────────────────┘
```

This Lambda function bridges your AWS Data Lake with external systems, providing a reliable and scalable data export solution.