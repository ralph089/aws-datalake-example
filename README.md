# AWS Glue ETL Examples

Simple, clean examples demonstrating AWS Glue ETL patterns with Apache Iceberg and best practices.

## Features

- **Simple Configuration**: Minimal setup with Pydantic models
- **Dual-Mode Processing**: S3 event-triggered or scheduled execution
- **API Integration**: OAuth authentication with AWS Secrets Manager
- **Data Lake Storage**: Apache Iceberg tables with ACID transactions
- **Notifications**: SNS alerts for job completion
- **Local Development**: Docker-based testing with sample data

## Quick Start

### Prerequisites
- Python 3.10+
- UV package manager  
- Docker (for local testing)
- AWS CLI configured (for deployment)

### Setup
```bash
# Clone and setup
git clone <repository>
cd aws-glue-example

# Install dependencies
cd glue-jobs && uv sync

# Run example job locally
cd glue-jobs && uv run python src/jobs/simple_etl.py

# Run tests
cd glue-jobs && uv run pytest
```

## Project Structure

```
glue-jobs/
├── src/
│   ├── jobs/
│   │   ├── base_job.py         # Base ETL class
│   │   ├── simple_etl.py       # CSV processing example
│   │   └── api_to_lake.py      # API integration example
│   ├── utils/
│   │   ├── api_client.py       # API client with OAuth
│   │   ├── logging.py          # Structured logging
│   │   ├── notifications.py    # SNS notifications
│   │   └── secrets.py          # AWS Secrets Manager
│   ├── config.py               # Simple configuration
│   └── transformations.py      # Data transformations
├── tests/                      # Unit tests
│   ├── test_simple_etl.py
│   ├── test_api_to_lake.py
│   └── test_transformations.py
├── test_data/                  # Sample data for local testing
└── scripts/
    └── run_local.sh            # Local execution script
```

## Example Jobs

### 1. Simple ETL (`simple_etl.py`)
**Pattern**: Basic CSV to Iceberg table transformation
- Reads customer CSV data
- Cleans email addresses and names
- Validates data quality
- Writes to Iceberg table

**Usage**:
```bash
# Local development
cd glue-jobs && uv run python src/jobs/simple_etl.py

# AWS Glue
--JOB_NAME simple_etl --env dev
```

### 2. API to Lake (`api_to_lake.py`) 
**Pattern**: REST API data ingestion
- Fetches data from REST API with OAuth
- Handles pagination automatically
- Validates API response structure
- Stores in data lake with metadata

**Usage**:
```bash
# Local development (uses mock data)
cd glue-jobs && uv run python src/jobs/api_to_lake.py

# AWS Glue (fetches from real API)
--JOB_NAME api_to_lake --env dev
```

## Key Features

### BaseGlueJob Pattern
All jobs inherit from `BaseGlueJob` providing:
- **ETL Lifecycle**: `extract()` → `transform()` → `validate()` → `load()`
- **Dual Triggers**: Automatic S3 event vs scheduled detection
- **Data Loading**: Smart path resolution and format detection  
- **Validation**: Configurable data quality checks
- **Notifications**: Success/failure SNS alerts

### Configuration
Simple Pydantic configuration in `config.py`:
```python
# AWS Glue arguments automatically parsed
config = create_config_from_glue_args(sys.argv)

# Local development
config = create_local_config("job_name")
```

### API Authentication
Secure credential management with AWS Secrets Manager:
```python
# Get OAuth credentials from secrets
api_client = job.get_api_client("prod/api/credentials")

# Make authenticated requests
response = api_client.get("/api/data")
```

### Data Transformations
Reusable transformation functions:
```python
from transformations import clean_email, standardize_name

# Clean data
df = df.withColumn("email", clean_email(col("email")))
df = df.withColumn("name", standardize_name(col("name")))
```

## Development Commands

```bash
# Setup
cd glue-jobs && uv sync                    # Install dependencies
cd glue-jobs && uv sync --group dev       # Include dev tools

# Code Quality
cd glue-jobs && uv run ruff check .       # Linting
cd glue-jobs && uv run black .            # Formatting  
cd glue-jobs && uv run mypy src/          # Type checking

# Testing
cd glue-jobs && uv run pytest             # Run tests
cd glue-jobs && uv run pytest -v          # Verbose output
cd glue-jobs && uv run pytest --cov       # With coverage

# Local Execution
cd glue-jobs && uv run python src/jobs/simple_etl.py
cd glue-jobs && uv run python src/jobs/api_to_lake.py
```

## AWS Deployment

### Secrets Setup
Create API credentials secret:
```bash
aws secretsmanager create-secret \
  --name "prod/api/credentials" \
  --description "API credentials for data ingestion" \
  --secret-string '{
    "client_id": "your_client_id",
    "client_secret": "your_client_secret", 
    "api_base_url": "https://api.example.com"
  }'
```

### SNS Notifications Setup  
Create notification topic:
```bash
aws sns create-topic --name glue-job-notifications
aws sns subscribe \
  --topic-arn arn:aws:sns:region:account:glue-job-notifications \
  --protocol email \
  --notification-endpoint your-email@example.com
```

### Job Deployment
Package and deploy jobs using AWS Glue console or Terraform.

## Architecture Patterns

### Medallion Architecture
- **Bronze**: Raw data ingestion (CSV, API responses)
- **Silver**: Cleaned and validated data  
- **Gold**: Business-ready aggregated data

### Iceberg Tables
```python
# Atomic writes with ACID guarantees
job.write_to_iceberg(df, "dev_customers_silver")

# Schema evolution and time travel supported
```

### Event-Driven Processing
Jobs automatically detect trigger type:
- **S3 Events**: Process specific uploaded files
- **Scheduled**: Process entire directories  
- **No code changes** required

## Testing Strategy

### Unit Tests
- Mock AWS services with `moto`
- Test data transformations in isolation
- Validate configuration and business logic

### Integration Tests  
- Full job execution in Docker
- End-to-end data pipeline validation
- AWS service integration testing

### Local Testing
```bash
# Uses sample data in test_data/
cd glue-jobs && uv run python src/jobs/simple_etl.py

# Verify outputs without AWS dependencies
```

### S3 Event Testing
Test S3 event-triggered execution locally:
```bash
# Quick interactive testing with multiple scenarios
./glue-jobs/scripts/test_s3_events.sh

# Manual testing - simulate S3 event for simple_etl job
cd glue-jobs && uv run python src/jobs/simple_etl.py \
  --bucket my-data-bucket \
  --object_key bronze/customers/daily_import.csv

# Manual testing - simulate S3 event for api_to_lake job  
cd glue-jobs && uv run python src/jobs/api_to_lake.py \
  --bucket api-bucket \
  --object_key raw/products/products_20240115.json

# Docker-based testing with S3 event simulation
./glue-jobs/scripts/run_local.sh simple_etl local my-bucket path/to/file.csv

# Job automatically detects it's event-triggered and processes specific file
```

## Best Practices

1. **Single Responsibility**: One job per data source or business process
2. **Configuration**: Use Pydantic for type-safe configuration
3. **Error Handling**: Comprehensive logging and notifications  
4. **Testing**: Unit tests for transformations, integration tests for jobs
5. **Security**: Store secrets in AWS Secrets Manager
6. **Monitoring**: Structured logging with correlation IDs

## Extending Examples

1. **Copy** an existing job that matches your use case
2. **Rename** the class and update configuration
3. **Modify** extract/transform/load methods for your data
4. **Add** test data to `test_data/your_job/`
5. **Write** unit tests following existing patterns
6. **Deploy** using same patterns as example jobs

## Troubleshooting

**Local Development Issues**:
- Ensure UV is installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Check Python version: `python --version` (3.10+ required)
- Verify test data exists: `ls test_data/simple_etl/`

**AWS Issues**:
- Check CloudWatch logs for job execution details
- Verify IAM permissions for Glue, S3, Secrets Manager, SNS
- Ensure secrets exist and have correct format

**API Integration**:
- Test credentials with local script first
- Check network connectivity from Glue subnet
- Verify API rate limits and pagination handling

## Support

- Review job logs in CloudWatch
- Check AWS Glue job metrics and errors  
- Create GitHub issue for bugs or enhancements