# AWS Glue ETL Pipeline

Production-grade ETL pipeline using AWS Glue, Apache Iceberg, and simple data quality validation.

## Architecture

**Data Flow**: CSV Files (S3) → AWS Glue Jobs → Data Lake (Bronze/Silver/Gold) → Analytics

**Technology Stack**:
- **Processing**: AWS Glue 5.0 with Spark 3.5 and Python 3.11
- **Storage**: Apache Iceberg tables on S3
- **Data Quality**: Simple validation
- **Infrastructure**: Terraform
- **CI/CD**: GitHub Actions

## Quick Start

### Prerequisites
- Python 3.11+
- UV package manager
- Docker
- AWS CLI configured
- Terraform

### Setup
```bash
# Setup development environment
make dev-setup

# Run a job locally
make run-local JOB=customer_import

# Run tests
make test
```

## Project Structure

```
├── glue-jobs/                    # PySpark ETL jobs
│   ├── src/jobs/                # ETL job implementations
│   ├── src/transformations/     # Reusable transformations  
│   ├── src/utils/               # Shared utilities
│   ├── great_expectations/      # GX configuration & expectations
│   └── tests/                   # Unit and integration tests
├── infrastructure/              # Terraform configurations
└── runbooks/                    # Operational documentation
```

## ETL Jobs

All jobs inherit from `BaseGlueJob` which provides logging, validation, notifications, and audit trails.

### Available Jobs
- **`customer_import`**: Process customer CSV files with email/phone validation
- **`sales_etl`**: Aggregate sales data with product information
- **`inventory_sync`**: Sync inventory from APIs with change detection  
- **`product_catalog`**: Normalize product data from multiple vendors

## Development Workflow

```bash
# Code quality checks
make lint                # Ruff linting
make type-check         # MyPy type checking
make format             # Black + Ruff formatting

# Testing
make test-unit          # Unit tests with coverage
make test-int           # Integration tests with Docker
make test-watch         # Continuous testing

# Local execution
make run-local JOB=customer_import
make run-all-local      # Run all jobs
```

## Deployment

```bash
# Deploy to environments
make deploy-jobs ENV=dev
make deploy-infra ENV=dev ACTION=plan
make deploy-all ENV=dev

# Environment shortcuts
make dev                # Deploy to dev
make staging            # Deploy to staging
make prod               # Deploy to prod (with confirmation)
```

## Monitoring

Built-in observability:
- **CloudWatch Logs**: Structured JSON logging with X-Ray tracing
- **SNS Notifications**: Job completion alerts
- **Custom Metrics**: Performance and data quality metrics
- **Audit Trail**: Complete data lineage tracking

## Configuration

Uses UV for dependency management with `pyproject.toml`:

```bash
cd glue-jobs && uv sync                    # Install dependencies
cd glue-jobs && uv sync --extra dev       # Development tools
cd glue-jobs && uv add "package>=1.0.0"   # Add new dependency
```

## Troubleshooting

```bash
# Check job logs
make logs JOB=customer_import ENV=dev

# Local debugging
make docker-logs        # Container logs
make docker-shell       # Open shell in container

# View job status
make job-status ENV=dev
```

## Support

- Check [runbooks](runbooks/) for operational issues
- Review common issues in troubleshooting section
- Create GitHub issue for bugs/feature requests