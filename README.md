# AWS Glue ETL Pipeline

A production-grade ETL pipeline using AWS Glue, Apache Iceberg, and Terraform with comprehensive CI/CD, monitoring, and testing capabilities.

## 🏗️ Architecture

### Data Flow
```
CSV Files (S3) → AWS Glue Jobs → Data Lake (Bronze/Silver/Gold) → Analytics/BI
                      ↓
              External APIs (Enrichment)
```

### Medallion Architecture
- **Bronze Layer**: Raw CSV files from various sources
- **Silver Layer**: Cleaned and validated data with Iceberg tables
- **Gold Layer**: Business-ready aggregated data for analytics

### Technology Stack
- **Processing**: AWS Glue 4.0 with PySpark 3.5
- **Storage**: Apache Iceberg tables on S3
- **Infrastructure**: Terraform modules
- **CI/CD**: GitHub Actions
- **Monitoring**: CloudWatch + X-Ray + SNS
- **Testing**: pytest + moto + Docker

## 📁 Project Structure

```
aws-glue-etl/
├── .github/workflows/          # CI/CD pipelines
├── infrastructure/             # Terraform configurations
│   ├── environments/          # Environment-specific configs
│   └── modules/               # Reusable Terraform modules
├── glue-jobs/                 # PySpark ETL jobs
│   ├── src/                   # Source code
│   │   ├── jobs/              # ETL job implementations
│   │   ├── transformations/   # Reusable transformation logic
│   │   ├── validators/        # Data quality checks
│   │   └── utils/             # Shared utilities
│   ├── tests/                 # Unit and integration tests
│   └── scripts/               # Deployment and local run scripts
├── runbooks/                  # Operational documentation
└── Makefile                   # Development commands
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Poetry
- Docker & Docker Compose
- AWS CLI configured
- Terraform 1.5+

### Setup Development Environment
```bash
# Clone and setup
git clone <repository-url>
cd aws-glue-etl
make dev-setup

# Run health check
make health-check
```

### Test Locally
```bash
# Run a single job
make run-local JOB=customer_import

# Run all jobs
make run-all-local

# Run tests
make test
```

## 🎯 Example Jobs

### 1. Customer Import (`customer_import.py`)
Processes customer CSV files with data cleaning and API enrichment.

**Features:**
- Email validation and standardization
- Phone number formatting
- API enrichment (optional)
- Duplicate detection
- Iceberg table output

### 2. Sales ETL (`sales_etl.py`)
Aggregates sales data with product information for analytics.

**Features:**
- Multiple CSV file processing
- Daily/product/customer aggregations
- Business rule calculations
- Partitioned Iceberg tables

### 3. Inventory Sync (`inventory_sync.py`)
Synchronizes inventory data from external APIs with change detection.

**Features:**
- API data fetching with pagination
- Change detection (new/updated/deleted)
- Reorder point calculations
- Current state snapshots

### 4. Product Catalog (`product_catalog.py`)
Standardizes product data from multiple vendors.

**Features:**
- Multi-vendor data normalization
- Category standardization
- Duplicate resolution
- Data quality scoring

## 🛠️ Development Workflow

### Local Development
```bash
# Start development environment
make docker-up

# Make code changes
# ...

# Test changes
make dev-test

# Package and test
make dev-package-test
```

### Code Quality
```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check

# Pre-commit checks
make pre-commit
```

### Testing Strategy
```bash
# Unit tests (with moto for AWS mocking)
make test-unit

# Integration tests (with Docker)
make test-int

# All tests
make test
```

## 🚀 Deployment

### Infrastructure Deployment
```bash
# Plan infrastructure changes
make deploy-infra ENV=dev ACTION=plan

# Deploy infrastructure
make deploy-infra ENV=dev ACTION=apply
```

### Job Deployment
```bash
# Deploy job scripts only
make deploy-jobs ENV=dev

# Deploy everything
make deploy-all ENV=dev
```

### Environment Promotion
```bash
# Deploy to staging
make staging

# Deploy to production (with confirmation)
make prod
```

## 🔄 CI/CD Pipeline

### Automated Workflows
1. **CI Pipeline**: Runs on every push/PR
   - Code linting and formatting
   - Unit tests with coverage
   - Terraform validation

2. **Job Deployment**: 
   - **Pull Requests** → Deploy to dev with PR-specific prefix
   - **Main branch** → Deploy to production
   - **Develop branch** → Deploy to staging

3. **Infrastructure Deployment**: Manual trigger only
   - Separate from job deployments
   - Environment-specific approval gates

### Feature Branch Workflow
1. Create feature branch
2. Make changes and push
3. PR automatically deploys jobs to dev for testing
4. Review and test via AWS Console
5. Merge PR to deploy to production

## 📊 Monitoring & Observability

### Built-in Monitoring
- **CloudWatch Logs**: Structured JSON logging
- **X-Ray Tracing**: Distributed request tracing
- **Custom Metrics**: Job performance and data quality
- **SNS Notifications**: Job completion alerts
- **Audit Trail**: Complete data lineage tracking

### Key Metrics
- Job success/failure rates
- Data processing volumes
- Execution times and performance
- Data quality scores
- API response times

### Dashboards
- Main ETL dashboard in CloudWatch
- Custom metrics for business KPIs
- Error rate and latency trends

## 🛡️ Data Quality

### Validation Framework
- **Completeness**: Required fields populated
- **Uniqueness**: No duplicates in key fields
- **Validity**: Data matches expected patterns
- **Consistency**: Referential integrity maintained

### Quality Checks
```python
# Example usage in jobs
validator = DataQualityChecker()

# Check completeness
completeness = validator.check_completeness(df, ["email", "customer_id"])

# Check patterns
patterns = validator.check_patterns(df, {"email": EMAIL_REGEX})

# Custom validation
if not self.custom_validation(df):
    raise ValueError("Data validation failed")
```

## 🔧 Configuration

### Environment Variables
```bash
# Local development
export AWS_PROFILE=your-profile
export ENVIRONMENT=local
```

### Job Configuration
Jobs are configured via Terraform in `infrastructure/environments/`:

```hcl
glue_jobs = {
  customer_import = {
    script_location = "s3://glue-scripts-dev/jobs/customer_import.py"
    max_capacity    = 2
    timeout         = 60
  }
}
```

## 📚 Operational Runbooks

Detailed operational guides available in `runbooks/`:

- **[Job Failure Recovery](runbooks/job-failure-recovery.md)**: Diagnose and fix job failures
- **[Data Quality Issues](runbooks/data-quality-issues.md)**: Handle data quality problems
- **[Monitoring Guide](runbooks/monitoring-guide.md)**: Comprehensive monitoring setup

## 🔍 Troubleshooting

### Common Issues

#### Job Fails Locally
```bash
# Check container logs
make docker-logs

# Open shell in container
make docker-shell

# Verify package structure
docker exec glue-local ls -la /home/glue_user/workspace/
```

#### Deployment Issues
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify S3 bucket exists
aws s3 ls s3://glue-scripts-dev/

# Check Terraform state
cd infrastructure/environments/dev
terraform show
```

#### Data Quality Failures
```bash
# Check job logs
make logs JOB=customer_import ENV=dev

# View recent job status
make job-status ENV=dev

# Inspect source data
aws s3 cp s3://data-lake-dev-bronze/customers/file.csv - | head -10
```

## 🧪 Testing

### Unit Tests
- Mock AWS services with moto
- Test transformations and validators
- Coverage reporting

### Integration Tests
- Full job execution in Docker
- Real AWS service interaction (dev environment)
- End-to-end data flow validation

### Local Testing
- AWS Glue Docker container
- Sample test data provided
- Output verification

## 📈 Performance Optimization

### Spark Optimization
- Adaptive query execution enabled
- Automatic coalescing of small partitions
- Broadcast joins for small lookup tables

### Iceberg Benefits
- Schema evolution support
- Time travel capabilities
- Optimized file layouts
- ACID transactions

### Resource Management
- DPU allocation per job type
- Automatic scaling based on data volume
- Cost monitoring and alerting

## 🤝 Contributing

### Development Guidelines
1. Follow existing code patterns
2. Add tests for new features
3. Update documentation
4. Run pre-commit checks

### Code Standards
- Python: Black formatting, Ruff linting
- Terraform: Standard formatting
- Type hints required
- Docstrings for public functions

### Pull Request Process
1. Create feature branch
2. Make changes with tests
3. Ensure CI passes
4. Request code review
5. Test in dev environment
6. Merge after approval

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

### Getting Help
- Check the [runbooks](runbooks/) for operational issues
- Review [common issues](#troubleshooting) section
- Create GitHub issue for bugs/feature requests

### Team Contact
- Data Engineering Team: #data-engineering Slack
- On-call: PagerDuty integration for production issues
- Documentation: This README and runbooks/

---

**Happy Data Processing!** 🚀