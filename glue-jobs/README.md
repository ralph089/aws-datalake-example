# AWS Glue ETL Example Jobs

This package contains example AWS Glue ETL jobs demonstrating best practices for data processing on AWS.

## Jobs Included

- **simple_etl.py**: Basic CSV to Iceberg table transformation
- **api_to_lake.py**: REST API data ingestion with OAuth authentication

## Features

- BaseGlueJob pattern for consistent job structure
- Dual-mode processing (S3 event vs scheduled)
- Built-in logging, notifications, and secrets management
- Pydantic configuration validation
- Local development support

For detailed documentation, see the main project README.