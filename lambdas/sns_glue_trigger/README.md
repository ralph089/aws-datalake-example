# SNS to Glue Job Trigger Lambda

This Lambda function subscribes to an external SNS topic that publishes S3 events and triggers the appropriate AWS Glue jobs with the correct parameters.

## Architecture

```
External S3 Bucket → SNS Topic (arn:aws:sns:eu-west-1:xxx:xxx) → Lambda Function → Glue Job
```

## Features

- **Multiple S3 Event Formats**: Supports various SNS message formats
- **Flexible Job Routing**: Maps S3 prefixes to specific Glue jobs
- **Environment Configuration**: Configurable via environment variables
- **Error Handling**: Comprehensive error handling and logging
- **Batch Processing**: Processes multiple SNS records in a single invocation

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ENVIRONMENT` | No | `dev` | Environment name (dev/staging/prod) |
| `GLUE_JOB_MAPPINGS` | No | See defaults | Comma-separated mappings: `prefix1:job1,prefix2:job2` |

## Default Job Mappings

If no `GLUE_JOB_MAPPINGS` environment variable is set, these defaults apply:

- `sales/` → `{env}-sales_etl`
- `customers/` → `{env}-customer_import` 
- `inventory/` → `{env}-inventory_sync`
- `products/` → `{env}-product_catalog`

## Supported SNS Message Formats

The Lambda function can parse S3 events from multiple SNS message formats:

### 1. Direct S3 Event Notification
```json
{
  "Records": [
    {
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {"name": "my-bucket"},
        "object": {"key": "sales/data.csv"}
      }
    }
  ]
}
```

### 2. EventBridge Event Format
```json
{
  "detail-type": "Object Created",
  "detail": {
    "bucket": {"name": "my-bucket"},
    "object": {"key": "sales/data.csv"}
  }
}
```

### 3. Simple Key-Value Format
```json
{
  "bucket": "my-bucket",
  "object_key": "sales/data.csv",
  "event_name": "ObjectCreated"
}
```

## Development

### Setup
```bash
cd lambdas/sns_glue_trigger
uv sync --group dev
```

### Testing
```bash
uv run pytest
```

### Local Testing
Create test events in `tests/test_events/` and run:
```bash
uv run python src/handler.py
```

## Deployment

This Lambda is deployed via Terraform using the `lambda-sns-trigger` module. See the infrastructure configuration for deployment details.

## IAM Permissions Required

The Lambda execution role needs:
- `glue:StartJobRun` - To trigger Glue jobs
- `logs:CreateLogGroup` - For CloudWatch logging  
- `logs:CreateLogStream` - For CloudWatch logging
- `logs:PutLogEvents` - For CloudWatch logging

## Monitoring

- **CloudWatch Logs**: `/aws/lambda/sns-glue-trigger`
- **CloudWatch Metrics**: Lambda invocations, errors, duration
- **Glue Job Metrics**: Job run status and execution metrics