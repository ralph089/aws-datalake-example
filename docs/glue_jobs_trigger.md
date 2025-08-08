# Glue Jobs Triggering Mechanisms

This document explains how AWS Glue jobs are automatically triggered when external services upload data to S3 buckets, and how the scheduling system works.

## Overview

Our ETL pipeline uses **AWS EventBridge** (formerly CloudWatch Events) to automatically trigger Glue jobs based on two mechanisms:

1. **S3 Event-Driven Triggers**: Real-time processing when files are uploaded
2. **Schedule-Based Triggers**: Cron-based batch processing at fixed intervals

## S3 Event-Driven Triggers (Real-time Processing)

### How It Works

When external services upload files to your S3 bronze layer, jobs are automatically triggered through this flow:

1. **External Service Upload**: Service uploads file to S3 bronze bucket
2. **S3 Event Emission**: S3 automatically sends "Object Created" event to EventBridge
3. **Event Pattern Matching**: EventBridge rule matches the event against configured patterns
4. **Job Execution**: EventBridge triggers the appropriate Glue job
5. **Data Processing**: Job processes the file and writes to silver layer

### Configuration Example

From `infrastructure/environments/dev/main.tf`:

```hcl
s3_event_rules = {
  sales_file_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "sales/"           # Only files in sales/ folder
    suffix      = ".csv"             # Only CSV files
    target_arn  = module.data_platform.glue_job_arns["sales_etl"]
  }
}
```

### EventBridge Event Pattern

The EventBridge rule uses this event pattern to match S3 events:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["data-lake-dev-bronze"]
    },
    "object": {
      "key": [{
        "prefix": "sales/",
        "suffix": ".csv"
      }]
    }
  }
}
```

### Practical Example

1. **External System Action**:
   ```bash
   # CRM system uploads daily sales export
   aws s3 cp daily_sales_2024-01-15.csv s3://data-lake-prod-bronze/sales/daily_sales_2024-01-15.csv
   ```

2. **Automatic Trigger Chain**:
   - S3 emits "Object Created" event
   - EventBridge captures event at `2024-01-15T09:30:15Z`
   - Rule matches: `prefix="sales/"` AND `suffix=".csv"`
   - Glue job `sales_etl` starts automatically with `JOB_RUN_ID=jr_abcd1234`

3. **Job Processing**:
   ```python
   # In SalesETLJob.extract()
   df = self.load_data("sales")  # Loads from bronze/sales/ location
   # Process, transform, validate, and load to silver layer
   ```

## Schedule-Based Triggers (Batch Processing)

### Configuration Example

Some jobs run on fixed schedules regardless of data arrival:

```hcl
schedules = {
  daily_customer_import = {
    schedule_expression = "cron(0 2 * * ? *)"    # 2 AM daily
    target_arn         = module.data_platform.glue_job_arns["customer_import"]
  }
  weekly_inventory_sync = {
    schedule_expression = "cron(0 3 ? * MON *)"  # Monday 3 AM
    target_arn         = module.data_platform.glue_job_arns["inventory_sync"]
  }
}
```

### Cron Expression Reference

| Schedule | Cron Expression | Description |
|----------|----------------|-------------|
| Daily at 2 AM | `cron(0 2 * * ? *)` | Every day at 2:00 AM UTC |
| Every Monday at 3 AM | `cron(0 3 ? * MON *)` | Weekly on Monday at 3:00 AM UTC |
| Every 6 hours | `cron(0 */6 * * ? *)` | At minute 0 past every 6th hour |
| Business days at 6 AM | `cron(0 6 ? * MON-FRI *)` | Monday-Friday at 6:00 AM UTC |

## Infrastructure Components

### EventBridge Configuration

The EventBridge module (`infrastructure/modules/eventbridge/main.tf`) creates:

```hcl
# Schedule-based rules
resource "aws_cloudwatch_event_rule" "schedules" {
  for_each = var.schedules

  name                = "${var.environment}-${each.key}"
  description         = "Schedule for ${each.key}"
  schedule_expression = each.value.schedule_expression
}

# S3 event-based rules
resource "aws_cloudwatch_event_rule" "s3_events" {
  for_each = var.s3_event_rules

  name        = "${var.environment}-${each.key}"
  description = "S3 event rule for ${each.key}"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [each.value.bucket_name]
      }
      object = {
        key = [{
          prefix = each.value.prefix
          suffix = each.value.suffix
        }]
      }
    }
  })
}

# Targets that trigger Glue jobs
resource "aws_cloudwatch_event_target" "s3_event_targets" {
  for_each = var.s3_event_rules

  rule      = aws_cloudwatch_event_rule.s3_events[each.key].name
  target_id = "${each.key}-target"
  arn       = each.value.target_arn
  role_arn  = aws_iam_role.eventbridge.arn
}
```

### IAM Permissions

EventBridge requires permission to start Glue jobs:

```hcl
resource "aws_iam_role_policy" "eventbridge" {
  name = "${var.environment}-eventbridge-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ]
        Resource = "*"
      }
    ]
  })
}
```

## Job Trigger Context

When jobs are triggered, they receive context about the trigger:

### Environment Variables Available to Jobs

```python
# In BaseGlueJob.__init__()
self.job_run_id = args.get("JOB_RUN_ID") or f"local-{datetime.now().isoformat()}"
self.environment = args.get("env", "local")

# For S3-triggered jobs, additional context may be available:
self.trigger_source = args.get("trigger_source", "manual")  # "s3_event" or "schedule"
```

### Trigger-Aware Processing

Jobs can adapt behavior based on trigger source:

```python
class SalesETLJob(BaseGlueJob):
    def extract(self) -> DataFrame:
        if self.trigger_source == "s3_event":
            # Process specific file that triggered the job
            # S3 event details could be passed as job parameters
            return self.load_specific_file()
        else:
            # Scheduled run - process all available files
            return self.load_all_pending_files()
```

## Benefits of Event-Driven Architecture

### Real-time Processing
- **Zero Latency**: Jobs start immediately when data arrives
- **No Polling**: Eliminates need to check for new files
- **Resource Efficient**: Only consumes resources when needed

### Scalability
- **Parallel Processing**: Multiple files can trigger concurrent jobs
- **Auto-scaling**: Glue automatically provisions resources per job
- **No Bottlenecks**: EventBridge handles high event volumes

### Reliability
- **Guaranteed Delivery**: EventBridge ensures events reach targets
- **Retry Logic**: Failed job starts are automatically retried
- **Dead Letter Queue**: Failed records are captured for investigation
- **Monitoring**: Complete audit trail of triggers and executions

### Cost Optimization
- **Pay-per-Use**: Only pay for actual job executions
- **No Idle Resources**: No always-on schedulers needed
- **Efficient Resource Usage**: Jobs start with right-sized capacity

## Adding New Triggers

### For New Data Sources

To add triggers for additional data sources, update the Terraform configuration:

```hcl
# In infrastructure/environments/{env}/main.tf
s3_event_rules = {
  # Existing triggers
  sales_file_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "sales/"
    suffix      = ".csv"
    target_arn  = module.data_platform.glue_job_arns["sales_etl"]
  }
  
  # Add new trigger for product catalog
  product_catalog_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "products/"
    suffix      = ".json"
    target_arn  = module.data_platform.glue_job_arns["product_catalog"]
  }
  
  # Add trigger for inventory updates
  inventory_updates_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "inventory/updates/"
    suffix      = ".xml"
    target_arn  = module.data_platform.glue_job_arns["inventory_sync"]
  }
}
```

### For New Schedules

Add schedule-based triggers:

```hcl
schedules = {
  # Existing schedules
  daily_customer_import = {
    schedule_expression = "cron(0 2 * * ? *)"
    target_arn         = module.data_platform.glue_job_arns["customer_import"]
  }
  
  # Add new schedules
  hourly_real_time_sync = {
    schedule_expression = "cron(0 * * * ? *)"  # Every hour
    target_arn         = module.data_platform.glue_job_arns["real_time_sync"]
  }
  
  monthly_data_cleanup = {
    schedule_expression = "cron(0 4 1 * ? *)"  # First day of month at 4 AM
    target_arn         = module.data_platform.glue_job_arns["data_cleanup"]
  }
}
```

## Common Trigger Patterns

### File Type-Specific Processing
```hcl
# Different jobs for different file types
csv_processor = {
  prefix = "raw-data/"
  suffix = ".csv"
  target_arn = module.data_platform.glue_job_arns["csv_processor"]
}

json_processor = {
  prefix = "raw-data/"
  suffix = ".json"
  target_arn = module.data_platform.glue_job_arns["json_processor"]
}
```

### Source System-Specific Processing
```hcl
# Different processing for different source systems
crm_data_processor = {
  prefix = "crm-exports/"
  suffix = ".csv"
  target_arn = module.data_platform.glue_job_arns["crm_processor"]
}

erp_data_processor = {
  prefix = "erp-exports/"
  suffix = ".csv"
  target_arn = module.data_platform.glue_job_arns["erp_processor"]
}
```

### Time-Based Partitioning
```hcl
# Process files based on upload patterns
daily_batch_processor = {
  prefix = "daily-batches/"
  suffix = ".csv"
  target_arn = module.data_platform.glue_job_arns["daily_processor"]
}

real_time_processor = {
  prefix = "streaming/"
  suffix = ".json"
  target_arn = module.data_platform.glue_job_arns["stream_processor"]
}
```

## Monitoring and Troubleshooting

### Viewing Trigger History

```bash
# List recent EventBridge rule invocations
aws logs filter-log-events \
  --log-group-name /aws/events/rule/dev-sales-file-trigger \
  --start-time $(date -d '1 hour ago' +%s)000

# Check Glue job runs triggered by events
aws glue get-job-runs \
  --job-name sales-etl-dev \
  --query 'JobRuns[?JobRunState==`RUNNING` || JobRunState==`SUCCEEDED`][Id,JobRunState,StartedOn]'
```

### Common Issues

#### Job Not Triggered
1. **Check EventBridge Rule**: Verify rule exists and is enabled
2. **Verify Event Pattern**: Ensure file path matches prefix/suffix
3. **Check IAM Permissions**: EventBridge role must have `glue:StartJobRun`
4. **S3 Event Configuration**: Ensure S3 bucket sends events to EventBridge

#### Multiple Triggers for Same File
1. **Review Overlapping Rules**: Check for multiple rules matching same pattern
2. **Implement Idempotency**: Jobs should handle duplicate processing gracefully
3. **Use Job Bookmarks**: Glue job bookmarks prevent reprocessing

#### Failed Job Starts
1. **Check CloudWatch Logs**: `/aws/events/rule/{rule-name}`
2. **Verify Glue Job Configuration**: Ensure job exists and is deployable
3. **Check Resource Limits**: Verify account limits for concurrent Glue jobs

## Best Practices

### Trigger Design
- **Specific Patterns**: Use precise prefix/suffix patterns to avoid unwanted triggers
- **Single Responsibility**: One trigger per job type for clearer debugging
- **Environment Isolation**: Separate triggers per environment (dev/staging/prod)

### Job Design
- **Idempotent Processing**: Jobs should produce same result if run multiple times
- **Error Handling**: Robust error handling for malformed or unexpected files
- **Logging**: Comprehensive logging of trigger context and processing details

### Monitoring
- **CloudWatch Alarms**: Alert on job failures or excessive trigger rates
- **Success Metrics**: Track successful trigger-to-completion rates
- **Performance Monitoring**: Monitor job execution times for trigger-based runs

### Cost Management
- **Right-size Jobs**: Configure appropriate DPU capacity for expected data volumes
- **Job Timeouts**: Set reasonable timeouts to prevent runaway jobs
- **Schedule Optimization**: Avoid overlapping scheduled jobs during peak hours

This event-driven architecture ensures your ETL pipeline responds immediately to new data while maintaining reliability, scalability, and cost-effectiveness.