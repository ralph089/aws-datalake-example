# S3 Event Processing for Glue Jobs

This document explains how our Glue jobs now support both S3 event-triggered processing and scheduled processing using a single, backward-compatible codebase.

## Overview

Our ETL pipeline now supports **dual-mode operation**:

1. **S3 Event-Triggered**: Process specific files immediately when uploaded to S3
2. **Scheduled Processing**: Process all files in directories on fixed schedules

This is achieved without Lambda functions, using EventBridge Input Transformers to pass S3 event details directly to Glue jobs.

## Architecture

### Event-Triggered Flow
```
S3 Upload → EventBridge (Input Transformer) → Glue Job → Process Specific File
```

### Scheduled Flow (Unchanged)
```
EventBridge Schedule → Glue Job → Process Directory
```

## Key Features

### ✅ Automatic Detection
Jobs automatically detect their trigger type and adapt behavior accordingly:

```python
# In any job's extract() method:
df = self.load_data("sales")  # Same call for both trigger types

# BaseGlueJob automatically:
# - Event-triggered: loads s3://bucket/sales/daily_2024-01-15.csv
# - Scheduled: loads s3://data-lake-prod-bronze/sales/ (all files)
```

### ✅ File Format Support
Supports multiple file formats with auto-detection:
- `.csv` files (with headers and schema inference)
- `.json` files (multiline JSON support)
- Extensible for additional formats

### ✅ Enhanced Logging
Clear distinction between trigger types in all logs:

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "job_name": "sales_etl",
  "trigger_type": "s3_event",
  "source_bucket": "data-lake-prod-bronze",
  "source_object_key": "sales/daily_2024-01-15.csv",
  "event": "job_started"
}
```

## How It Works

### EventBridge Input Transformer

The EventBridge rule now includes an input transformer that extracts S3 event details:

```hcl
# infrastructure/modules/eventbridge/main.tf
input_transformer {
  input_paths = {
    bucket = "$.detail.bucket.name"
    key    = "$.detail.object.key"
  }
  input_template = jsonencode({
    "--bucket"     = "<bucket>"
    "--object_key" = "<key>"
    "--env"        = var.environment
  })
}
```

This passes the specific S3 bucket and object key that triggered the event as job arguments.

### BaseGlueJob Enhancement

The BaseGlueJob constructor now detects S3 event parameters:

```python
# In base_job.py __init__():
self.source_bucket = args.get("bucket")
self.source_object_key = args.get("object_key")
self.is_event_triggered = bool(self.source_bucket and self.source_object_key)
```

### Smart Data Loading

The `load_data()` method automatically chooses the appropriate loading strategy:

```python
def load_data(self, data_type: str, fallback_path: str | None = None) -> DataFrame:
    if self.environment == "local":
        return self._load_local_data(data_type, fallback_path)
    
    if self.is_event_triggered:
        # Load specific file: s3://bucket/path/file.csv
        return self._load_event_file()
    else:
        # Load directory: s3://data-lake-env-bronze/data_type/
        return self._load_directory_data(data_type)
```

## Job Examples

### Sales ETL Job - No Changes Required

```python
class SalesETLJob(BaseGlueJob):
    def extract(self) -> DataFrame:
        # This call now works for both trigger types automatically
        return self.load_data("sales")  # No changes needed!
    
    # ... rest of job unchanged
```

**Event-triggered behavior**:
- File: `s3://data-lake-prod-bronze/sales/daily_2024-01-15.csv`
- Processes: Only this specific file
- Duration: ~5 minutes
- Cost: ~0.17 DPU-hours

**Scheduled behavior**:
- Directory: `s3://data-lake-prod-bronze/sales/`
- Processes: All CSV files in directory
- Duration: ~30 minutes  
- Cost: ~1 DPU-hour

### Customer Import Job - Also Works Unchanged

```python
class CustomerImportJob(BaseGlueJob):
    def extract(self) -> DataFrame:
        fallback_path = "tests/test_data/customer_import/customers.csv"
        return self.load_data("customers", fallback_path)  # No changes needed!
```

## Configuration Examples

### EventBridge S3 Event Rules

```hcl
# infrastructure/environments/dev/main.tf
s3_event_rules = {
  sales_file_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "sales/"
    suffix      = ".csv"
    target_arn  = module.data_platform.glue_job_arns["sales_etl"]
  }
  
  customer_file_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "customers/"
    suffix      = ".csv"
    target_arn  = module.data_platform.glue_job_arns["customer_import"]
  }
  
  # Support different file formats
  product_json_trigger = {
    bucket_name = module.data_platform.s3_buckets["bronze"]
    prefix      = "products/"
    suffix      = ".json"
    target_arn  = module.data_platform.glue_job_arns["product_catalog"]
  }
}
```

### Scheduled Processing (Unchanged)

```hcl
schedules = {
  daily_customer_import = {
    schedule_expression = "cron(0 2 * * ? *)"  # 2 AM daily
    target_arn         = module.data_platform.glue_job_arns["customer_import"]
  }
  
  weekly_sales_summary = {
    schedule_expression = "cron(0 3 ? * MON *)"  # Monday 3 AM
    target_arn         = module.data_platform.glue_job_arns["sales_etl"]
  }
}
```

## File Naming Patterns

The system supports various file naming patterns automatically:

### Sales Data Examples
```bash
# All trigger sales_etl job
s3://data-lake-prod-bronze/sales/daily_2024-01-15.csv
s3://data-lake-prod-bronze/sales/weekly_summary_2024-W03.csv
s3://data-lake-prod-bronze/sales/corrections_2024-01-15_v2.csv
s3://data-lake-prod-bronze/sales/realtime_batch_20240115_143022.csv
```

### Customer Data Examples  
```bash
# All trigger customer_import job
s3://data-lake-prod-bronze/customers/daily_export_20240115.csv
s3://data-lake-prod-bronze/customers/incremental_update_20240115_080000.csv
s3://data-lake-prod-bronze/customers/full_extract_20240115.csv
```

## Cost Benefits

### Event-Triggered Processing
- **Processing Time**: 5-10 minutes per file
- **DPU Usage**: 0.17-0.33 DPU-hours per file
- **S3 Operations**: Reads only the specific uploaded file
- **Latency**: Near real-time processing (< 1 minute from upload)

### Scheduled Processing (Batch)
- **Processing Time**: 30-60 minutes for all files
- **DPU Usage**: 1-2 DPU-hours per run
- **S3 Operations**: Reads all files in directory
- **Latency**: Up to 24 hours depending on schedule

**Cost Savings**: 80-90% reduction in processing costs for event-triggered jobs.

## Monitoring and Observability

### Enhanced CloudWatch Logs

All log entries now include trigger context:

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO",
  "job_name": "sales_etl",
  "job_run_id": "jr_abc123def456",
  "trigger_type": "s3_event",
  "source_bucket": "data-lake-prod-bronze",
  "source_object_key": "sales/daily_2024-01-15.csv",
  "event": "loaded_event_file",
  "row_count": 15420,
  "file_format": "csv"
}
```

### CloudWatch Queries

Query event-triggered jobs:
```sql
fields @timestamp, job_name, source_object_key, event
| filter trigger_type = "s3_event"
| filter event = "job_started"
| sort @timestamp desc
```

Query scheduled jobs:
```sql
fields @timestamp, job_name, event
| filter trigger_type = "scheduled"
| filter event = "job_started"
| sort @timestamp desc
```

### Custom Metrics

Track processing patterns:
```sql
fields @timestamp, trigger_type
| filter event = "job_completed_successfully"
| stats count() by trigger_type, bin(1h)
```

## Testing

### Local Development
Local testing works exactly as before - no changes needed:

```bash
# Test specific job locally
make run-local JOB=sales_etl

# All existing test patterns work unchanged
make test-unit
make test-int
```

### Integration Testing

Test both trigger types:

```python
def test_scheduled_processing():
    """Test traditional directory processing"""
    job = SalesETLJob("test_job", {"env": "test"})
    assert job.trigger_type == "scheduled"
    assert not job.is_event_triggered

def test_event_processing():
    """Test S3 event-specific file processing"""
    job = SalesETLJob("test_job", {
        "env": "test",
        "bucket": "test-bucket",
        "object_key": "sales/daily_2024-01-15.csv"
    })
    assert job.trigger_type == "s3_event"
    assert job.is_event_triggered
```

## Migration Guide

### For Existing Jobs

**No changes required!** Existing jobs work exactly as before:

1. ✅ Scheduled jobs continue running on their schedules
2. ✅ Local development works unchanged
3. ✅ All existing `load_data()` calls work as expected
4. ✅ Test suites pass without modification

### For New Event Processing

To add S3 event processing to a data source:

1. **Add EventBridge Rule** in Terraform:
```hcl
new_data_trigger = {
  bucket_name = module.data_platform.s3_buckets["bronze"]
  prefix      = "new_data_source/"
  suffix      = ".csv"
  target_arn  = module.data_platform.glue_job_arns["new_data_job"]
}
```

2. **Deploy Infrastructure**:
```bash
make deploy-infra ENV=dev ACTION=apply
```

3. **Job Works Automatically** - no code changes needed!

### Rollback Plan

If issues arise, simply remove the input_transformer from EventBridge targets:

```hcl
resource "aws_cloudwatch_event_target" "s3_event_targets" {
  # Remove input_transformer block to revert to original behavior
  # input_transformer { ... }  # Comment out this block
}
```

Jobs will fall back to scheduled directory processing.

## Best Practices

### File Organization
Organize S3 files with clear prefixes for targeted triggering:

```
s3://data-lake-prod-bronze/
├── sales/
│   ├── daily_2024-01-15.csv      ← triggers sales_etl
│   ├── weekly_2024-W03.csv       ← triggers sales_etl
│   └── corrections_2024-01.csv   ← triggers sales_etl
├── customers/
│   ├── daily_export_20240115.csv  ← triggers customer_import
│   └── incremental_20240115.csv   ← triggers customer_import
└── products/
    ├── catalog_update.json        ← triggers product_catalog
    └── pricing_update.json        ← triggers product_catalog
```

### Error Handling
Event-triggered jobs benefit from file-specific error isolation:

```python
def custom_validation(self, df: DataFrame) -> bool:
    if self.is_event_triggered:
        # More strict validation for real-time files
        return self._validate_realtime_file(df)
    else:
        # More lenient for batch processing
        return self._validate_batch_files(df)
```

### Performance Optimization
Event-triggered jobs can use smaller DPU configurations:

```hcl
glue_jobs = {
  sales_etl = {
    max_capacity = 2   # Sufficient for single file processing
    timeout      = 15  # Shorter timeout for single files
  }
}
```

## Troubleshooting

### Common Issues

#### Job Processes All Files Instead of Specific File
**Cause**: EventBridge input transformer not configured
**Solution**: Verify input_transformer block exists in EventBridge target

#### No S3 Event Parameters in Logs
**Cause**: Missing `--bucket` or `--object_key` arguments
**Solution**: Check EventBridge input transformer input_paths and input_template

#### Event Not Triggering Job
**Cause**: S3 event pattern doesn't match uploaded file
**Solution**: Verify prefix/suffix in EventBridge rule matches file path

### Debug Commands

Check recent job runs:
```bash
aws glue get-job-runs --job-name sales-etl-prod \
  --query 'JobRuns[0:5].[Id,JobRunState,StartedOn,Arguments]' \
  --output table
```

Check EventBridge rule invocations:
```bash
aws logs filter-log-events \
  --log-group-name /aws/events/rule/prod-sales-file-trigger \
  --start-time $(date -d '1 hour ago' +%s)000
```

## Future Enhancements

### Potential Improvements
1. **Batch Event Processing**: Process multiple files in single job run
2. **File Validation**: Pre-validate files before triggering jobs
3. **Dynamic Resource Allocation**: Adjust DPU based on file size
4. **Cross-Region Support**: Handle files from multiple regions

### Advanced Patterns
1. **Conditional Processing**: Different logic based on file name patterns
2. **Multi-Source Jobs**: Combine event and scheduled data sources
3. **Pipeline Orchestration**: Chain jobs based on completion events

This dual-mode capability provides the flexibility to handle both real-time data processing and traditional batch operations with a single, maintainable codebase.