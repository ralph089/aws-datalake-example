# Monitoring Guide for AWS Glue ETL Pipeline

## Overview
This guide covers monitoring, alerting, and observability for the AWS Glue ETL pipeline using native AWS services.

## Monitoring Stack
- **AWS CloudWatch**: Metrics, logs, and dashboards
- **AWS X-Ray**: Distributed tracing
- **Amazon SNS**: Notifications and alerting
- **AWS Glue**: Built-in job metrics
- **Custom Audit Trail**: Business-level tracking

## Key Metrics to Monitor

### Job-Level Metrics
- **Success Rate**: Percentage of successful job runs
- **Execution Time**: Average and P95 job duration
- **Data Volume**: Records processed per job
- **Error Rate**: Failed jobs per time period
- **DPU Utilization**: Resource consumption

### Data Quality Metrics
- **Validation Pass Rate**: Percentage of data passing quality checks
- **Data Freshness**: Time since last successful data load
- **Record Counts**: Trend analysis of data volume
- **Schema Changes**: Detection of schema evolution

### Infrastructure Metrics
- **S3 Request Rates**: GET/PUT operations on data lake
- **API Call Latency**: External API response times
- **Dead Letter Queue Depth**: Failed messages accumulation
- **Secrets Manager Access**: Authentication patterns

## CloudWatch Dashboards

### Main ETL Dashboard
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks"],
          ["AWS/Glue", "glue.driver.aggregate.numFailedTasks"]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "us-east-1",
        "title": "Glue Task Success/Failure"
      }
    },
    {
      "type": "log",
      "properties": {
        "query": "SOURCE '/aws-glue/jobs/logs-v2' | fields @timestamp, @message\n| filter @message like /job_completed_successfully/\n| stats count() by bin(5m)",
        "region": "us-east-1",
        "title": "Successful Job Completions (5min bins)"
      }
    }
  ]
}
```

### Custom Metrics Dashboard
Create custom metrics from job logs:

```python
# In base_job.py - add custom metrics
def publish_custom_metrics(self, metric_name: str, value: float, unit: str = "Count"):
    """Publish custom metrics to CloudWatch"""
    if self.environment != "local":
        cloudwatch = boto3.client('cloudwatch')
        
        cloudwatch.put_metric_data(
            Namespace=f'Glue/ETL/{self.environment}',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Dimensions': [
                        {
                            'Name': 'JobName',
                            'Value': self.job_name
                        },
                        {
                            'Name': 'Environment',
                            'Value': self.environment
                        }
                    ]
                }
            ]
        )
```

## Alerting Configuration

### Critical Alerts (PagerDuty/Phone)
```yaml
# CloudFormation/Terraform for critical alarms
JobFailureAlarm:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: CriticalJobFailure-${Environment}
    MetricName: JobFailures
    Namespace: AWS/Glue
    Statistic: Sum
    Threshold: 1
    ComparisonOperator: GreaterThanOrEqualToThreshold
    EvaluationPeriods: 1
    Period: 300
    TreatMissingData: notBreaching
    AlarmActions:
      - !Ref CriticalSNSTopic

DataFreshnessAlarm:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: StaleData-${Environment}
    MetricName: DataAgeHours
    Namespace: Custom/Glue/ETL
    Statistic: Maximum
    Threshold: 25  # 25 hours = more than 1 day
    ComparisonOperator: GreaterThanThreshold
    EvaluationPeriods: 1
```

### Warning Alerts (Email/Slack)
- High validation failure rates (> 10%)
- Increased job execution time (> 2x average)
- API error rates increasing
- DLQ message accumulation

### Info Notifications
- Job completion notifications
- Data volume anomalies
- Weekly summary reports

## Logging Best Practices

### Structured Logging Format
All jobs use structured JSON logging via `structlog`:

```json
{
  "timestamp": "2023-01-15T10:30:00.000Z",
  "level": "INFO",
  "job_name": "customer_import",
  "job_run_id": "jr_abc123",
  "environment": "prod",
  "event": "transform_phase_completed",
  "row_count": 15420,
  "processing_time_seconds": 45.2,
  "trace_id": "1-abc123-def456"
}
```

### Log Correlation
Use correlation IDs to trace requests across services:
- **Job Run ID**: Glue-provided unique identifier
- **Trace ID**: X-Ray trace identifier
- **Correlation ID**: Custom business identifier

### Log Retention Policy
```bash
# Set retention periods per environment
aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/logs-v2 \
  --retention-in-days 30  # Dev: 7 days, Staging: 30 days, Prod: 90 days
```

## X-Ray Tracing Setup

### Configuration
X-Ray is automatically enabled in `base_job.py`:

```python
from aws_xray_sdk.core import xray_recorder, patch_all

# Patch AWS SDK calls
patch_all()

@xray_recorder.capture('extract')
def extract(self):
    # Method is automatically traced
    pass
```

### Custom Segments
Add custom segments for detailed tracing:

```python
def complex_transformation(self, df):
    with xray_recorder.in_subsegment('data_cleaning'):
        df_clean = self.clean_data(df)
    
    with xray_recorder.in_subsegment('api_enrichment'):
        df_enriched = self.enrich_with_api(df_clean)
    
    return df_enriched
```

### Trace Analysis
Use X-Ray console to:
- Identify performance bottlenecks
- Track external API latencies
- Debug cross-service issues
- Analyze error patterns

## Audit Trail Monitoring

### Audit Data Structure
```json
{
  "job_name": "customer_import",
  "job_run_id": "jr_abc123",
  "event_type": "job_completed",
  "timestamp": "2023-01-15T10:30:00.000Z",
  "status": "SUCCESS",
  "records_processed": 15420,
  "output_paths": ["s3://data-lake-prod-silver/customers/"],
  "data_quality_score": 0.95
}
```

### Audit Queries
```sql
-- Job success rate over time
SELECT 
    DATE_TRUNC('day', timestamp) as date,
    job_name,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful_runs,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') * 100.0 / COUNT(*) as success_rate
FROM audit_trail.job_events
WHERE event_type = 'job_completed'
    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp), job_name
ORDER BY date DESC, job_name;

-- Data volume trends
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    job_name,
    AVG(records_processed) as avg_records,
    MIN(records_processed) as min_records,
    MAX(records_processed) as max_records
FROM audit_trail.job_events
WHERE event_type = 'load_completed'
    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', timestamp), job_name
ORDER BY hour DESC, job_name;
```

## Performance Monitoring

### Spark UI Access
```bash
# Port forward to access Spark UI during local development
docker-compose exec glue-local bash
# Spark UI available at localhost:4040 when job is running
```

### Key Performance Indicators
- **Throughput**: Records processed per minute
- **Resource Utilization**: DPU usage patterns
- **Stage Duration**: Time spent in each Spark stage
- **Shuffle Operations**: Data movement between partitions

### Performance Optimization Monitoring
```python
def monitor_performance(self, df: DataFrame, stage: str):
    """Add performance monitoring to job stages"""
    start_time = time.time()
    record_count = df.count()
    
    # Your processing logic here
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = record_count / duration if duration > 0 else 0
    
    self.logger.info("performance_metrics",
                    stage=stage,
                    duration_seconds=duration,
                    record_count=record_count,
                    throughput_records_per_second=throughput)
    
    # Publish to CloudWatch
    self.publish_custom_metrics(f"{stage}_duration", duration, "Seconds")
    self.publish_custom_metrics(f"{stage}_throughput", throughput, "Count/Second")
```

## Data Quality Monitoring

### Quality Score Tracking
```python
def calculate_overall_quality_score(self, validation_results: Dict) -> float:
    """Calculate composite quality score"""
    scores = []
    
    # Completeness score
    completeness_score = sum(
        1 for check in validation_results.get("completeness", {}).values() 
        if check.get("passed", False)
    ) / len(validation_results.get("completeness", {}))
    
    scores.append(completeness_score)
    
    # Add other quality dimensions
    # uniqueness_score, validity_score, consistency_score
    
    overall_score = sum(scores) / len(scores)
    
    # Publish metric
    self.publish_custom_metrics("data_quality_score", overall_score, "None")
    
    return overall_score
```

### Quality Trend Analysis
```sql
-- Weekly quality trend
SELECT 
    DATE_TRUNC('week', timestamp) as week,
    job_name,
    AVG(quality_score) as avg_quality_score,
    MIN(quality_score) as min_quality_score,
    STDDEV(quality_score) as quality_score_stddev
FROM audit_trail.data_quality_events
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY DATE_TRUNC('week', timestamp), job_name
ORDER BY week DESC, job_name;
```

## Operational Monitoring

### Daily Health Check
```bash
#!/bin/bash
# daily-health-check.sh

echo "=== Daily ETL Health Check ==="
echo "Date: $(date)"

# Check job success rates (last 24 hours)
aws logs start-query \
  --log-group-name /aws-glue/jobs/logs-v2 \
  --start-time $(date -d '1 day ago' +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, job_name, @message | filter @message like /job_completed/ | stats count() by job_name'

# Check DLQ depth
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/prod-glue-dlq \
  --attribute-names ApproximateNumberOfMessages

# Check data freshness
aws s3api list-objects-v2 \
  --bucket data-lake-prod-silver \
  --prefix customers/ \
  --query 'sort_by(Contents, &LastModified)[-1].LastModified'
```

### Weekly Report Generation
```python
def generate_weekly_report():
    """Generate automated weekly ETL report"""
    
    report_data = {
        "period": "last_7_days",
        "job_statistics": get_job_statistics(),
        "data_quality_summary": get_quality_summary(),
        "performance_metrics": get_performance_metrics(),
        "error_analysis": get_error_patterns(),
        "recommendations": generate_recommendations()
    }
    
    # Send via SNS
    sns_client.publish(
        TopicArn="arn:aws:sns:us-east-1:123456789012:weekly-etl-report",
        Subject="Weekly ETL Pipeline Report",
        Message=json.dumps(report_data, indent=2)
    )
```

## Troubleshooting Monitoring Issues

### CloudWatch Logs Not Appearing
1. Check IAM permissions for Glue service role
2. Verify log group exists and has correct retention
3. Check if logs are going to wrong region

### X-Ray Traces Missing
1. Verify X-Ray permissions in Glue job IAM role
2. Check if tracing is enabled in job configuration
3. Ensure X-Ray SDK is properly patched

### Custom Metrics Not Showing
1. Verify CloudWatch PutMetricData permissions
2. Check metric namespace and dimensions
3. Ensure metrics are published from correct region

### SNS Notifications Not Received
1. Verify SNS topic permissions and subscriptions
2. Check if emails are in spam folder
3. Validate SNS topic ARN in notification service

## Cost Monitoring

### Glue Job Costs
```bash
# Get Glue DPU usage for cost estimation
aws glue get-job-runs --job-name customer-import-prod \
  --query 'JobRuns[?JobRunState==`SUCCEEDED`][AllocatedCapacity,ExecutionTime]' \
  --output table
```

### S3 Storage Costs
```bash
# Monitor S3 storage growth
aws s3api list-objects-v2 \
  --bucket data-lake-prod-silver \
  --query 'sum(Contents[].Size)' \
  --output text | awk '{print $1/1024/1024/1024 " GB"}'
```

### Cost Optimization Alerts
Set up billing alerts for:
- Glue DPU-hours exceeding budget
- S3 storage growth rate
- API Gateway request charges (if applicable)

## Monitoring Automation

### Infrastructure as Code
```hcl
# Terraform for monitoring resources
resource "aws_cloudwatch_dashboard" "etl_dashboard" {
  dashboard_name = "${var.environment}-glue-etl"
  
  dashboard_body = jsonencode({
    widgets = [
      # Dashboard configuration
    ]
  })
}

resource "aws_cloudwatch_log_group" "glue_logs" {
  name              = "/aws-glue/jobs/logs-v2"
  retention_in_days = var.log_retention_days
}
```

### Automated Remediation
```python
# Lambda function for automated responses
def lambda_handler(event, context):
    """Respond to CloudWatch alarms automatically"""
    
    alarm_name = event['AlarmName']
    
    if 'JobFailure' in alarm_name:
        # Auto-retry failed job
        retry_failed_job(event)
    
    elif 'DataFreshness' in alarm_name:
        # Send escalation notification
        send_escalation_alert(event)
    
    elif 'DLQDepth' in alarm_name:
        # Process DLQ messages
        process_dlq_messages(event)
```

This comprehensive monitoring setup ensures visibility into all aspects of the ETL pipeline, enabling proactive issue detection and resolution.