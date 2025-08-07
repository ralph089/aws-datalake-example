# Glue Job Failure Recovery Runbook

## Overview
This runbook provides step-by-step instructions for diagnosing and recovering from AWS Glue job failures.

## Prerequisites
- AWS CLI configured with appropriate credentials
- Access to CloudWatch Logs and AWS Glue Console
- Terraform installed locally (for infrastructure changes)

## Quick Diagnosis

### 1. Check CloudWatch Logs
```bash
# Get recent job runs
aws glue get-job-runs --job-name <JOB_NAME> --max-results 5

# View logs for specific run
aws logs tail /aws-glue/jobs/logs-v2 --follow --filter-pattern <JOB_RUN_ID>
```

### 2. Check Dead Letter Queue
```bash
# List messages in DLQ
aws sqs receive-message \
  --queue-url https://sqs.<region>.amazonaws.com/<account>/glue-dlq-<env> \
  --max-number-of-messages 10
```

### 3. Check X-Ray Traces
- Navigate to AWS X-Ray Console
- Filter by: `service("AWS::Glue")`
- Look for error segments and high latency

## Common Issues & Solutions

### Data Validation Failure
**Symptoms:** Job fails with "Data validation failed" or "validation_phase_failed"

**Diagnosis Steps:**
1. Check validation logs in CloudWatch for specific issues
2. Review source data quality in bronze layer
3. Check data quality metrics in job logs

**Solutions:**
- **Bad source data**: Fix upstream data and re-run
- **Strict validation rules**: 
  ```python
  # Adjust validation thresholds in validators/data_quality.py
  null_percentage < 10  # Instead of < 5
  ```
- **Schema changes**: Update job schemas to handle new fields

### API Connection Failure
**Symptoms:** HTTP errors, timeout exceptions, "api_error" in logs

**Diagnosis Steps:**
1. Verify API credentials in Secrets Manager:
   ```bash
   aws secretsmanager get-secret-value --secret-id <env>/api/credentials
   ```
2. Check API service status and rate limits
3. Review retry configuration in logs

**Solutions:**
- **Invalid credentials**: Update secrets in AWS Secrets Manager
- **Rate limiting**: Increase retry delays or implement exponential backoff
- **Service downtime**: Re-run job after service recovery

### Out of Memory Error
**Symptoms:** "Container killed due to memory", "OutOfMemoryError", job timeout

**Diagnosis Steps:**
1. Check Spark UI metrics for memory usage
2. Review data volume in source files
3. Check partition distribution

**Solutions:**
1. **Increase DPUs** in Terraform:
   ```hcl
   # infrastructure/modules/glue-jobs/main.tf
   max_capacity = 10  # Increase from default 2
   ```
2. **Optimize transformations**:
   - Add `.coalesce()` or `.repartition()` calls
   - Process data in smaller batches
   - Use broadcast joins for small lookup tables

### Schema Mismatch
**Symptoms:** "Column not found", "AnalysisException", type casting errors

**Diagnosis Steps:**
1. Compare current schema with expected schema
2. Check source file changes or new columns
3. Review schema evolution settings

**Solutions:**
- **New columns**: Update job schemas and transformations
- **Missing columns**: Add default values or make fields optional
- **Type changes**: Add explicit casting in transformations

### Iceberg Table Issues
**Symptoms:** "Table not found", "IcebergException", write failures

**Diagnosis Steps:**
1. Check Glue Catalog for table existence
2. Verify S3 permissions and bucket access
3. Review Iceberg configuration in Spark

**Solutions:**
- **Missing table**: Use `createOrReplace()` instead of `append()`
- **Permission issues**: Update IAM roles for S3 access
- **Configuration**: Verify Iceberg catalog settings in base_job.py

## Recovery Procedures

### Re-running Failed Jobs

#### From AWS Console
1. Navigate to AWS Glue > Jobs
2. Select the failed job
3. Click "Run job" with same parameters

#### From CLI
```bash
aws glue start-job-run \
  --job-name <JOB_NAME> \
  --arguments '{"--env":"<ENV>","--JOB_NAME":"<JOB_NAME>"}'
```

#### From Local Development
```bash
# Test with sample data locally first
make run-local JOB=customer_import

# Then deploy and run in AWS
make deploy-jobs ENV=dev
```

### Data Reprocessing
If data corruption occurred, follow these steps:

1. **Identify affected data**:
   ```sql
   -- Query audit trail to find affected records
   SELECT * FROM gold_layer.audit_trail 
   WHERE job_run_id = '<FAILED_RUN_ID>' 
   AND event_type = 'load_completed';
   ```

2. **Clean corrupted data**:
   ```bash
   # Remove corrupted files from S3
   aws s3 rm s3://data-lake-<env>-silver/customers/ --recursive
   ```

3. **Re-run from bronze layer**:
   ```bash
   aws glue start-job-run --job-name customer_import-<env>
   ```

### Testing Changes

#### Deploy to Dev from Feature Branch
1. Create feature branch with fixes:
   ```bash
   git checkout -b fix/validation-issue
   # Make changes
   git push origin fix/validation-issue
   ```

2. Create PR - jobs automatically deploy to dev with feature prefix

3. Test with sample data:
   ```bash
   # Jobs use PR-specific scripts
   # Check CloudWatch logs for results
   ```

4. Merge PR after validation

#### Manual Testing
```bash
# Run locally with test data
make run-local JOB=customer_import

# Check output
ls -la dist/local_output/
```

## Monitoring After Recovery

### Health Checks
1. **Job Metrics**: Check CloudWatch dashboard for job success rates
2. **Data Quality**: Verify output data in silver/gold layers:
   ```sql
   SELECT COUNT(*), COUNT(DISTINCT id) 
   FROM silver.customers
   WHERE processed_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR;
   ```
3. **Audit Trail**: Review audit logs for completeness
4. **SNS Notifications**: Confirm success notifications were sent

### Performance Monitoring
- Monitor job execution times
- Check DPU utilization in CloudWatch
- Review X-Ray traces for bottlenecks
- Monitor S3 request patterns

## Prevention Strategies

### Data Quality
- Implement data contracts with upstream systems
- Add data profiling to detect schema changes
- Set up alerting on data quality degradation

### Infrastructure
- Use infrastructure as code for consistent deployments
- Implement automated testing in CI/CD pipeline
- Set up monitoring for all job dependencies

### Operations
- Regular review of failed job patterns
- Proactive capacity planning for data growth
- Documentation updates for new failure patterns

## Escalation

### Level 1: Self-Service (< 30 minutes)
- Check common issues above
- Review recent deployments
- Re-run job if transient failure

### Level 2: Team Support (< 2 hours)
- Create incident ticket with:
  - Job name and run ID
  - Error messages and logs
  - Steps already attempted
- Notify data engineering team
- Consider temporary manual processing

### Level 3: Critical Incident (< 4 hours)
- Page on-call engineer if production impact
- Set up incident bridge
- Implement emergency workarounds
- Post-incident review and documentation

## Common CLI Commands Reference

```bash
# Job management
aws glue get-job --job-name <JOB_NAME>
aws glue get-job-runs --job-name <JOB_NAME>
aws glue stop-job-run --job-name <JOB_NAME> --job-run-id <RUN_ID>

# Logs
aws logs describe-log-groups --log-group-name-prefix /aws-glue
aws logs get-log-events --log-group-name <GROUP> --log-stream-name <STREAM>

# S3 data inspection
aws s3 ls s3://data-lake-<env>-bronze/ --recursive
aws s3 cp s3://data-lake-<env>-bronze/customers/file.csv - | head -n 10

# Secrets management
aws secretsmanager list-secrets
aws secretsmanager get-secret-value --secret-id <env>/api/credentials
```