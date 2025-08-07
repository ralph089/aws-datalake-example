# Data Quality Issues Runbook

## Overview
This runbook covers diagnosis and resolution of data quality issues in the Glue ETL pipeline.

## Data Quality Framework
Our jobs use custom data quality checks in `validators/data_quality.py`:
- **Completeness**: Required fields are populated
- **Uniqueness**: No duplicates in key fields
- **Validity**: Data matches expected patterns/ranges
- **Consistency**: Referential integrity maintained

## Common Data Quality Issues

### High Null Percentage
**Symptoms:** Jobs fail with "completeness_check_failed"

**Diagnosis:**
```sql
-- Check null patterns in source data
SELECT 
    column_name,
    COUNT(*) as total_records,
    COUNT(column_name) as non_null_records,
    (COUNT(*) - COUNT(column_name)) * 100.0 / COUNT(*) as null_percentage
FROM bronze_layer.table_name 
GROUP BY column_name;
```

**Solutions:**
1. **Temporary fix** - Adjust threshold in job:
   ```python
   # In custom_validation method
   null_percentage < 10  # Instead of < 5
   ```

2. **Root cause** - Check upstream data sources:
   - Contact data providers about missing data
   - Implement default value strategies
   - Update ETL to handle nulls gracefully

### Duplicate Records
**Symptoms:** Jobs fail with "duplicate_emails_found" or "uniqueness_check_failed"

**Diagnosis:**
```sql
-- Find duplicate records
SELECT email, COUNT(*) as duplicate_count
FROM bronze_layer.customers
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

**Solutions:**
1. **Immediate**: Implement deduplication logic:
   ```python
   # In transform method
   df_deduped = df.dropDuplicates(['email']) \
                  .withColumn("dedup_timestamp", current_timestamp())
   ```

2. **Long-term**: 
   - Investigate source system data quality
   - Implement primary key constraints upstream
   - Add data lineage tracking

### Invalid Data Patterns
**Symptoms:** Jobs fail with "pattern_check_failed", "invalid_emails_found"

**Diagnosis:**
```python
# Add diagnostic logging in job
invalid_emails = df.filter(~col("email").rlike(email_pattern))
invalid_emails.show(20, truncate=False)  # Show examples
```

**Solutions:**
1. **Data cleaning** - Add cleaning transformations:
   ```python
   # Clean email addresses
   df_clean = df.withColumn("email", 
       regexp_replace(col("email"), r'\s+', ''))  # Remove whitespace
   ```

2. **Pattern relaxation** - Adjust validation rules:
   ```python
   # More permissive email pattern
   email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
   ```

### Range Violations
**Symptoms:** Jobs fail with "range_check_failed", negative values detected

**Diagnosis:**
```sql
-- Check value distributions
SELECT 
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    COUNT(*) FILTER (WHERE price < 0) as negative_count,
    COUNT(*) FILTER (WHERE price > 10000) as excessive_count
FROM bronze_layer.products;
```

**Solutions:**
1. **Outlier handling**:
   ```python
   # Cap extreme values
   df_capped = df.withColumn("price",
       when(col("price") < 0, 0)
       .when(col("price") > 10000, 10000)
       .otherwise(col("price")))
   ```

2. **Business rule validation** with upstream teams

### Referential Integrity Issues
**Symptoms:** Orphaned records, foreign key violations

**Diagnosis:**
```sql
-- Find orphaned sales records
SELECT s.transaction_id, s.customer_id
FROM bronze_layer.sales s
LEFT JOIN silver_layer.customers c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

**Solutions:**
1. **Data enrichment** - Fetch missing reference data:
   ```python
   # Left join with broadcast for small lookup tables
   df_enriched = df_sales.join(
       broadcast(df_customers),
       on="customer_id",
       how="left"
   )
   ```

2. **Graceful handling** - Create placeholder records or filter orphans

## Data Profiling & Monitoring

### Automated Data Profiling
Add profiling to jobs for continuous monitoring:

```python
def profile_dataframe(self, df: DataFrame, table_name: str):
    """Generate data profile metrics"""
    profile = {
        "table_name": table_name,
        "record_count": df.count(),
        "column_count": len(df.columns),
        "timestamp": datetime.now().isoformat()
    }
    
    for col_name in df.columns:
        col_stats = {
            "null_count": df.filter(col(col_name).isNull()).count(),
            "distinct_count": df.select(col_name).distinct().count()
        }
        
        # Numeric columns
        if df.schema[col_name].dataType in [IntegerType(), DoubleType(), FloatType()]:
            stats = df.select(
                min(col_name).alias("min"),
                max(col_name).alias("max"),
                avg(col_name).alias("avg")
            ).collect()[0]
            col_stats.update(stats.asDict())
        
        profile[f"column_{col_name}"] = col_stats
    
    # Write profile to S3
    self._write_data_profile(profile)
```

### Quality Metrics Dashboard
Create CloudWatch dashboard with:
- Job success/failure rates by data quality checks
- Data volume trends
- Quality score distributions
- Alert thresholds for quality degradation

### Alerting Setup
```yaml
# CloudWatch Alarms
DataQualityFailureAlarm:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: HighDataQualityFailures
    MetricName: JobFailures
    Namespace: AWS/Glue
    Statistic: Sum
    Threshold: 3
    ComparisonOperator: GreaterThanThreshold
    EvaluationPeriods: 1
```

## Data Quality Testing

### Unit Tests for Validators
```python
def test_email_validation():
    """Test email validation logic"""
    test_data = [
        ("valid@example.com", True),
        ("invalid-email", False),
        ("", False)
    ]
    
    for email, expected in test_data:
        df = spark.createDataFrame([(email,)], ["email"])
        result = DataQualityChecker.check_patterns(
            df, {"email": EMAIL_PATTERN}
        )
        assert result["passed"] == expected
```

### Integration Tests
```python
def test_end_to_end_quality():
    """Test full job with quality checks"""
    # Use test data with known quality issues
    test_data = load_test_data_with_issues()
    
    job = CustomerImportJob("test_job", {"env": "test"})
    
    # Should handle quality issues gracefully
    result = job.run_with_test_data(test_data)
    
    assert result["status"] == "success"
    assert result["quality_score"] > 0.8
```

## Data Quality Remediation

### Batch Remediation Process
For large-scale data quality issues:

1. **Identify scope**:
   ```sql
   SELECT DATE(processed_timestamp) as date, 
          COUNT(*) as affected_records
   FROM silver_layer.table_name
   WHERE quality_score < 0.5
   GROUP BY DATE(processed_timestamp)
   ORDER BY date DESC;
   ```

2. **Create remediation job**:
   ```python
   class DataRemediationJob(BaseGlueJob):
       def extract(self):
           # Get problematic records
           return self.spark.sql("""
               SELECT * FROM silver_layer.customers
               WHERE quality_score < 0.8
               AND processed_timestamp >= '2023-01-01'
           """)
       
       def transform(self, df):
           # Apply remediation rules
           return self.apply_remediation_rules(df)
   ```

3. **Validate remediation**:
   - Test with sample data first
   - Run quality checks on remediated data
   - Compare before/after metrics

### Preventive Measures

#### Schema Evolution Handling
```python
def handle_schema_changes(self, df: DataFrame) -> DataFrame:
    """Handle schema evolution gracefully"""
    expected_columns = self.get_expected_schema()
    current_columns = set(df.columns)
    
    # Add missing columns with defaults
    for col_name, col_type in expected_columns.items():
        if col_name not in current_columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))
    
    # Remove unexpected columns (log for investigation)
    unexpected_cols = current_columns - set(expected_columns.keys())
    if unexpected_cols:
        self.logger.warning("unexpected_columns", columns=list(unexpected_cols))
    
    return df.select(*expected_columns.keys())
```

#### Data Contracts
Implement data contracts with upstream systems:

```yaml
# data-contracts/customers.yml
table: customers
columns:
  customer_id:
    type: string
    required: true
    unique: true
  email:
    type: string
    required: true
    pattern: "^[^@]+@[^@]+\\.[^@]+$"
  created_date:
    type: timestamp
    required: true
    range:
      min: "2020-01-01"
      max: "now + 1 day"
```

## Recovery from Quality Issues

### Quick Recovery (< 1 hour)
1. **Disable strict validation** temporarily:
   ```python
   # In job configuration
   ENABLE_STRICT_VALIDATION = False
   ```

2. **Reprocess with relaxed rules**
3. **Monitor downstream impact**

### Full Recovery (< 4 hours)
1. **Root cause analysis** - identify data quality degradation source
2. **Fix upstream issues** or implement permanent handling
3. **Reprocess affected data** with corrected logic
4. **Validate downstream systems** are updated

## Continuous Improvement

### Quality Metrics Tracking
- Weekly data quality scorecards
- Trend analysis of quality degradation
- Root cause categorization
- Improvement initiative tracking

### Process Enhancements
- Automated anomaly detection in data patterns
- Machine learning models for quality prediction
- Self-healing data pipelines
- Cross-team data quality governance

## Tools and Resources

### SQL Queries for Investigation
```sql
-- Data freshness check
SELECT MAX(processed_timestamp) as latest_data,
       CURRENT_TIMESTAMP - MAX(processed_timestamp) as data_age
FROM silver_layer.table_name;

-- Column-level null analysis
SELECT 
    'column1' as column_name, 
    COUNT(*) - COUNT(column1) as null_count,
    COUNT(*) as total_count
FROM table_name
UNION ALL
SELECT 
    'column2' as column_name,
    COUNT(*) - COUNT(column2) as null_count,
    COUNT(*) as total_count
FROM table_name;
```

### Monitoring Scripts
```bash
# Check recent job quality scores
aws s3 cp s3://data-lake-prod-gold/audit_trail/ - --recursive | \
  grep quality_score | \
  tail -n 100
```

### Useful Spark Operations
```python
# Data sampling for investigation
sample_df = df.sample(0.01, seed=42)  # 1% sample

# Quick data preview
df.describe().show()  # Statistical summary
df.printSchema()      # Column types and nullability
```