# Testing API Data Fetch Job

## Running the Job Locally

```bash
make run-local JOB=api_data_fetch
```

This will:
- Package the job and dependencies
- Start AWS Glue Docker container
- Execute the job with test data from `glue-jobs/tests/test_data/api_response.json`
- Write output to local CSV file

## Viewing Output

After successful execution, check the generated CSV:

```bash
# Find the output file
ls dist/local_output/api_data/

# View the content
head dist/local_output/api_data/part-*.csv
```

Expected output includes all processing metadata columns:
- `job_run_id`: Local timestamp identifier (e.g., `local-2025-08-08T05:38:32.621648`)
- `processed_timestamp`: When the job processed the data
- `processed_by_job`: Job name (`api_data_fetch`)
- `api_endpoint`: Source API endpoint
- `extraction_timestamp`: When data was extracted

## Key Columns

The job adds standard processing metadata and API-specific enrichments to track data lineage and processing history.