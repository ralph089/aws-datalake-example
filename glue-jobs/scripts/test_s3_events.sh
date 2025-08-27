#!/bin/bash
# Test S3 event-triggered Glue jobs locally
# Provides easy way to simulate S3 events for development and testing

set -e

echo "🧪 S3 Event Testing for AWS Glue Jobs"
echo "===================================="

# Test configurations
TESTS=(
    "simple_etl:my-data-bucket:bronze/customers/daily_import_20240115.csv"
    "simple_etl:processing-bucket:raw/customer_data/batch_001.dat"
    "api_to_lake:api-data-bucket:ingestion/products/products_20240115.json"
    "api_to_lake:lake-bucket:bronze/api_responses/inventory_sync.json"
)

echo "Available test scenarios:"
echo ""
for i in "${!TESTS[@]}"; do
    IFS=':' read -r job bucket key <<< "${TESTS[$i]}"
    echo "  $((i+1)). Job: $job"
    echo "     Bucket: $bucket"
    echo "     Object: $key"
    echo ""
done

echo "Select test scenario (1-${#TESTS[@]}), or press Enter to run all:"
read -r selection

if [[ -z "$selection" ]]; then
    echo "🚀 Running all test scenarios..."
    echo ""
    
    for test in "${TESTS[@]}"; do
        IFS=':' read -r job bucket key <<< "$test"
        echo "📂 Testing $job with S3 event:"
        echo "   s3://$bucket/$key"
        echo ""
        
        # Run the job with S3 event simulation
        cd glue-jobs && uv run python "src/jobs/$job.py" \
            --bucket "$bucket" \
            --object_key "$key"
        
        echo ""
        echo "✅ Test completed for $job"
        echo "----------------------------------------"
        echo ""
    done
    
    echo "🎉 All S3 event tests completed successfully!"
    
elif [[ "$selection" =~ ^[1-9][0-9]*$ ]] && (( selection <= ${#TESTS[@]} )); then
    test_index=$((selection - 1))
    IFS=':' read -r job bucket key <<< "${TESTS[$test_index]}"
    
    echo "🚀 Running selected test scenario..."
    echo ""
    echo "📂 Testing $job with S3 event:"
    echo "   s3://$bucket/$key"
    echo ""
    
    # Run the selected job
    cd glue-jobs && uv run python "src/jobs/$job.py" \
        --bucket "$bucket" \
        --object_key "$key"
    
    echo ""
    echo "✅ Test completed successfully!"
    
else
    echo "❌ Invalid selection. Please choose 1-${#TESTS[@]} or press Enter for all tests."
    exit 1
fi

echo ""
echo "💡 Tips for S3 event testing:"
echo "- Jobs automatically detect event vs scheduled triggers"
echo "- Use different file extensions (.csv, .dat, .json) to test format detection"
echo "- Check logs for 'Trigger type: s3_event' confirmation"
echo "- Event-triggered jobs process single files, scheduled jobs process patterns"