#!/bin/bash
# Run integration tests using wheel package (production-like AWS Glue 4.0 setup)
# This script simulates how AWS Glue actually loads dependencies via --extra-py-files

set -e

JOB_NAME=${1:-simple_etl}
ENV=${2:-local}
VERBOSE=${VERBOSE:-false}

if [[ "$VERBOSE" == "true" ]]; then
    echo "Running integration test for job: $JOB_NAME in environment: $ENV (verbose mode)"
else
    echo "🧪 Testing job: $JOB_NAME (wheel-based, production-like)"
fi

# Ensure container is running
if [[ "$VERBOSE" == "true" ]]; then
    docker-compose up -d glue
    echo "Waiting for Glue container to be ready..."
    sleep 5
else
    docker-compose up -d glue > /dev/null 2>&1
    echo "📦 Container ready"
    sleep 5
fi

# Check if wheel exists
if ! ls ../dist/*.whl 1> /dev/null 2>&1; then
    echo "❌ Error: No wheel package found in dist/"
    echo "Run 'make package-jobs' first to build the wheel"
    exit 1
fi

# Install wheel package (simulating --extra-py-files in AWS Glue)
echo "🔧 Installing wheel package (simulating AWS Glue --extra-py-files)..."
if [[ "$VERBOSE" == "true" ]]; then
    docker exec glue-local pip install --user /home/hadoop/workspace/dist/*.whl
else
    docker exec glue-local pip install --user /home/hadoop/workspace/dist/*.whl > /dev/null 2>&1
fi

# Build job arguments
JOB_ARGS="--JOB_NAME ${JOB_NAME} --env ${ENV}"

# Configure logging based on verbose mode
if [[ "$VERBOSE" == "true" ]]; then
    LOG_LEVEL="INFO"
    echo "Running job with wheel dependencies (verbose mode)..."
else
    LOG_LEVEL="WARN"
    echo "⚡ Running job with wheel dependencies..."
fi

# Run the job using spark-submit (production-like execution)
if [[ "$VERBOSE" == "true" ]]; then
    # Run with standard Spark logging for debugging
    echo "Running with full verbose output..."
    docker exec glue-local spark-submit \
        --master local[*] \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        /home/hadoop/workspace/src/jobs/${JOB_NAME}.py \
        ${JOB_ARGS}
else
    # Run with clean logging using our custom log4j configuration
    echo "📊 Processing data with wheel dependencies..."
    docker exec glue-local spark-submit \
        --master local[*] \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        /home/hadoop/workspace/src/jobs/${JOB_NAME}.py \
        ${JOB_ARGS}
    echo "💾 Job processing completed"
fi

if [[ "$VERBOSE" == "true" ]]; then
    echo "Integration test completed!"
else
    echo "✅ Integration test completed successfully!"
fi