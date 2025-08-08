#!/bin/bash
# Run a Glue job locally using the AWS Glue Docker container

set -e

JOB_NAME=${1:-customer_import}
ENV=${2:-local}
VERBOSE=${VERBOSE:-false}

if [[ "$VERBOSE" == "true" ]]; then
    echo "Running Glue job: $JOB_NAME in environment: $ENV (verbose mode)"
else
    echo "🚀 Running job: $JOB_NAME"
fi

# Ensure container is running (quietly unless verbose)
if [[ "$VERBOSE" == "true" ]]; then
    docker-compose -f glue-jobs/docker-compose.yml up -d glue
    echo "Waiting for Glue container to be ready..."
    sleep 5
else
    docker-compose -f glue-jobs/docker-compose.yml up -d glue > /dev/null 2>&1
    echo "📦 Container ready"
    sleep 5
fi

# Configure Spark logging based on verbose mode
if [[ "$VERBOSE" == "true" ]]; then
    LOG_LEVEL="INFO"
    echo "Running with full verbose output..."
else
    LOG_LEVEL="WARN"
    echo "⚡ Starting job execution..."
fi

# Run the job with proper log4j configuration
if [[ "$VERBOSE" == "true" ]]; then
    # Run with standard Spark logging for debugging
    echo "Running with full verbose output..."
    docker exec glue-local spark-submit \
        --master local[*] \
        --py-files /home/hadoop/workspace/dist/utils.zip \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        /home/hadoop/workspace/src/jobs/${JOB_NAME}.py \
        --JOB_NAME ${JOB_NAME} \
        --env ${ENV}
else
    # Run with clean logging using our custom log4j configuration
    echo "📊 Processing data..."
    docker exec glue-local spark-submit \
        --master local[*] \
        --py-files /home/hadoop/workspace/dist/utils.zip \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/workspace/log4j-local.properties" \
        /home/hadoop/workspace/src/jobs/${JOB_NAME}.py \
        --JOB_NAME ${JOB_NAME} \
        --env ${ENV}
    echo "💾 Job processing completed"
fi

if [[ "$VERBOSE" == "true" ]]; then
    echo "Job completed!"
else
    echo "✅ Job completed successfully!"
fi