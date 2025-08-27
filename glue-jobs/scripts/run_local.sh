#!/bin/bash
# Run a Glue job locally using the AWS Glue Docker container
# Supports S3 event simulation for testing event-triggered jobs

set -e

JOB_NAME=${1:-simple_etl}
ENV=${2:-local}
BUCKET=${3:-}
OBJECT_KEY=${4:-}
VERBOSE=${VERBOSE:-false}

TRIGGER_TYPE="scheduled"
if [[ -n "$BUCKET" && -n "$OBJECT_KEY" ]]; then
    TRIGGER_TYPE="S3 event"
fi

if [[ "$VERBOSE" == "true" ]]; then
    echo "Running Glue job: $JOB_NAME in environment: $ENV (verbose mode)"
    echo "Trigger type: $TRIGGER_TYPE"
    if [[ "$TRIGGER_TYPE" == "S3 event" ]]; then
        echo "S3 bucket: $BUCKET"
        echo "Object key: $OBJECT_KEY"
    fi
else
    echo "🚀 Running job: $JOB_NAME ($TRIGGER_TYPE)"
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

# Build job arguments
JOB_ARGS="--JOB_NAME ${JOB_NAME} --env ${ENV}"
if [[ -n "$BUCKET" && -n "$OBJECT_KEY" ]]; then
    JOB_ARGS="${JOB_ARGS} --bucket ${BUCKET} --object_key ${OBJECT_KEY}"
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
        ${JOB_ARGS}
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
        ${JOB_ARGS}
    echo "💾 Job processing completed"
fi

if [[ "$VERBOSE" == "true" ]]; then
    echo "Job completed!"
else
    echo "✅ Job completed successfully!"
fi