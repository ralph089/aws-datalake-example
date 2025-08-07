#!/bin/bash
# Run a Glue job locally using the AWS Glue Docker container

set -e

JOB_NAME=${1:-customer_import}
ENV=${2:-local}

echo "Running Glue job: $JOB_NAME in environment: $ENV"

# Ensure container is running
docker-compose up -d glue

# Wait for container to be ready
echo "Waiting for Glue container to be ready..."
sleep 5

# Run the job
docker exec -it glue-local spark-submit \
    --master local[*] \
    --py-files /home/glue_user/workspace/dist/utils.zip \
    /home/glue_user/workspace/src/jobs/${JOB_NAME}.py \
    --JOB_NAME ${JOB_NAME} \
    --env ${ENV}

echo "Job completed!"