#!/bin/bash

set -a
[ -f .env ] && source .env
set +a

# Get Job ID is running
JOB_ID=$(docker compose exec jobmanager ./bin/flink list -r | grep 'RUNNING' | awk '{print $4')

if [ -z "$JOB_ID" ]; then
  echo "Not found any job is running"
  exit 1
fi

echo "Triggering savepoint for Job ID: $JOB_ID"

# Run command savepoint
docker compose exec jobmanager ./bin/flink savepoint $JOB_ID s3a://${TARGET_S3_BUCKET}/flink-savepoints/

