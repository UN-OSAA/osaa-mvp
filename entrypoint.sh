#!/bin/bash
set -e  # Exit on error

# Debug AWS environment variables (masking sensitive values)
echo "=== AWS ENVIRONMENT VARIABLES ==="
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
  echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:0:4}...${AWS_ACCESS_KEY_ID: -4} (length: ${#AWS_ACCESS_KEY_ID})"
else
  echo "AWS_ACCESS_KEY_ID: NOT SET"
fi

if [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:0:1}...${AWS_SECRET_ACCESS_KEY: -4} (length: ${#AWS_SECRET_ACCESS_KEY})"
else
  echo "AWS_SECRET_ACCESS_KEY: NOT SET"
fi

if [ -n "$AWS_SESSION_TOKEN" ]; then
  echo "AWS_SESSION_TOKEN is SET (length: ${#AWS_SESSION_TOKEN})"
else
  echo "AWS_SESSION_TOKEN: NOT SET"
fi

echo "AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"
echo "AWS_ROLE_ARN: $AWS_ROLE_ARN"
echo "=== END ENVIRONMENT DEBUG ==="

case "$1" in
  "debug-aws")
    echo "Debugging AWS environment variables..."
    env | grep -E 'AWS_|S3_'
    echo "Debug complete"
    ;;
  "ingest")
    uv run python -m pipeline.ingest.run
    ;;
  "transform")
    cd sqlMesh
    uv run sqlmesh --gateway "${GATEWAY:-local}" plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
    ;;
  "transform_dry_run")
    export RAW_DATA_DIR=/app/data/raw
    export DRY_RUN_FLG=true

    echo "Start local ingestion"
    uv run python -m pipeline.ingest.run
    echo "End ingestion"

    # Skip SQLMesh if requested
    if [ "${SKIP_SQLMESH:-false}" != "true" ]; then
      echo "Start sqlMesh"
      cd sqlMesh
      uv run sqlmesh --gateway "${GATEWAY:-local}" plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
      echo "End sqlMesh"
    else
      echo "Skipping SQLMesh as requested by SKIP_SQLMESH=true"
    fi
    ;;
  "ui")
    uv run sqlmesh ui --host "0.0.0.0" --port "${UI_PORT:-8080}"
    ;;
  "etl")
    echo "Starting pipeline"

    # Download DB from S3 (or create new if doesn't exist)
    echo "Downloading DB from S3..."
    uv run python -m pipeline.s3_sync.run download

    echo "Start sqlMesh"
    cd sqlMesh
    uv run sqlmesh --gateway "${GATEWAY:-local}" plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
    echo "End sqlMesh"

    # Upload updated DB back to S3
    cd ..  # Return to root directory
    echo "Uploading DB to S3..."
    uv run python -m pipeline.s3_sync.run upload
    ;;
  "config_test")
    uv run python -m pipeline.config_test
    ;;
  "promote")
    echo "Starting promotion from dev to prod..."
    uv run python -m pipeline.s3_promote.run
    echo "Promotion completed"
    ;;
  "env-test")
    echo "Testing AWS credentials with a direct STS call..."
    # Install AWS CLI (may already be installed in your image)
    apt-get update && apt-get install -y awscli
    # Test credentials directly with AWS CLI
    aws sts get-caller-identity
    echo "Credential test completed"
    ;;
  *)
    echo "Error: Invalid command '$1'"
    echo
    echo "Available commands:"
    echo "  ingest       - Run the data ingestion process"
    echo "  transform    - Run SQLMesh transformations"
    echo "  etl          - Run the complete pipeline (ingest + transform + upload)"
    echo "  ui           - Start the SQLMesh UI server"
    echo "  config_test  - Test and display current configuration settings"
    echo "  env-test     - Test AWS credentials directly with AWS CLI"
    echo
    echo "Usage: docker compose run pipeline <command>"
    exit 1
    ;;
esac
