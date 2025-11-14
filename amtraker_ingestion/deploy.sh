#!/bin/bash
# Deployment script for amtraker-ingestion

set -e

STAGE=${1:-dev}
LAYER_ARN="arn:aws:lambda:us-east-1:081194948292:layer:amtraker-ingestion-deps:1"

echo "Deploying to stage: $STAGE"
echo "Cleaning deployment cache..."
rm -rf .chalice/deployments/*.zip

echo "Running chalice deploy..."
uv run chalice deploy --stage $STAGE

echo "Attaching Lambda layer to all functions..."
FUNCTIONS=(
    "amtraker-ingestion-${STAGE}-update_gtfs_cache"
    "amtraker-ingestion-${STAGE}-consume_amtraker_api"
    "amtraker-ingestion-${STAGE}-collate_previous_day"
    "amtraker-ingestion-${STAGE}"
)

for func in "${FUNCTIONS[@]}"; do
    echo "  Updating $func..."
    aws lambda update-function-configuration \
        --function-name "$func" \
        --layers "$LAYER_ARN" \
        --no-cli-pager > /dev/null 2>&1 && echo "    ✓ Layer attached" || echo "    ✗ Failed"
done

echo ""
echo "Deployment complete!"
echo "API URL: https://0kyd886cw0.execute-api.us-east-1.amazonaws.com/api/"
echo ""
echo "To view logs: uv run chalice logs --name consume_amtraker_api --stage $STAGE"
