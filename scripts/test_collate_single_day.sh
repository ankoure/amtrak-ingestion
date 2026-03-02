#!/bin/bash
# Test collation for a single day
# Usage: ./test_collate_single_day.sh [year] [month] [day]
# Example: ./test_collate_single_day.sh 2025 11 15

set -e

YEAR=${1:-2025}
MONTH=${2:-11}
DAY=${3:-15}

API_URL="http://127.0.0.1:8000/amtraker/collate"

echo "Testing collation for ${YEAR}-$(printf '%02d' $MONTH)-$(printf '%02d' $DAY)"
echo ""

echo "Collating Amtrak..."
curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d "{\"year\": $YEAR, \"month\": $MONTH, \"day\": $DAY, \"mode\": \"Amtrak\"}" | jq .

echo ""
echo "Collating VIA..."
curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d "{\"year\": $YEAR, \"month\": $MONTH, \"day\": $DAY, \"mode\": \"VIA\"}" | jq .

echo ""
echo "Done! Check S3 for results:"
echo "  aws s3 ls s3://amtrak-performance/Events-live/daily-Amtrak-data/ --recursive | grep 'Day=$(printf '%02d' $DAY)'"
