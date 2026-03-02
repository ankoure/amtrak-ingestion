#!/bin/bash
# Backfill collation from Nov 15, 2025 to Feb 1, 2026
# This script processes all missing days after the timezone bug was fixed

set -e

API_URL="http://127.0.0.1:8001/amtraker/collate"

collate_day() {
    local year=$1
    local month=$2
    local day=$3

    echo "Collating ${year}-$(printf '%02d' $month)-$(printf '%02d' $day)..."

    # Collate Amtrak
    result=$(curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
      -d "{\"year\": $year, \"month\": $month, \"day\": $day, \"mode\": \"Amtrak\"}")
    amtrak_events=$(echo "$result" | jq -r '.events_count // 0')

    # Collate VIA
    result=$(curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
      -d "{\"year\": $year, \"month\": $month, \"day\": $day, \"mode\": \"VIA\"}")
    via_events=$(echo "$result" | jq -r '.events_count // 0')

    echo "  Amtrak: $amtrak_events events, VIA: $via_events events"

    # Small delay to avoid overwhelming the API
    sleep 0.5
}

echo "Starting backfill..."
echo "================================"

# # November 2025 (days 15-30)
# echo ""
# echo "November 2025"
# echo "-------------"
# for day in {15..30}; do
#     collate_day 2025 11 $day
# done

# # December 2025
# echo ""
# echo "December 2025"
# echo "-------------"
# for day in {1..31}; do
#     collate_day 2025 12 $day
# done

# January 2026
echo ""
echo "January 2026"
echo "------------"
for day in {1..31}; do
    collate_day 2026 1 $day
done

# February 2026 (up to yesterday - Feb 1)
echo ""
echo "February 2026"
echo "-------------"
for day in {1..28}; do
    collate_day 2026 2 $day
done

echo ""
echo "================================"
echo "Backfill complete!"
echo ""
echo "Verify with:"
echo "  aws s3 ls s3://amtrak-performance/Events-live/daily-Amtrak-data/ --recursive | grep -oP 'Year=\\d+/Month=\\d+/Day=\\d+' | sort -u"
