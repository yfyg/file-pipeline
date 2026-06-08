#!/bin/bash
# Run a pipeline test end-to-end:
#   1. Upload a file with a pipeline definition
#   2. Print the job_id
#   3. Wait for the job to complete (or fail)
#   4. Print final status + durations
#
# Usage:
#   ./run_test.sh <file_path> '<pipeline_json>'
#
# Example:
#   ./run_test.sh /tmp/big.json '[
#     {"step":"validate",  "params":{"expected_type":"json"}},
#     {"step":"transform", "params":{"select_columns":["id","name","email","age"],
#                                     "filter_rows":{"column":"age","gt":50}}},
#     {"step":"convert",   "params":{"output_format":"csv"}},
#     {"step":"compress",  "params":{"algorithm":"gzip"}}
#   ]'

set -e

FILE=$1
PIPELINE=$2
API=${API:-http://localhost:8080}

if [ -z "$FILE" ] || [ -z "$PIPELINE" ]; then
    echo "Usage: $0 <file_path> '<pipeline_json>'"
    exit 1
fi

if [ ! -f "$FILE" ]; then
    echo "File not found: $FILE"
    exit 1
fi

echo "=== Uploading $FILE ==="
RESPONSE=$(curl -s -X POST $API/upload \
    -F "file=@$FILE" \
    -F "pipeline=$PIPELINE")

echo "$RESPONSE" | python3 -m json.tool

JOB_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")

if [ -z "$JOB_ID" ] || [ "$JOB_ID" = "None" ]; then
    echo "Upload failed — no job_id returned."
    exit 1
fi

echo ""
echo "=== Job ID: $JOB_ID ==="
echo ""

# Poll for completion — every 1s, up to 5 minutes
echo "=== Waiting for job to finish ==="
for i in $(seq 1 300); do
    STATUS=$(curl -s $API/jobs/$JOB_ID | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
    if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "Final status: $STATUS"
        break
    fi
    sleep 1
done

echo ""
echo "=== Final job details ==="
./check_job.sh $JOB_ID

echo ""
echo "JOB_ID=$JOB_ID"
