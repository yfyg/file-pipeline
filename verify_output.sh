#!/bin/bash
# Inspect a job's output and verify it against expectations.
# Works for .csv, .json, .csv.gz, .json.gz.
#
# Usage:
#   ./verify_output.sh <job_id> [expected_input_rows] [expected_output_rows]
#
# All-positional. Pass "-" to skip a specific check.
#
# Examples:
#   ./verify_output.sh <job_id>
#       just inspect — no assertions
#   ./verify_output.sh <job_id> 1000000 483333
#       assert API reports 1000000 input rows and 483333 output rows on the
#       last row-based step, AND assert the file on disk has matching row count
#   ./verify_output.sh <job_id> - 700000
#       only assert output rows; skip input rows check

JOB_ID=$1
EXPECT_IN=$2
EXPECT_OUT=$3

API=${API:-http://localhost:8080}

if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 <job_id> [expected_input_rows] [expected_output_rows]"
    exit 1
fi

# ---------- find output file ----------
OUT_FILE=$(ls storage/outputs/${JOB_ID}_* 2>/dev/null | head -1)
if [ -z "$OUT_FILE" ]; then
    echo "No output file found for job $JOB_ID in storage/outputs/"
    echo "Available outputs:"
    ls storage/outputs/ 2>/dev/null
    exit 1
fi

echo "=== Output file ==="
ls -lh "$OUT_FILE"
echo ""

# ---------- pick reader based on extension ----------
case "$OUT_FILE" in
    *.gz)   READER="gunzip -c \"$OUT_FILE\"" ;;
    *)      READER="cat \"$OUT_FILE\"" ;;
esac

# ---------- format peek ----------
echo "=== Header / first 3 lines ==="
eval $READER | head -3
echo ""
echo "=== Last line ==="
eval $READER | tail -1
echo ""

# ---------- count rows in file ----------
case "$OUT_FILE" in
    *.csv|*.csv.gz)
        # data rows = total lines - header
        TOTAL_LINES=$(eval $READER | wc -l | tr -d ' ')
        FILE_DATA_ROWS=$((TOTAL_LINES - 1))
        FILE_FORMAT="CSV"
        ;;
    *.json|*.json.gz)
        # count items in the top-level array by grepping for object opens
        # (works for our pretty-printed one-object-per-line format)
        FILE_DATA_ROWS=$(eval $READER | grep -c '^{')
        FILE_FORMAT="JSON"
        ;;
    *)
        FILE_DATA_ROWS="?"
        FILE_FORMAT="unknown"
        ;;
esac

echo "=== Row count in output file ==="
echo "Format:    $FILE_FORMAT"
echo "Data rows: $FILE_DATA_ROWS"
echo ""

# ---------- pull row counts from API ----------
echo "=== Row counts from API (per row-based step) ==="
# Get the JSON, extract input_rows/output_rows for each step that has them
API_STEPS=$(curl -s $API/jobs/$JOB_ID)
echo "$API_STEPS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
last_in  = None
last_out = None
for s in data['steps']:
    if s.get('input_rows') is not None or s.get('output_rows') is not None:
        line = f\"  {s['index']}. {s['type']:10}  in={s.get('input_rows')}  out={s.get('output_rows')}\"
        print(line)
        last_in  = s.get('input_rows')
        last_out = s.get('output_rows')
# Print machine-readable hints for the shell to pick up
print(f'__LAST_IN={last_in}')
print(f'__LAST_OUT={last_out}')
" > /tmp/verify_api_$$.txt
cat /tmp/verify_api_$$.txt | grep -v '^__'

API_LAST_IN=$(grep '^__LAST_IN=' /tmp/verify_api_$$.txt | cut -d= -f2)
API_LAST_OUT=$(grep '^__LAST_OUT=' /tmp/verify_api_$$.txt | cut -d= -f2)
rm -f /tmp/verify_api_$$.txt
echo ""

# ---------- assertions ----------
PASS=0
FAIL=0

assert_eq() {
    local label="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  PASS  $label: $actual"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $label: expected $expected, got $actual"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Assertions ==="

# Check API last-step input_rows matches expected
if [ -n "$EXPECT_IN" ] && [ "$EXPECT_IN" != "-" ]; then
    assert_eq "API input_rows  (last row-based step)" "$API_LAST_IN" "$EXPECT_IN"
fi

# Check API last-step output_rows matches expected
if [ -n "$EXPECT_OUT" ] && [ "$EXPECT_OUT" != "-" ]; then
    assert_eq "API output_rows (last row-based step)" "$API_LAST_OUT" "$EXPECT_OUT"
fi

# Cross-check: file row count must match the API's last output_rows
if [ -n "$API_LAST_OUT" ] && [ "$API_LAST_OUT" != "None" ] && [ "$FILE_DATA_ROWS" != "?" ]; then
    assert_eq "File rows == API output_rows" "$FILE_DATA_ROWS" "$API_LAST_OUT"
fi

# If user gave expected_output, also assert the file has that many rows
if [ -n "$EXPECT_OUT" ] && [ "$EXPECT_OUT" != "-" ] && [ "$FILE_DATA_ROWS" != "?" ]; then
    assert_eq "File data rows" "$FILE_DATA_ROWS" "$EXPECT_OUT"
fi

echo ""
echo "=== Summary: $PASS passed, $FAIL failed ==="
[ $FAIL -eq 0 ] && exit 0 || exit 1
