#!/bin/bash
#
# Demo: 90MB CSV → transform (keep 90%) → convert to JSON
# Measures: input size, each step duration, output size, growth ratio.
#
# Requires the stack to be up: docker compose up
#
# Usage: ./run_big_demo.sh

set -e
cd "$(dirname "$0")"

API=${API:-http://localhost:8080}
INPUT=/tmp/big90.csv

echo "===== Step 1: Generate ~90MB CSV ====="
# 1.3M rows of the standard 7-column shape lands around 90MB.
# Age formula 20 + (i % 60) → ages 20..79.
# Filter age > 25 keeps ages 26..79 = 54 of every 60 = 90.0%
python3 -c "
import csv
with open('$INPUT', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['id','name','email','age','city','country','score'])
    for i in range(1_300_000):
        w.writerow([i, f'User{i}', f'user{i}@example.com', 20 + (i % 60),
                    f'City{i%100}', f'Country{i%50}', i * 1.5])
print('done')
"

INPUT_BYTES=$(stat -f%z "$INPUT" 2>/dev/null || stat -c%s "$INPUT")
INPUT_MB=$(echo "scale=2; $INPUT_BYTES / 1024 / 1024" | bc)
echo "Input file: $INPUT_MB MB ($INPUT_BYTES bytes)"
echo ""

echo "===== Step 2: Sanity-check filter expectation ====="
# How many rows survive age > 25?
SURVIVORS=$(awk -F',' 'NR > 1 && $4 > 25 { c++ } END { print c }' "$INPUT")
echo "Rows surviving 'age > 25': $SURVIVORS / 1300000"
echo ""

echo "===== Step 3: Upload + run pipeline ====="
# transform keeps all 7 columns (so JSON output reflects the full row shape)
# and filters age > 25 (~90% kept)
RESPONSE=$(curl -s -X POST "$API/upload" \
  -F "file=@$INPUT" \
  -F 'pipeline=[
    {"step":"validate", "params":{"expected_type":"csv"}},
    {"step":"transform","params":{"filter_rows":{"column":"age","gt":25}}},
    {"step":"convert",  "params":{"output_format":"json"}}
  ]')
echo "$RESPONSE" | python3 -m json.tool

JOB_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")
echo ""

if [ -z "$JOB_ID" ] || [ "$JOB_ID" = "None" ]; then
    echo "Upload rejected (likely the 100MB cap). Status above explains why."
    exit 1
fi

echo "===== Step 4: Poll until done ====="
for i in $(seq 1 600); do
    STATUS=$(curl -s "$API/jobs/$JOB_ID" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
    if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        break
    fi
    sleep 1
done
echo "Final status: $STATUS"
echo ""

echo "===== Step 5: Step durations + row counts ====="
./check_job.sh "$JOB_ID"
echo ""

if [ "$STATUS" = "COMPLETED" ]; then
    echo "===== Step 6: Output file sizes ====="
    OUT_FILE=$(ls storage/outputs/${JOB_ID}_* | head -1)
    OUT_BYTES=$(stat -f%z "$OUT_FILE" 2>/dev/null || stat -c%s "$OUT_FILE")
    OUT_MB=$(echo "scale=2; $OUT_BYTES / 1024 / 1024" | bc)
    RATIO=$(echo "scale=2; $OUT_BYTES / $INPUT_BYTES" | bc)
    echo "Input  CSV:  $INPUT_MB MB  ($INPUT_BYTES bytes, 1300000 rows)"
    echo "Output JSON: $OUT_MB MB  ($OUT_BYTES bytes, $SURVIVORS rows)"
    echo "Growth ratio (output / input): ${RATIO}x"
    echo ""
    echo "Even with ~10% fewer rows, JSON is bigger than CSV — column names"
    echo "and quotes are repeated on every row."
    echo ""

    echo "===== Step 7: Worker memory while running ====="
    echo "Run this in another terminal during the next test:"
    echo "    docker stats \$(docker compose ps -q worker)"
    echo "Memory should stay roughly flat — streaming holds even with a big"
    echo "JSON output larger than the input."
fi

echo ""
echo "===== Done ====="
