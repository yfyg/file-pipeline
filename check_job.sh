#!/bin/bash
JOB_ID=$1

if [ -z "$JOB_ID" ]; then
    echo "Usage: ./check_job.sh <job_id>"
    exit 1
fi

echo "Checking job: $JOB_ID"
echo "================================"

curl -s http://localhost:8080/jobs/$JOB_ID | python3 -c "
import json, sys

data = json.load(sys.stdin)

print(f'Status:    {data[\"status\"]}')
print(f'Progress:  {data[\"overall_progress\"]}')
print(f'Error:     {data.get(\"error\") or \"none\"}')
print(f'Duration:  {data.get(\"duration_seconds\") or \"running...\"}s')
print()
print('Steps:')
seen = set()
for step in data['steps']:
    key = (step['index'], step['type'])
    if key not in seen:
        seen.add(key)
        status_icon = {
            'COMPLETED': '✅',
            'FAILED':    '❌',
            'RUNNING':   '⏳',
            'PENDING':   '⏸️',
            'SKIPPED':   '⏭️'
        }.get(step['status'], '❓')
        print(f'  {step[\"index\"]}. {step[\"type\"]:12} {status_icon} {step[\"status\"]}')
        if step.get('error'):
            print(f'     Error: {step[\"error\"]}')
        if step.get('duration_seconds'):
            print(f'     Duration: {step[\"duration_seconds\"]}s')
        # Show row counts when present (transform, convert produce them)
        if step.get('input_rows') is not None or step.get('output_rows') is not None:
            in_n  = step.get('input_rows')
            out_n = step.get('output_rows')
            print(f'     Rows: {in_n} in -> {out_n} out')
"
