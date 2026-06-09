#!/bin/bash
# Dump the entire pipeline DB joined into one block per step.
# Usage: ./dump_db.sh  [path/to/pipeline.db]
#
# Default DB location is storage/pipeline.db (relative to repo root).

DB=${1:-storage/pipeline.db}

if [ ! -f "$DB" ]; then
    echo "DB not found at: $DB"
    exit 1
fi

sqlite3 "$DB" << 'EOF'
.headers on
.mode line
SELECT
  s.step_index           AS step_index,
  s.step_type            AS step_type,
  s.status               AS step_status,
  s.duration             AS step_duration,
  s.input_rows           AS step_input_rows,
  s.output_rows          AS step_output_rows,
  s.error_message        AS step_error,
  s.started_at           AS step_started_at,
  s.completed_at         AS step_completed_at,
  j.id                   AS job_id,
  j.status               AS job_status,
  j.current_step_index   AS job_current_step,
  j.error_message        AS job_error,
  j.created_at           AS job_created_at,
  j.started_at           AS job_started_at,
  j.completed_at         AS job_completed_at,
  jin.storage_path       AS job_input_path,
  jin.size               AS job_input_size,
  jin.original_filename  AS job_input_name,
  jout.storage_path      AS job_output_path,
  jout.size              AS job_output_size,
  jout.original_filename AS job_output_name,
  sin.storage_path       AS step_input_path,
  sout.storage_path      AS step_output_path
FROM job_steps s
JOIN jobs            j    ON j.id   = s.job_id
LEFT JOIN file_references jin  ON jin.id  = j.input_file_id
LEFT JOIN file_references jout ON jout.id = j.output_file_id
LEFT JOIN file_references sin  ON sin.id  = s.input_file_id
LEFT JOIN file_references sout ON sout.id = s.output_file_id
ORDER BY j.created_at, s.step_index;
EOF
