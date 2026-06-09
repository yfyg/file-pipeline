"""
Filter behavior — the trickiest part of transform.

Covers three scenarios:
1. Normal filter — some rows pass, some don't
2. Filter that eliminates every row (the empty-output edge case)
3. Filter on a non-existent column (silent no-op, per DECISIONS §16)
"""

import json

from tests.conftest import make_csv, make_json, upload_file, wait_for_status


ROWS = [
    {"name": "Alice",   "age": 30},
    {"name": "Bob",     "age": 25},
    {"name": "Charlie", "age": 40},
    {"name": "Diana",   "age": 22},
]
CSV  = make_csv(ROWS)
JSON = make_json(ROWS)


def test_filter_keeps_matching_rows(client):
    """
    Filter age > 28 → Alice (30) and Charlie (40) survive; Bob and Diana drop.
    Row counts reflect this; output file matches.
    """
    pipeline = [
        {"step": "transform", "params": {"select_columns": ["name", "age"],
                                          "filter_rows": {"column": "age", "gt": 28}}},
    ]
    job_id = upload_file(client, CSV, "people.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    transform_step = body["steps"][0]
    assert transform_step["input_rows"]  == 4
    assert transform_step["output_rows"] == 2

    response = client.get(f"/jobs/{job_id}/result")
    csv_text = response.content.decode()
    lines = csv_text.strip().split("\n")
    assert len(lines) == 3  # header + 2 rows

    # Verify the right rows survived
    body_lines = "\n".join(lines[1:])
    assert "Alice"   in body_lines
    assert "Charlie" in body_lines
    assert "Bob"     not in body_lines
    assert "Diana"   not in body_lines


def test_filter_eliminating_all_rows_csv(client):
    """
    Filter that no row satisfies → CSV path: output is a header-only file,
    job still COMPLETED, row counts show input N and output 0.
    """
    pipeline = [
        {"step": "transform", "params": {"select_columns": ["name", "age"],
                                          "filter_rows": {"column": "age", "eq": 999}}},
    ]
    job_id = upload_file(client, CSV, "people.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    transform_step = body["steps"][0]
    assert transform_step["input_rows"]  == 4
    assert transform_step["output_rows"] == 0

    # Output is just the header line
    response = client.get(f"/jobs/{job_id}/result")
    csv_text = response.content.decode()
    lines = csv_text.strip().split("\n")
    assert len(lines) == 1  # header only, no data
    assert "name" in lines[0] and "age" in lines[0]


def test_filter_eliminating_all_rows_then_compress(client):
    """
    Filter drops every row, then gzip on the resulting header-only CSV.
    Confirms the empty-result path goes all the way to a downloadable .gz
    file — important because compress on a small input still has to produce
    a non-empty (~30-byte) gzip stream, which passes the post-step sanity
    check at processor._verify_step_output.

    End-to-end assertions:
    - Job COMPLETED (filter eliminating everything is not an error)
    - transform: 4 in, 0 out
    - Final output is a valid gzip stream (magic bytes 1f 8b)
    - Decompressed content is the original header line only
    """
    import gzip

    pipeline = [
        {"step": "transform", "params": {"select_columns": ["name", "age"],
                                          "filter_rows": {"column": "age", "eq": 999}}},
        {"step": "compress",  "params": {"algorithm": "gzip"}},
    ]
    job_id = upload_file(client, CSV, "people.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    transform_step = next(s for s in body["steps"] if s["type"] == "transform")
    assert transform_step["input_rows"]  == 4
    assert transform_step["output_rows"] == 0

    compress_step = next(s for s in body["steps"] if s["type"] == "compress")
    assert compress_step["status"] == "COMPLETED"

    # Result is a valid gzip stream
    response = client.get(f"/jobs/{job_id}/result")
    assert response.status_code == 200
    raw_bytes = response.content
    assert raw_bytes[:2] == b"\x1f\x8b"   # gzip magic number

    # Decompresses cleanly to a header-only CSV (no data lines)
    decompressed = gzip.decompress(raw_bytes).decode()
    lines = decompressed.strip().split("\n")
    assert len(lines) == 1                      # header only
    assert "name" in lines[0] and "age" in lines[0]


def test_filter_eliminating_all_rows_json_to_csv_fails(client):
    """
    Asymmetry from DECISIONS §16:
    JSON-input → filter-everything → convert-to-CSV → the convert step FAILS
    because there are no items to derive headers from.
    Documented and asserted here so the behavior is locked in.
    """
    pipeline = [
        {"step": "transform", "params": {"select_columns": ["name", "age"],
                                          "filter_rows": {"column": "age", "eq": 999}}},
        {"step": "convert",   "params": {"output_format": "csv"}},
    ]
    job_id = upload_file(client, JSON, "people.json", pipeline).json()["job_id"]

    # Convert step is expected to fail → job FAILED
    body = wait_for_status(client, job_id, expected_statuses=("FAILED",))
    assert body["status"] == "FAILED"

    steps_by_type = {s["type"]: s for s in body["steps"]}
    assert steps_by_type["transform"]["status"] == "COMPLETED"
    assert steps_by_type["transform"]["output_rows"] == 0
    assert steps_by_type["convert"]["status"]   == "FAILED"
    assert "empty" in steps_by_type["convert"]["error"].lower()


def test_filter_gt_drops_non_numeric_values(client):
    """
    Non-numeric values in a column being filtered with gt/lt are DROPPED,
    not silently kept. Locks in the fix from transform.py:_apply_filter —
    matches SQL semantics for `WHERE age > 50` against NULL/non-numeric.
    """
    rows = [
        {"name": "Alice",   "age": "30"},
        {"name": "Bob",     "age": "N/A"},      # non-numeric — should drop
        {"name": "Charlie", "age": "40"},
        {"name": "Diana",   "age": "unknown"},  # non-numeric — should drop
        {"name": "Eve",     "age": "25"},       # numeric but < 28 — should drop
    ]
    csv_bytes = make_csv(rows)

    pipeline = [
        {"step": "transform", "params": {"filter_rows": {"column": "age", "gt": 28}}},
    ]
    job_id = upload_file(client, csv_bytes, "people.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    transform_step = body["steps"][0]
    assert transform_step["input_rows"]  == 5
    # Only Alice and Charlie survive — N/A and unknown get dropped
    assert transform_step["output_rows"] == 2

    response = client.get(f"/jobs/{job_id}/result")
    csv_text = response.content.decode()
    assert "Alice"   in csv_text
    assert "Charlie" in csv_text
    assert "Bob"     not in csv_text
    assert "N/A"     not in csv_text
    assert "Diana"   not in csv_text
    assert "Eve"     not in csv_text


def test_filter_on_nonexistent_column_is_silent_noop(client):
    """
    Per DECISIONS §16: filtering on a column that doesn't exist (typo, or
    dropped by select_columns) is a silent no-op — every row passes through.
    Locks the behavior in so it can't regress silently.
    """
    pipeline = [
        {"step": "transform", "params": {"filter_rows": {"column": "nope", "gt": 0}}},
    ]
    job_id = upload_file(client, CSV, "people.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    transform_step = body["steps"][0]
    assert transform_step["input_rows"]  == 4
    # Every row passes through unchanged → output_rows == input_rows
    assert transform_step["output_rows"] == 4
