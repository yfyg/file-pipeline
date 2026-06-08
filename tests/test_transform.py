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
