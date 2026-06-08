"""
Pipeline execution tests: end-to-end, failure handling, conversions, compression.
"""

import gzip
import json
import os

from tests.conftest import make_csv, make_json, upload_file, wait_for_status


SIMPLE_CSV_ROWS = [
    {"name": "Alice",   "email": "alice@example.com",   "age": 30},
    {"name": "Bob",     "email": "bob@example.com",     "age": 25},
    {"name": "Charlie", "email": "charlie@example.com", "age": 40},
]
SIMPLE_CSV  = make_csv(SIMPLE_CSV_ROWS)
SIMPLE_JSON = make_json(SIMPLE_CSV_ROWS)


def test_full_pipeline_end_to_end(client):
    """
    validate → transform → convert → compress.
    Job completes, all steps COMPLETED, output file exists in storage/outputs/.
    """
    pipeline = [
        {"step": "validate",  "params": {"expected_type": "csv"}},
        {"step": "transform", "params": {"select_columns": ["name", "email"]}},
        {"step": "convert",   "params": {"output_format": "json"}},
        {"step": "compress",  "params": {"algorithm": "gzip"}},
    ]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"

    # All 4 steps COMPLETED
    statuses = [s["status"] for s in body["steps"]]
    assert statuses == ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED"]

    # Output file actually exists on disk
    assert body["output_file"] is not None
    output_files = os.listdir("storage/outputs")
    assert len(output_files) == 1
    assert output_files[0].endswith(".gz")

    # And the result endpoint returns it
    response = client.get(f"/jobs/{job_id}/result")
    assert response.status_code == 200
    # Decompress and parse — should be JSON with 3 items having only name+email
    decompressed = gzip.decompress(response.content)
    data = json.loads(decompressed)
    assert len(data) == 3
    assert set(data[0].keys()) == {"name", "email"}


def test_step_failure_marks_remaining_skipped(client):
    """
    A failing step → job FAILED, remaining steps SKIPPED, no output.
    We force failure with a CSV → CSV convert, which raises ValueError
    ("File is already in csv format") deterministically.
    """
    pipeline = [
        {"step": "validate",  "params": {"expected_type": "csv"}},
        {"step": "convert",   "params": {"output_format": "csv"}},   # will fail
        {"step": "compress",  "params": {"algorithm": "gzip"}},      # should be SKIPPED
    ]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("FAILED",))
    assert body["status"] == "FAILED"
    assert body["error"] is not None
    assert "already in csv format" in body["error"].lower()

    steps_by_type = {s["type"]: s for s in body["steps"]}
    assert steps_by_type["validate"]["status"] == "COMPLETED"
    assert steps_by_type["convert"]["status"]  == "FAILED"
    assert steps_by_type["convert"]["error"]   is not None
    assert steps_by_type["compress"]["status"] == "SKIPPED"

    # No final output file should have been produced
    assert body["output_file"] is None


def test_csv_to_json_conversion(client):
    """Convert step CSV → JSON: output is valid JSON array, row count matches."""
    pipeline = [{"step": "convert", "params": {"output_format": "json"}}]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    convert_step = body["steps"][0]
    assert convert_step["input_rows"]  == 3
    assert convert_step["output_rows"] == 3

    response = client.get(f"/jobs/{job_id}/result")
    data = json.loads(response.content)
    assert isinstance(data, list)
    assert len(data) == 3
    # CSV values come through as strings (no schema in CSV) — by-design
    assert data[0]["name"] == "Alice"
    assert data[0]["age"]  == "30"


def test_json_to_csv_conversion(client):
    """Convert step JSON → CSV: output is valid CSV, row count matches."""
    pipeline = [{"step": "convert", "params": {"output_format": "csv"}}]
    job_id = upload_file(client, SIMPLE_JSON, "data.json", pipeline).json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    convert_step = body["steps"][0]
    assert convert_step["input_rows"]  == 3
    assert convert_step["output_rows"] == 3

    response = client.get(f"/jobs/{job_id}/result")
    csv_text = response.content.decode()
    lines = csv_text.strip().split("\n")
    # Header + 3 data rows
    assert len(lines) == 4
    assert "name" in lines[0]
    assert "email" in lines[0]
    assert "age" in lines[0]
    assert "Alice" in lines[1]


def test_compression_on_csv_and_json(client):
    """
    gzip compress works on both CSV and JSON inputs.
    Decompressed bytes match the original (no transform in pipeline).
    """
    # CSV path
    pipeline = [{"step": "compress", "params": {"algorithm": "gzip"}}]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"
    response = client.get(f"/jobs/{job_id}/result")
    assert gzip.decompress(response.content) == SIMPLE_CSV

    # JSON path
    job_id = upload_file(client, SIMPLE_JSON, "data.json", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"
    response = client.get(f"/jobs/{job_id}/result")
    assert gzip.decompress(response.content) == SIMPLE_JSON
