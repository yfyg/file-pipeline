"""
Slow / large-file tests.

These are excluded from the default `pytest` run (see pytest.ini). Run with:

    pytest -m slow

Each test generates a multi-MB file in memory or in the test workspace,
runs it through the pipeline, and asserts both correctness and "the
output isn't catastrophically larger than expected."
"""

import csv
import io
import json
import os

import pytest

from tests.conftest import upload_file, wait_for_status


def _make_big_csv_bytes(num_rows: int) -> bytes:
    """
    Build a CSV of `num_rows` rows with the standard 7-column shape used
    throughout this codebase. Returns bytes. Streamed via StringIO so we
    don't build the whole thing as a Python string + then encode again.
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "name", "email", "age", "city", "country", "score"])
    for i in range(num_rows):
        w.writerow([
            i,
            f"User{i}",
            f"user{i}@example.com",
            20 + (i % 60),                 # ages cycle 20..79
            f"City{i % 100}",
            f"Country{i % 50}",
            i * 1.5,
        ])
    return buf.getvalue().encode()


@pytest.mark.slow
def test_90mb_csv_transform_then_convert_to_json(client):
    """
    Demonstrates: a near-cap CSV input, filter that keeps ~90% of rows,
    convert to JSON. The JSON output will be LARGER than the input CSV
    despite having fewer rows — column names + quotes repeat per row.
    Streaming must hold throughout.

    Assertions:
    - Job COMPLETED
    - transform reports input_rows=1_300_000, output_rows=expected
    - convert preserves the row count (no drops)
    - Output JSON is valid (starts with `[`, ends with `]`)
    - JSON output size is BIGGER than the CSV input (the property under test)
    - File row count matches what the API claims
    """
    num_rows = 1_300_000
    csv_bytes = _make_big_csv_bytes(num_rows)
    csv_size  = len(csv_bytes)
    # Sanity: this should be roughly 90MB. If the row shape ever changes
    # significantly this assertion catches us re-tuning silently.
    assert 60 * 1024 * 1024 < csv_size < 100 * 1024 * 1024, (
        f"CSV is {csv_size / 1024 / 1024:.1f} MB — expected 60-100 MB. "
        f"Has the row shape changed?"
    )

    # Filter age > 25 keeps i%60 in {6..59} = 54 of every 60 values.
    # 1_300_000 rows ÷ 60 = 21_666 full cycles (×54 survivors) + 40 remainder.
    # Remainder i%60 ∈ {0..39}: 34 survive (i%60 ∈ {6..39}).
    # Total: 21_666 * 54 + 34 = 1_169_998
    expected_survivors = 21_666 * 54 + 34
    assert expected_survivors == 1_169_998   # sanity for the test math

    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "transform",
         "params": {"filter_rows": {"column": "age", "gt": 25}}},
        {"step": "convert",  "params": {"output_format": "json"}},
    ]

    response = upload_file(client, csv_bytes, "big90.csv", pipeline)
    assert response.status_code == 200, response.text
    job_id = response.json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"

    # ---- Per-step row counts ----
    by_type = {s["type"]: s for s in body["steps"]}
    assert by_type["transform"]["input_rows"]  == num_rows
    assert by_type["transform"]["output_rows"] == expected_survivors
    # Convert preserves rows
    assert by_type["convert"]["input_rows"]  == expected_survivors
    assert by_type["convert"]["output_rows"] == expected_survivors

    # ---- Output file checks ----
    result = client.get(f"/jobs/{job_id}/result")
    assert result.status_code == 200
    out_bytes = result.content
    out_size  = len(out_bytes)

    # The property under test: JSON output is BIGGER than CSV input even
    # though we kept only ~90% of the rows. Column names + quotes repeated
    # on every row inflates the size.
    assert out_size > csv_size, (
        f"Expected JSON output to grow vs CSV input "
        f"(in={csv_size} bytes, out={out_size} bytes)"
    )

    # Sanity: starts with `[`, ends with `]`. We don't json.loads() the
    # whole thing (would defeat the streaming demo by loading it all
    # into memory in the test). Spot-check shape only.
    assert out_bytes[:1]  == b"["
    assert out_bytes[-1:] == b"]"

    # Cross-check: API output_rows equals the actual number of objects
    # in the JSON file. Count `{"id"` occurrences — cheap and exact for
    # our row shape (id is the first key of every object).
    actual_objects = out_bytes.count(b'{"id"')
    assert actual_objects == expected_survivors, (
        f"File contains {actual_objects} JSON objects but API reports "
        f"{expected_survivors} output_rows"
    )
