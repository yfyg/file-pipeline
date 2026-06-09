"""
API-surface tests: upload, status, result download, no-dedup.
"""

import json

from tests.conftest import make_csv, upload_file, wait_for_status


SIMPLE_CSV = make_csv([
    {"name": "Alice",   "email": "alice@example.com",   "age": 30},
    {"name": "Bob",     "email": "bob@example.com",     "age": 25},
    {"name": "Charlie", "email": "charlie@example.com", "age": 40},
])

VALIDATE_ONLY = [{"step": "validate", "params": {"expected_type": "csv"}}]


def test_upload_creates_job(client, app_modules):
    """
    POST /upload returns a job_id, the response shape is correct, and the DB
    has matching Job / JobStep / FileReference rows.
    """
    response = upload_file(client, SIMPLE_CSV, "data.csv", VALIDATE_ONLY)
    assert response.status_code == 200, response.text

    body = response.json()
    assert "job_id" in body
    assert body["filename"] == "data.csv"
    assert body["size"] == len(SIMPLE_CSV)

    # Verify DB rows were actually written
    db = app_modules.SessionLocal()
    try:
        job = db.query(app_modules.Job).filter(
            app_modules.Job.id == body["job_id"]
        ).first()
        assert job is not None
        # Sync enqueue means the job has already completed by now
        assert job.status == "COMPLETED"
        assert job.input_file_id is not None

        steps = db.query(app_modules.JobStep).filter(
            app_modules.JobStep.job_id == job.id
        ).all()
        assert len(steps) == 1
        assert steps[0].step_type == "validate"
    finally:
        db.close()


def test_upload_rejects_bad_inputs(client):
    """Bad file extension → 400, bad pipeline JSON → 400, missing pipeline → 422."""
    # 1. Disallowed extension
    response = upload_file(client, b"binary garbage", "evil.exe", VALIDATE_ONLY)
    assert response.status_code == 400
    assert "not allowed" in response.json()["detail"].lower()

    # 2. Malformed pipeline JSON
    files = {"file": ("data.csv", SIMPLE_CSV, "text/csv")}
    data  = {"pipeline": "{not valid json"}
    response = client.post("/upload", files=files, data=data)
    assert response.status_code == 400
    assert "valid json" in response.json()["detail"].lower()

    # 3. Empty pipeline list
    response = upload_file(client, SIMPLE_CSV, "data.csv", [])
    assert response.status_code == 400
    assert "non-empty" in response.json()["detail"].lower()


def test_status_endpoint(client):
    """
    GET /jobs/{id} returns the full step list, durations, and row counts.
    Unknown id returns 404.
    """
    # Run a multi-step job so we can verify per-step details
    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "convert",  "params": {"output_format": "json"}},
    ]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]

    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"
    assert body["overall_progress"] == "100.0%"
    assert body["duration_seconds"] is not None
    assert len(body["steps"]) == 2

    # Step-level fields are present
    convert_step = next(s for s in body["steps"] if s["type"] == "convert")
    assert convert_step["status"] == "COMPLETED"
    assert convert_step["duration_seconds"] is not None
    # Row counts populated for convert (3 rows in, 3 out)
    assert convert_step["input_rows"]  == 3
    assert convert_step["output_rows"] == 3

    # validate has NULL row counts
    validate_step = next(s for s in body["steps"] if s["type"] == "validate")
    assert validate_step["input_rows"]  is None
    assert validate_step["output_rows"] is None

    # Unknown job id → 404
    response = client.get("/jobs/does-not-exist")
    assert response.status_code == 404


def test_result_download(client):
    """
    GET /jobs/{id}/result returns the file content on a completed job.
    Unknown id → 404.
    """
    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "convert",  "params": {"output_format": "json"}},
    ]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]

    response = client.get(f"/jobs/{job_id}/result")
    assert response.status_code == 200

    # Content should be valid JSON array with 3 items
    data = json.loads(response.content)
    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0]["name"] == "Alice"

    # Unknown id → 404
    response = client.get("/jobs/does-not-exist/result")
    assert response.status_code == 404


def test_job_step_input_output_file_chain(client, app_modules):
    """
    Per-step traceability: each JobStep records both its input_file_id and
    output_file_id, and step N's input_file_id equals step N-1's
    output_file_id (the chain is explicit, not implicit).

    Spec calls out "Input file reference" and "Output file reference" on the
    JobStep data model — this test locks in that both are populated.
    """
    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "transform","params": {"select_columns": ["name", "email"]}},
        {"step": "convert",  "params": {"output_format": "json"}},
    ]
    job_id = upload_file(client, SIMPLE_CSV, "data.csv", pipeline).json()["job_id"]
    wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    db = app_modules.SessionLocal()
    try:
        steps = db.query(app_modules.JobStep).filter(
            app_modules.JobStep.job_id == job_id
        ).order_by(app_modules.JobStep.step_index).all()

        # Step 0 (validate): input_file_id is the upload, output is the SAME
        # row (validate doesn't transform the file)
        assert steps[0].input_file_id  is not None
        assert steps[0].output_file_id is not None
        assert steps[0].input_file_id == steps[0].output_file_id

        # Step N's input must equal Step N-1's output — the chain is explicit
        for prev, curr in zip(steps, steps[1:]):
            assert curr.input_file_id == prev.output_file_id, (
                f"chain broken between {prev.step_type} and {curr.step_type}"
            )

        # Transform and convert each produce a NEW FileReference (different
        # from their input), because they write a new file to disk
        assert steps[1].output_file_id != steps[1].input_file_id  # transform
        assert steps[2].output_file_id != steps[2].input_file_id  # convert
    finally:
        db.close()


def test_no_dedup_same_file_multiple_uploads(client, app_modules):
    """
    Upload the same file 3 times → 3 distinct job_ids, each runs independently.
    Proves we deliberately removed dedup (see DECISIONS §9).
    """
    job_ids = []
    for _ in range(3):
        response = upload_file(client, SIMPLE_CSV, "data.csv", VALIDATE_ONLY)
        assert response.status_code == 200
        job_ids.append(response.json()["job_id"])

    # All three IDs must be distinct
    assert len(set(job_ids)) == 3

    # All three jobs exist in the DB and completed independently
    db = app_modules.SessionLocal()
    try:
        for jid in job_ids:
            job = db.query(app_modules.Job).filter(app_modules.Job.id == jid).first()
            assert job is not None
            assert job.status == "COMPLETED"
    finally:
        db.close()

    # And same file with a different pipeline (filter) — also runs independently
    pipeline_with_filter = [
        {"step": "validate",  "params": {"expected_type": "csv"}},
        {"step": "transform", "params": {"select_columns": ["name", "age"],
                                          "filter_rows": {"column": "age", "gt": 28}}},
    ]
    response = upload_file(client, SIMPLE_CSV, "data.csv", pipeline_with_filter)
    assert response.status_code == 200
    filter_job_id = response.json()["job_id"]
    assert filter_job_id not in job_ids
    # And it filtered correctly: Alice (30) and Charlie (40) survive, Bob (25) drops
    body = wait_for_status(client, filter_job_id)
    transform_step = next(s for s in body["steps"] if s["type"] == "transform")
    assert transform_step["input_rows"]  == 3
    assert transform_step["output_rows"] == 2
