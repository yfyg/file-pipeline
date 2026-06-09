"""
Cancel endpoint tests.

The test fixture normally runs jobs synchronously the moment an upload returns
(see conftest.DrainingClient). To exercise cancellation against a still-PENDING
job we toggle `client.auto_drain = False` before uploading, then call cancel
before manually draining.
"""

from tests.conftest import make_csv, upload_file


SIMPLE_CSV = make_csv([
    {"name": "Alice", "age": 30},
    {"name": "Bob",   "age": 25},
])

PIPELINE = [
    {"step": "validate",  "params": {"expected_type": "csv"}},
    {"step": "transform", "params": {"select_columns": ["name"]}},
    {"step": "convert",   "params": {"output_format": "json"}},
]


def test_cancel_pending_job_marks_steps_skipped(client, app_modules):
    """
    A job that's still PENDING can be cancelled. Job status flips to CANCELLED;
    all unstarted steps flip to SKIPPED; no output file is produced.
    """
    # Don't auto-run the job — we want it to sit in PENDING
    client.auto_drain = False

    response = upload_file(client, SIMPLE_CSV, "data.csv", PIPELINE)
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    # Sanity: job is PENDING right after upload
    status_resp = client.get(f"/jobs/{job_id}")
    assert status_resp.json()["status"] == "PENDING"

    # Cancel it
    cancel_resp = client.post(f"/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "CANCELLED"

    # Job is CANCELLED, all 3 steps are SKIPPED
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "CANCELLED"
    assert body["output_file"] is None
    for step in body["steps"]:
        assert step["status"] == "SKIPPED", f"step {step['type']} status={step['status']}"

    # If we drain now, the worker should respect the cancelled status and exit
    # (cooperative cancel — see DECISIONS §17)
    client.drain()
    body_after = client.get(f"/jobs/{job_id}").json()
    assert body_after["status"] == "CANCELLED"


def test_cancel_completed_job_returns_400(client):
    """A COMPLETED job cannot be cancelled."""
    # Default behavior: drain runs the pipeline to COMPLETED
    response = upload_file(client, SIMPLE_CSV, "data.csv", PIPELINE)
    job_id = response.json()["job_id"]

    # Confirm it ran
    assert client.get(f"/jobs/{job_id}").json()["status"] == "COMPLETED"

    cancel_resp = client.post(f"/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 400
    assert "completed" in cancel_resp.json()["detail"].lower()


def test_cancel_nonexistent_job_returns_404(client):
    """Cancel on an unknown job_id returns 404 with a helpful message."""
    cancel_resp = client.post("/jobs/does-not-exist-anywhere/cancel")
    assert cancel_resp.status_code == 404
    assert "not found" in cancel_resp.json()["detail"].lower()


def test_cancel_twice_returns_400_second_time(client, app_modules):
    """Once a job is CANCELLED, cancelling again returns 400."""
    client.auto_drain = False
    response = upload_file(client, SIMPLE_CSV, "data.csv", PIPELINE)
    job_id = response.json()["job_id"]

    first  = client.post(f"/jobs/{job_id}/cancel")
    second = client.post(f"/jobs/{job_id}/cancel")

    assert first.status_code  == 200
    assert second.status_code == 400
    assert "cancelled" in second.json()["detail"].lower()
