"""
Shared fixtures for the test suite.

Goal: tests run without Docker, Redis, or the worker container.

Strategy:
- Each test gets its own temp directory containing storage/ and the SQLite file
- We patch sqlalchemy's engine to point at the temp DB before importing the app
- We patch app.api.upload.job_queue.enqueue to call process_job() synchronously
  in the same process, so the full pipeline runs inside the test
"""

import csv
import io
import json
import os
import sys
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_workspace(monkeypatch, tmp_path):
    """
    Create an isolated workspace for one test: temp storage dirs and DB.
    Returns the workspace root path. Cleans itself up afterwards.
    """
    # chdir into the temp workspace so storage/ paths resolve here.
    # The app uses relative paths like "storage/uploads/..." which means CWD
    # determines where files go. Tests must not write to the real storage/.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.chdir(workspace)

    # Create the storage tree the app expects
    (workspace / "storage" / "uploads").mkdir(parents=True)
    (workspace / "storage" / "intermediate").mkdir(parents=True)
    (workspace / "storage" / "outputs").mkdir(parents=True)

    yield workspace


@pytest.fixture
def app_modules(temp_workspace, monkeypatch):
    """
    Import the app fresh for each test so the DB engine binds to the
    test's temp directory. Returns a namespace of useful modules.
    """
    # Drop any cached app modules so they re-init against the temp CWD
    for mod_name in list(sys.modules):
        if mod_name.startswith("app."):
            del sys.modules[mod_name]

    # Import after CWD is set so SessionLocal binds to ./storage/pipeline.db
    from app.main import app
    from app.models.database import Base, engine, SessionLocal
    from app.models.job import Job, JobStep, FileReference
    from app.workers import processor
    from app.api import upload as upload_mod

    # Create tables in the test DB
    Base.metadata.create_all(bind=engine)

    # Patch enqueue to record (func, job_id) pairs instead of running inline.
    # We can't run synchronously inside enqueue because upload.py enqueues
    # BEFORE committing (so the worker session would see no Job row yet).
    # Pending jobs are drained by the upload_file() helper after each request.
    pending = []

    def _record_enqueue(func_path, job_id, **kwargs):
        pending.append((func_path, job_id))

    monkeypatch.setattr(upload_mod.job_queue, "enqueue", _record_enqueue)

    yield type("AppModules", (), {
        "app": app,
        "Job": Job,
        "JobStep": JobStep,
        "FileReference": FileReference,
        "SessionLocal": SessionLocal,
        "processor": processor,
        "pending_jobs": pending,
    })


@pytest.fixture
def client(app_modules):
    """
    A FastAPI TestClient that automatically drains the pending-jobs queue
    after every POST /upload. Sync execution of process_job runs in the
    test process; this is what makes "no Docker, no Redis" work.
    """
    from fastapi.testclient import TestClient

    raw = TestClient(app_modules.app)

    class DrainingClient:
        # By default uploads run synchronously (drain immediately after POST).
        # Set client.auto_drain = False BEFORE the upload to keep jobs PENDING
        # — useful for testing cancel behavior on a job that hasn't run.
        auto_drain = True

        def post(self, *args, **kwargs):
            response = raw.post(*args, **kwargs)
            if self.auto_drain:
                # Drain any jobs queued during the request — by now the upload
                # has committed, so the worker session can see the Job row.
                for _, job_id in list(app_modules.pending_jobs):
                    app_modules.processor.process_job(job_id)
                app_modules.pending_jobs.clear()
            return response

        def get(self, *args, **kwargs):
            return raw.get(*args, **kwargs)

        def drain(self):
            """Manually run any queued jobs. For tests that toggle auto_drain off."""
            for _, job_id in list(app_modules.pending_jobs):
                app_modules.processor.process_job(job_id)
            app_modules.pending_jobs.clear()

    return DrainingClient()


# ---------- Test data helpers ----------

def make_csv(rows, headers=None):
    """
    Build a CSV string in memory.
    rows: list of dicts. headers: column order; defaults to keys of rows[0].
    """
    if not rows:
        return (",".join(headers or []) + "\n").encode()
    headers = headers or list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode()


def make_json(rows):
    """Build a JSON-array byte string from a list of dicts."""
    return json.dumps(rows).encode()


def upload_file(client, content, filename, pipeline):
    """
    POST to /upload with given bytes, filename, and pipeline list.
    Returns the parsed response JSON.
    """
    files = {"file": (filename, content, "application/octet-stream")}
    data  = {"pipeline": json.dumps(pipeline)}
    response = client.post("/upload", files=files, data=data)
    return response


def wait_for_status(client, job_id, expected_statuses=("COMPLETED", "FAILED", "CANCELLED"), timeout_attempts=30):
    """
    Because enqueue is synchronous in tests, the job is already done by the
    time /upload returns. This helper exists as a safety net and to return
    the final state. It just GETs once.
    """
    response = client.get(f"/jobs/{job_id}")
    assert response.status_code == 200, f"status endpoint returned {response.status_code}"
    body = response.json()
    assert body["status"] in expected_statuses, (
        f"expected job status in {expected_statuses}, got {body['status']}: {body.get('error')}"
    )
    return body
