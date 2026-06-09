"""
Stuck-job recovery sweeper tests.

The sweeper handles two recovery paths:
- PENDING longer than PENDING_THRESHOLD → re-enqueue (work hasn't started)
- PROCESSING longer than PROCESSING_THRESHOLD → mark FAILED (worker died)

These tests verify the PROCESSING-timeout fix specifically: the threshold
must check `started_at`, not `created_at`. A job that was queued for a long
time but only just started processing should NOT be marked FAILED — only
jobs whose worker has actually been running for too long should.
"""

import importlib
from datetime import datetime, timedelta


def _make_processing_job(app_modules, created_at, started_at):
    """Insert a Job and a minimal input FileReference directly into the DB."""
    db = app_modules.SessionLocal()
    try:
        # The FK on Job.input_file_id requires a real FileReference row
        file_ref = app_modules.FileReference(
            storage_path      = "storage/uploads/fake.csv",
            original_filename = "fake.csv",
            size              = 1,
            content_type      = "text/csv",
            created_at        = created_at,
            expires_at        = created_at + timedelta(hours=24),
        )
        db.add(file_ref)
        db.flush()

        job = app_modules.Job(
            input_file_id = file_ref.id,
            pipeline      = [{"step": "validate", "params": {}}],
            status        = "PROCESSING",
            created_at    = created_at,
            started_at    = started_at,
        )
        db.add(job)
        db.commit()
        return job.id
    finally:
        db.close()


def test_processing_job_not_failed_if_started_recently(app_modules):
    """
    A job that was QUEUED two hours ago but only STARTED two minutes ago is
    a legitimate in-flight job. The sweeper must NOT mark it as FAILED just
    because its created_at is old.
    """
    sweeper = importlib.import_module("app.workers.sweeper")

    now = datetime.utcnow()
    job_id = _make_processing_job(
        app_modules,
        created_at = now - timedelta(hours=2),    # uploaded long ago
        started_at = now - timedelta(minutes=2),  # actually started recently
    )

    db = app_modules.SessionLocal()
    try:
        sweeper.recover_stuck_jobs(db)
        # Sweeper must not touch this job — it just started running
        job = db.query(app_modules.Job).filter(
            app_modules.Job.id == job_id
        ).first()
        assert job.status == "PROCESSING", (
            f"sweeper wrongly marked a recently-started job as {job.status}"
        )
    finally:
        db.close()


def test_processing_job_failed_when_started_long_ago(app_modules):
    """
    A job that started running >1 hour ago and never finished is genuinely
    stuck (worker probably crashed). Sweeper marks it FAILED.
    """
    sweeper = importlib.import_module("app.workers.sweeper")

    now = datetime.utcnow()
    job_id = _make_processing_job(
        app_modules,
        created_at = now - timedelta(hours=3),
        started_at = now - timedelta(hours=2),  # worker has been "running" for 2h
    )

    db = app_modules.SessionLocal()
    try:
        sweeper.recover_stuck_jobs(db)
        job = db.query(app_modules.Job).filter(
            app_modules.Job.id == job_id
        ).first()
        assert job.status == "FAILED"
        assert "1-hour timeout" in (job.error_message or "")
        assert job.completed_at is not None
    finally:
        db.close()
