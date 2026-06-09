"""
Stuck-job recovery.

Two scenarios:

1. PENDING — the job_id was committed to the DB but Redis lost track of it
   (Redis restart without persistence, enqueue failed after commit, message
   evicted, worker crashed before pulling). The work hasn't started, so
   re-enqueueing is safe and idempotent.

2. PROCESSING — a worker picked up the job, marked it PROCESSING, and then
   died mid-pipeline. We do NOT try to resume mid-step (would require
   per-step checkpointing — see DECISIONS §10). Mark the job FAILED so the
   user gets a definitive answer and can re-upload.

Both functions are read/write but defensive — wrapped in try/except so a
sweep failure never crashes the request that triggered it.
"""

import logging
import os
from datetime import datetime, timedelta

log = logging.getLogger("sweeper")

# How long a job can sit in PENDING before we assume Redis dropped it.
# Most jobs finish in seconds; 5 minutes is a safe "something's wrong" line.
PENDING_THRESHOLD = timedelta(minutes=5)

# How long a job can be PROCESSING before we assume the worker died.
# Matches the job_timeout=3600 set in upload.py.
PROCESSING_THRESHOLD = timedelta(hours=1)


def _get_job_queue():
    """
    Build the RQ queue lazily so this module has no import-time side effects
    on test environments that patch the queue.
    """
    from redis import Redis
    from rq import Queue
    redis_conn = Redis(
        host = os.getenv("REDIS_HOST", "localhost"),
        port = int(os.getenv("REDIS_PORT", "6379"))
    )
    return Queue("pipeline", connection=redis_conn)


def recover_stuck_jobs(db):
    """
    One-shot recovery sweep. Called on startup AND on every status query.
    - PENDING longer than PENDING_THRESHOLD → re-enqueue
    - PROCESSING longer than PROCESSING_THRESHOLD → mark FAILED
    Returns a small summary dict for logging.
    """
    from app.models.job import Job

    now = datetime.utcnow()
    summary = {"requeued": 0, "failed": 0}

    # 1. Re-enqueue stuck PENDING jobs
    pending_cutoff = now - PENDING_THRESHOLD
    stuck_pending = db.query(Job).filter(
        Job.status == "PENDING",
        Job.created_at < pending_cutoff
    ).all()

    if stuck_pending:
        try:
            queue = _get_job_queue()
        except Exception as e:
            log.warning(f"Could not connect to Redis for re-enqueue: {e}")
            queue = None

        for job in stuck_pending:
            if queue is None:
                # Can't re-enqueue without Redis — leave row PENDING for next sweep
                continue
            try:
                queue.enqueue(
                    "app.workers.processor.process_job",
                    job.id,
                    job_timeout=3600,
                )
                log.info(f"Re-enqueued stuck PENDING job: {job.id}")
                summary["requeued"] += 1
            except Exception as e:
                log.warning(f"Failed to re-enqueue {job.id}: {e}")

    # 2. Mark stuck PROCESSING jobs as FAILED.
    # Use started_at (not created_at) — we want "the worker has been running
    # this for more than 1 hour", not "the job was uploaded more than 1 hour
    # ago". A job that sat queued for 50 minutes and just started shouldn't
    # be killed 10 minutes into its run.
    # started_at is set atomically when the worker transitions PENDING →
    # PROCESSING in processor.process_job, so any PROCESSING row has a
    # non-NULL started_at.
    processing_cutoff = now - PROCESSING_THRESHOLD
    stuck_processing = db.query(Job).filter(
        Job.status == "PROCESSING",
        Job.started_at < processing_cutoff
    ).all()

    for job in stuck_processing:
        job.status        = "FAILED"
        job.error_message = (
            "Worker died mid-pipeline — job exceeded the 1-hour timeout. "
            "Re-upload to retry from scratch."
        )
        job.completed_at  = now
        log.warning(f"Marked stuck PROCESSING job as FAILED: {job.id}")
        summary["failed"] += 1

    if summary["requeued"] or summary["failed"]:
        db.commit()
        log.info(
            f"Recovery sweep: re-enqueued={summary['requeued']} "
            f"failed={summary['failed']}"
        )

    return summary
