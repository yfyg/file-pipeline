import os
import time
import logging
from datetime import datetime
from app.models.database import SessionLocal
from app.models.job import Job, JobStep, FileReference
from app.steps.validate import validate
from app.steps.transform import transform
from app.steps.convert import convert
from app.steps.compress import compress

# Structured logging with job context
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] job=%(job_id)s step=%(step)s %(message)s"
)

MAX_RETRIES = 3
STEP_FUNCTIONS = {
    "validate":   validate,
    "transform":  transform,
    "convert":    convert,
    "compress":   compress,
}


def process_job(job_id: str):
    """
    Main entry point called by RQ worker.
    Runs all pipeline steps sequentially.
    Updates job and step status in DB after each step.
    """
    db = SessionLocal()
    log = _make_logger(job_id, "init")

    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            log.error("Job not found")
            return

        # Guard against duplicate processing
        # If another worker already picked this up — exit immediately
        if job.status != "PENDING":
            log.info(f"Job already in status {job.status} — skipping")
            return

        # Atomically mark as PROCESSING before doing any work
        job.status = "PROCESSING"
        job.started_at = datetime.utcnow()
        db.commit()
        log.info("Job started")

        # Get input file path
        input_file = db.query(FileReference).filter(
            FileReference.id == job.input_file_id
        ).first()
        if not input_file:
            _fail_job(db, job, "Input file not found", log)
            return

        # current_file_path tracks which file to pass to the next step
        # starts as the uploaded file, updated after each step
        current_file_path = input_file.storage_path

        # Run each step in order
        pipeline = job.pipeline  # list of {"step": "...", "params": {...}}
        for index, step_def in enumerate(pipeline):
            step_type = step_def.get("step")
            params    = step_def.get("params", {})
            log       = _make_logger(job_id, f"{index}:{step_type}")

            # Skip notify — handled separately at the end
            if step_type == "notify":
                log.info("Notify step deferred to end of pipeline")
                continue

            # Update job current step
            job.current_step_index = index
            db.commit()

            # Get or create JobStep record
            job_step = db.query(JobStep).filter(
                JobStep.job_id == job_id,
                JobStep.step_index == index
            ).first()

            if not job_step:
                job_step = JobStep(
                    job_id      = job_id,
                    step_index  = index,
                    step_type   = step_type,
                    parameters  = params,
                    status      = "PENDING"
                )
                db.add(job_step)
                db.commit()

            # Run the step with retries
            success, output_path, error = _run_step_with_retries(
                step_type       = step_type,
                file_path       = current_file_path,
                params          = params,
                job_step        = job_step,
                db              = db,
                log             = log
            )

            if not success:
                # Mark remaining steps as SKIPPED
                _skip_remaining_steps(db, job, pipeline, index + 1)
                _fail_job(db, job, error, log)
                return

            # Step succeeded — pass its output to next step
            current_file_path = output_path
            log.info(f"Step completed — output: {output_path}")

        # All steps completed — handle notify if present
        _run_notify_if_present(db, job, pipeline, current_file_path, log)

        # Save final output file reference
        output_file = _save_file_reference(db, current_file_path)
        job.output_file_id  = output_file.id
        job.status          = "COMPLETED"
        job.completed_at    = datetime.utcnow()
        db.commit()
        log.info("Job completed successfully")

    except Exception as e:
        log.error(f"Unexpected error: {str(e)}")
        if job:
            _fail_job(db, job, f"Unexpected error: {str(e)}", log)
    finally:
        db.close()


def _run_step_with_retries(step_type, file_path, params, job_step, db, log):
    """
    Runs a single step with up to MAX_RETRIES attempts.
    Returns (success, output_path, error_message)
    """
    step_fn = STEP_FUNCTIONS.get(step_type)
    if not step_fn:
        return False, None, f"Unknown step type: {step_type}"

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Attempt {attempt}/{MAX_RETRIES}")

            # Mark step as RUNNING
            job_step.status     = "RUNNING"
            job_step.started_at = datetime.utcnow()
            db.commit()

            # Run the step
            output_path = step_fn(file_path, params)

            # Internal sanity check — did step produce a valid output?
            _verify_step_output(output_path)

            # Mark step as COMPLETED
            job_step.status       = "COMPLETED"
            job_step.completed_at = datetime.utcnow()
            job_step.duration     = (
                job_step.completed_at - job_step.started_at
            ).total_seconds()
            job_step.output_file_id = _save_file_reference(db, output_path).id
            db.commit()

            return True, output_path, None

        except Exception as e:
            last_error = str(e)
            log.warning(f"Attempt {attempt} failed: {last_error}")

            if attempt < MAX_RETRIES:
                wait = 2 ** attempt  # exponential backoff: 2s, 4s, 8s
                log.info(f"Retrying in {wait}s...")
                time.sleep(wait)

    # All retries exhausted
    job_step.status        = "FAILED"
    job_step.error_message = last_error
    job_step.completed_at  = datetime.utcnow()
    db.commit()

    return False, None, last_error


def _run_notify_if_present(db, job, pipeline, final_file_path, log):
    """
    Runs the notify step if defined in the pipeline.
    Webhook failure does NOT fail the job.
    """
    notify_steps = [
        (i, s) for i, s in enumerate(pipeline)
        if s.get("step") == "notify"
    ]

    if not notify_steps:
        return

    # Import here to avoid circular imports
    from app.steps.notify import notify

    for index, step_def in notify_steps:
        params = step_def.get("params", {})
        log    = _make_logger(job.id, f"{index}:notify")

        job_step = JobStep(
            job_id     = job.id,
            step_index = index,
            step_type  = "notify",
            parameters = params,
            status     = "RUNNING",
            started_at = datetime.utcnow()
        )
        db.add(job_step)
        db.commit()

        try:
            notify(job.id, final_file_path, params)
            job_step.status       = "COMPLETED"
            job_step.completed_at = datetime.utcnow()
            log.info("Notify step completed")
        except Exception as e:
            # Webhook failure — mark step failed but don't fail the job
            job_step.status        = "FAILED"
            job_step.error_message = str(e)
            job_step.completed_at  = datetime.utcnow()
            log.warning(f"Notify step failed (job still completed): {str(e)}")

        db.commit()


def _verify_step_output(output_path: str):
    """
    Lightweight sanity check after every step.
    Confirms step produced a real non-empty file.
    """
    if not output_path:
        raise ValueError("Step returned no output path")
    if not os.path.exists(output_path):
        raise ValueError(f"Step produced no output file at: {output_path}")
    if os.path.getsize(output_path) == 0:
        raise ValueError(f"Step produced an empty output file at: {output_path}")


def _fail_job(db, job, error_message: str, log):
    """Marks job as FAILED with error message"""
    job.status        = "FAILED"
    job.error_message = error_message
    job.completed_at  = datetime.utcnow()
    db.commit()
    log.error(f"Job failed: {error_message}")


def _skip_remaining_steps(db, job, pipeline, from_index: int):
    """
    Marks all remaining steps as SKIPPED after a failure.
    """
    for index in range(from_index, len(pipeline)):
        step_def = pipeline[index]
        job_step = JobStep(
            job_id     = job.id,
            step_index = index,
            step_type  = step_def.get("step"),
            parameters = step_def.get("params", {}),
            status     = "SKIPPED"
        )
        db.add(job_step)
    db.commit()


def _save_file_reference(db, file_path: str) -> FileReference:
    """
    Creates a FileReference record for a file on disk.
    Used to track intermediate and output files.
    """
    from datetime import timedelta

    size     = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    _, ext   = os.path.splitext(file_path)

    # Map extension to content type
    content_types = {
        ".csv":  "text/csv",
        ".json": "application/json",
        ".gz":   "application/gzip",
        ".zip":  "application/zip",
        ".txt":  "text/plain",
    }
    content_type = content_types.get(ext.lower(), "application/octet-stream")

    file_ref = FileReference(
        storage_path      = file_path,
        original_filename = filename,
        size              = size,
        content_type      = content_type,
        created_at        = datetime.utcnow(),
        expires_at        = datetime.utcnow() + timedelta(hours=24)
    )
    db.add(file_ref)
    db.commit()
    return file_ref


def _make_logger(job_id: str, step: str):
    """Returns a logger with job and step context"""
    logger = logging.getLogger("processor")
    # Inject context into log records
    return logging.LoggerAdapter(logger, {"job_id": job_id, "step": step})
