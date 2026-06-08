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

# Structured logging setup
# Root logger uses basic format — safe for RQ internal logs
# Our processor logger uses extended format with job_id and step context
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)

# Custom formatter for processor logger only
# Safely handles missing job_id/step fields (e.g. from RQ internal logs)
class JobFormatter(logging.Formatter):
    def format(self, record):
        record.job_id = getattr(record, "job_id", "unknown")
        record.step   = getattr(record, "step",   "unknown")
        return super().format(record)

_handler = logging.StreamHandler()
_handler.setFormatter(JobFormatter(
    "%(asctime)s [%(levelname)s] job=%(job_id)s step=%(step)s — %(message)s"
))

_processor_logger = logging.getLogger("processor")
_processor_logger.handlers = [_handler]
_processor_logger.propagate = False  # don't pass to root logger

MAX_RETRIES = 3
STEP_FUNCTIONS = {
    "validate":   validate,
    "transform":  transform,
    "convert":    convert,
    "compress":   compress,
}

# Only transient errors are worth retrying. OSError covers temporary disk-write
# failures, file locks, and similar I/O issues. Logical errors (bad format,
# validation failure, unsupported conversion, empty output) raise ValueError and
# are deterministic — retrying them cannot succeed and only wastes time, so we
# fail fast on those.
RETRYABLE_EXCEPTIONS = (OSError,)


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

            # Honor cooperative cancellation requested via the API mid-flight.
            # The /cancel endpoint runs in a separate DB session, so re-read the
            # row before each step. We cannot interrupt a step that is already
            # running, but we stop cleanly before starting the next one.
            db.refresh(job)
            if job.status == "CANCELLED":
                log.info("Cancellation detected — stopping before next step")
                _skip_remaining_steps(db, job, pipeline, index)
                return

            # Skip notify — handled separately at the end
            if step_type == "notify":
                log.info("Notify step deferred to end of pipeline")
                continue

            # Update job current step
            job.current_step_index = index
            db.commit()

            # Get the JobStep created at upload time (or create it if missing).
            # Always reuse the existing row — never insert a duplicate.
            job_step = _get_or_create_step(db, job_id, index, step_type, params)

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

            # Step succeeded — move output to intermediate/ folder
            # validate returns same file — no need to move
            if step_type != "validate" and output_path != current_file_path:
                import shutil
                os.makedirs("storage/intermediate", exist_ok=True)
                intermediate_filename = os.path.basename(output_path)
                intermediate_path     = os.path.join("storage/intermediate", intermediate_filename)
                shutil.move(output_path, intermediate_path)
                output_path = intermediate_path
                log.info(f"Step output moved to intermediate: {intermediate_path}")

                # Update FileReference.storage_path to reflect the new location
                # Without this, cleanup can't find the file in storage/intermediate/
                if job_step.output_file_id:
                    file_ref = db.query(FileReference).filter(
                        FileReference.id == job_step.output_file_id
                    ).first()
                    if file_ref:
                        file_ref.storage_path = intermediate_path
                        db.commit()

            # Pass output to next step
            current_file_path = output_path
            log.info(f"Step completed — output: {output_path}")

        # Final cancellation guard — if the job was cancelled while the last
        # step ran, don't fire the webhook or overwrite CANCELLED with COMPLETED.
        db.refresh(job)
        if job.status == "CANCELLED":
            log.info("Cancellation detected after final step — not completing")
            return

        # All steps completed — handle notify if present
        _run_notify_if_present(db, job, pipeline, current_file_path, log)

        # Move final output file to outputs/ folder
        import shutil
        output_dir      = "storage/outputs"
        os.makedirs(output_dir, exist_ok=True)
        final_filename  = os.path.basename(current_file_path)
        final_path      = os.path.join(output_dir, final_filename)

        # If no step produced a new file (e.g. a validate-only pipeline),
        # current_file_path is still the original upload. Copy it instead of
        # moving — moving would remove the input from uploads/ and orphan the
        # input FileReference, destroying the source file.
        if os.path.realpath(current_file_path) == os.path.realpath(input_file.storage_path):
            shutil.copy2(current_file_path, final_path)
            log.info(f"Final output copied, input preserved: {final_path}")
        else:
            shutil.move(current_file_path, final_path)
            log.info(f"Final output moved to: {final_path}")
        current_file_path = final_path

        # Save final output file reference
        output_file = _save_file_reference(db, current_file_path)
        job.output_file_id  = output_file.id
        job.status          = "COMPLETED"
        job.completed_at    = datetime.utcnow()
        db.commit()
        log.info("Job completed successfully")

        # Clean up intermediate files immediately after job completes
        # DECISIONS §4: intermediate files deleted on job completion
        _cleanup_intermediate_files(job_id, db, log)

    except Exception as e:
        log.error(f"Unexpected error: {str(e)}")
        if job:
            _fail_job(db, job, f"Unexpected error: {str(e)}", log)
    finally:
        db.close()


def _run_step_with_retries(step_type, file_path, params, job_step, db, log):
    """
    Runs a single step, retrying only on transient errors.

    Retries (up to MAX_RETRIES, with exponential backoff) are attempted only for
    RETRYABLE_EXCEPTIONS — transient OS/I/O problems. Deterministic failures
    (bad format, validation failure, unsupported conversion, empty output) raise
    ValueError and are NOT retried, since retrying cannot help and only wastes
    time. Returns (success, output_path, error_message).
    """
    step_fn = STEP_FUNCTIONS.get(step_type)
    if not step_fn:
        return False, None, f"Unknown step type: {step_type}"

    # Record the real start once so duration spans all attempts, not just the
    # last one. (Previously started_at was overwritten on every retry.)
    job_step.status     = "RUNNING"
    job_step.started_at = datetime.utcnow()
    db.commit()

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Attempt {attempt}/{MAX_RETRIES}")

            # Run the step. Every step returns (output_path, stats).
            # stats is {} for steps that don't operate on rows (validate, compress).
            output_path, stats = step_fn(file_path, params)

            # Internal sanity check — did step produce a valid output?
            _verify_step_output(output_path)

            # Mark step as COMPLETED
            job_step.status       = "COMPLETED"
            job_step.completed_at = datetime.utcnow()
            job_step.duration     = (
                job_step.completed_at - job_step.started_at
            ).total_seconds()
            job_step.output_file_id = _save_file_reference(db, output_path).id

            # Record row counts if the step produced them (transform, convert).
            # NULL for non-row steps so the API can show "-" instead of "0".
            job_step.input_rows  = stats.get("input_rows")
            job_step.output_rows = stats.get("output_rows")

            db.commit()

            return True, output_path, None

        except Exception as e:
            last_error   = str(e)
            is_retryable = isinstance(e, RETRYABLE_EXCEPTIONS)

            if is_retryable and attempt < MAX_RETRIES:
                wait = 2 ** attempt  # exponential backoff: 2s, 4s, 8s
                log.warning(f"Attempt {attempt} failed (transient): {last_error} — retrying in {wait}s")
                time.sleep(wait)
                continue

            # Permanent error, or retries exhausted — stop trying
            reason = "retries exhausted" if is_retryable else "permanent error, not retrying"
            log.warning(f"Attempt {attempt} failed ({reason}): {last_error}")
            break

    # Step failed for good
    job_step.status        = "FAILED"
    job_step.error_message = last_error
    job_step.completed_at  = datetime.utcnow()
    if job_step.started_at:
        job_step.duration  = (job_step.completed_at - job_step.started_at).total_seconds()
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

        # Reuse the JobStep created at upload time — do NOT insert a duplicate.
        job_step = _get_or_create_step(db, job.id, index, "notify", params)
        job_step.status     = "RUNNING"
        job_step.started_at = datetime.utcnow()
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


def _get_or_create_step(db, job_id, index, step_type, params):
    """
    Returns the existing JobStep for this (job, step_index) — normally the row
    created at upload time — or creates one if it is somehow missing.

    Using this everywhere (main loop, notify, skip) guarantees a single row per
    step index. Previously notify and skip inserted fresh rows that already
    existed, producing duplicate steps and a wrong overall_progress percentage.
    """
    job_step = db.query(JobStep).filter(
        JobStep.job_id == job_id,
        JobStep.step_index == index
    ).first()

    if not job_step:
        job_step = JobStep(
            job_id     = job_id,
            step_index = index,
            step_type  = step_type,
            parameters = params,
            status     = "PENDING",
        )
        db.add(job_step)
        db.commit()

    return job_step


def _skip_remaining_steps(db, job, pipeline, from_index: int):
    """
    Marks all remaining steps as SKIPPED (after a failure or a cancellation).
    Reuses the existing step rows — never inserts duplicates.
    """
    for index in range(from_index, len(pipeline)):
        step_def = pipeline[index]
        job_step = _get_or_create_step(
            db, job.id, index, step_def.get("step"), step_def.get("params", {})
        )
        job_step.status = "SKIPPED"
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


def _cleanup_intermediate_files(job_id: str, db, log):
    """
    Deletes intermediate files for this job immediately after completion.
    DECISIONS §4: intermediate files deleted on job completion — not at startup.

    Strategy:
    - Find all JobStep output file references for this job
    - If the file path is inside storage/intermediate/ — delete from disk and DB
    - Skip final output (already moved to storage/outputs/)
    - Wrapped in try/except — cleanup failure never fails the job
    """
    try:
        intermediate_dir = os.path.realpath("storage/intermediate")

        # Get all JobStep records for this job
        steps = db.query(JobStep).filter(JobStep.job_id == job_id).all()

        for step in steps:
            if not step.output_file_id:
                continue

            file_ref = db.query(FileReference).filter(
                FileReference.id == step.output_file_id
            ).first()

            if not file_ref:
                continue

            # Only delete files inside storage/intermediate/
            file_abs = os.path.realpath(file_ref.storage_path)
            if not file_abs.startswith(intermediate_dir):
                continue

            # Delete from disk
            if os.path.exists(file_ref.storage_path):
                os.remove(file_ref.storage_path)
                log.info(f"Deleted intermediate file: {file_ref.storage_path}")

            # Delete DB record
            db.delete(file_ref)

        db.commit()
        log.info(f"Intermediate cleanup complete for job {job_id}")

    except Exception as e:
        # Cleanup failure must never fail the job
        log.warning(f"Intermediate cleanup failed (non-fatal): {str(e)}")


def _make_logger(job_id: str, step: str):
    """Returns a logger with job and step context"""
    logger = logging.getLogger("processor")
    # Inject context into log records
    return logging.LoggerAdapter(logger, {"job_id": job_id, "step": step})
