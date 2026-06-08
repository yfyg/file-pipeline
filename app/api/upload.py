import os
import re
import uuid
import shutil
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
import json
from redis import Redis
from rq import Queue
from app.models.database import get_db
from app.models.job import Job, JobStep, FileReference

router = APIRouter()
log = logging.getLogger("upload")

# Constants
MAX_FILE_SIZE    = 100 * 1024 * 1024  # 100MB
CHUNK_SIZE       = 8 * 1024           # 8KB chunks for streaming
ALLOWED_TYPES    = ["csv", "json", "txt", "zip", "gz"]
UPLOAD_DIR       = "storage/uploads"
RETENTION_HOURS  = int(os.getenv("RETENTION_HOURS", "24"))

# Redis queue
redis_conn = Redis(
    host = os.getenv("REDIS_HOST", "localhost"),
    port = int(os.getenv("REDIS_PORT", "6379"))
)
job_queue = Queue("pipeline", connection=redis_conn)


@router.post("/upload")
async def upload_file(
    file:     UploadFile = File(...),
    pipeline: str        = Form(...),  # JSON string
    db:       Session    = Depends(get_db)
):
    """
    Upload a file and start a processing pipeline.

    - Streams file to disk in 8KB chunks — never loads full file into memory
    - Checks file size and type before accepting
    - Returns job_id immediately — processing happens asynchronously

    Every upload creates a new job. We deliberately do not deduplicate uploads
    here — see DECISIONS.md §9 for the trade-off.

    Parameters:
        file:     The file to upload
        pipeline: JSON string defining processing steps
    """
    temp_path = None

    try:
        # Step 1 — Parse and validate pipeline definition
        try:
            pipeline_def = json.loads(pipeline)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="pipeline must be valid JSON")

        if not isinstance(pipeline_def, list) or len(pipeline_def) == 0:
            raise HTTPException(status_code=400, detail="pipeline must be a non-empty list of steps")

        # Step 2 — Validate file extension before saving anything
        original_filename = file.filename or "unknown"
        _, ext = os.path.splitext(original_filename)
        ext = ext.lstrip(".").lower()

        if ext not in ALLOWED_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"File type '{ext}' not allowed. Allowed types: {ALLOWED_TYPES}"
            )

        # Sanitize the name used for ON-DISK paths. The raw client filename is
        # never trusted in a filesystem path — a name like "../../etc/evil.csv"
        # would otherwise traverse out of storage/uploads. The raw
        # original_filename is still stored in the DB for display (Step 6).
        safe_name = _sanitize_filename(original_filename)

        # Pre-allocate the job_id so we can use it as the on-disk file prefix.
        # This way the file on disk, the Job row, and the status API all share
        # one UUID — no second mystery prefix to chase down.
        job_id = str(uuid.uuid4())

        # Step 3 — Stream file to temp location in 8KB chunks
        # Temp path used so partial uploads never reach permanent storage
        temp_filename = f"tmp_{job_id}_{safe_name}"
        temp_path     = os.path.join(UPLOAD_DIR, temp_filename)
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        total_size = 0

        with open(temp_path, "wb") as temp_file:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break

                total_size += len(chunk)

                # Check size limit during upload — fail fast before writing more
                if total_size > MAX_FILE_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum allowed size is 100MB"
                    )

                # Write chunk to disk
                temp_file.write(chunk)

        log.info(f"File streamed to temp: {temp_path} size={total_size}")

        # Step 4 — Move from temp to permanent location
        # The job_id (pre-allocated above) is used as the file prefix so the
        # filename on disk matches the Job.id — easy to correlate when
        # debugging. Every upload gets a fresh job_id, so nothing on disk is
        # ever overwritten and no dedup is needed (see DECISIONS §9).
        final_filename = f"{job_id}_{safe_name}"
        final_path     = os.path.join(UPLOAD_DIR, final_filename)
        shutil.move(temp_path, final_path)
        temp_path = None  # no longer temp — don't delete on error
        log.info(f"File moved to permanent storage: {final_path}")

        # Step 5 — Save FileReference to DB
        file_ref = FileReference(
            storage_path      = final_path,
            original_filename = original_filename,
            size              = total_size,
            content_type      = _get_content_type(ext),
            created_at        = datetime.utcnow(),
            expires_at        = datetime.utcnow() + timedelta(hours=RETENTION_HOURS)
        )
        db.add(file_ref)
        db.flush()  # get file_ref.id without full commit yet

        # Step 6 — Create Job record
        # Job.id is the pre-allocated job_id so it matches the file prefix
        job = Job(
            id            = job_id,
            input_file_id = file_ref.id,
            pipeline      = pipeline_def,
            status        = "PENDING",
            created_at    = datetime.utcnow()
        )
        db.add(job)
        db.flush()

        # Step 7 — Create JobStep records for each step
        for index, step_def in enumerate(pipeline_def):
            step = JobStep(
                job_id     = job.id,
                step_index = index,
                step_type  = step_def.get("step"),
                parameters = step_def.get("params", {}),
                status     = "PENDING"
            )
            db.add(step)

        # Step 8 — Enqueue job BEFORE committing
        # If enqueue fails we roll back DB — file stays but job is not created
        try:
            job_queue.enqueue(
                "app.workers.processor.process_job",
                job.id,
                job_timeout = 3600  # 1 hour max per job
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to enqueue job: {str(e)}"
            )

        # Step 9 — Commit everything together
        db.commit()
        log.info(f"Job created and enqueued: {job.id}")

        return {
            "job_id":   job.id,
            "status":   "PENDING",
            "filename": original_filename,
            "size":     total_size,
            "message":  f"File uploaded successfully. Use /jobs/{job.id} to track progress."
        }

    except HTTPException:
        raise  # re-raise HTTP errors as-is

    except Exception as e:
        log.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

    finally:
        # Always clean up temp file if it still exists
        # This handles: size exceeded, DB error, queue error, any crash
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
            log.info(f"Cleaned up temp file: {temp_path}")


def _get_content_type(ext: str) -> str:
    """Map file extension to content type"""
    content_types = {
        "csv":  "text/csv",
        "json": "application/json",
        "gz":   "application/gzip",
        "zip":  "application/zip",
        "txt":  "text/plain",
    }
    return content_types.get(ext, "application/octet-stream")


def _sanitize_filename(name: str) -> str:
    """
    Returns a filesystem-safe version of a client-supplied filename for use in
    on-disk paths (path-traversal protection).

    - Strips any directory components a client may have sent, handling both
      POSIX ("../../x") and Windows ("..\\..\\x") separators.
    - Replaces anything outside a conservative whitelist with "_".
    - Guards against names that are empty or only dots after sanitizing.

    The raw filename is still stored in the DB for display — only the on-disk
    name is sanitized. Uniqueness on disk comes from the UUID prefix, so this
    name only needs to be safe, not unique.
    """
    name = os.path.basename(name.replace("\\", "/"))
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    name = name.lstrip(".") or "file"
    return name
