import os
import uuid
import hashlib
import shutil
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_
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
    file:            UploadFile = File(...),
    pipeline:        str        = Form(...),  # JSON string
    allow_duplicate: bool       = Form(False),
    db:              Session    = Depends(get_db)
):
    """
    Upload a file and start a processing pipeline.

    - Streams file to disk in 8KB chunks — never loads full file into memory
    - Checks file size, type, and duplicate status before accepting
    - Returns job_id immediately — processing happens asynchronously

    Parameters:
        file:            The file to upload
        pipeline:        JSON string defining processing steps
        allow_duplicate: If false (default), reject if same filename or hash exists
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

        # Step 3 — Stream file to temp location in 8KB chunks
        # Temp path used so partial uploads never reach permanent storage
        temp_filename = f"tmp_{uuid.uuid4()}_{original_filename}"
        temp_path     = os.path.join(UPLOAD_DIR, temp_filename)
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        total_size = 0
        hasher     = hashlib.md5()

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

                # Update hash incrementally — no extra memory needed
                hasher.update(chunk)

        file_hash = hasher.hexdigest()
        log.info(f"File streamed to temp: {temp_path} size={total_size} hash={file_hash}")

        # Step 4 — Check for duplicates (filename OR hash)
        if not allow_duplicate:
            existing_file = db.query(FileReference).filter(
                or_(
                    FileReference.original_filename == original_filename,
                    FileReference.file_hash         == file_hash
                )
            ).first()

            if existing_file:
                # Find the most recent job for this file
                existing_job = db.query(Job).filter(
                    Job.input_file_id == existing_file.id
                ).order_by(Job.created_at.desc()).first()

                # Delete temp file — we don't need it
                os.remove(temp_path)
                temp_path = None

                log.info(f"Duplicate file detected — returning existing job {existing_job.id}")
                return {
                    "job_id":  existing_job.id if existing_job else None,
                    "status":  "already_exists",
                    "message": "File already uploaded. Use allow_duplicate=true to reprocess."
                }

        # Step 5 — Move from temp to permanent location
        final_filename = f"{uuid.uuid4()}_{original_filename}"
        final_path     = os.path.join(UPLOAD_DIR, final_filename)
        shutil.move(temp_path, final_path)
        temp_path = None  # no longer temp — don't delete on error
        log.info(f"File moved to permanent storage: {final_path}")

        # Step 6 — Save FileReference to DB
        file_ref = FileReference(
            storage_path      = final_path,
            original_filename = original_filename,
            size              = total_size,
            content_type      = _get_content_type(ext),
            file_hash         = file_hash,
            created_at        = datetime.utcnow(),
            expires_at        = datetime.utcnow() + timedelta(hours=RETENTION_HOURS)
        )
        db.add(file_ref)
        db.flush()  # get file_ref.id without full commit yet

        # Step 7 — Create Job record
        job = Job(
            input_file_id = file_ref.id,
            pipeline      = pipeline_def,
            status        = "PENDING",
            created_at    = datetime.utcnow()
        )
        db.add(job)
        db.flush()  # get job.id without full commit yet

        # Step 8 — Create JobStep records for each step
        for index, step_def in enumerate(pipeline_def):
            step = JobStep(
                job_id     = job.id,
                step_index = index,
                step_type  = step_def.get("step"),
                parameters = step_def.get("params", {}),
                status     = "PENDING"
            )
            db.add(step)

        # Step 9 — Enqueue job BEFORE committing
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

        # Step 10 — Commit everything together
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
