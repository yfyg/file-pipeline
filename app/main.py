import os
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.models.database import engine, Base
from app.api.upload import router as upload_router
from app.api.status import router as status_router

# Configure structured logging
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs on startup and shutdown.
    Creates DB tables and cleans up orphan/expired files.
    """
    # Startup
    log.info("Starting up...")

    # Create all DB tables if they don't exist
    Base.metadata.create_all(bind=engine)
    log.info("Database tables ready")

    # Create storage directories if they don't exist
    os.makedirs("storage/uploads", exist_ok=True)
    os.makedirs("storage/outputs", exist_ok=True)
    os.makedirs("storage/intermediate", exist_ok=True)
    log.info("Storage directories ready")

    # Clean up any leftover temp files from previous crashes
    _cleanup_temp_files()

    # Clean up expired files
    _cleanup_expired_files()

    log.info("Startup complete")

    yield  # app runs here

    # Shutdown
    log.info("Shutting down...")


app = FastAPI(
    title       = "File Processing Pipeline",
    description = "Upload files and process them through configurable pipelines",
    version     = "1.0.0",
    lifespan    = lifespan
)

# Register routers
app.include_router(upload_router, tags=["Upload"])
app.include_router(status_router, tags=["Status"])


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    Returns service status and current time.
    """
    return {
        "status":  "healthy",
        "time":    datetime.utcnow().isoformat(),
        "service": "file-processing-pipeline"
    }


def _cleanup_temp_files():
    """
    Deletes any tmp_ files left over from crashed uploads.
    These are safe to delete — they are never complete files.
    """
    upload_dir = "storage/uploads"
    if not os.path.exists(upload_dir):
        return

    cleaned = 0
    for filename in os.listdir(upload_dir):
        if filename.startswith("tmp_"):
            path = os.path.join(upload_dir, filename)
            try:
                os.remove(path)
                cleaned += 1
                log.info(f"Cleaned up temp file: {path}")
            except Exception as e:
                log.warning(f"Could not delete temp file {path}: {str(e)}")

    if cleaned > 0:
        log.info(f"Cleaned up {cleaned} temp files on startup")


def _cleanup_expired_files():
    """
    Deletes files whose retention period has expired.
    Checks expires_at on FileReference records.
    Also marks stuck PENDING/PROCESSING jobs as FAILED.
    """
    from app.models.database import SessionLocal
    from app.models.job import Job, FileReference

    db = SessionLocal()
    try:
        now = datetime.utcnow()

        # Find expired file references
        expired_files = db.query(FileReference).filter(
            FileReference.expires_at < now
        ).all()

        for file_ref in expired_files:
            # Skip rows we already soft-deleted in a previous sweep
            if file_ref.deleted_at is not None:
                continue

            # Delete from disk
            if os.path.exists(file_ref.storage_path):
                try:
                    os.remove(file_ref.storage_path)
                    log.info(f"Deleted expired file: {file_ref.storage_path}")
                except Exception as e:
                    log.warning(f"Could not delete expired file: {str(e)}")

            # Soft delete — preserves audit history while preventing
            # duplicate detection from returning a job whose file is gone.
            # Hard delete would break foreign keys from Job.input_file_id etc.
            file_ref.deleted_at = now

        if expired_files:
            db.commit()
            log.info(f"Cleaned up {len(expired_files)} expired files (soft-deleted)")

        # Mark stuck jobs as FAILED
        # Any job stuck in PENDING or PROCESSING for more than 1 hour
        stuck_cutoff = now - timedelta(hours=1)
        stuck_jobs   = db.query(Job).filter(
            Job.status.in_(["PENDING", "PROCESSING"]),
            Job.created_at < stuck_cutoff
        ).all()

        for job in stuck_jobs:
            job.status        = "FAILED"
            job.error_message = "Job timed out — stuck in queue for more than 1 hour"
            job.completed_at  = now
            log.warning(f"Marked stuck job as FAILED: {job.id}")

        if stuck_jobs:
            log.info(f"Marked {len(stuck_jobs)} stuck jobs as FAILED")

        db.commit()

    except Exception as e:
        log.error(f"Cleanup failed: {str(e)}")
    finally:
        db.close()
