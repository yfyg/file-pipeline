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
    Deep health check: pings Redis and the DB so orchestration can tell
    "process is up" from "process is up but a dependency is down".

    Returns 200 with status="healthy" only when both checks pass.
    Returns 503 with per-dependency detail when one or more fails.
    Worth doing here because a shallow "always 200" endpoint fools load
    balancers into routing traffic to a degraded instance.
    """
    from fastapi.responses import JSONResponse
    from sqlalchemy import text
    from app.models.database import SessionLocal

    checks = {"redis": "unknown", "db": "unknown"}

    # Redis — quick PING, short timeout so a hung Redis doesn't hang /health
    try:
        from redis import Redis
        redis_conn = Redis(
            host = os.getenv("REDIS_HOST", "localhost"),
            port = int(os.getenv("REDIS_PORT", "6379")),
            socket_connect_timeout = 2,
            socket_timeout = 2,
        )
        redis_conn.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"down: {str(e)[:100]}"

    # DB — trivial SELECT 1 confirms the file is reachable and the schema is up
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"down: {str(e)[:100]}"
    finally:
        db.close()

    healthy = all(v == "ok" for v in checks.values())
    body = {
        "status":   "healthy" if healthy else "unhealthy",
        "time":     datetime.utcnow().isoformat(),
        "service":  "file-processing-pipeline",
        "checks":   checks,
    }
    return JSONResponse(content=body, status_code=200 if healthy else 503)


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
    Deletes files whose retention period has expired and recovers stuck jobs:
    - PENDING > 5min → re-enqueue (likely lost by Redis or enqueue failed
      after the upload commit — work hasn't started so re-running is safe)
    - PROCESSING > 1h → mark FAILED (worker died mid-pipeline; resuming a
      half-finished step is out of scope, see DECISIONS §10)
    """
    from app.models.database import SessionLocal
    from app.models.job import FileReference
    from app.workers.sweeper import recover_stuck_jobs

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

        # Recover stuck jobs (re-enqueue PENDING, fail PROCESSING)
        recover_stuck_jobs(db)

    except Exception as e:
        log.error(f"Cleanup failed: {str(e)}")
    finally:
        db.close()
