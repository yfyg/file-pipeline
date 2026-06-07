import os
import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.job import Job, JobStep, FileReference

router = APIRouter()
log = logging.getLogger("status")


@router.get("/jobs/{job_id}")
def get_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get full status of a job including all step details.

    Returns:
        - Overall job status
        - Current step index
        - Per-step status, duration, progress
        - Output file location when complete
        - Error details if failed
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    # Get input file info
    input_file = db.query(FileReference).filter(
        FileReference.id == job.input_file_id
    ).first()

    # Get output file info if job completed
    output_file = None
    if job.output_file_id:
        output_file = db.query(FileReference).filter(
            FileReference.id == job.output_file_id
        ).first()

    # Get all steps
    steps = db.query(JobStep).filter(
        JobStep.job_id == job_id
    ).order_by(JobStep.step_index).all()

    # Calculate overall progress
    total_steps     = len(steps)
    completed_steps = len([s for s in steps if s.status == "COMPLETED"])
    overall_progress = (
        round((completed_steps / total_steps) * 100, 1)
        if total_steps > 0 else 0
    )

    return {
        "job_id":           job.id,
        "status":           job.status,
        "current_step":     job.current_step_index,
        "overall_progress": f"{overall_progress}%",
        "error":            job.error_message,
        "created_at":       job.created_at,
        "started_at":       job.started_at,
        "completed_at":     job.completed_at,
        "duration_seconds": (
            (job.completed_at - job.started_at).total_seconds()
            if job.completed_at and job.started_at else None
        ),
        "input_file": {
            "filename":   input_file.original_filename if input_file else None,
            "size":       input_file.size if input_file else None,
            "size_mb":    round(input_file.size / (1024 * 1024), 2) if input_file else None,
        },
        "output_file": {
            "filename":   output_file.original_filename if output_file else None,
            "size":       output_file.size if output_file else None,
            "size_mb":    round(output_file.size / (1024 * 1024), 2) if output_file else None,
            "expires_at": output_file.expires_at if output_file else None,
            "download_url": f"/jobs/{job_id}/result" if output_file else None,
        } if output_file else None,
        "pipeline": job.pipeline,
        "steps": [
            {
                "index":      step.step_index,
                "type":       step.step_type,
                "status":     step.status,
                "progress":   f"{step.progress}%",
                "error":      step.error_message,
                "started_at": step.started_at,
                "completed_at": step.completed_at,
                "duration_seconds": step.duration,
            }
            for step in steps
        ]
    }


@router.get("/jobs/{job_id}/result")
def download_result(job_id: str, db: Session = Depends(get_db)):
    """
    Download the processed output file.

    Returns the file as a download.
    Returns 404 if job not found or not completed yet.
    Returns 410 Gone if file has expired.
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    # Job must be completed to download result
    if job.status != "COMPLETED":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed yet. Current status: {job.status}"
        )

    if not job.output_file_id:
        raise HTTPException(status_code=404, detail="No output file found for this job")

    # Get output file reference
    output_file = db.query(FileReference).filter(
        FileReference.id == job.output_file_id
    ).first()

    if not output_file:
        raise HTTPException(status_code=404, detail="Output file record not found")

    # Check if file has expired
    from datetime import datetime
    if output_file.expires_at and output_file.expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=410,
            detail="Result file has expired. Please reprocess the file."
        )

    # Check file exists on disk
    if not os.path.exists(output_file.storage_path):
        raise HTTPException(
            status_code=404,
            detail="Output file not found on disk. It may have been cleaned up."
        )

    log.info(f"Serving result file for job {job_id}: {output_file.storage_path}")

    return FileResponse(
        path              = output_file.storage_path,
        filename          = output_file.original_filename,
        media_type        = output_file.content_type
    )


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    """
    Cancel a job that is still PENDING or PROCESSING.
    Marks all remaining steps as SKIPPED.
    Cannot cancel a COMPLETED or FAILED job.
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    # Can only cancel jobs that haven't finished
    if job.status in ("COMPLETED", "FAILED", "CANCELLED"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status {job.status}"
        )

    # Mark pending steps as SKIPPED
    pending_steps = db.query(JobStep).filter(
        JobStep.job_id == job_id,
        JobStep.status == "PENDING"
    ).all()

    for step in pending_steps:
        step.status = "SKIPPED"

    # Mark job as CANCELLED
    from datetime import datetime
    job.status       = "CANCELLED"
    job.completed_at = datetime.utcnow()
    db.commit()

    log.info(f"Job {job_id} cancelled")

    return {
        "job_id":  job_id,
        "status":  "CANCELLED",
        "message": "Job cancelled successfully"
    }
