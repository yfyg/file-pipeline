import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from app.models.database import Base

def generate_uuid():
    return str(uuid.uuid4())

class FileReference(Base):
    __tablename__ = "file_references"

    id = Column(String, primary_key=True, default=generate_uuid)
    storage_path = Column(String, nullable=False)
    original_filename = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    content_type = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=generate_uuid)
    input_file_id = Column(String, ForeignKey("file_references.id"), nullable=False)
    output_file_id = Column(String, ForeignKey("file_references.id"), nullable=True)
    pipeline = Column(JSON, nullable=False)
    status = Column(String, default="PENDING")  # PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED
    current_step_index = Column(Integer, default=0)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    input_file = relationship("FileReference", foreign_keys=[input_file_id])
    output_file = relationship("FileReference", foreign_keys=[output_file_id])
    steps = relationship("JobStep", back_populates="job", order_by="JobStep.step_index")


class JobStep(Base):
    __tablename__ = "job_steps"

    id = Column(String, primary_key=True, default=generate_uuid)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False)
    step_index = Column(Integer, nullable=False)
    step_type = Column(String, nullable=False)
    parameters = Column(JSON, nullable=True)
    status = Column(String, default="PENDING")  # PENDING, RUNNING, COMPLETED, FAILED, SKIPPED
    input_file_id = Column(String, ForeignKey("file_references.id"), nullable=True)
    output_file_id = Column(String, ForeignKey("file_references.id"), nullable=True)
    error_message = Column(String, nullable=True)
    progress = Column(Float, default=0.0)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)

    job = relationship("Job", back_populates="steps")
    input_file = relationship("FileReference", foreign_keys=[input_file_id])
    output_file = relationship("FileReference", foreign_keys=[output_file_id])
