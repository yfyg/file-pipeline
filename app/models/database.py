from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import os

# Store DB inside storage/ — already mounted as a Docker volume
# This means both api and worker containers share the same DB
os.makedirs("storage", exist_ok=True)
SQLALCHEMY_DATABASE_URL = "sqlite:///./storage/pipeline.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
