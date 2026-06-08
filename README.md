# File Processing Pipeline

A file processing pipeline that accepts file uploads, processes them through
configurable steps, and delivers results asynchronously.

---

## How to Run

Requirements: Docker Desktop or Rancher Desktop

    git clone git@github.com:YOUR_USERNAME/file-pipeline.git
    cd file-pipeline
    docker compose up --build

API available at: http://localhost:8080

---

## How to Run Tests

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    pytest tests/ -v

---

## Example API Calls

### 1. Upload a file and start a pipeline

    curl -X POST http://localhost:8080/upload \
      -F "file=@/path/to/your/data.csv" \
      -F 'pipeline=[
        {"step":"validate",  "params":{"expected_type":"csv"}},
        {"step":"transform", "params":{"select_columns":["name","email"]}},
        {"step":"convert",   "params":{"output_format":"json"}},
        {"step":"compress",  "params":{"algorithm":"gzip"}},
        {"step":"notify",    "params":{"webhook_url":"https://your-webhook.com"}}
      ]'

Response:

    {
      "job_id": "943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2",
      "status": "PENDING",
      "filename": "data.csv",
      "size": 1234,
      "message": "File uploaded successfully. Use /jobs/943e4ca9-... to track progress."
    }

---

### 2. Check job status — human readable

    ./check_job.sh 943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2

Output:

    Checking job: 943e4ca9-...
    ================================
    Status:    COMPLETED
    Progress:  100.0%
    Error:     none
    Duration:  2.3s

    Steps:
      0. validate     COMPLETED
      1. transform    COMPLETED  Duration: 0.5s
      2. convert      COMPLETED  Duration: 0.8s
      3. compress     COMPLETED  Duration: 0.4s
      4. notify       COMPLETED

---

### 3. Check job status — raw JSON

    curl http://localhost:8080/jobs/943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2

---

### 4. Download result

    curl -O http://localhost:8080/jobs/943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2/result

---

### 5. Cancel a job

    curl -X POST http://localhost:8080/jobs/943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2/cancel

---

## Processing Steps Implemented

| Step      | Description                                                     |
|-----------|-----------------------------------------------------------------|
| validate  | Check file type, size limit, corruption. Runs at any pipeline point |
| transform | Filter rows, select columns, text transforms (upper/lower/trim) |
| convert   | Convert CSV to JSON or JSON to CSV                              |
| compress  | Gzip compress/decompress, zip extraction                        |
| notify    | Webhook callback on completion with exponential backoff retry   |

---

## Health Check

    curl http://localhost:8080/health

---

## Project Structure

    file-pipeline/
    ├── app/
    │   ├── api/
    │   │   ├── upload.py      # POST /upload
    │   │   └── status.py      # GET /jobs/{id}, GET /jobs/{id}/result, POST /jobs/{id}/cancel
    │   ├── models/
    │   │   ├── database.py    # SQLite setup
    │   │   └── job.py         # Job, JobStep, FileReference models
    │   ├── steps/
    │   │   ├── validate.py    # Validation step
    │   │   ├── transform.py   # Transform step
    │   │   ├── convert.py     # Conversion step
    │   │   ├── compress.py    # Compression step
    │   │   └── notify.py      # Webhook notification step
    │   ├── workers/
    │   │   └── processor.py   # Job runner — orchestrates all steps
    │   └── main.py            # FastAPI app, startup, routing, cleanup
    ├── tests/                 # Test suite
    ├── storage/
    │   ├── uploads/           # Uploaded and intermediate files
    │   └── outputs/           # Final processed output files
    ├── check_job.sh           # Human readable job status checker
    ├── DECISIONS.md           # All architecture and design decisions
    ├── AI_USAGE.md            # AI tool usage documentation
    ├── docker-compose.yml     # Redis + API + Worker services
    ├── Dockerfile
    └── requirements.txt
