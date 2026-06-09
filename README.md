# File Processing Pipeline

A file processing pipeline that accepts file uploads, processes them through
configurable steps, and delivers results asynchronously. Built with FastAPI,
Redis + RQ, and SQLite.

- **5 step types:** validate, transform, convert, compress, notify
- **Streaming end-to-end** — never loads a full file into memory (tested up
  to 90MB / 1.45M rows)
- **39 pytest tests** (38 fast + 1 slow) — run in-process, no Docker / Redis required
- **Stuck-job recovery** — PENDING jobs auto-re-enqueued on startup and on
  every status query
- **Security:** path-traversal sanitization, Zip Slip protection, SSRF guard
  on webhook URLs

See `DECISIONS.md` for full design rationale and `AI_USAGE.md` for AI-tool
usage notes.

---

## Quick Start

Requirements: Docker Desktop or Rancher Desktop.

```bash
git clone git@github.com:YOUR_USERNAME/file-pipeline.git
cd file-pipeline
docker compose up --build
```

API available at: **http://localhost:8080**.

Health check:

```bash
curl http://localhost:8080/health
```

---

## How to Run Tests

The test suite runs in-process — no Docker, no Redis, no worker container
needed.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Fast suite (39 tests, ~2 seconds)
pytest tests/ -v

# Include the slow large-file test (~30 seconds total)
pytest tests/ -m slow -v
pytest tests/ -m "" -v        # run everything
```

Slow tests are excluded from the default run via `pytest.ini`.

---

## Example API Calls

### 1. Upload a file and start a pipeline

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@/path/to/your/data.csv" \
  -F 'pipeline=[
    {"step":"validate",  "params":{"expected_type":"csv"}},
    {"step":"transform", "params":{"select_columns":["name","email"]}},
    {"step":"convert",   "params":{"output_format":"json"}},
    {"step":"compress",  "params":{"algorithm":"gzip"}},
    {"step":"notify",    "params":{"webhook_url":"https://your-webhook.com"}}
  ]'
```

Response:

```json
{
  "job_id":   "943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2",
  "status":   "PENDING",
  "filename": "data.csv",
  "size":     1234,
  "message":  "File uploaded successfully. Use /jobs/<id> to track progress."
}
```

### 2. Check job status (human-readable)

```bash
./check_job.sh 943e4ca9-5970-48e9-b01d-f9a5bcbd7ef2
```

Output:

```
Status:    COMPLETED
Progress:  100.0%
Duration:  2.3s

Steps:
  0. validate    ✅ COMPLETED
  1. transform   ✅ COMPLETED  Duration: 0.5s  Rows: 100 in -> 100 out
  2. convert     ✅ COMPLETED  Duration: 0.8s  Rows: 100 in -> 100 out
  3. compress    ✅ COMPLETED  Duration: 0.4s
  4. notify      ✅ COMPLETED
```

### 3. Check job status (raw JSON)

```bash
curl http://localhost:8080/jobs/<job_id>
```

### 4. Download the result

```bash
curl -O http://localhost:8080/jobs/<job_id>/result
```

### 5. Cancel a job

```bash
curl -X POST http://localhost:8080/jobs/<job_id>/cancel
```

A job that's still PENDING or PROCESSING is marked CANCELLED and any
unstarted steps become SKIPPED. Already-COMPLETED or FAILED jobs cannot be
cancelled (returns 400).

---

## Processing Steps

| Step      | Purpose                                                              |
|-----------|----------------------------------------------------------------------|
| validate  | Check file type, size, corruption. Re-runnable after a `convert` step. |
| transform | Filter rows, select columns, text transforms (upper / lower / trim). |
| convert   | CSV ↔ JSON, fully streamed (row-by-row + `ijson`).                   |
| compress  | gzip compress / decompress, zip extract (with Zip Slip guard).       |
| notify    | Webhook callback on completion. SSRF-protected, retried with backoff. |

Each step is `(file_path, params) -> (output_path, stats)`. Steps are
chained by passing the output path of one step as the input to the next.

---

## Helper Scripts

All scripts assume you're in the repo root and the stack is running
(`docker compose up`). Each is a thin convenience wrapper around the API.

| Script              | What it does                                                                                                                                    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `check_job.sh`      | `./check_job.sh <job_id>` — pretty-prints job status, per-step durations, and row counts.                                                       |
| `run_test.sh`       | `./run_test.sh <file> '<pipeline-json>'` — uploads the file, polls until the job finishes, then prints the final status. Easiest end-to-end.    |
| `verify_output.sh`  | `./verify_output.sh <job_id> [expected_input_rows] [expected_output_rows]` — inspects the output file and asserts row counts match the API.     |
| `run_big_demo.sh`   | Generates a ~90MB CSV, runs filter + convert through the pipeline, and reports input/output sizes — demonstrates streaming on a near-cap file.  |
| `dump_db.sh`        | Dumps the entire pipeline DB joined into one block per step (`.mode line`). Useful for debugging or showing the data model.                     |

### Example flow with the helpers

```bash
# 1. Generate a small test CSV
cat > /tmp/data.csv <<'CSV'
name,email,age
Alice,alice@example.com,30
Bob,bob@example.com,25
Charlie,charlie@example.com,40
CSV

# 2. Run the pipeline (returns JOB_ID at the end)
./run_test.sh /tmp/data.csv '[
  {"step":"validate", "params":{"expected_type":"csv"}},
  {"step":"transform","params":{"select_columns":["name","email"],
                                 "filter_rows":{"column":"age","gt":28}}},
  {"step":"convert",  "params":{"output_format":"json"}}
]'

# Copy the JOB_ID line from the output, then:
JOB=<paste-job-id>

# 3. Verify the output (expecting 3 rows in, 2 rows out)
./verify_output.sh $JOB 3 2

# 4. Download the result
curl -O http://localhost:8080/jobs/$JOB/result

# 5. (Optional) Inspect the DB
./dump_db.sh | less
```

---

## Project Structure

```
file-pipeline/
├── app/
│   ├── api/
│   │   ├── upload.py        # POST /upload  (commit-then-enqueue, sanitize filename)
│   │   └── status.py        # GET /jobs/{id}, GET /jobs/{id}/result, POST /jobs/{id}/cancel
│   │                        # status query also runs stuck-job sweeper (self-heal)
│   ├── models/
│   │   ├── database.py      # SQLite engine + SessionLocal
│   │   └── job.py           # Job, JobStep, FileReference (with soft-delete)
│   ├── steps/
│   │   ├── validate.py      # Sample-based corruption check + expected_type
│   │   ├── transform.py     # Streaming filter / select / text transform
│   │   ├── convert.py       # CSV ↔ JSON streamed
│   │   ├── compress.py      # gzip + zip (with Zip Slip guard)
│   │   └── notify.py        # Webhook with SSRF guard + idempotency header
│   ├── workers/
│   │   ├── processor.py     # Pipeline orchestrator + retry policy
│   │   └── sweeper.py       # Stuck-job recovery (PENDING re-enqueue, PROCESSING fail)
│   └── main.py              # FastAPI app, startup hooks, /health
├── tests/                   # 40 pytest tests (39 fast + 1 slow), no Docker needed
│   ├── conftest.py          # Temp DB + storage per test; synchronous job runner patch
│   ├── test_api.py          # Upload, status, result download, no-dedup, file chain (7)
│   ├── test_cancel.py       # Cancel endpoint behavior (4)
│   ├── test_health.py       # Deep /health: Redis + DB pings (3)
│   ├── test_notify.py       # Webhook payload, retries, SSRF (6)
│   ├── test_pipeline.py     # End-to-end, failure, conversion, compression (5)
│   ├── test_retry_and_compress.py  # OSError retry policy + gzip/zip edge cases (6)
│   ├── test_sweeper.py      # Stuck-job recovery (2)
│   ├── test_transform.py    # Filter behavior (6)
│   └── test_slow_large_file.py     # 90MB streaming test (1, opt-in via -m slow)
├── storage/                 # Bind-mounted into containers
│   ├── uploads/             # Original uploaded files
│   ├── intermediate/        # Files between pipeline steps (kept on failure for debugging)
│   └── outputs/             # Final results, retention via RETENTION_HOURS (default 24h)
├── check_job.sh             # Pretty-print job status
├── run_test.sh              # Upload + poll until done
├── verify_output.sh         # Inspect output file + assert row counts
├── run_big_demo.sh          # 90MB streaming demo
├── dump_db.sh               # Dump entire DB joined per step
├── DECISIONS.md             # Architecture and design rationale
├── AI_USAGE.md              # AI-tool usage notes
├── docker-compose.yml       # Redis + API + Worker
├── Dockerfile
├── pytest.ini               # Registers @pytest.mark.slow (excluded by default)
└── requirements.txt
```

---

## Environment Variables

| Variable          | Default                  | Purpose                                          |
|-------------------|--------------------------|--------------------------------------------------|
| `REDIS_HOST`      | `localhost`              | Redis hostname (set to `redis` in docker-compose). |
| `REDIS_PORT`      | `6379`                   | Redis port.                                      |
| `RETENTION_HOURS` | `24`                     | How long uploaded / intermediate / output files live before the expiry sweeper deletes them. |

---

## Resetting Local State

If you change the data model (add or remove columns) you need to delete the
dev DB so SQLAlchemy recreates it with the new schema. `Base.metadata.create_all`
does not migrate existing tables.

```bash
docker compose down
rm -f storage/pipeline.db
docker compose up --build
```

To also wipe leftover files from previous runs:

```bash
rm -rf storage/uploads/* storage/intermediate/* storage/outputs/*
```

---

## Limitations & Known Trade-offs

This is an interview-scope implementation; some real choices were
deliberately deferred. The honest list is in **DECISIONS §16** ("Known
Limitations & Deliberate Simplifications") and **DECISIONS §10** ("Things
We Would Do Differently With More Time"). Highlights:

- No auth — any caller can upload and access any job
- SQLite — single-writer, swap to Postgres at any real scale
- Resume from last successful step — not implemented; FAILED jobs restart
  from scratch
- Within-step row-count progress — we expose row counts *after* each step
  finishes, but not live progress while a step is running
- 100MB upload cap — sufficient for the spec, raise the constant in
  `app/api/upload.py:21` to lift it

---

## Documentation Map

If you're new here, read in this order:

1. **README.md** (this file) — what is it, how to run it
2. **DECISIONS.md** — the design choices and trade-offs, ~640 lines
3. **AI_USAGE.md** — how AI tools were used during development
