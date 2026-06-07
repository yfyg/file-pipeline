# Design Decisions

## Summary of Approach
This pipeline is built with one core principle: never load a full file into memory.
Files can be up to 100MB, so every read, transform, and write operation is streamed
or chunked. Below are the specific decisions made for each area.

---

## 1. Large File Handling

**Approach chosen:** Streaming and chunked processing throughout the entire pipeline.

**How we handle each file type:**

| File Type | Strategy | Tool |
|-----------|-----------|------|
| CSV | Row by row | csv.DictReader — yields one row at a time |
| JSON | Object by object | ijson.items — yields one object at a time |
| Binary / TXT / GZ / ZIP | 8KB chunks | file.read(8192) in a loop |

**Why 8KB chunks for binary files:**
- Matches typical disk block size — efficient I/O
- Too small (1KB) = too many disk reads = slow
- Too large (10MB) = too much memory usage
- 8KB is the sweet spot

**Memory usage:**
- CSV: size of one row (bytes)
- JSON: size of one object (bytes)
- Binary: exactly 8KB
- Never proportional to file size

**Upload handling:**
- Files are streamed to disk in chunks during upload
- Full file is never held in memory during upload
- Size is checked before accepting (100MB limit enforced at upload API level)

**Disk-based intermediate storage:**
- Each step reads its input from disk and writes output to disk
- No step passes data to the next step via memory
- This means we can resume from any step if needed

---

## 2. File Validation Strategy

**Two layers of validation:**

**Layer 1 — At upload time (fast, lightweight):**
- Check file size is 100MB or less
- Check file extension is in allowed list
- Read small sample to catch obvious corruption:
  - JSON: read first 4KB, check starts with { or [
  - CSV: read header + first data row only
  - Other: read first 1KB bytes

**Why sample only at upload:**
- We do not want to block the upload response while reading 100MB
- Obvious corruption (wrong format, empty file) is caught immediately
- Deep corruption is caught during actual processing step

**Layer 2 — During validate step (thorough):**
- Processes file fully during the pipeline execution
- Runs as an async background job — does not block the user

---

## 3. Step Failure Strategy

**Approach chosen:** Fail the job on step failure, preserve all intermediate files.

**Why fail instead of skip:**
- Steps depend on each other (transform output feeds convert input)
- Skipping a failed step would pass corrupted data to the next step
- Better to fail clearly than silently produce wrong output

**Retry logic:**
- Each step gets up to 3 retry attempts before marking as FAILED
- Retries help with transient issues (disk write errors, temporary locks)
- After 3 failures the job status becomes FAILED with clear error message

**Partial progress:**
- Each step writes its output to a separate file on disk before marking COMPLETED
- If a step fails, all previous steps output files are preserved
- A failed job can be inspected at exactly the step that failed

---

## 4. File Cleanup Strategy

**Approach chosen:** Time-based expiry with a background cleanup process.

**Input files:** Deleted 24 hours after job completes (success or failure)
**Intermediate files:** Deleted 24 hours after job completes
**Output files:** Deleted 24 hours after job completes

**If service crashes:**
- Files stay on disk (safe — nothing is lost)
- On restart, a cleanup job scans for expired files
- Jobs stuck in PROCESSING are marked FAILED after a timeout

---

## 5. Progress Tracking

**Approach chosen:** Step-level progress stored in database.

**What we track:**
- Per job: current step index, overall status
- Per step: status (PENDING/RUNNING/COMPLETED/FAILED/SKIPPED), start time, end time, duration
- For CSV/JSON steps: row count progress updated every 1000 rows

**Why every 1000 rows:**
- More frequent = too many DB writes = overhead
- Less frequent = progress looks stuck
- 1000 rows is a good balance for files up to 100MB

**Trade-offs:**
- Binary files (compress/decompress) show 0% then 100% with no in-between
- JSON streaming via ijson does not know total count upfront
- JSON shows rows processed count rather than percentage

---

## 6. Webhook Reliability

**Approach chosen:** Retry with exponential backoff. Job is NOT failed if webhook fails.

**Retry strategy:**
- Up to 5 retry attempts
- Wait times: 5s then 15s then 30s then 60s then 120s
- If all 5 attempts fail: step marked FAILED but job marked COMPLETED
- Webhook failure does not fail the job — the file was processed successfully

**Why webhook failure does not equal job failure:**
- The file processing succeeded — the result is there
- User can still retrieve result via the status API

**Preventing duplicate notifications:**
- Step status is set to RUNNING before first attempt
- Status is only set to COMPLETED after a successful response
- Idempotency key sent in webhook header so receiver can deduplicate

---

## 7. Tech Stack Decisions

| Component | Choice | Why |
|-----------|--------|-----|
| Web framework | FastAPI | Async, streaming uploads built-in, automatic API docs |
| Database | SQLite | Simple, no extra service needed, fine for this scale |
| Job queue | Redis + RQ | Simple, Docker-friendly, good visibility tools |
| Storage | Local filesystem | Assignment explicitly allows it |
| JSON streaming | ijson | True item-by-item streaming, same mental model as csv.DictReader |
| Code style | Functions over classes | Simpler to read and understand; classes only where required (SQLAlchemy) |

---

## 8. Duplicate Handling

**Three types of duplication considered:**

**Type 1 — Duplicate file uploads:**
- Detected via MD5 hash of file content (streamed 8KB chunks — memory safe)
- Also checked by original filename
- Hash and filename stored in FileReference table

**Type 2 — Duplicate job processing (two workers same job):**
- Prevented by atomically setting status to PROCESSING before starting work
- Worker checks status == PENDING before proceeding
- If another worker already set it to PROCESSING the current worker exits immediately

**Type 3 — Duplicate webhook notifications:**
- Step status checked before each retry attempt
- If status is already COMPLETED the webhook call is skipped
- Idempotency key sent in webhook header so receiver can deduplicate

---

## 9. Duplicate Upload Parameter

**Approach chosen:** Optional allow_duplicate parameter on upload API (default: false).

**Duplicate detection rule:**

If filename matches OR hash matches AND allow_duplicate is false
    - Reject upload
    - Return existing job ID
    - Delete temp file from disk
    - HTTP 200 — this is not an error, it is idempotent behavior

If filename matches OR hash matches AND allow_duplicate is true
    - Accept upload
    - Create new job
    - Process normally

If no match at all
    - Always accept and create new job regardless of allow_duplicate

**Why OR logic (filename OR hash):**
- Same filename, different content — still confusing, treat as duplicate
- Same content, different filename — same data, no point reprocessing
- Either condition alone is enough to flag as duplicate

**Storage safety:**
- Files always saved with UUID prefix: {uuid}_{original_name}
- Prevents any overwrite on disk even when allow_duplicate is true
- Original filename preserved in DB for display purposes only

---

## 10. One Thing I Would Do Differently With More Time

**Resume failed jobs from last successful step:**
- Currently a failed job must be restarted from the beginning
- All intermediate files are preserved on disk — the data is there
- With more time: add a /jobs/{id}/retry endpoint that resumes from last COMPLETED step
- This would save significant processing time for long pipelines that fail late

**True JSON streaming for all cases:**
- ijson works perfectly for arrays of objects
- For deeply nested JSON structures a different approach would be needed
- For this assignment we assume JSON files are arrays of flat objects

**Replace SQLite with PostgreSQL in production:**
- SQLite has write locking issues under concurrent load
- Multiple workers writing job status simultaneously can cause contention
- PostgreSQL handles this cleanly and is the right choice for production

---

## 11. Validation After Conversion

**Question:** Should we re-run validation after a convert step?

**Answer:** Two separate concerns:

**User-defined validation (explicit pipeline step):**
- The validate step is user-controlled
- If user wants to validate after convert they add a second validate step
- Example: validate(csv) → convert → validate(json) → compress
- We never force or auto-insert validate steps

**Internal sanity check (automatic after every step):**
- After every step the worker checks the output file:
  - File exists on disk
  - File is not empty
- This is lightweight — not a full validation
- Catches cases where a step silently failed to produce output
- Runs automatically regardless of pipeline definition

**Why this separation:**
- Keeps the pipeline predictable — user sees exactly what runs
- Internal check is a safety net, not a business rule
- User validation rules (expected_type etc.) are intentional choices

---

## 12. Recommended Pipeline Pattern

**We recommend always running validate after convert:**

validate (original type)
  → transform
    → convert
      → validate (new type)   <- recommended
        → compress
          → notify

**Why:**
- Convert changes the file format completely
- The new file is essentially a new file — should be validated fresh
- Catches conversion errors early before compress or other steps run
- If validate fails after convert it means conversion produced bad output

**Example pipeline with recommended pattern:**
{
  "pipeline": [
    {"step": "validate",  "params": {"expected_type": "csv"}},
    {"step": "transform", "params": {"select_columns": ["name", "email"]}},
    {"step": "convert",   "params": {"output_format": "json"}},
    {"step": "validate",  "params": {"expected_type": "json"}},
    {"step": "compress",  "params": {"algorithm": "gzip"}},
    {"step": "notify",    "params": {"webhook_url": "https://..."}}
  ]
}

**Important — expected_type must match the converted format:**
- After CSV to JSON conversion: expected_type should be "json"
- After JSON to CSV conversion: expected_type should be "csv"
- Passing wrong expected_type will correctly fail the job

**User is in control:**
- Validation after convert is a recommendation not enforced by the system
- User can skip it if they trust the conversion output
- System will still run the internal lightweight sanity check regardless
