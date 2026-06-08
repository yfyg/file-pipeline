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
- Files are organized in three folders: uploads/ intermediate/ outputs/

---

## 2. File Validation Strategy

**Two places where validation runs:**

**Place 1 — At upload time (in the upload API):**
- Check file size is 100MB or less (enforced during streaming, fails fast)
- Check file extension is in the allowed list
- Reject the upload immediately if either check fails

**Place 2 — As a pipeline step (`validate` step in the worker):**
- Runs in the background worker as part of the pipeline, not in the request path
- Re-checks file size and extension against the allowed list
- Adds an `expected_type` check — useful after a `convert` step, where the
  file's extension has changed and the pipeline should re-confirm the new type
- Reads a small sample to catch obvious corruption:
  - JSON: read first 4KB, check it starts with `{` or `[`
  - CSV: read the header row and confirm a second row can be parsed
  - Other: read first 1KB

**Why sample-based instead of full parse:**
- A full parse of a 100MB CSV/JSON would be slow and largely redundant —
  the downstream `transform` / `convert` steps will fail clearly on truly
  malformed content as they stream through it.
- The sample check catches the common failure modes cheaply: wrong format,
  empty file, binary garbage in a "csv" upload.
- Deep semantic corruption (e.g. one bad row in the middle) is caught
  during the actual processing step that touches it.

---

## 3. Step Failure Strategy

**Approach chosen:** Fail the job on step failure, preserve all intermediate files.

**Why fail instead of skip:**
- Steps depend on each other (transform output feeds convert input)
- Skipping a failed step would pass corrupted data to the next step
- Better to fail clearly than silently produce wrong output

**Retry logic:**
- Retries are scoped to *transient* errors only — OS / I/O failures such as a
  temporary disk-write error or file lock (`RETRYABLE_EXCEPTIONS = (OSError,)`).
  A transient step gets up to 3 attempts with exponential backoff (2s, 4s).
- Deterministic failures (bad format, validation failure, unsupported
  conversion, empty output) raise `ValueError` and are NOT retried — retrying
  cannot succeed and only wastes time, so the step fails immediately with a
  clear error.
- On a permanent error, or once a transient step exhausts its 3 attempts, the
  job status becomes FAILED with the last error message.
- `started_at` is recorded once at the first attempt so step `duration` spans
  all attempts, not just the last one.
- Retry count is hardcoded to 3. In production this should be read from
  config / env so it can be tuned per environment — we kept it simple here.

**Partial progress:**
- Each step writes its output to a separate file on disk before marking COMPLETED
- If a step fails, all intermediate files produced up to that point stay on disk
- A failed job can be inspected at exactly the step that failed
- See §4 for how those intermediates are eventually cleaned up
  (kept on failure, removed by the next startup expiry sweep)

---

## 4. File Cleanup Strategy

**Approach chosen:** Time-based expiry, swept at startup (and therefore on every
restart). There is no continuously-running background sweeper — see §16 for that
trade-off.

**Three folder structure:**

    storage/
    ├── uploads/       ← original uploaded file (clean record of what the user sent)
    ├── intermediate/  ← files produced between pipeline steps (debuggable on failure)
    └── outputs/       ← final result the user downloads

**Example file journey:**

    User uploads data.csv
        → storage/uploads/{uuid}_data.csv

    After transform / convert steps:
        → storage/intermediate/{uuid}_data_transformed.csv
        → storage/intermediate/{uuid}_data_transformed.json

    Job completes successfully — final file moved to:
        → storage/outputs/{uuid}_data_transformed.json

**Edge case — a pipeline that produces no new file (e.g. validate-only):**
When no step generates a new file, the "final" file is still the original
upload. In that case the worker **copies** it to `outputs/` instead of moving
it, so the input stays intact in `uploads/` and its `FileReference` is not
orphaned. (Moving it would destroy the source and break the input reference.)

**Retention rules:**

| Folder | Lifetime |
|---|---|
| uploads/ | Deleted 24h after the job completes (success or failure) |
| intermediate/ | Deleted on successful completion. On failure, kept until the next startup expiry sweep so the failed step is debuggable. |
| outputs/ | Deleted 24h after the job completes |

**If the service crashes:**
- Complete files stay on disk — nothing the user uploaded is lost
- Partial uploads (anything named `tmp_*` in uploads/) are deleted on next startup,
  since they are never recoverable
- The expiry sweep on startup deletes any files past `expires_at`
- Jobs stuck in PENDING or PROCESSING for more than 1 hour are marked FAILED

**Soft delete for FileReference rows:**

When the expiry sweep deletes a file from disk, the corresponding `FileReference`
row is NOT hard-deleted. Instead a `deleted_at` timestamp is set on the row.

**Why soft delete:**
- Preserves audit history — we can still see what files passed through the
  system, when they were uploaded, and when they were cleaned up. A Job row
  whose input file has expired can still resolve its `input_file_id` to a
  (soft-deleted) FileReference row, so the job history stays intact.
- Avoids breaking foreign keys. `Job.input_file_id`, `Job.output_file_id`, and
  `JobStep.input_file_id` / `output_file_id` all reference `file_references.id`.
  A hard delete would either cascade (losing the job history) or raise an
  IntegrityError. Soft delete sidesteps both problems.

**Why not a dedicated history / audit table:**

A separate `file_history` table would be the more "correct" design for a
production system that needs full audit trails (it lets you record additional
events like manual deletions, restores, retention-policy changes, etc.). For
this assignment we deliberately chose the simpler single-column soft-delete:
- One new nullable column instead of a new table + model + insert path.
- No need to keep two tables in sync.

In a real production deployment with compliance / forensics requirements, a
dedicated history table would be the better choice.

---

## 5. Progress Tracking

**Approach chosen:** Step-level granularity only — no within-step percentage.

**What we track:**
- Per job: current step index, overall status, overall % of steps completed
- Per step: status (PENDING / RUNNING / COMPLETED / FAILED / SKIPPED), start time, end time, duration

**What we do NOT track:**
- Within-step row-count progress (e.g. "50% of rows processed")

**Why this is enough for this system:**

- File size is bounded at 100MB. In practice every step finishes in seconds —
  there is no long-running step where a user would sit and watch a progress bar.
- Step-level status already answers the questions that matter: which step is
  running, how long it has been running, did it succeed or fail.
- This pipeline is meant to be operated by a data engineer via the API, not
  watched on a GUI. What matters in that workflow is reliable status
  transitions and clear failure messages, not a row counter — we prioritized
  making the system work correctly over adding cosmetic progress detail.

**What it would cost to add row-count progress:**

- Each step function today is pure: `(file_path, params) -> file_path`. They
  do not know about the DB session or the JobStep record.
- To update progress every N rows we would have to pass the DB session and
  the JobStep into every step function, coupling pure transforms to persistence.
  That hurts testability (steps can no longer be tested without a DB) and
  makes the code less simple.
- For a high-load system processing GB-scale files, that trade-off would be
  worth it. For 100MB files at interview scale, it is not — it is a
  nice-to-have we deliberately skipped.

**Spec compliance:**

The spec asks for "detailed progress (which step, percentage if available)".
"Which step" is covered by `current_step_index`. "Percentage" is covered at
the job level (% of steps completed). Within-step percentage is explicitly
qualified "if available" in the spec, and the Critical Implementation Details
section invites us to document the trade-off here.

---

## 6. Webhook Reliability

**Approach chosen:** Retry with exponential backoff. Job is NOT failed if webhook fails.

**Retry strategy:**
- Up to 5 total attempts
- Sleeps between attempts: 5s, 15s, 30s, 60s, 120s (the last attempt has no following sleep)
- If all 5 attempts fail: step marked FAILED but job marked COMPLETED
- Webhook failure does not fail the job — the file was processed successfully

**Why webhook failure does not equal job failure:**
- The file processing succeeded — the result is there
- User can still retrieve result via the status API

**Preventing duplicate notifications:**
- Step status is set to RUNNING before first attempt
- Status is only set to COMPLETED after a successful response
- Idempotency key sent in webhook header so receiver can deduplicate

**Webhook URL validation (SSRF protection):**

The `notify` step blocks Server-Side Request Forgery before issuing any request
(`notify._validate_webhook_host`):
- Scheme must be `http://` or `https://`.
- The hostname `localhost` is rejected outright.
- The hostname is resolved with `socket.gethostbyname`, and the request is
  rejected if the resolved IP falls in a blocked range: loopback (`127.`,
  `::1`), unspecified (`0.0.0.0`), private (`10.`, `192.168.`, `172.`),
  link-local / cloud-metadata (`169.254.`, `fe80:`).

**Known residual limitations (acceptable at this scope):**
- *DNS rebinding / TOCTOU.* We resolve the host once for the check, but `urllib`
  resolves it again when the request is actually made. A hostname that returns a
  public IP at check time and a private IP at request time would slip through. A
  full fix resolves once and connects to that pinned IP (e.g. a custom opener),
  which we judged out of scope for a trusted single-tenant tool.
- *IPv4-only resolution.* `gethostbyname` returns a single IPv4 address. An
  AAAA-only host, or a host whose first A record is public but others are
  private, is not fully covered. A production version would enumerate all
  resolved addresses (`getaddrinfo`) and test each with
  `ipaddress.ip_address(...).is_private`.
- *`172.` is over-broad.* Only `172.16.0.0/12` (172.16–172.31) is actually
  private, but our simple string-prefix check blocks all of `172.x`, so some
  legitimate public IPs are rejected. We chose the simpler prefix check over
  parsing CIDR ranges; over-blocking is the safe direction for a blocklist.

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

**Two types of duplication considered:**

(Duplicate file *uploads* are intentionally not deduplicated — see §9.)

**Type 1 — Duplicate job processing (two workers picking up the same job):**
- Prevented by atomically setting status to PROCESSING before starting work
- Worker checks status == PENDING before proceeding
- If another worker already set it to PROCESSING the current worker exits immediately

**Type 2 — Duplicate webhook notifications:**
- The retry loop stops as soon as one attempt succeeds, so we never POST twice
  on success.
- An idempotency key (`X-Pipeline-Job-Id`) is sent in the webhook header so the
  receiver can deduplicate if a previous request actually arrived but its
  response was lost in transit.

---

## 9. Upload Deduplication — Deliberately Not Implemented

**Approach chosen:** Every upload creates a new job. We do not deduplicate by
filename, by content, or by pipeline.

**Why no dedup:**

Earlier versions of this code matched a new upload against the
`FileReference` table on filename OR MD5 of content, and returned the existing
`job_id` if either matched. That looked reasonable at first but missed a real
case: **the user can re-upload the same file with a different pipeline.**
Treating that as a duplicate would return the old job whose output was
shaped by the old pipeline — silently giving the user the wrong result.

We considered fixing this by including a canonicalized hash of the pipeline
JSON in the dedup key, but for the scope of this assignment it adds complexity
without a clear benefit:
- Storage uniqueness is already guaranteed: every uploaded file lands at
  `storage/uploads/{uuid}_{sanitized_name}`. The UUID prefix means re-uploading
  the same filename never overwrites anything on disk.
- The `job_id` in the upload response gives the user a stable handle to their
  exact processing run.
- Skipping dedup keeps the upload path simple and free of subtle "you got
  someone else's output" failure modes.

**What we would do in production:**

For a high-volume system where reprocessing is expensive, dedup is worth
adding — but on the **full** key:
- Filename (for display / human matching)
- Content hash (SHA-256, computed while streaming)
- Canonicalized pipeline definition (sorted keys, normalized whitespace) hash

Only when all three match should the system return the existing `job_id`.
Anything less risks returning results that don't match what the user asked
for.

**Storage safety (still applies):**

- Files are saved on disk as `{uuid}_{sanitized_name}`. The UUID guarantees
  uniqueness — nothing on disk is ever overwritten regardless of filename.
- **Path-traversal protection — upload filename (Security):** the raw client
  filename is never used to build a filesystem path. `_sanitize_filename`
  strips directory components (both POSIX `/` and Windows `\` separators) and
  whitelists characters, so a malicious name like `../../etc/evil.csv` is
  reduced to a safe basename and cannot escape `storage/uploads/`. On-disk
  names are derived only from the sanitized value; uniqueness comes from the
  UUID.
- **Path-traversal protection — Zip Slip (Security):** the `compress` step
  extracts uploaded `.zip` files, and the paths *inside* a zip are attacker-
  controlled (a malicious archive can contain entries named
  `../../etc/passwd`). Before writing any extracted file, `compress._zip_extract`
  resolves both the storage directory and the proposed output path with
  `os.path.realpath` and rejects the archive if the resolved output path is
  not contained inside the storage directory. This blocks the Zip Slip class
  of attacks where extraction would otherwise write files outside
  `storage/`.
- The raw original filename is preserved in the DB for display purposes only —
  it is never used on disk.

---

## 10. Things We Would Do Differently With More Time

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

```
validate (original type)
  → transform
    → convert
      → validate (new type)   ← recommended
        → compress
          → notify
```

**Why:**
- Convert changes the file format completely
- The new file is essentially a new file — should be validated fresh
- Catches conversion errors early before compress or other steps run
- If validate fails after convert it means conversion produced bad output

**Example pipeline with recommended pattern:**

```json
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
```

**Important — expected_type must match the converted format:**
- After CSV to JSON conversion: expected_type should be "json"
- After JSON to CSV conversion: expected_type should be "csv"
- Passing wrong expected_type will correctly fail the job

**User is in control:**
- Validation after convert is a recommendation not enforced by the system
- User can skip it if they trust the conversion output
- System will still run the internal lightweight sanity check regardless

---

## 13. Why We Use a Database

**The spec does not explicitly say "use a database" but requires:**
- Persistent job status tracking across restarts
- Step-level status and duration tracking
- File reference management with expiry
- Query job by ID from the status API

**All of these require durable storage — not just a queue.**

**Why SQLite specifically:**
- Assignment says "local filesystem is fine for this exercise"
- No extra Docker service required (unlike PostgreSQL)
- Data survives service restarts (unlike in-memory dict)
- Simple to inspect during development (single .db file)
- Sufficient for single-node use case described in spec

**What we would use in production:**
- PostgreSQL — handles concurrent writes from multiple workers
- SQLite has write locking issues under concurrent load

---

## 14. Upload Failure Cleanup

**Three failure scenarios and how we handle them:**

**Scenario 1 — Upload fails midway (network drop):**
- File is written to a temp path first: storage/uploads/tmp_{uuid}
- If upload stream fails the temp file is deleted in the except block
- Final path only assigned after complete successful upload

**Scenario 2 — File saved but DB insert fails:**
- Wrapped in try/except — temp file deleted on any exception
- DB record and file are created together or not at all

**Scenario 3 — File + DB saved but queue fails:**
- Wrapped in try/except — temp file deleted on any exception
- DB record rolled back if queue fails
- File and DB record are created together or not at all

**Crash recovery (service dies mid-upload):**
- Cleanup runs at startup — and therefore on every restart — not as a
  continuously-running background process.
- Deletes leftover partial uploads: anything named `tmp_*` in `uploads/`, which
  is never recoverable.
- Deletes any file whose `expires_at` has passed, soft-deleting its
  FileReference row (see §4).
- Marks jobs stuck in PENDING or PROCESSING for more than 1 hour as FAILED.
- We do NOT scan for orphan files (a file on disk with no DB record). In
  practice the `tmp_*` sweep covers the realistic crash case; a general orphan
  reconciliation is noted as future work in §16.

**Why temp path first:**
- Prevents partial files ever reaching permanent storage
- Clean separation between in-progress and complete uploads
- Easy to identify and clean up: anything named tmp_ is safe to delete

---

## 15. Output File Retention Period

**Required by spec:**
- Spec explicitly states: "Results available for configurable retention period (e.g., 24 hours)"
- 24 hours is the default — not hardcoded

**Why retention periods exist:**
- Storage costs money — processed files should not sit forever
- Output files are temporary by nature — user downloads result then done
- Security — processed files may contain sensitive data
- Keeping files longer than needed is a liability

**Implementation:**
- Retention period is configurable via environment variable: RETENTION_HOURS (default: 24)
- expires_at is set on FileReference record at creation time
- The startup cleanup sweep checks expires_at and deletes files past expiry
  (so expiry is enforced on the next restart, not the instant it lapses)
- Applies to: input files, intermediate files, output files

**What happens after expiry:**
- File deleted from disk
- FileReference row soft-deleted (`deleted_at` set) — see §4 for why we keep
  the row instead of hard-deleting it
- `GET /jobs/{id}/result` returns 410 Gone with message "Result expired, please reprocess"

---

## 16. Known Limitations & Deliberate Simplifications

These are things we are aware of and chose to document rather than fully solve,
to keep the implementation simple for the scope of this exercise. Each notes
what we would do in a production version.

### Transform / convert assume an array of flat objects
- `transform` and `convert` assume JSON input is a top-level array of flat
  objects (`[{...}, {...}]`), matching the `ijson.items(f, "item")` access path.
- A single top-level object (`{...}`) yields an empty array / empty output
  rather than an error — `ijson` finds no `item` entries. Deeply nested or
  ragged objects are not handled.
- `_json_to_csv` derives CSV headers from the **first** object only and uses
  `extrasaction="ignore"`, so keys that appear only in later objects are
  silently dropped, and missing keys become empty cells.
- *Production:* validate the JSON shape up front (or stream a union of keys for
  the header) and fail loudly on unexpected structure instead of producing
  truncated output.

### `select_columns` with a non-existent column produces empty output silently
- `transform` keeps only `row.keys()` that are also in `select_columns`. A
  typo'd or missing column name simply drops that column; if none match, every
  row becomes empty.
- The post-step sanity check only confirms the file is non-empty (a header line
  is enough), so the job still reports COMPLETED with effectively empty data.
- *Production:* check requested columns against the header and fail with a clear
  error when a requested column is absent.

### Unknown step names fail asynchronously, not at upload
- `/upload` accepts any pipeline JSON whose entries have a `step` field; an
  unrecognized step name (or a mis-cased `Notify`) is only caught later by the
  worker, which marks the job FAILED.
- *Production:* validate step names (and required params) at upload time and
  return HTTP 400 immediately, so callers get fast, synchronous feedback.

### Operational / polish notes
- *Cleanup is startup-only.* The expiry sweep, `tmp_*` removal, and "stuck job →
  FAILED" check all run once in the FastAPI startup hook — there is no periodic
  background sweeper. A long-running instance therefore only enforces retention
  and stuck-job timeouts when it next restarts, and there is no reconciliation
  for orphan files (a file on disk with no DB record). For this single-node,
  interview-scale tool that is acceptable; production would add a periodic job
  (RQ Scheduler, cron, or a background task) and an orphan-file reconciliation
  pass.
- *Dev reload flag.* `docker-compose.yml` runs the API with `uvicorn --reload`,
  a development convenience (file-watching, extra overhead). A production image
  would drop `--reload` and run multiple workers behind the server.
- *Shallow health check.* `/health` returns "healthy" without checking Redis or
  the database. A deeper check would ping both so orchestration can detect a
  degraded dependency.
- *No queue-depth endpoint.* Queue visibility is via RQ / logs only; we did not
  expose pending / started / failed counts over the API. RQ makes this a few
  lines (`Queue.count`, registries) and would be the first observability add.
- *`datetime.utcnow()`.* Used throughout; deprecated as of Python 3.12+ (our
  Docker image targets 3.11, where it is fine). Would migrate to
  `datetime.now(timezone.utc)` for forward compatibility.
- *Repo hygiene.* `venv/`, `.idea/`, and `.DS_Store` are git-ignored; verify
  they are not tracked before submitting so the repo ships only source.

---

## 17. Cancellation Behavior

**Approach chosen:** Cooperative cancellation, checked at step boundaries.

- `POST /jobs/{id}/cancel` is allowed only for jobs that are PENDING or
  PROCESSING. It sets the job to CANCELLED and flips any not-yet-run steps to
  SKIPPED.
- If the job was still queued (PENDING) when cancelled, the worker's startup
  guard (`status != "PENDING"`) makes it exit immediately when it is picked up.
- If the job is already PROCESSING, the worker re-reads the job status before
  each step and again before final completion. On CANCELLED it marks the
  remaining steps SKIPPED and stops — without firing the webhook or overwriting
  the status with COMPLETED.

**What this does NOT do (deliberate, documented limitation):**
- A step that is *already running* is not interrupted — the step functions are
  synchronous and run to completion. Cancellation takes effect at the next step
  boundary. For 100MB files where each step finishes in seconds this is
  acceptable; truly pre-emptive cancellation would require running each step as
  a separate, killable process.
- There is a millisecond-scale race: a cancel that lands during the final move,
  after the last completion check, can still let the job finish as COMPLETED. We
  accept this for a cooperative model rather than introduce locking.

**Cross-session note:** the cancel endpoint (API request session) and the worker
run in separate DB sessions. The worker re-reads (`db.refresh`) so it observes
the committed CANCELLED status. This relies on the API and workers sharing the
same SQLite database file, which they do via the mounted `storage/` volume.
