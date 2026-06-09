# Design Decisions

**Core principle:** never load a full file into memory. Files can be up to
100MB; every read, transform, and write is streamed or chunked.

## Contents

1.  [Large File Handling](#1-large-file-handling) — streaming primitives; why not pandas
2.  [File Validation Strategy](#2-file-validation-strategy) — two-stage validation (upload + step)
3.  [Step Failure Strategy](#3-step-failure-strategy) — retry policy (transient vs deterministic)
4.  [File Cleanup Strategy](#4-file-cleanup-strategy) — three folders, soft delete, expiry
5.  [Progress Tracking](#5-progress-tracking) — step-level granularity, no in-step %
6.  [Webhook Reliability](#6-webhook-reliability) — retry, SSRF guard, payload shape
7.  [Tech Stack Decisions](#7-tech-stack-decisions) — FastAPI / SQLite / Redis-RQ rationale
8.  [Duplicate Handling](#8-duplicate-handling) — worker and webhook dedup
9.  [Upload Deduplication](#9-upload-deduplication--deliberately-not-implemented) — deliberately not implemented; why
10. [Out of Scope — Production Roadmap](#10-out-of-scope-for-this-assignment--production-roadmap) — items deferred from the assignment scope
11. [Validation After Conversion](#11-validation-after-conversion) — user-defined vs internal sanity check
12. [Recommended Pipeline Pattern](#12-recommended-pipeline-pattern) — validate after convert
13. [Why We Use a Database](#13-why-we-use-a-database) — SQLite trade-offs, Postgres in prod
14. [Upload Failure Cleanup & Stuck-Job Recovery](#14-upload-failure-cleanup--stuck-job-recovery) — commit-then-enqueue, sweeper
15. [Output File Retention Period](#15-output-file-retention-period) — `RETENTION_HOURS` semantics
16. [Known Limitations & Deliberate Simplifications](#16-known-limitations--deliberate-simplifications) — security, semantic, operational gaps
17. [Cancellation Behavior](#17-cancellation-behavior) — cooperative cancel at step boundaries
18. [Real Production Considerations](#18-real-production-considerations) — concurrency at scale, faster streaming

---

## 1. Large File Handling

**Approach:** streaming and chunked processing end-to-end. Memory never
scales with file size.

| File Type | Strategy | Tool | Memory in-flight |
|-----------|----------|------|------------------|
| CSV | Row by row | `csv.DictReader` | one row |
| JSON | Object by object | `ijson.items` | one object |
| Binary / TXT / GZ / ZIP | 8KB chunks | `file.read(8192)` loop | 8KB |

**Why 8KB:** matches typical disk block size — efficient I/O without the
memory cost of larger reads.

**Upload:** chunks written to a temp file directly; size cap (100MB)
enforced during the stream, not after. Intermediate files between steps go
to disk, not memory — this also makes each step boundary a natural
checkpoint for future resume support.

**Why stdlib over pandas / Polars / DuckDB at this scale:** the five-line
streaming loop is reviewable in one read, the streaming guarantee is
self-evident from the code, and at 100MB the runtime is dominated by
gzip and JSON serialization — vectorizing the row loop wouldn't show up.
The scaling ladder for larger files (and the "why not pandas" answer)
lives in §18.2.

---

## 2. File Validation Strategy

Validation runs in **two places**:

1. **Upload API** — size cap (100MB, enforced during streaming) and
   extension whitelist. Fails fast, no file ever moves to permanent storage.
2. **`validate` pipeline step** — re-checks size + extension, adds an
   `expected_type` param (useful after `convert`), and **samples** the file
   for obvious corruption:
   - JSON: first 4KB must start with `{` or `[`
   - CSV: header + first data row must parse
   - Other: read first 1KB to confirm the file isn't empty / unreadable

**Why sample, not full parse:** a full parse of a 100MB file would be slow
and redundant — `transform` / `convert` stream through everything anyway
and will fail clearly on truly malformed content. The sample catches the
common failures (wrong format, empty file, binary garbage in a `.csv`)
cheaply.

---

## 3. Step Failure Strategy

**Approach:** fail the job on step failure; preserve intermediates for
inspection. Steps depend on each other — skipping would silently produce
wrong output.

**Retry policy** (`_run_step_with_retries`):

| Exception class | Behavior |
|---|---|
| `OSError` (transient — disk write, file lock) | Retry up to 3 times, exponential backoff (2s, 4s) |
| `ValueError` (deterministic — bad format, empty output) | Fail immediately, no retry |

`started_at` is set once on the first attempt so `duration` spans all
retries, not just the last one. Retry count is hardcoded to 3 — would come
from config in production.

**Partial progress:** every step writes its output to disk before marking
COMPLETED, so a failed job can be inspected at the exact failed step. See
§4 for how those intermediates are cleaned up (kept on failure, removed by
the next expiry sweep).

---

## 4. File Cleanup Strategy

**Approach:** time-based expiry, swept at startup. No continuously-running
background sweeper (see §16).

**Three folders:**

```
storage/
├── uploads/       ← original uploaded file
├── intermediate/  ← files between pipeline steps (kept on failure for debugging)
└── outputs/       ← final result the user downloads
```

**Retention** (`RETENTION_HOURS`, default 24):

| Folder | Lifetime |
|---|---|
| `uploads/` | Deleted N hours after the job completes |
| `intermediate/` | Deleted on **successful** completion. Kept on failure until the next expiry sweep — so the failed step is debuggable. |
| `outputs/` | Deleted N hours after the job completes |

**Edge case — pipeline with no transforming step (e.g. validate-only):**
the worker **copies** the input to `outputs/` rather than moving it, so the
upload stays intact and its `FileReference` isn't orphaned.

**Soft delete for `FileReference` rows.** When the expiry sweep deletes a
file from disk, the `FileReference` row gets a `deleted_at` timestamp
rather than being hard-deleted. Two reasons:

1. **Audit history.** A Job whose input file has expired can still resolve
   its `input_file_id` to a (soft-deleted) FileReference, preserving the
   job history.
2. **Foreign-key integrity.** `Job` and `JobStep` both reference
   `file_references.id`; hard delete would either cascade (losing history)
   or raise IntegrityError.

A dedicated history / audit table would be more "correct" for production
compliance. We chose single-column soft-delete here because it's the
minimum that solves both problems above.

**Crash recovery** is fully covered in §14 (commit-before-enqueue, stuck-
job sweeper, partial-upload cleanup).

---

## 5. Progress Tracking

**Approach chosen:** Step-level granularity only — no within-step percentage.

**What we track:**

| Level | Fields |
|---|---|
| Job | current step index, overall status, % of steps completed |
| Step (all) | status, start time, end time, duration |
| Step (row-based: transform, convert) | `input_rows`, `output_rows` counted during streaming, saved on completion |

`input_rows`/`output_rows` are NULL for steps where the concept doesn't
apply (validate, compress, notify). This answers "did transform actually
drop those rows?" exactly, after the fact.

**What we don't track:** real-time within-step progress (e.g. "50% of rows
processed while running").

**Why that's enough:** files are bounded at 100MB and steps finish in
seconds — there's no long-running step a user would watch on a progress
bar. Step-level granularity already answers the questions that matter
(which step is running, how long, did it succeed). Adding real-time row
counts would require passing the DB session into every step function,
coupling pure transforms to persistence — bad trade-off at this scale.

**Spec compliance:** the spec asks for "detailed progress (which step,
percentage if available)". "Which step" → `current_step_index`.
"Percentage" → job-level % of steps completed. Within-step percentage is
qualified "if available" — we don't expose it.

**Parallelization caveat:** the `count += 1` in the streaming loop assumes
the step runs in a single process. If a future version split a transform
across workers, the count would race (lost updates in SQLite). Fix would
be either aggregate at the end in one process or use an atomic
`UPDATE ... SET input_rows = input_rows + N`. The spec only requires
parallelism across independent steps (Nice to Have), not within a step, so
this is out of scope.

---

## 6. Webhook Reliability

**Retry:** 5 attempts, sleeps 5s / 15s / 30s / 60s / 120s between them. If
all fail, the notify step is marked FAILED but the **job is still
COMPLETED** — the file was processed; the webhook is just a notification.

**Duplicate prevention:** the retry loop exits on the first 2xx response,
so we never POST twice on success. An `X-Pipeline-Job-Id` idempotency
header lets the receiver dedup if a previous request actually arrived but
its response was lost in transit.

**Payload — `result_url`, not filesystem path:**

```json
{
  "job_id":     "abc123-...",
  "status":     "COMPLETED",
  "result_url": "/jobs/abc123-.../result"
}
```

We deliberately don't send `storage/outputs/abc123_data.json.gz` —
that's info disclosure (leaks our storage layout) and the receiver can't
GET an internal path anyway.

**`result_url` is path-only by design.** The receiver gets `/jobs/{id}/result`,
not `https://api.example.com/jobs/{id}/result`. Production would either add
a `SERVICE_BASE_URL` env var that's prepended here, or expect the receiver
to know the base URL out-of-band (typically from API onboarding). We chose
path-only because the service base URL varies by deployment and embedding
it in the worker requires extra configuration the assignment doesn't have.

**SSRF protection** (`_validate_webhook_host`): before issuing any
request, we reject schemes other than `http(s)://`, reject `localhost`,
resolve the host with `socket.gethostbyname`, and reject if the resolved
IP starts with `127.`, `0.0.0.0`, `10.`, `192.168.`, `172.`, `169.254.`,
`::1`, or `fe80:`.

Known limitations of this check (acceptable at this scope):
- **DNS rebinding / TOCTOU.** We resolve once for the check; `urllib`
  resolves again at request time. A full fix would pin the IP after the
  check (custom opener).
- **IPv4-only.** `gethostbyname` returns one address. AAAA-only hosts or
  multi-A hosts aren't fully covered. Production would use `getaddrinfo`
  + `ipaddress.ip_address(...).is_private`.
- **`172.` is over-broad.** Only `172.16.0.0/12` is actually private, but
  our string-prefix check blocks all of `172.x`. Over-blocking is the safe
  direction for a blocklist.

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

Duplicate file *uploads* are intentionally not deduplicated (see §9). Two
other dedup concerns remain:

**Same job picked up by two workers.** The primary guarantee is from
**RQ/Redis** — `BLPOP` is atomic, two workers never receive the same
job_id. The worker also checks `Job.status == PENDING` before doing
anything as defense-in-depth (catches a manual re-enqueue or
sweeper-induced re-queue). This check is not atomic in the SQL sense, but
Redis already provides the at-most-once delivery.

**Duplicate webhook notifications.** The retry loop exits on the first
2xx response, so we never POST twice on success. The `X-Pipeline-Job-Id`
idempotency header lets the receiver dedup if a request arrived but its
response was lost in transit. See §6.

---

## 9. Upload Deduplication — Deliberately Not Implemented

**Approach:** every upload creates a new job. No dedup by filename,
content, or pipeline.

**Why not:** an earlier version matched on filename or content hash and
returned the existing `job_id`. That missed a real case — **the same file
re-uploaded with a different pipeline** would return the old result,
silently giving the user output shaped by the wrong recipe.

A correct dedup key would be filename + content hash + canonicalized
pipeline JSON. We chose to skip it instead:
- Storage uniqueness is already guaranteed by `{job_id}_{sanitized_name}`.
  The UUID prefix means nothing on disk is ever overwritten.
- The `job_id` returned to the user is the stable handle to that exact
  run; there's no UX problem to solve.
- The upload path stays simple, with no "you got someone else's output"
  failure modes.

In production, a high-volume system might want dedup to skip
reprocessing — but only on the full three-part key above.

**Storage safety (still applies):**

- Files saved as `{job_id}_{sanitized_name}` — UUID guarantees uniqueness.
- **Path-traversal protection — upload filename.** Raw client filename is
  never used in a filesystem path. `_sanitize_filename` strips directory
  components (POSIX and Windows separators) and whitelists characters, so
  `../../etc/evil.csv` is reduced to a safe basename. On-disk names use
  only the sanitized value.
- **Path-traversal protection — Zip Slip.** `_zip_extract` resolves both
  the storage dir and the proposed output path with `os.path.realpath`,
  and rejects any zip entry whose resolved path falls outside storage.
- The raw filename is preserved in the DB for display only — never used
  on disk.

---

## 10. Out of Scope for This Assignment — Production Roadmap

Items deliberately deferred because they add real complexity without
moving the needle for the assignment's scope. Each is a clear step toward
a production deployment.

- **Resume failed jobs from last successful step.** Intermediates are
  already on disk; need a `/jobs/{id}/retry` endpoint that picks up from
  the last COMPLETED step. Would save real time on long pipelines that
  fail late.
- **Authentication + per-tenant scoping.** Add an API-key dependency and a
  `Job.owner_id` column (see §16). The dependency injection pattern is
  already in place.
- **PostgreSQL instead of SQLite.** SQLite's single-writer locking bites
  under concurrent worker writes. Postgres + Alembic also fixes the
  "drop the DB to add a column" annoyance.
- **Polars / DuckDB for transforms.** Same streaming guarantees, ~5×
  faster on big files. Worth it at GB-scale.
- **Queue / worker observability endpoint.** Expose queue depth, started
  jobs, failed jobs over the API (RQ provides these via registries).
- **Deep JSON streaming.** `ijson.items(f, "item")` assumes the top-level
  is an array of flat objects. Nested structures would need a different
  approach.

---

## 11. Validation After Conversion

User-defined validation (the `validate` pipeline step) and an internal
sanity check both exist, with different jobs:

| | When | What it checks |
|---|---|---|
| `validate` step | When the user includes it | Sample-based content + `expected_type` |
| Internal sanity check | Automatic after every step | File exists, file is non-empty |

The internal check is a safety net — catches "step silently failed to
produce output". The user-facing `validate` step is for business rules
(`expected_type` after a convert, file-type assertions). We deliberately
don't auto-insert `validate` — keeps the pipeline predictable; user sees
exactly what runs.

---

## 12. Recommended Pipeline Pattern

We recommend `validate` after `convert`:

```
validate(csv) → transform → convert → validate(json) → compress → notify
```

Convert changes the file format completely; the output is effectively a
new file and should be re-validated before downstream steps touch it. If
the second `validate` fails, the convert produced bad output.

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

`expected_type` must match the **current** file type after any conversion
— passing the wrong one will fail the job, which is the correct behavior.

---

## 13. Why We Use a Database

The spec doesn't say "use a database" but requires persistent job status,
step-level tracking, file expiry, and query-by-ID — all of which need
durable storage, not just a queue.

**Why SQLite:** the spec allows local filesystem, no extra Docker service
needed, data survives restarts, single `.db` file is easy to inspect.
Sufficient for single-node use.

**What we'd use in production:** Postgres. SQLite has single-writer
locking that bites under concurrent worker writes.

---

## 14. Upload Failure Cleanup & Stuck-Job Recovery

**Upload failure paths** — the upload code (`upload.py`) handles three
ways uploads can fail:

| Failure point | What happens |
|---|---|
| Network drop mid-stream | File was written to `tmp_*` path; deleted in the `except` block. Final path never assigned. |
| File saved but DB insert raises | Same try/except cleans up the temp file; DB rolls back. |
| File + DB saved, but enqueue fails | Job row stays PENDING; 500 returned. Sweeper re-enqueues on next startup or next `GET /jobs/{id}`. |

The temp-path-first pattern prevents partial files ever reaching
permanent storage. Anything named `tmp_*` in `uploads/` is safe to delete.

**Why commit before enqueue.** An earlier version enqueued first and then
committed. A fast worker could pop the message and `SELECT` the Job row
before the commit landed, see nothing, log "Job not found", and silently
return — permanent data loss. Committing first means the worker is
guaranteed to see the row by the time it pulls the message. The new
failure mode (enqueue fails after commit) leaves a PENDING row that the
sweeper recovers — strictly better than silent loss.

**Stuck-job recovery** (`app/workers/sweeper.py:recover_stuck_jobs`):

| State | Threshold | Action | Why safe |
|---|---|---|---|
| PENDING | 5 min | Re-enqueue | Work hasn't started; re-running is idempotent |
| PROCESSING | 1 hour | Mark FAILED | Worker died mid-step; resuming would need per-step replay (out of scope, §10) |

Runs in **two places**: on service startup (catches Redis-was-down case)
AND on every `GET /jobs/{id}` (a user polling for status self-heals their
own stuck job — no separate scheduler needed).

**"Re-enqueue" is safe but not strictly idempotent.** A user polling
`GET /jobs/{id}` while their job is stuck will trigger a re-enqueue on
every poll past the 5-minute threshold. Functionally this is safe — the
worker's `status == PENDING` check is the at-most-once guard, so only the
first worker to pick up any of the queued messages actually runs the job;
the rest no-op out. The queue is briefly polluted with duplicate
messages, but no work is duplicated. Production would add a
"re-enqueued_at" timestamp on the Job and skip re-enqueue if it's recent;
we judged that not worth the extra column at this scale.

**Orphan files at startup:**
- `tmp_*` in `uploads/` → deleted (never recoverable).
- Files past `expires_at` → deleted from disk, FileReference soft-deleted
  (see §4).
- We do NOT scan for files on disk without a DB record. The `tmp_*` sweep
  covers the realistic crash case; full orphan reconciliation is noted as
  future work in §16.

---

## 15. Output File Retention Period

Spec calls for "configurable retention period (e.g. 24 hours)". The
`RETENTION_HOURS` env var (default 24) controls this. Applies to inputs,
intermediates, and outputs — every FileReference's `expires_at` is set
relative to it.

Expiry is enforced by the **startup sweep** (not a continuously-running
job — see §16). After expiry: file removed from disk, FileReference row
soft-deleted (`deleted_at` set, see §4), and
`GET /jobs/{id}/result` returns 410 Gone with "Result expired, please
reprocess".

Why retention matters: storage cost, temporary nature of outputs, and the
security liability of keeping potentially-sensitive processed files
longer than needed.

---

## 16. Known Limitations & Deliberate Simplifications

Items we know about and chose to document rather than fix, organized by
category. Each notes what a production version would do differently.

### Security gaps

**No authentication or authorization (biggest real gap).** `/upload`,
`/jobs/{id}`, `/jobs/{id}/cancel`, and `/jobs/{id}/result` accept any
caller — no API key, no user/tenant scoping, no rate limiting. Acceptable
for single-tenant trusted-network scope, stated explicitly here rather
than hidden.

This also explains why we did NOT add `Job.owner_id` (the spec's
"User/API key reference" field). A column with no auth check populating
it is clutter, not observability. Auth + the column belong together and
are out of scope as one unit. *Production:* API key in a FastAPI
dependency populates `Job.owner_id`, every status / cancel / result query
scopes by it, plus rate-limit `/upload` to prevent disk-fill DoS.

**Upload-time type validation is extension-only.** `data.csv` containing
PE32 bytes passes the extension check. Mitigation is the `validate`
pipeline step's sample-based content check — a `.csv` with binary
garbage fails there with a clear error. We pushed content validation to
the pipeline so the upload endpoint stays fast (no blocking on a 100MB
read). *Production:* magic-byte check at upload time via `python-magic`
or `filetype` — ~10 lines + one dependency we didn't take here.

### Semantic gaps in the step layer

**JSON input must be an array of flat objects.** `transform` and `convert`
use `ijson.items(f, "item")`, which assumes top-level array. A single
top-level `{...}` yields empty output rather than an error. `_json_to_csv`
also derives headers from the **first** object only with
`extrasaction="ignore"` — keys that appear only in later objects are
silently dropped. *Production:* validate the JSON shape up front (or
stream a union of keys for the header).

**`select_columns` with a typo produces silent empty output.** Only the
listed columns are kept; if none match the source headers, every row
becomes empty. The post-step sanity check sees a non-empty header line
and accepts it. *Production:* check requested columns against the header
and fail clearly.

**`filter_rows` is a silent no-op when its column was dropped by
`select_columns`.** Step order is select → filter → text_transform on
both paths. Filtering on a dropped column means there's nothing to
compare; rows pass through unchanged. Workaround: include the filter
column in `select_columns` (the §12 example does this). *Production:*
reorder to filter-then-select, or reject at upload time when the columns
disagree.

**Unknown step names fail asynchronously.** `/upload` accepts any
pipeline JSON whose entries have a `step` field; a mis-cased `Notify`
only fails later in the worker. *Production:* validate step names + params
at upload time and return 400 immediately.

### Operational gaps

- **Cleanup is startup-only.** The expiry sweep and `tmp_*` cleanup only
  run on FastAPI startup — a long-running instance only enforces retention
  when it next restarts, and there's no orphan-file reconciliation.
  Production would add a periodic job (RQ Scheduler / cron) and an
  orphan-file pass.
- **No queue-depth endpoint.** Queue visibility is via RQ / logs only. RQ
  provides `Queue.count` and per-state registries — a 10-line endpoint
  is the first observability add.
- **`--reload` in dev compose.** The API container runs with
  `uvicorn --reload` for file-watching during development. Production
  image would drop it and run multiple workers behind the server.
- **`datetime.utcnow()` deprecated in Python 3.12+.** Our image targets
  3.11 where it's fine. Migration to `datetime.now(timezone.utc)` is
  forward-compatibility work.

---

## 17. Cancellation Behavior

**Cooperative cancellation, checked at step boundaries.**

`POST /jobs/{id}/cancel` is allowed for PENDING or PROCESSING jobs. It
sets the job to CANCELLED and flips not-yet-run steps to SKIPPED.

- **PENDING when cancelled:** the worker's startup guard
  (`status != "PENDING"`) makes it exit immediately when it picks the
  message up.
- **PROCESSING when cancelled:** the worker re-reads the job status
  before each step (and again before final completion). On CANCELLED it
  marks remaining steps SKIPPED and stops — without firing the webhook
  or overwriting the status with COMPLETED.

**Limitations (deliberate):**
- A step that's already running is NOT interrupted — steps are
  synchronous; cancellation takes effect at the next boundary. Truly
  pre-emptive cancellation would need running each step as a killable
  subprocess.
- Millisecond-scale race: a cancel that lands during the final move,
  after the last completion check, can still let the job finish as
  COMPLETED. Accepted as the cost of a cooperative model.

The API and worker run in separate DB sessions; the worker uses
`db.refresh` to observe the committed CANCELLED status. This relies on
both sharing the same SQLite file via the mounted `storage/` volume.

---

## 18. Real Production Considerations

This section describes what the architecture would look like for a busy
production deployment — explicitly *not* what we built. Today's system
targets the assignment's scope (single-tenant, ≤100MB inputs, one worker
container); the items below are the next layer.

### 18.1 Concurrency — supporting many simultaneous uploads

Today `docker-compose.yml` runs **one worker container**, so jobs
process strictly serially. FastAPI handles the request side fine —
multiple uploads stream in parallel — but processing throughput is
capped at one job at a time. We did not validate multi-worker behavior.

If we knew the system needed to handle ~100 concurrent uploads, three
things would break around the same time, in this order:

1. **SQLite's single-writer lock.** With dozens of workers writing job
   status concurrently, you'd see "database is locked" errors and
   serialized throughput. **First fix:** Postgres (already noted in
   §13). Adds a service to compose and Alembic migrations, ~1 day of
   work.
2. **Local-disk-only storage.** Multiple API or worker replicas need
   to share storage; bind-mounted local disk doesn't work across hosts.
   **Second fix:** S3 (or any S3-compatible like MinIO).
   `FileReference.storage_path` becomes an S3 URL; the streaming
   primitives don't change shape — `csv.DictReader` over an
   `s3.get_object` stream works the same as over a file.
3. **No backpressure or rate limits.** A burst of 100 uploads could
   fill disk before the worker has a chance to clean up. **Third fix:**
   per-API-key rate limits on `/upload` (e.g. `slowapi`), plus reject
   with 503 when Redis queue depth exceeds a configurable threshold.

Once those are in place, scaling out is **one command** — `docker
compose up --scale worker=N` or an HPA in Kubernetes that auto-scales
workers on queue depth. The application code mostly doesn't change
because steps are pure functions, the pipeline is JSON, and Redis is
the coordination point.

For full production readiness on top:

- **Multi-tenant auth.** `Job.owner_id` column, API key per tenant,
  every status / cancel / result query scopes by `owner_id`. Out of
  scope for the assignment, documented in §16.
- **API replicas behind a load balancer.** 2–3 API containers behind
  nginx (or a cloud LB), `--reload` dropped, multiple uvicorn workers
  per container.
- **Observability stack.** Prometheus scraping queue depth + step
  duration histograms, Grafana dashboard, structured logs to an
  aggregator. Today's `docker logs` doesn't cut it at this scale.
- **Periodic sweeper.** Today's stuck-job sweep only runs at startup
  and on status query (§14, §16). Production would add a periodic job
  (RQ Scheduler / cron / FastAPI background task) so retention and
  recovery enforce continuously, not on restart.

**What does NOT change at scale** (worth noting because it's the strong
part):

- Step functions stay `(file_path, params) -> (output_path, stats)`.
  No DB session passed in, no shared state. Run them anywhere.
- The pipeline-as-JSON extension point — adding a new step type is
  still a function + one dict entry.
- The streaming primitives (`csv.DictReader`, `ijson`, 8KB chunks).
  They hold at any scale, just over different storage backends.
- The retry policy and cooperative cancel design are scale-independent.

### 18.2 Faster streaming — when files grow beyond 100MB

The current row-by-row streaming (`csv.DictReader`, `ijson.items`,
8KB binary chunks) is the right call at the assignment's 100MB scale —
explained in §1.

At larger scale the trade-off flips. Quick scaling ladder:

| Input size | Approach | Why |
|---|---|---|
| ≤ 100MB | Row-by-row (today) | Memory bounded to one row, code is reviewable in 5 lines, no dependency. Runtime is dominated by I/O and gzip — vectorizing the row loop wouldn't show up. |
| 500MB – 10GB | **Polars lazy** or **DuckDB** | Both stream natively, multi-core by default, 10–30× faster than the Python row loop. Polars exposes a DataFrame API; DuckDB exposes SQL. Either is a clean fit. |
| 10GB+ / TB scale | Columnar storage (**Parquet + Arrow**) + DuckDB or Spark | Format change — Parquet is columnar, compressed, predicate-pushdown for free. Distributed compute if needed. Real orchestrator (Airflow / Dagster) for dependency graphs. |

**Why not pandas at the middle tier:** pandas is the right tool for
*analytical workflows* — notebooks, in-memory slicing, the broader
ML/plotting ecosystem (matplotlib, scikit-learn). For a streaming ETL
pipeline it's the wrong shape: `pd.read_csv` defaults to loading the
whole file, `chunksize` is essentially the row loop with a 50MB
dependency, and it's mostly single-threaded. Polars and DuckDB were
designed for this exact workload. At this assignment's scale none of
the three are worth the dependency.

Output format also changes when files get large. Single `[...]` JSON
arrays don't stream cleanly because you can't append to a closed array
— **JSON Lines** (`.jsonl`, one JSON object per line) becomes the
default, and most downstream systems prefer it anyway.
