"""
Tests for the retry policy in processor._run_step_with_retries, and for the
gzip-decompress / zip-extract compression paths (including Zip Slip protection).

Retry tests inject a fake step into STEP_FUNCTIONS that raises a controlled
exception. time.sleep is patched so the suite stays fast.
"""

import gzip
import io
import os
import zipfile

import pytest

from tests.conftest import make_csv, upload_file, wait_for_status


# ---------- Retry policy ----------

def _patch_step(monkeypatch, app_modules, step_name, fn):
    """Helper: replace a step function in the worker's STEP_FUNCTIONS dict."""
    monkeypatch.setitem(app_modules.processor.STEP_FUNCTIONS, step_name, fn)


def test_transient_oserror_succeeds_on_retry(client, app_modules, monkeypatch):
    """
    An OSError is transient → retried with backoff. If the second attempt
    succeeds, the job COMPLETES and the step is marked COMPLETED.
    """
    attempts = {"n": 0}

    def flaky_transform(file_path, params):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise OSError("transient disk hiccup")
        # On retry: succeed by returning the file unchanged + empty stats
        return file_path, {}

    _patch_step(monkeypatch, app_modules, "transform", flaky_transform)
    # Skip the real backoff (2s, 4s) so the test doesn't actually wait
    monkeypatch.setattr(app_modules.processor.time, "sleep", lambda s: None)

    csv = make_csv([{"name": "Alice", "age": 30}])
    pipeline = [{"step": "transform", "params": {}}]
    job_id = upload_file(client, csv, "data.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    assert body["status"] == "COMPLETED"
    assert attempts["n"] == 2   # one failure + one success
    transform_step = next(s for s in body["steps"] if s["type"] == "transform")
    assert transform_step["status"] == "COMPLETED"


def test_persistent_oserror_exhausts_retries(client, app_modules, monkeypatch):
    """
    An OSError that keeps happening → step retried up to MAX_RETRIES, then
    marked FAILED. Job is FAILED. time.sleep called exactly MAX_RETRIES-1 times.
    """
    attempts = {"n": 0}
    sleeps   = {"n": 0}

    def always_fails(file_path, params):
        attempts["n"] += 1
        raise OSError("disk is on fire")

    def counting_sleep(s):
        sleeps["n"] += 1

    _patch_step(monkeypatch, app_modules, "transform", always_fails)
    monkeypatch.setattr(app_modules.processor.time, "sleep", counting_sleep)

    csv = make_csv([{"name": "Alice", "age": 30}])
    pipeline = [{"step": "transform", "params": {}}]
    job_id = upload_file(client, csv, "data.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("FAILED",))

    assert body["status"] == "FAILED"
    assert attempts["n"] == app_modules.processor.MAX_RETRIES
    # Sleep happens between attempts, not after the last one
    assert sleeps["n"]   == app_modules.processor.MAX_RETRIES - 1

    transform_step = next(s for s in body["steps"] if s["type"] == "transform")
    assert transform_step["status"] == "FAILED"
    assert "disk is on fire" in (transform_step["error"] or "")


def test_valueerror_is_not_retried(client, app_modules, monkeypatch):
    """
    ValueError is a permanent / deterministic failure → step fails on the
    first attempt with no retry, time.sleep never called.
    """
    attempts = {"n": 0}
    sleeps   = {"n": 0}

    def bad_input(file_path, params):
        attempts["n"] += 1
        raise ValueError("file format invalid")

    _patch_step(monkeypatch, app_modules, "transform", bad_input)
    monkeypatch.setattr(app_modules.processor.time, "sleep",
                        lambda s: sleeps.update(n=sleeps["n"] + 1))

    csv = make_csv([{"name": "Alice", "age": 30}])
    pipeline = [{"step": "transform", "params": {}}]
    job_id = upload_file(client, csv, "data.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("FAILED",))

    assert body["status"] == "FAILED"
    assert attempts["n"] == 1   # one attempt, no retries
    assert sleeps["n"]   == 0   # no backoff sleep ever


# ---------- Compression: decompress + zip extract + Zip Slip ----------

def test_gzip_compress_decompress_roundtrip(client, app_modules):
    """
    gzip-compress then gzip-decompress should recover the original bytes.
    """
    original_csv = make_csv([
        {"name": "Alice", "age": 30},
        {"name": "Bob",   "age": 25},
    ])

    # Step 1 — compress
    compress_pipeline = [{"step": "compress", "params": {"algorithm": "gzip"}}]
    job_id = upload_file(client, original_csv, "data.csv", compress_pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))
    assert body["status"] == "COMPLETED"

    compressed_bytes = client.get(f"/jobs/{job_id}/result").content
    # Sanity — gzip magic number
    assert compressed_bytes[:2] == b"\x1f\x8b"

    # Step 2 — decompress (re-upload the .gz and run gzip decompress)
    decompress_pipeline = [
        {"step": "compress", "params": {"algorithm": "gzip", "action": "decompress"}}
    ]
    job_id2 = upload_file(
        client, compressed_bytes, "data.csv.gz", decompress_pipeline
    ).json()["job_id"]
    body2 = wait_for_status(client, job_id2, expected_statuses=("COMPLETED",))
    assert body2["status"] == "COMPLETED"

    recovered = client.get(f"/jobs/{job_id2}/result").content
    assert recovered == original_csv


def test_zip_extract_rejects_zip_slip(client, app_modules):
    """
    Security: a malicious zip with a path-traversal entry (../../etc/passwd)
    must be rejected before any file is written.
    """
    # Build a malicious zip in memory — entry name escapes the storage folder
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("../../etc/evil.csv", b"id,name\n1,Mallory\n")
    malicious_zip_bytes = buf.getvalue()

    pipeline = [
        {"step": "compress", "params": {"algorithm": "zip", "action": "decompress"}}
    ]
    job_id = upload_file(
        client, malicious_zip_bytes, "evil.zip", pipeline
    ).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("FAILED",))

    assert body["status"] == "FAILED"
    compress_step = next(s for s in body["steps"] if s["type"] == "compress")
    assert compress_step["status"] == "FAILED"
    assert "zip slip" in (compress_step["error"] or "").lower()


def test_zip_extract_skips_directory_entries(client, app_modules):
    """
    A zip whose first entry is a directory (name ending in /) must skip past
    it and extract the first regular file, not fail with FileNotFoundError.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        # Directory entry first — this used to break the extractor
        zf.writestr("subdir/", b"")
        zf.writestr("real_file.csv", b"id,name\n1,Alice\n2,Bob\n")
    zip_bytes = buf.getvalue()

    pipeline = [
        {"step": "compress", "params": {"algorithm": "zip", "action": "decompress"}}
    ]
    job_id = upload_file(
        client, zip_bytes, "archive.zip", pipeline
    ).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    assert body["status"] == "COMPLETED"
    extracted = client.get(f"/jobs/{job_id}/result").content
    assert b"Alice" in extracted
    assert b"Bob"   in extracted
