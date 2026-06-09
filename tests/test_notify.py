"""
Notify step tests — webhook callbacks, idempotency, SSRF protection.

We don't make real HTTP requests. urllib.request.urlopen is monkeypatched per
test to capture what was sent (or to simulate failures). The 5-tier retry
delay (5s–120s) is also short-circuited so the test suite stays fast.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

# Note: `from app.steps import notify` returns the function (re-exported in
# the package __init__). To monkeypatch urllib / socket / time on the module
# itself we have to grab it via importlib.
import importlib
notify_mod = importlib.import_module("app.steps.notify")


def _fake_response(status=200):
    """Build a fake urlopen() context-manager response."""
    fake = MagicMock()
    fake.__enter__ = MagicMock(return_value=MagicMock(status=status))
    fake.__exit__ = MagicMock(return_value=False)
    return fake


def test_notify_posts_correct_payload_and_idempotency_header(monkeypatch):
    """
    A successful notify should POST exactly once with the expected JSON body
    and the idempotency header set to the job_id.
    """
    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["url"]     = req.full_url
        captured["method"]  = req.get_method()
        captured["headers"] = dict(req.header_items())
        captured["body"]    = req.data
        return _fake_response(status=200)

    monkeypatch.setattr(notify_mod.urllib.request, "urlopen", fake_urlopen)
    # Don't actually resolve DNS — pretend the host is public
    monkeypatch.setattr(notify_mod.socket, "gethostbyname", lambda h: "8.8.8.8")

    notify_mod.notify(
        job_id           = "job-abc-123",
        output_file_path = "storage/outputs/result.csv",
        params           = {"webhook_url": "https://example.com/hook"},
    )

    assert captured["url"]    == "https://example.com/hook"
    assert captured["method"] == "POST"
    # Idempotency header carries the job_id so receivers can dedup
    header_keys_lower = {k.lower(): v for k, v in captured["headers"].items()}
    assert header_keys_lower[notify_mod.IDEMPOTENCY_HEADER.lower()] == "job-abc-123"
    assert header_keys_lower["content-type"] == "application/json"

    # Body is the expected JSON shape.
    # Note we send a result_url (actionable for the receiver), NOT the internal
    # filesystem path (info-disclosure). See DECISIONS §6.
    body = json.loads(captured["body"])
    assert body == {
        "job_id":     "job-abc-123",
        "status":     "COMPLETED",
        "result_url": "/jobs/job-abc-123/result",
    }


def test_notify_retries_then_raises_on_persistent_failure(monkeypatch):
    """
    If every attempt fails, notify() raises ValueError after MAX_RETRIES tries.
    We don't want the test to actually wait 5+15+30+60s, so patch time.sleep.
    """
    call_count = {"n": 0}

    def fake_urlopen(req, timeout=None):
        call_count["n"] += 1
        # Simulate a connection error
        import urllib.error
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(notify_mod.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(notify_mod.socket, "gethostbyname", lambda h: "8.8.8.8")
    monkeypatch.setattr(notify_mod.time, "sleep", lambda s: None)  # no real waiting

    with pytest.raises(ValueError, match="Webhook failed after 5 attempts"):
        notify_mod.notify(
            job_id           = "job-fail",
            output_file_path = "out.csv",
            params           = {"webhook_url": "https://example.com/hook"},
        )

    assert call_count["n"] == notify_mod.MAX_RETRIES


def test_notify_blocks_localhost_ssrf(monkeypatch):
    """
    SSRF protection: webhook_url pointing to localhost is rejected with a
    clear error BEFORE any HTTP call is made.
    """
    def boom(*a, **kw):
        pytest.fail("urlopen should never be called when host is blocked")

    monkeypatch.setattr(notify_mod.urllib.request, "urlopen", boom)

    with pytest.raises(ValueError, match="blocked host"):
        notify_mod.notify(
            job_id           = "job-ssrf",
            output_file_path = "out.csv",
            params           = {"webhook_url": "http://localhost:8080/hook"},
        )


def test_notify_blocks_private_ip_after_dns_resolution(monkeypatch):
    """
    SSRF protection: even with a public-looking hostname, if DNS resolves
    to a private IP (e.g. attacker-controlled DNS pointing to 10.0.0.1),
    the request is rejected.
    """
    def boom(*a, **kw):
        pytest.fail("urlopen should never be called when resolved IP is private")

    monkeypatch.setattr(notify_mod.urllib.request, "urlopen", boom)
    monkeypatch.setattr(notify_mod.socket, "gethostbyname", lambda h: "10.0.0.42")

    with pytest.raises(ValueError, match="blocked IP"):
        notify_mod.notify(
            job_id           = "job-ssrf2",
            output_file_path = "out.csv",
            params           = {"webhook_url": "http://attacker-controlled.example.com/hook"},
        )


def test_notify_rejects_non_http_scheme():
    """webhook_url must use http or https — not file://, ftp://, etc."""
    with pytest.raises(ValueError, match="http:// or https://"):
        notify_mod.notify(
            job_id           = "job-scheme",
            output_file_path = "out.csv",
            params           = {"webhook_url": "file:///etc/passwd"},
        )


def test_notify_step_failure_does_not_fail_job(client, monkeypatch):
    """
    End-to-end: if the notify step throws (e.g. webhook unreachable), the JOB
    still reports COMPLETED. The notify *step* is FAILED but the file was
    processed successfully — that's the contract from DECISIONS §6.
    """
    from tests.conftest import make_csv, upload_file, wait_for_status

    # Make every webhook attempt fail
    import urllib.error
    def failing_urlopen(req, timeout=None):
        raise urllib.error.URLError("simulated outage")

    # Pretend the host resolves OK so we reach the urlopen call
    monkeypatch.setattr(notify_mod.urllib.request, "urlopen", failing_urlopen)
    monkeypatch.setattr(notify_mod.socket, "gethostbyname", lambda h: "8.8.8.8")
    monkeypatch.setattr(notify_mod.time, "sleep", lambda s: None)

    csv = make_csv([{"name": "Alice", "age": 30}])
    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "notify",   "params": {"webhook_url": "https://example.com/hook"}},
    ]
    job_id = upload_file(client, csv, "data.csv", pipeline).json()["job_id"]
    body = wait_for_status(client, job_id, expected_statuses=("COMPLETED",))

    # Job is COMPLETED even though notify failed
    assert body["status"] == "COMPLETED"

    # The notify step itself is marked FAILED
    notify_step = next(s for s in body["steps"] if s["type"] == "notify")
    assert notify_step["status"] == "FAILED"
    assert "Webhook failed" in (notify_step["error"] or "")
