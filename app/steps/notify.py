import time
import urllib.request
import urllib.error
import urllib.parse
import socket
import json

MAX_RETRIES = 5
RETRY_DELAYS = [5, 15, 30, 60, 120]  # exponential backoff in seconds
IDEMPOTENCY_HEADER = "X-Pipeline-Job-Id"


# Blocked prefixes — private networks, loopback, cloud metadata
BLOCKED_IP_PREFIXES = [
    "127.",       # loopback
    "0.0.0.0",    # unspecified
    "10.",        # private class A
    "192.168.",   # private class C
    "169.254.",   # link-local / AWS+GCP metadata service
    "::1",        # IPv6 loopback
    "fe80:",      # IPv6 link-local
    "172.",       # private class B (172.16.0.0–172.31.255.255)
]

BLOCKED_HOSTNAMES = ["localhost"]


def _validate_webhook_host(webhook_url: str):
    """
    SSRF protection — blocks requests to internal/private/metadata endpoints.
    Checks both the raw hostname string and the resolved IP address.
    Prevents DNS rebinding by resolving the hostname before connecting.
    """
    parsed = urllib.parse.urlparse(webhook_url)
    host   = parsed.hostname or ""

    # Check hostname string directly
    if host in BLOCKED_HOSTNAMES:
        raise ValueError(f"webhook_url points to a blocked host: {host}")

    # Resolve to IP and check resolved address
    # This catches cases like http://localtest.me that resolve to 127.0.0.1
    try:
        resolved_ip = socket.gethostbyname(host)
    except socket.gaierror:
        raise ValueError(f"webhook_url host could not be resolved: {host}")

    for prefix in BLOCKED_IP_PREFIXES:
        if resolved_ip.startswith(prefix):
            raise ValueError(
                f"webhook_url resolves to a blocked IP: {resolved_ip}. "
                f"Internal and private addresses are not allowed."
            )


def notify(job_id: str, output_file_path: str, params: dict):
    """
    Sends a webhook notification when job completes.
    Retries up to 5 times with exponential backoff.
    Sends idempotency key in header to prevent duplicate processing.
    Webhook failure does NOT fail the job — handled in processor.py
    """
    webhook_url = params.get("webhook_url")
    if not webhook_url:
        raise ValueError("webhook_url is required for notify step")

    # Validate scheme — must be http or https
    if not webhook_url.startswith(("http://", "https://")):
        raise ValueError("webhook_url must start with http:// or https://")

    # SSRF protection — block internal/private/metadata hosts
    _validate_webhook_host(webhook_url)

    # Send an actionable download URL instead of the internal filesystem path.
    # The receiver can fetch the result via this endpoint; they have no business
    # knowing our storage layout (info-disclosure) and they couldn't use it
    # anyway. See DECISIONS §6.
    payload = json.dumps({
        "job_id":     job_id,
        "status":     "COMPLETED",
        "result_url": f"/jobs/{job_id}/result",
    }).encode("utf-8")

    last_error = None

    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        try:
            req = urllib.request.Request(
                webhook_url,
                data    = payload,
                method  = "POST",
                headers = {
                    "Content-Type":       "application/json",
                    IDEMPOTENCY_HEADER:   job_id,  # prevents duplicate processing
                }
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status < 300:
                    return  # success

            last_error = f"Webhook returned status {response.status}"

        except urllib.error.URLError as e:
            last_error = str(e)

        if attempt < MAX_RETRIES:
            time.sleep(delay)

    raise ValueError(
        f"Webhook failed after {MAX_RETRIES} attempts. Last error: {last_error}"
    )
