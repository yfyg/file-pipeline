import time
import urllib.request
import urllib.error
import json

MAX_RETRIES = 5
RETRY_DELAYS = [5, 15, 30, 60, 120]  # exponential backoff in seconds
IDEMPOTENCY_HEADER = "X-Pipeline-Job-Id"


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

    # Validate webhook URL is http/https — prevent SSRF attacks
    if not webhook_url.startswith(("http://", "https://")):
        raise ValueError("webhook_url must start with http:// or https://")

    payload = json.dumps({
        "job_id":      job_id,
        "status":      "COMPLETED",
        "output_file": output_file_path,
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
