import os
import json
import csv

ALLOWED_TYPES = ["csv", "json", "txt", "zip", "gz"]
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB in bytes

def validate(file_path: str, params: dict) -> dict:
    """
    Validates the file exists, matches expected type, and isn't corrupted.
    Also enforces 100MB size limit.
    Returns a result dict with metadata.
    """
    # Check file exists
    if not os.path.exists(file_path):
        raise ValueError(f"File not found: {file_path}")

    # Check file size
    size = os.path.getsize(file_path)
    if size == 0:
        raise ValueError("File is empty")

    # Check 100MB limit
    if size > MAX_FILE_SIZE:
        size_in_mb = size / (1024 * 1024)
        raise ValueError(f"File too large: {size_in_mb:.1f}MB. Maximum allowed size is 100MB")

    # Get file extension
    _, ext = os.path.splitext(file_path)
    ext = ext.lstrip(".").lower()

    # Check file type is allowed
    if ext not in ALLOWED_TYPES:
        raise ValueError(f"File type '{ext}' is not allowed. Allowed types: {ALLOWED_TYPES}")

    # Check expected type if provided
    expected_type = params.get("expected_type")
    if expected_type and ext != expected_type.lower():
        raise ValueError(f"Expected {expected_type} but got {ext}")

    # Sample the file to catch obvious corruption
    # We intentionally don't read the full file to keep memory bounded
    _verify_file_content(file_path, ext)

    return {
        "size": size,
        "size_mb": round(size / (1024 * 1024), 2),
        "extension": ext,
        "valid": True
    }


def _verify_file_content(file_path: str, ext: str):
    """
    Read a small sample to catch obvious corruption.
    We intentionally avoid loading the full file into memory.
    Deep validation happens row-by-row during actual processing.
    """
    try:
        if ext == "json":
            # Read first 4KB and check it starts like valid JSON
            with open(file_path, "r") as f:
                start = f.read(4096).strip()
                if not start.startswith(("{", "[")):
                    raise ValueError("File does not appear to be valid JSON")

        elif ext == "csv":
            # Read first 2 rows only
            with open(file_path, "r") as f:
                reader = csv.reader(f)
                next(reader)        # header row
                next(reader, None)  # first data row (None if file has only header)

        else:
            # For zip, gz, txt — just check first 1KB is readable
            with open(file_path, "rb") as f:
                f.read(1024)

    except ValueError:
        raise  # re-raise our own messages as-is
    except Exception as e:
        raise ValueError(f"File appears corrupted: {str(e)}")
