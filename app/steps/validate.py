import os
import csv

ALLOWED_TYPES = ["csv", "json", "txt", "zip", "gz"]
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB in bytes

def validate(file_path: str, params: dict) -> str:
    """
    Validates a file at any point in the pipeline.
    Can run on original uploads or on converted/transformed output files.
    Returns the same file_path — validate does not produce a new file.
    """
    if not os.path.exists(file_path):
        raise ValueError(f"File not found: {file_path}")

    size = os.path.getsize(file_path)
    if size == 0:
        raise ValueError("File is empty")

    if size > MAX_FILE_SIZE:
        size_in_mb = size / (1024 * 1024)
        raise ValueError(f"File too large: {size_in_mb:.1f}MB. Maximum allowed size is 100MB")

    _, ext = os.path.splitext(file_path)
    ext = ext.lstrip(".").lower()

    if ext not in ALLOWED_TYPES:
        raise ValueError(f"File type '{ext}' is not allowed. Allowed types: {ALLOWED_TYPES}")

    expected_type = params.get("expected_type")
    if expected_type and ext != expected_type.lower():
        raise ValueError(
            f"Expected {expected_type} but got {ext}. "
            f"If you just ran a convert step make sure expected_type matches the new format."
        )

    _verify_file_content(file_path, ext)

    # Return same file path — validate does not transform the file
    return file_path


def _verify_file_content(file_path: str, ext: str):
    """
    Read a small sample to catch obvious corruption.
    Never loads full file into memory.
    """
    try:
        if ext == "json":
            with open(file_path, "r") as f:
                start = f.read(4096).strip()
                if not start.startswith(("{", "[")):
                    raise ValueError("File does not appear to be valid JSON")

        elif ext == "csv":
            with open(file_path, "r") as f:
                reader = csv.reader(f)
                next(reader)
                next(reader, None)

        else:
            with open(file_path, "rb") as f:
                f.read(1024)

    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"File appears corrupted: {str(e)}")
