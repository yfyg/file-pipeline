import gzip
import os
import zipfile

CHUNK_SIZE = 8 * 1024  # 8KB per chunk

def compress(file_path: str, params: dict):
    """
    Compresses or decompresses a file.
    Always processed in 8KB chunks — memory never exceeds CHUNK_SIZE.
    Supports: gzip compress, gzip decompress, zip extraction.

    Returns (output_path, stats). compress operates on bytes, not rows,
    so stats is always {}.
    """
    algorithm = params.get("algorithm", "gzip")
    action = params.get("action", "compress")  # compress or decompress

    if algorithm == "gzip":
        if action == "compress":
            return _gzip_compress(file_path), {}
        elif action == "decompress":
            return _gzip_decompress(file_path), {}
        else:
            raise ValueError(f"Unknown action: {action}. Use compress or decompress")

    elif algorithm == "zip":
        if action == "decompress":
            return _zip_extract(file_path), {}
        else:
            raise ValueError("zip algorithm only supports decompress for now")

    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}. Use gzip or zip")


def _gzip_compress(file_path: str) -> str:
    """
    Compresses file using gzip.
    Reads input in 8KB chunks — output written directly to .gz file.
    """
    output_path = file_path + ".gz"

    with open(file_path, "rb") as infile, gzip.open(output_path, "wb") as outfile:
        while True:
            chunk = infile.read(CHUNK_SIZE)
            if not chunk:
                break
            outfile.write(chunk)

    return output_path


def _gzip_decompress(file_path: str) -> str:
    """
    Decompresses a .gz file.
    Reads compressed chunks — writes decompressed output directly to disk.
    """
    if not file_path.endswith(".gz"):
        raise ValueError(f"Expected .gz file but got: {file_path}")

    # Use splitext to safely remove .gz extension
    output_path = os.path.splitext(file_path)[0]

    with gzip.open(file_path, "rb") as infile, open(output_path, "wb") as outfile:
        while True:
            chunk = infile.read(CHUNK_SIZE)
            if not chunk:
                break
            outfile.write(chunk)

    return output_path


def _zip_extract(file_path: str) -> str:
    """
    Extracts first file from a zip archive.
    Returns path to the extracted file.
    Protects against Zip Slip — path traversal attack via malicious zip filenames.
    """
    if not file_path.endswith(".zip"):
        raise ValueError(f"Expected .zip file but got: {file_path}")

    output_dir = os.path.dirname(file_path)
    # Resolve to absolute path — used to detect traversal attempts
    output_dir_abs = os.path.realpath(output_dir)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        names = zip_ref.namelist()
        if not names:
            raise ValueError("Zip archive is empty")

        target = names[0]

        # Resolve the full output path
        output_path = os.path.realpath(os.path.join(output_dir, target))

        # Zip Slip protection — ensure output is inside our storage folder
        if not output_path.startswith(output_dir_abs + os.sep):
            raise ValueError(
                f"Zip Slip attack detected — "
                f"file '{target}' would be written outside storage directory"
            )

        with zip_ref.open(target) as infile, open(output_path, "wb") as outfile:
            while True:
                chunk = infile.read(CHUNK_SIZE)
                if not chunk:
                    break
                outfile.write(chunk)

    return output_path
