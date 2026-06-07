import gzip
import os
import zipfile

CHUNK_SIZE = 8 * 1024  # 8KB per chunk

def compress(file_path: str, params: dict) -> str:
    """
    Compresses or decompresses a file.
    Always processed in 8KB chunks — memory never exceeds CHUNK_SIZE.
    Supports: gzip compress, gzip decompress, zip extraction.
    Returns path to the output file.
    """
    algorithm = params.get("algorithm", "gzip")
    action = params.get("action", "compress")  # compress or decompress

    if algorithm == "gzip":
        if action == "compress":
            return _gzip_compress(file_path)
        elif action == "decompress":
            return _gzip_decompress(file_path)
        else:
            raise ValueError(f"Unknown action: {action}. Use compress or decompress")

    elif algorithm == "zip":
        if action == "decompress":
            return _zip_extract(file_path)
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

    output_path = file_path[:-3]  # remove .gz extension

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
    """
    if not file_path.endswith(".zip"):
        raise ValueError(f"Expected .zip file but got: {file_path}")

    output_dir = os.path.dirname(file_path)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        # Get list of files in zip
        names = zip_ref.namelist()
        if not names:
            raise ValueError("Zip archive is empty")

        # Extract first file only — chunked to avoid memory issues
        target = names[0]
        output_path = os.path.join(output_dir, target)

        with zip_ref.open(target) as infile, open(output_path, "wb") as outfile:
            while True:
                chunk = infile.read(CHUNK_SIZE)
                if not chunk:
                    break
                outfile.write(chunk)

    return output_path
