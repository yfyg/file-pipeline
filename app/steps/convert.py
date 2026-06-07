import csv
import json
import os
import ijson

def convert(file_path: str, params: dict) -> str:
    """
    Converts between CSV and JSON formats.
    Fully streamed — never loads full file into memory.
    Returns path to the converted output file.
    """
    output_format = params.get("output_format")
    if not output_format:
        raise ValueError("output_format is required (csv or json)")

    _, ext = os.path.splitext(file_path)
    ext = ext.lstrip(".").lower()

    if ext == "csv" and output_format == "json":
        return _csv_to_json(file_path)
    elif ext == "json" and output_format == "csv":
        return _json_to_csv(file_path)
    elif ext == output_format:
        raise ValueError(f"File is already in {output_format} format")
    else:
        raise ValueError(f"Unsupported conversion: {ext} to {output_format}")


def _csv_to_json(file_path: str) -> str:
    """
    Converts CSV to JSON array.
    Reads one row at a time — memory usage is size of one row.
    Output: [{col1: val1, col2: val2}, ...]
    """
    output_path = file_path.replace(".csv", ".json")

    with open(file_path, "r") as infile, open(output_path, "w") as outfile:
        reader = csv.DictReader(infile)
        outfile.write("[\n")
        first = True

        for row in reader:
            if not first:
                outfile.write(",\n")
            json.dump(row, outfile)
            first = False

        outfile.write("\n]")

    return output_path


def _json_to_csv(file_path: str) -> str:
    """
    Converts JSON array to CSV.
    Uses ijson to read one object at a time — memory usage is size of one object.
    Assumption: JSON is an array of flat objects [{...}, {...}]
    """
    output_path = file_path.replace(".json", ".csv")

    with open(file_path, "rb") as infile:
        # First pass — get headers from first object only
        # We need headers before we can write CSV
        headers = None
        for item in ijson.items(infile, "item"):
            headers = list(item.keys())
            break  # only need first item for headers

    if not headers:
        raise ValueError("JSON file is empty or not an array of objects")

    # Second pass — write all rows
    with open(file_path, "rb") as infile, open(output_path, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()

        for item in ijson.items(infile, "item"):
            writer.writerow(item)

    return output_path
