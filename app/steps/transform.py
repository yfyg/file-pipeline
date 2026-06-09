import csv
import json
import os
import ijson

def transform(file_path: str, params: dict):
    """
    Transforms CSV or JSON files.
    Supports: select_columns, filter_rows, text_transform (uppercase/lowercase/trim)
    Both CSV and JSON are processed row by row — full file never loaded into memory.

    Returns (output_path, stats) where stats is
    {"input_rows": N, "output_rows": M}.
    """
    _, ext = os.path.splitext(file_path)
    ext = ext.lstrip(".").lower()

    if ext == "csv":
        return _transform_csv(file_path, params)
    elif ext == "json":
        return _transform_json(file_path, params)
    else:
        raise ValueError(f"Transform does not support file type: {ext}")


def _transform_csv(file_path: str, params: dict) -> str:
    """
    Streams CSV row by row — never loads full file into memory.
    Writes each processed row directly to output file.
    """
    select_columns = params.get("select_columns")
    filter_rows    = params.get("filter_rows")
    text_transform = params.get("text_transform")

    base = os.path.splitext(file_path)[0]  # safe extension handling
    output_path = base + "_transformed.csv"

    input_rows  = 0
    output_rows = 0

    with open(file_path, "r") as infile, open(output_path, "w", newline="") as outfile:
        reader = csv.DictReader(infile)
        writer = None  # created after we know the output columns

        for row in reader:
            input_rows += 1

            # Step 1 — select columns
            if select_columns:
                row = {k: v for k, v in row.items() if k in select_columns}

            # Step 2 — filter rows
            if filter_rows:
                col = filter_rows.get("column")
                if col in row and not _apply_filter(row[col], filter_rows):
                    continue  # skip this row

            # Step 3 — text transformation
            if text_transform:
                row = _apply_text_transform(row, text_transform)

            # Create writer on first kept row
            if writer is None:
                writer = csv.DictWriter(outfile, fieldnames=row.keys())
                writer.writeheader()

            # Write immediately — not stored in memory
            writer.writerow(row)
            output_rows += 1

        # Handle empty file or all rows filtered out
        if writer is None:
            headers = select_columns or reader.fieldnames or []
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()

    return output_path, {"input_rows": input_rows, "output_rows": output_rows}


def _transform_json(file_path: str, params: dict) -> str:
    """
    Streams JSON array item by item using ijson.
    Only one object in memory at a time.
    Assumption: JSON file is an array of objects e.g. [{...}, {...}, ...]

    Step order matches the CSV path: select → filter → text_transform.
    If you filter on a column that was dropped by select_columns the filter
    is a silent no-op — include the filter column in select_columns to avoid
    that. See DECISIONS §16.
    """
    select_fields  = params.get("select_columns")
    filter_rows    = params.get("filter_rows")
    text_transform = params.get("text_transform")

    base = os.path.splitext(file_path)[0]  # safe extension handling
    output_path = base + "_transformed.json"

    input_rows  = 0
    output_rows = 0

    with open(file_path, "rb") as infile, open(output_path, "w") as outfile:
        outfile.write("[\n")
        first = True

        for item in ijson.items(infile, "item"):
            input_rows += 1

            # Step 1 — select fields
            if select_fields:
                item = {k: v for k, v in item.items() if k in select_fields}

            # Step 2 — filter rows
            if filter_rows:
                col = filter_rows.get("column")
                if col in item and not _apply_filter(item[col], filter_rows):
                    continue  # skip this row

            # Step 3 — text transform
            if text_transform:
                item = _apply_text_transform(item, text_transform)

            # Write item directly to file
            if not first:
                outfile.write(",\n")
            json.dump(item, outfile)
            first = False
            output_rows += 1

        outfile.write("\n]")

    return output_path, {"input_rows": input_rows, "output_rows": output_rows}


def _apply_filter(value: str, filter_params: dict) -> bool:
    """
    Returns True if the row should be KEPT, False if it should be dropped.

    Non-numeric values with a numeric filter (`gt` / `lt`) are dropped, not
    kept. The user asked for a numeric comparison; a value we can't parse
    cannot satisfy it, so the safe default is to drop. Matches SQL semantics:
    `WHERE age > 50` against a NULL or non-numeric cell drops the row.

    `eq` works with either numeric or string compare, so it's allowed in the
    non-numeric branch.
    """
    try:
        num = float(value)
        if "gt" in filter_params and num <= float(filter_params["gt"]):
            return False
        if "lt" in filter_params and num >= float(filter_params["lt"]):
            return False
        if "eq" in filter_params and num != float(filter_params["eq"]):
            return False
    except ValueError:
        # Value isn't numeric. gt/lt cannot be evaluated → drop the row.
        if "gt" in filter_params or "lt" in filter_params:
            return False
        if "eq" in filter_params and value != filter_params["eq"]:
            return False
    return True


def _apply_text_transform(row: dict, transform_type: str) -> dict:
    result = {}
    for k, v in row.items():
        if isinstance(v, str):
            if transform_type == "uppercase":
                v = v.upper()
            elif transform_type == "lowercase":
                v = v.lower()
            elif transform_type == "trim":
                v = v.strip()
        result[k] = v
    return result
