"""
MongoDB operations for social_data_pipeline.

Provides collection management, mongoimport-based bulk ingestion,
index creation, and metadata tracking for the mongo_ingest profile.

Adapted from db_tools/db_tools/mongodb.py with project-consistent conventions.
"""

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def get_mongo_uri(host: str, port: int, user: str = None, password: str = None) -> str:
    """Build MongoDB connection URI. Uses authSource=admin when credentials are provided."""
    if user and password:
        from urllib.parse import quote_plus
        return f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/?authSource=admin"
    if user:
        from urllib.parse import quote_plus
        return f"mongodb://{quote_plus(user)}:@{host}:{port}/?authSource=admin"
    return f"mongodb://{host}:{port}"


def _get_client(host: str, port: int, user: str = None, password: str = None):
    """Lazy-import pymongo and return a MongoClient."""
    from pymongo import MongoClient
    return MongoClient(get_mongo_uri(host, port, user, password))


def ensure_collection(
    db_name: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> bool:
    """
    Ensure a collection exists with zstd WiredTiger compression.

    Returns True if the collection was newly created, False if it already existed.
    """
    client = _get_client(host, port, user, password)
    try:
        if collection_name in client[db_name].list_collection_names():
            return False

        client[db_name].create_collection(
            collection_name,
            storageEngine={
                'wiredTiger': {
                    'configString': 'block_compressor=zstd',
                }
            },
        )
        print(f"[sdp] Created collection {db_name}.{collection_name} (zstd)")
        return True
    finally:
        client.close()


def _redact_uri(uri: str) -> str:
    """Replace password in a MongoDB URI with ***."""
    import re
    return re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', uri)


def _validate_ndjson_tail(filepath: str) -> str:
    """Validate an NDJSON file by checking only the last 8KB.

    Catches truncated files (the most common failure mode) at near-zero cost.

    Returns:
        "ok" if valid, "truncated" if last line is incomplete JSON.

    Raises:
        ValueError: If the file is empty or contains no non-empty lines.
    """
    size = os.path.getsize(filepath)
    if size == 0:
        raise ValueError(f"Empty file: {filepath}")

    with open(filepath, 'rb') as f:
        # Read last 8KB (or entire file if smaller)
        offset = max(0, size - 8192)
        f.seek(offset)
        tail = f.read().decode('utf-8', errors='replace')

    # Find last non-empty line
    lines = tail.split('\n')
    last_line = ''
    for line in reversed(lines):
        stripped = line.strip()
        if stripped:
            last_line = stripped
            break

    if not last_line:
        raise ValueError(f"File contains no non-empty lines: {filepath}")

    try:
        json.loads(last_line)
    except json.JSONDecodeError:
        return "truncated"

    return "ok"


def _validate_ndjson_full(filepath: str) -> str:
    """Validate every line of an NDJSON file.

    Streams line-by-line with O(1) memory. Distinguishes truncation (last line
    is bad, all prior lines valid) from malformed (bad line in the middle).

    Returns:
        "ok" if all lines valid, "truncated" if only the last line is bad.

    Raises:
        ValueError: If the file is empty, has no content, or has malformed
            JSON in the middle (not just truncation at the end).
    """
    size = os.path.getsize(filepath)
    if size == 0:
        raise ValueError(f"Empty file: {filepath}")

    line_count = 0
    last_nonempty_num = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            stripped = line.strip()
            if not stripped:
                continue
            line_count += 1
            last_nonempty_num = line_num
            try:
                json.loads(stripped)
            except json.JSONDecodeError as e:
                # Check if this is the last non-empty line (truncation)
                # by reading ahead to see if there are more non-empty lines
                remaining_has_content = False
                for rest_line in f:
                    if rest_line.strip():
                        remaining_has_content = True
                        break

                if remaining_has_content:
                    # Bad line in the middle → malformed
                    preview = stripped[:200]
                    raise ValueError(
                        f"Malformed JSON at line {line_num} in {filepath}\n"
                        f"Parse error: {e}\n"
                        f"Content (first 200 chars): {preview}"
                    ) from e
                else:
                    # Bad line at the end, all prior lines valid → truncated
                    return "truncated"

    if line_count == 0:
        raise ValueError(f"File contains no non-empty lines: {filepath}")

    return "ok"


def _validate_csv_tail(filepath: str) -> str:
    """Validate a CSV file by checking the tail for truncation.

    Verifies the file ends with a newline and the last line has the same
    number of fields as the header. Catches truncated files at near-zero cost.

    Returns:
        "ok" if valid, "truncated" if the file appears truncated.

    Raises:
        ValueError: If the file is empty or has no header.
    """
    size = os.path.getsize(filepath)
    if size == 0:
        raise ValueError(f"Empty file: {filepath}")

    # Read header (first line)
    with open(filepath, 'r', encoding='utf-8') as f:
        header = f.readline()
    if not header.strip():
        raise ValueError(f"CSV file has empty header: {filepath}")
    header_fields = header.strip().count(',') + 1

    # Read tail
    with open(filepath, 'rb') as f:
        offset = max(0, size - 8192)
        f.seek(offset)
        tail = f.read().decode('utf-8', errors='replace')

    if not tail.endswith('\n'):
        return "truncated"

    # Check last non-empty line field count
    lines = tail.split('\n')
    for line in reversed(lines):
        stripped = line.strip()
        if stripped:
            tail_fields = stripped.count(',') + 1
            if tail_fields != header_fields:
                return "truncated"
            break

    return "ok"


def validate_file(filepath: str, mode: str = "full") -> str:
    """Validate a file before mongoimport ingestion.

    Args:
        filepath: Path to the file to validate (NDJSON or CSV).
        mode: "full" (default, validate every line), "tail" (check last 8KB),
              or "none" (skip validation).

    Returns:
        "ok" if valid, "truncated" if file is truncated, "none" if skipped.

    Raises:
        ValueError: If the file has malformed content (not just truncation).
    """
    if mode == "none":
        return "none"

    is_csv = filepath.lower().endswith('.csv')

    if is_csv:
        return _validate_csv_tail(filepath)
    elif mode == "full":
        return _validate_ndjson_full(filepath)
    else:
        return _validate_ndjson_tail(filepath)


def _parquet_to_ndjson(filepath: str, exclude_columns: List[str] = None) -> str:
    """Convert a Parquet file to temp NDJSON for mongoimport.

    Reads in small batches (1024 rows) for strict memory control.
    Temp file placed alongside input: {filepath}.ndjson.tmp

    Args:
        filepath: Path to the parquet file.
        exclude_columns: Column names to skip (from platform config).

    Returns:
        Path to the temp NDJSON file.
    """
    import pyarrow.parquet as pq

    temp_path = filepath + '.ndjson.tmp'
    pf = pq.ParquetFile(filepath)

    # Determine columns to read
    exclude_set = set(exclude_columns or [])
    if exclude_set:
        schema = pf.schema_arrow
        columns = [schema.field(i).name for i in range(len(schema))
                   if schema.field(i).name not in exclude_set]
        print(f"[sdp] Excluding {len(exclude_set)} columns: {', '.join(sorted(exclude_set))}")
    else:
        columns = None

    with open(temp_path, 'w', encoding='utf-8') as f:
        for batch in pf.iter_batches(batch_size=1024, columns=columns):
            rows = batch.to_pydict()
            n_rows = len(next(iter(rows.values()))) if rows else 0
            col_names = list(rows.keys())
            for i in range(n_rows):
                row = {}
                for col in col_names:
                    val = rows[col][i]
                    if val is not None:
                        row[col] = val
                f.write(json.dumps(row, default=str) + '\n')

    return temp_path


def mongoimport_file(
    filepath: str,
    db_name: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
    num_workers: int = 4,
    log_dir: str = "/data/mongo/logs",
    exclude_columns: List[str] = None,
    allow_truncated: bool = False,
) -> None:
    """
    Ingest a file using mongoimport subprocess.

    Supports JSON/NDJSON (default), CSV (auto-detected from .csv extension),
    and Parquet (auto-detected from .parquet extension, converted to temp NDJSON).
    Logs are appended to {log_dir}/mongoimport_{db}_{collection}.log.
    Raises RuntimeError on failure (never exposes credentials in exceptions).

    File validation is handled by the orchestrator's pre-flight phase, not here.

    Args:
        exclude_columns: For parquet files, column names to skip during conversion
            (e.g., embedding arrays). List-type columns are auto-excluded.
        allow_truncated: If True, tolerate "unexpected EOF" from mongoimport
            (partial data before truncation is kept). Used for user-approved
            truncated files.
    """
    # Transparent parquet → temp NDJSON conversion
    ndjson_temp = None
    import_path = filepath
    if filepath.lower().endswith('.parquet'):
        print(f"[sdp] Converting parquet to NDJSON: {Path(filepath).name}")
        ndjson_temp = _parquet_to_ndjson(filepath, exclude_columns=exclude_columns)
        import_path = ndjson_temp

    uri = get_mongo_uri(host, port, user, password)
    command = [
        "mongoimport",
        f"--uri={uri}",
        "--db", db_name,
        "--collection", collection_name,
        "--file", import_path,
        "--stopOnError",
        "--legacy",
        "--numInsertionWorkers", str(num_workers),
    ]

    # Auto-detect CSV input from file extension
    if import_path.lower().endswith('.csv'):
        command.extend(["--type", "csv", "--headerline"])

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"mongoimport_{db_name}_{collection_name}.log")

    # Build redacted command for logging (never write credentials to logs)
    redacted_command = [_redact_uri(arg) if '://' in arg else arg for arg in command]

    try:
        with open(log_file, 'a') as log:
            log.write(f"\n{'='*60}\n")
            log.write(f"Timestamp: {datetime.now().isoformat()}\n")
            log.write(f"Command: {' '.join(redacted_command)}\n")
            log.write(f"File: {filepath}\n")
            if ndjson_temp:
                log.write(f"Converted from: {filepath} (parquet)\n")
            log.write(f"{'='*60}\n")
            result = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )

        if result.returncode != 0:
            # Check if this is a truncation EOF that the user approved
            if allow_truncated:
                with open(log_file, 'r') as lf:
                    log_tail = lf.read()[-500:]
                if 'unexpected EOF' in log_tail:
                    print(f"[sdp] Truncated file accepted (partial data ingested): {Path(filepath).name}")
                else:
                    raise RuntimeError(
                        f"mongoimport failed for {filepath} -> {db_name}.{collection_name}. "
                        f"See log: {log_file}"
                    )
            else:
                raise RuntimeError(
                    f"mongoimport failed for {filepath} -> {db_name}.{collection_name}. "
                    f"See log: {log_file}"
                )

        # Success: clean up temp NDJSON
        if ndjson_temp and os.path.exists(ndjson_temp):
            os.remove(ndjson_temp)

    except Exception:
        if ndjson_temp and os.path.exists(ndjson_temp):
            print(f"[sdp] Temp NDJSON kept for inspection: {ndjson_temp}")
        raise


def create_index(
    db_name: str,
    collection_name: str,
    field: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> None:
    """Create a single ascending index on a collection."""
    from pymongo import ASCENDING

    client = _get_client(host, port, user, password)
    try:
        index_name = f"{field}_1"
        existing = client[db_name][collection_name].list_indexes()
        existing_names = [idx['name'] for idx in existing]

        if index_name in existing_names:
            print(f"[sdp] Index {index_name} already exists on {db_name}.{collection_name}")
            return

        client[db_name][collection_name].create_index(
            [(field, ASCENDING)],
            name=index_name,
        )
        print(f"[sdp] Created index {index_name} on {db_name}.{collection_name}")
    finally:
        client.close()


def get_collection_names(db_name: str, host: str, port: int, user: str = None, password: str = None) -> List[str]:
    """Get list of collection names in a database (excludes system/metadata collections)."""
    client = _get_client(host, port, user, password)
    try:
        names = client[db_name].list_collection_names()
        return [n for n in sorted(names) if not n.startswith('_')]
    finally:
        client.close()


def get_collections_by_data_type(
    db_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> Dict[str, List[str]]:
    """Get mapping of data_type -> collection names from _sdp_metadata.

    Falls back to listing all non-system collections under 'unknown' if no metadata exists.
    """
    client = _get_client(host, port, user, password)
    try:
        metadata = client[db_name]['_sdp_metadata']
        if metadata.estimated_document_count() == 0:
            # No metadata — fall back to listing collections
            names = [n for n in client[db_name].list_collection_names() if not n.startswith('_')]
            return {'unknown': sorted(names)} if names else {}

        pipeline = [
            {'$group': {'_id': '$data_type', 'collections': {'$addToSet': '$collection'}}},
            {'$sort': {'_id': 1}},
        ]
        result = {}
        for doc in metadata.aggregate(pipeline):
            dt = doc['_id']
            result[dt] = sorted(doc['collections'])
        return result
    finally:
        client.close()


def get_existing_indexes(
    db_name: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> List[str]:
    """Get list of index names on a collection (excluding _id_)."""
    client = _get_client(host, port, user, password)
    try:
        indexes = client[db_name][collection_name].list_indexes()
        return [idx['name'] for idx in indexes if idx['name'] != '_id_']
    finally:
        client.close()


def record_ingested_file(
    db_name: str,
    file_id: str,
    data_type: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
    validation: str = "pass",
) -> None:
    """
    Record an ingested file in the _sdp_metadata collection.

    Stores file_id, data_type, collection_name, validation status, and timestamp.
    Used for state recovery when the state JSON file is lost.

    Args:
        validation: File validation status: "pass", "truncated", or "none".
    """
    client = _get_client(host, port, user, password)
    try:
        metadata = client[db_name]['_sdp_metadata']
        metadata.update_one(
            {'file_id': file_id, 'data_type': data_type},
            {
                '$set': {
                    'file_id': file_id,
                    'data_type': data_type,
                    'collection': collection_name,
                    'validation': validation,
                    'ingested_at': datetime.now().isoformat(),
                }
            },
            upsert=True,
        )
    finally:
        client.close()


def get_ingested_files(
    db_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
    data_type: Optional[str] = None,
) -> List[str]:
    """
    Get list of ingested file_ids from the _sdp_metadata collection.

    Args:
        db_name: MongoDB database name
        host: MongoDB host
        port: MongoDB port
        data_type: If provided, filter by data_type

    Returns:
        List of file_id strings
    """
    client = _get_client(host, port, user, password)
    try:
        metadata = client[db_name]['_sdp_metadata']
        query = {'data_type': data_type} if data_type else {}
        return [doc['file_id'] for doc in metadata.find(query, {'file_id': 1, '_id': 0})]
    finally:
        client.close()
