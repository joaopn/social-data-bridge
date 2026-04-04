"""Test data generation for E2E tests.

Compresses NDJSON fixtures to .zst format and places them in the
appropriate data directories within the workspace.
"""

import shutil
from pathlib import Path

import pyzstd

WORKSPACE = Path("/workspace")
FIXTURES = WORKSPACE / "tests" / "fixtures"


def compress_to_zst(src, dst):
    """Compress a file to .zst format.

    Args:
        src: Source file path.
        dst: Destination .zst file path.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    data = src.read_bytes()
    dst.write_bytes(pyzstd.compress(data))


def place_reddit_fixtures(source_name="reddit", data_types=None):
    """Place Reddit test fixtures as compressed .zst dumps.

    Args:
        source_name: Source name (determines dump directory).
        data_types: List of data types to place. Default: ["comments"].
    """
    if data_types is None:
        data_types = ["comments"]

    fixture_map = {
        "comments": ("RC_2024-01.ndjson", "RC_2024-01.zst"),
        "submissions": ("RS_2024-01.ndjson", "RS_2024-01.zst"),
    }

    for dt in data_types:
        if dt not in fixture_map:
            raise ValueError(f"Unknown reddit data type: {dt}")
        src_name, dst_name = fixture_map[dt]
        # Dumps are organized into data_type subdirectories (e.g. dumps/reddit/comments/)
        dt_dir = WORKSPACE / "data" / "dumps" / source_name / dt
        dt_dir.mkdir(parents=True, exist_ok=True)
        compress_to_zst(FIXTURES / "reddit" / src_name, dt_dir / dst_name)


def place_custom_fixtures(source_name, data_types=None):
    """Place custom platform test fixtures as uncompressed NDJSON.

    Custom platform fixtures go to data/extracted/<source>/<data_type>/
    since the custom parser reads from the extracted directory.

    Args:
        source_name: Source name.
        data_types: List of data types. Default: ["events"].
    """
    if data_types is None:
        data_types = ["events"]

    # Fixture config (valid_platform_custom.yaml) uses json pattern ^events$
    # (no extension), so strip .ndjson when placing.
    fixture_map = {
        "events": ("events.ndjson", "events"),
    }

    for dt in data_types:
        if dt not in fixture_map:
            raise ValueError(f"Unknown custom data type: {dt}")
        src_name, dst_name = fixture_map[dt]
        src = FIXTURES / "custom" / src_name
        dst_dir = WORKSPACE / "data" / "extracted" / source_name / dt
        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst_dir / dst_name)


def place_reddit_extracted(source_name="reddit", data_types=None):
    """Place Reddit NDJSON fixtures directly in extracted/ for mongo_ingest.

    mongo_ingest reads from extracted/ not dumps/, so we place uncompressed
    NDJSON files there.

    Args:
        source_name: Source name.
        data_types: List of data types. Default: ["comments"].
    """
    if data_types is None:
        data_types = ["comments"]

    # Reddit json file pattern expects no extension (e.g. RC_2024-01, not RC_2024-01.ndjson)
    fixture_map = {
        "comments": ("RC_2024-01.ndjson", "RC_2024-01"),
        "submissions": ("RS_2024-01.ndjson", "RS_2024-01"),
    }

    for dt in data_types:
        if dt not in fixture_map:
            raise ValueError(f"Unknown reddit data type: {dt}")
        src_name, dst_name = fixture_map[dt]
        src = FIXTURES / "reddit" / src_name
        dst_dir = WORKSPACE / "data" / "extracted" / source_name / dt
        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst_dir / dst_name)
