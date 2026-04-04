"""E2E: Custom platform parse pipeline (Parquet output).

Full flow:
  sdp db setup       → postgres, no auth
  sdp source add mydata → custom platform, events, ndjson input, parquet output
  [replace platform.yaml with test fixture config that has fields defined]
  [place events.ndjson in extracted/]
  sdp run parse      → verify Parquet output

Note: source add for non-HF custom platforms produces an empty fields list
(fields are populated via HF metadata or manual editing). This test replaces
platform.yaml with the unit test fixture config after source add to provide
the parser with field definitions.
"""

import shutil
from pathlib import Path

import polars

from tests.e2e.helpers.sdp import SDPSession, run_sdp
from tests.e2e.helpers.fixtures import place_custom_fixtures
from tests.e2e.helpers.workspace import WORKSPACE


DB_SETUP_ANSWERS = {
    "db_data_path": "",
    "db_databases": "1",         # postgres
    "db_pgdata_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",     # skip
    "db_auth": "",
    "db_write_files": "",
}

# Custom platform: events data type, ndjson input, parquet output, parse only
SOURCE_ADD_ANSWERS = {
    "src_data_types": "events",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",          # parquet
    "src_parquet_rg_size": "",       # accept default 1M
    "src_profiles": "1",             # parse only
    "src_parse_workers": "2",
    # Custom platform prompts
    "src_db_schema": "",             # accept default (mydata)
    "src_input_format": "1",         # ndjson
    "src_dump_glob_events": "events*.ndjson.zst",
    "src_write_files": "",
}

FIXTURE_PLATFORM_YAML = Path("/workspace/tests/fixtures/config/valid_platform_custom.yaml")


def test_parse_custom_parquet(workspace):
    """Parse custom NDJSON to Parquet and verify output."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add (custom platform — creates directory structure and parse.yaml)
    session = SDPSession(SOURCE_ADD_ANSWERS)
    rc, output = session.run_interactive("source add mydata")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Replace platform.yaml with fixture config that has fields + file patterns
    platform_yaml = workspace / "config" / "sources" / "mydata" / "platform.yaml"
    shutil.copy2(FIXTURE_PLATFORM_YAML, platform_yaml)

    # 4. Place fixtures (uncompressed NDJSON → extracted/)
    place_custom_fixtures("mydata", data_types=["events"])

    # 5. Run parse
    result = run_sdp("run parse --source mydata --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    # 6. Verify Parquet output
    parsed_dir = workspace / "data" / "parsed" / "mydata" / "events"
    parquets = list(parsed_dir.glob("*.parquet"))
    assert len(parquets) >= 1, f"Expected parquet files, found: {list(parsed_dir.iterdir()) if parsed_dir.exists() else 'dir missing'}"

    df = polars.read_parquet(parquets[0])
    assert len(df) == 5, f"Expected 5 rows, got {len(df)}"

    # Fields from the fixture platform config
    assert "id" in df.columns
    assert "timestamp" in df.columns
    assert "score" in df.columns
    assert "content" in df.columns
    # Dot-notation fields resolved (user.name → name, user.profile.age → age)
    assert "name" in df.columns
    assert "age" in df.columns
