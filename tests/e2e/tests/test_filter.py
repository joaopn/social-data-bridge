"""E2E: --filter flag selects only matching files.

Bug class: the `--filter` flag is a 1-line `fnmatch(fid, FILE_FILTER)`
copy-pasted into 5 orchestrators (parse, postgres_ingest, postgres_ml,
sr_ingest, sr_ml). The risk is one of those copies being missing or
buggy. Phase B4 deferred unit testing because fnmatch is stdlib; the
real risk is integration ("did we wire it everywhere?") and only one
orchestrator needs to be exercised here — wiring is uniform.

Flow:
  Place RC_2024-01.zst AND RC_2024-02.zst in dumps/
  sdp run parse --filter RC_2024-01
  → only RC_2024-01.parquet is produced; RC_2024-02 is untouched.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, WORKSPACE
from tests.e2e.helpers.fixtures import (
    place_reddit_fixtures,
    compress_to_zst,
    FIXTURES,
)


DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",          # postgres (we never start it; just need a DB configured)
    "db_pgdata_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",
    "db_pg_mem_limit": "0",
    "db_auth": "",
    "db_write_files": "",
}

SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1",          # parse only — filter is applied by parse orchestrator
    "src_parse_workers": "2",
    "src_write_files": "",
}


def test_parse_filter(workspace):
    """`sdp run parse --filter RC_2024-01` only parses the matching dump."""
    rc, output = SDPSession(DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # Place RC_2024-01.zst (real fixture) and a duplicate as RC_2024-02.zst.
    # Same content is fine — filter is on the file id (`RC_YYYY-MM`), not data.
    place_reddit_fixtures("reddit", data_types=["comments"])
    dumps_dir = WORKSPACE / "data" / "dumps" / "reddit" / "comments"
    compress_to_zst(FIXTURES / "reddit" / "RC_2024-01.ndjson",
                    dumps_dir / "RC_2024-02.zst")

    # Run parse with the filter. No DB needed for parse.
    result = run_sdp("run parse --source reddit --filter RC_2024-01 --build")
    assert result.returncode == 0, f"parse failed:\n{result.stderr}"

    parsed_dir = workspace / "data" / "parsed" / "reddit" / "comments"
    matched = parsed_dir / "RC_2024-01.parquet"
    excluded = parsed_dir / "RC_2024-02.parquet"

    assert matched.exists(), (
        f"Filter dropped the matching file. Parse stdout:\n{result.stdout}"
    )
    assert not excluded.exists(), (
        f"Filter let RC_2024-02 through. Parse stdout:\n{result.stdout}"
    )
