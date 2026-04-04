"""E2E: Reddit parse pipeline (CSV output).

Full flow:
  sdp db setup       → postgres, no auth, defaults
  sdp source add reddit → parse profile only, csv format
  [compress fixtures → .zst]
  sdp run parse      → verify CSV output
"""

import polars

from tests.e2e.helpers.sdp import SDPSession, run_sdp
from tests.e2e.helpers.fixtures import place_reddit_fixtures


# Answers for: sdp db setup (postgres, no auth, all defaults)
DB_SETUP_ANSWERS = {
    "db_data_path": "",          # accept default ./data
    "db_databases": "1",         # postgres only
    "db_pgdata_path": "",        # accept default
    "db_name": "",               # datasets
    "db_pg_port": "",            # 5432
    "db_tablespaces": "",        # no
    "db_filesystem": "1",        # standard
    "db_pgtune_method": "3",     # skip
    "db_auth": "",               # no
    "db_write_files": "",        # yes
}

# Answers for: sdp source add reddit (parse only, csv format)
SOURCE_ADD_ANSWERS = {
    "src_data_types": "",            # accept default [submissions, comments]
    "src_dumps_path": "",            # accept default
    "src_extracted_path": "",        # accept default
    "src_parsed_path": "",           # accept default
    "src_output_path": "",           # accept default
    "src_file_format": "2",          # csv
    "src_profiles": "1",             # parse only
    "src_parse_workers": "2",        # low for test speed
    "src_write_files": "",           # yes
}


def test_parse_reddit_csv(workspace):
    """Parse Reddit .zst dumps to CSV and verify output."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add
    session = SDPSession(SOURCE_ADD_ANSWERS)
    rc, output = session.run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Place test data (compress NDJSON → .zst)
    place_reddit_fixtures("reddit", data_types=["comments", "submissions"])

    # 4. Run parse
    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"

    # 5. Verify comments CSV
    comments_dir = workspace / "data" / "parsed" / "reddit" / "comments"
    assert comments_dir.exists(), f"Parsed dir missing. Parse output:\n{result.stdout}"
    csvs = list(comments_dir.glob("RC_2024-01.csv"))
    assert len(csvs) == 1, f"Expected 1 comments CSV, found: {list(comments_dir.iterdir())}"

    df = polars.read_csv(csvs[0])
    assert len(df) == 10, f"Expected 10 rows, got {len(df)}"

    # Mandatory columns present and non-null
    for col in ["dataset", "id", "retrieved_utc"]:
        assert col in df.columns, f"Missing mandatory column: {col}"
        assert df[col].null_count() == 0, f"Null values in mandatory column: {col}"

    # Expected Reddit fields present
    for col in ["body", "author", "subreddit", "score"]:
        assert col in df.columns, f"Missing expected column: {col}"

    # 6. Verify submissions CSV
    subs_dir = workspace / "data" / "parsed" / "reddit" / "submissions"
    csvs = list(subs_dir.glob("RS_2024-01.csv"))
    assert len(csvs) == 1, f"Expected 1 submissions CSV, found: {list(subs_dir.iterdir())}"

    df = polars.read_csv(csvs[0])
    assert len(df) == 10, f"Expected 10 rows, got {len(df)}"
    assert "title" in df.columns
