"""E2E: PostgreSQL full flow — parse → ingest → verify.

Full flow:
  sdp db setup       → postgres, no auth
  sdp source add reddit → parse + postgres_ingest
  [compress fixtures → .zst]
  sdp db start postgres
  sdp run parse
  sdp run postgres_ingest
  → verify: schema exists, rows correct, no dupes, indexes created
  sdp db stop postgres

Runs once per intermediate file format (parquet, csv) — CSV exercises the
COPY-with-header path and pins the parser↔COPY column-list contract that
broke in v2.0.0.
"""

import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import pg_connect, pg_table_exists, pg_row_count, pg_query_scalar, pg_index_count


DB_SETUP_ANSWERS = {
    "db_data_path": "",
    "db_databases": "1",         # postgres
    "db_pgdata_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",     # skip
    "db_pg_mem_limit": "0",      # unlimited
    "db_auth": "",
    "db_write_files": "",
}


def _source_add_answers(file_format_choice: str) -> dict:
    answers = {
        "src_data_types": "",            # accept default [submissions, comments]
        "src_dumps_path": "",
        "src_extracted_path": "",
        "src_parsed_path": "",
        "src_output_path": "",
        "src_file_format": file_format_choice,
        "src_profiles": "1,4",           # parse + postgres_ingest
        "src_parse_workers": "2",
        "src_pg_prefer_lingua": "n",
        "src_pg_index_workers": "2",
        "src_write_files": "",
    }
    if file_format_choice == "1":  # parquet asks an extra row-group-size question
        answers["src_parquet_rg_size"] = ""
    return answers


@pytest.mark.parametrize(
    "file_format,format_choice,parsed_ext",
    [("parquet", "1", "parquet"), ("csv", "2", "csv")],
)
def test_postgres_full_flow(workspace, file_format, format_choice, parsed_ext):
    """Parse Reddit dumps, ingest into PostgreSQL, verify data."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add
    session = SDPSession(_source_add_answers(format_choice))
    rc, output = session.run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Place test data
    place_reddit_fixtures("reddit", data_types=["comments"])

    # 4. Start PostgreSQL
    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    # 5. Run parse
    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    # Verify parsed output was created
    parsed_path = workspace / "data" / "parsed" / "reddit" / "comments" / f"RC_2024-01.{parsed_ext}"
    assert parsed_path.exists(), f"Parsed {parsed_ext} not found. Parse output:\n{result.stdout}"

    # 6. Run postgres_ingest
    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, f"run postgres_ingest failed:\n{result.stderr}"

    # 7. Verify PostgreSQL data
    conn = pg_connect()
    try:
        # Schema and table exist
        assert pg_table_exists(conn, "reddit", "comments"), "Table reddit.comments not found"

        # Row count matches fixture (10 comments)
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"

        # No duplicate IDs
        dupes = pg_query_scalar(
            conn, "SELECT count(*) - count(DISTINCT id) FROM reddit.comments"
        )
        assert dupes == 0, f"Found {dupes} duplicate IDs"

        # Primary key column type
        col_type = pg_query_scalar(
            conn,
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'reddit' AND table_name = 'comments' AND column_name = 'id'",
        )
        assert col_type == "character varying", f"Expected varchar for id, got {col_type}"

        # Indexes created (reddit template defines author, subreddit, link_id for comments)
        idx_count = pg_index_count(conn, "reddit")
        assert idx_count >= 1, f"Expected at least 1 index, got {idx_count}"
    finally:
        conn.close()

    # 8. Stop PostgreSQL
    run_sdp("db stop postgres")
