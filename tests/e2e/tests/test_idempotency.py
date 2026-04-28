"""E2E: Re-running ingestion profiles is idempotent.

Bug class: state-tracking divergence on re-run.
  - PG state lives in pgdata_path/pipeline_state.json. If the JSON drifts from
    DB reality, re-run repeats the ingestion. PG's ON CONFLICT DO UPDATE
    swallows the duplicates silently — no error, just wasted work and the
    risk of clobbering newer data with older.
  - Mongo per_file collections have no PK constraint. If `_sdp_metadata` state
    is missed, mongoimport silently inserts duplicate documents — collection
    counts double on each re-run. Highest silent-bug risk of the three.
  - SR is covered by re-running sr_ingest in test_sr_flow.py: PK upsert
    handles dedup at the DB layer, so this test focuses on PG + Mongo.

Each test runs the same parse → ingest sequence twice in the same workspace
(no teardown between) and asserts row counts unchanged after the second run.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures, place_reddit_extracted
from tests.e2e.helpers.db import (
    pg_connect,
    pg_row_count,
    mongo_connect,
)


# ---- PostgreSQL ----------------------------------------------------------

PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",
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

PG_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",            # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_postgres_ingest_idempotent(workspace):
    """Run parse + postgres_ingest twice; row count must be unchanged."""
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    try:
        result = run_sdp("run parse --source reddit --build")
        assert result.returncode == 0, f"parse failed:\n{result.stderr}"

        result = run_sdp("run postgres_ingest --source reddit --build")
        assert result.returncode == 0, f"first ingest failed:\n{result.stderr}"

        conn = pg_connect()
        try:
            first = pg_row_count(conn, "reddit", "comments")
            assert first == 10, f"first run: expected 10 rows, got {first}"
        finally:
            conn.close()

        # Re-run ingest. State should mark all files processed → no work.
        result = run_sdp("run postgres_ingest --source reddit")
        assert result.returncode == 0, f"second ingest failed:\n{result.stderr}"

        conn = pg_connect()
        try:
            second = pg_row_count(conn, "reddit", "comments")
            assert second == first, (
                f"row count drift on re-run: was {first}, now {second}. "
                f"second-run output:\n{result.stdout}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


# ---- MongoDB -------------------------------------------------------------

MONGO_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "2",
    "db_mongo_data_path": "",
    "db_export_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",
    "db_mongo_mem_limit": "0",
    "db_mongo_validate": "",
    "db_auth": "",
    "db_write_files": "",
}

MONGO_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    # mongo-only all_profiles = [parse, lingua, ml, mongo_ingest] → 4 = mongo_ingest.
    "src_profiles": "4",
    "src_write_files": "",
}


def test_mongo_ingest_idempotent(workspace):
    """Run mongo_ingest twice; collection size must be unchanged.

    Mongo per_file collections have no PK constraint, so a state-tracking miss
    silently doubles document counts. This is the most failure-prone of the
    three backends.
    """
    rc, output = SDPSession(MONGO_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(MONGO_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_extracted("reddit", data_types=["comments"])

    result = run_sdp("db start mongo")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    try:
        result = run_sdp("run mongo_ingest --source reddit --build")
        assert result.returncode == 0, f"first ingest failed:\n{result.stderr}"

        client = mongo_connect()
        try:
            first = client["reddit_comments"]["2024-01"].count_documents({})
            assert first == 10, f"first run: expected 10 docs, got {first}"
        finally:
            client.close()

        # Re-run. State in `_sdp_metadata` should mark RC_2024-01 processed.
        result = run_sdp("run mongo_ingest --source reddit")
        assert result.returncode == 0, f"second ingest failed:\n{result.stderr}"

        client = mongo_connect()
        try:
            second = client["reddit_comments"]["2024-01"].count_documents({})
            assert second == first, (
                f"document drift on re-run: was {first}, now {second}. "
                f"second-run output:\n{result.stdout}"
            )
        finally:
            client.close()
    finally:
        run_sdp("db stop mongo")
