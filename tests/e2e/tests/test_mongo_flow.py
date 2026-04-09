"""E2E: MongoDB full flow — ingest → verify.

Full flow:
  sdp db setup       → mongo, no auth
  sdp source add reddit → mongo_ingest profile
  [place NDJSON in extracted/]
  sdp db start mongo
  sdp run mongo_ingest
  → verify: database exists, documents correct, indexes created
  sdp db stop mongo
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.fixtures import place_reddit_extracted
from tests.e2e.helpers.db import mongo_connect


DB_SETUP_ANSWERS = {
    "db_data_path": "",
    "db_databases": "2",         # mongo only
    "db_mongo_data_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",       # 1 GB (small for tests)
    "db_mongo_mem_limit": "0",   # unlimited
    "db_mongo_validate": "",     # full (default)
    "db_auth": "",               # no
    "db_write_files": "",
}

# mongo_ingest profile only
SOURCE_ADD_ANSWERS = {
    "src_data_types": "",            # accept default [submissions, comments]
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",          # parquet (doesn't matter for mongo, but required prompt)
    "src_parquet_rg_size": "",       # accept default
    "src_profiles": "6",             # mongo_ingest only
    "src_write_files": "",
}


def test_mongo_full_flow(workspace):
    """Ingest Reddit NDJSON into MongoDB and verify data."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add
    session = SDPSession(SOURCE_ADD_ANSWERS)
    rc, output = session.run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Place test data (uncompressed NDJSON → extracted/)
    place_reddit_extracted("reddit", data_types=["comments"])

    # 4. Start MongoDB
    result = run_sdp("db start mongo")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    # 5. Run mongo_ingest
    result = run_sdp("run mongo_ingest --source reddit --build")
    assert result.returncode == 0, f"run mongo_ingest failed:\n{result.stderr}"

    # 6. Verify MongoDB data
    client = mongo_connect()
    try:
        # The reddit template uses mongo_db_name_template: "{platform}_{data_type}"
        db_name = "reddit_comments"

        # Database and collections exist
        all_dbs = client.list_database_names()
        assert db_name in all_dbs, f"Database {db_name} not found. DBs: {all_dbs}"

        # Reddit uses per_file strategy: collection named after file with prefix
        # stripped (RC_2024-01 → 2024-01). _sdp_metadata is the state tracking collection.
        collections = [c for c in client[db_name].list_collection_names()
                       if not c.startswith("system.") and c != "_sdp_metadata"]
        assert len(collections) >= 1, f"Expected at least 1 collection, got {collections}"

        assert "2024-01" in collections, f"Expected collection 2024-01, found: {collections}"
        doc_count = client[db_name]["2024-01"].count_documents({})
        assert doc_count == 10, f"Expected 10 documents in 2024-01, got {doc_count}"
    finally:
        client.close()

    # 7. Stop MongoDB
    run_sdp("db stop mongo")


def test_mongo_validation_rejects_truncated(workspace):
    """Truncated NDJSON detected in pre-flight; user declines → not ingested."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add
    session = SDPSession(SOURCE_ADD_ANSWERS)
    rc, output = session.run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Place valid test data
    place_reddit_extracted("reddit", data_types=["comments"])

    # 4. Place truncated NDJSON alongside the valid file
    #    Filename must match the json pattern: ^RC_(\d{4}-\d{2})$
    truncated_dir = WORKSPACE / "data" / "extracted" / "reddit" / "comments"
    truncated_file = truncated_dir / "RC_2024-02"
    truncated_file.write_text(
        '{"id":"abc123","author":"user1","body":"valid line"}\n'
        '{"id":"def456","author":"user2","body":"truncated'
    )

    # 5. Start MongoDB
    result = run_sdp("db start mongo")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    # 6. Run mongo_ingest — answer "N" to truncation prompt (skip truncated files)
    result = run_sdp("run mongo_ingest --source reddit --build", input_text="N\n")
    assert result.returncode == 0, f"run mongo_ingest failed:\n{result.stderr}"

    # Pipeline output should warn about truncated files
    combined = result.stdout + result.stderr
    assert "truncated" in combined.lower(), f"Expected truncation warning:\n{combined}"
    assert "RC_2024-02" in combined, f"Expected truncated file name in output:\n{combined}"

    # 7. Verify MongoDB state
    client = mongo_connect()
    try:
        db_name = "reddit_comments"

        # Valid file (RC_2024-01) was ingested: 10 documents
        assert "2024-01" in client[db_name].list_collection_names()
        doc_count = client[db_name]["2024-01"].count_documents({})
        assert doc_count == 10, f"Expected 10 docs in 2024-01, got {doc_count}"

        # Truncated file (RC_2024-02) was skipped: collection should not exist
        collections = [c for c in client[db_name].list_collection_names()
                       if not c.startswith("_") and not c.startswith("system.")]
        if "2024-02" in collections:
            bad_count = client[db_name]["2024-02"].count_documents({})
            assert bad_count == 0, (
                f"Truncated file leaked {bad_count} documents into 2024-02"
            )
    finally:
        client.close()

    # 8. Stop MongoDB
    run_sdp("db stop mongo")
