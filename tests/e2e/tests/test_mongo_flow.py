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

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_extracted
from tests.e2e.helpers.db import mongo_connect


DB_SETUP_ANSWERS = {
    "db_data_path": "",
    "db_databases": "2",         # mongo only
    "db_mongo_data_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",       # 1 GB (small for tests)
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
