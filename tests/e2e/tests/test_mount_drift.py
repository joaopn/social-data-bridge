"""E2E: per-source mount drift between PG and the running container.

The bug class:

    sdp db setup postgres
    sdp db start postgres        # override.yml regenerated; no per-source mounts
    sdp source add reddit        # adds source files, override is now stale
    sdp run postgres_ingest -s reddit
        → before C6: dies deep in pg_parquet COPY with an opaque "no such file"
        → after C6: exits 1 at the CLI with a recovery hint

This test pins the new behavior: the failure surface is the CLI's mount
validator, the error names the exact `db stop && db start` recovery line,
and after running that line ingest succeeds normally.
"""

import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import pg_connect, pg_table_exists, pg_row_count


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",         # postgres only
    "db_pgdata_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",     # skip
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
    "src_file_format": "1",       # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_mount_drift_blocks_run_then_recovery_succeeds(workspace):
    """Postgres started before `source add` → `run postgres_ingest` fails fast."""
    # 1. db setup, no sources yet.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Start postgres BEFORE adding the source. override.yml is regenerated
    #    with an empty per-source mount set.
    result = run_sdp("db start postgres")
    assert result.returncode == 0, (
        f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    # 3. Add the source AFTER PG is running. override.yml is now stale: it
    #    has no /data/parsed/reddit mount, but the container is already up
    #    with that stale mount set.
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # `source add` itself should warn about the drift (PG is running and
    # override no longer matches the configured sources).
    assert "[WARN] Mount drift" in output or "missing source mount" in output, (
        f"source add did not warn about mount drift:\n{output}"
    )

    # 4. Place fixtures and parse — parse runs in a fresh container with
    #    source-scoped mounts, so it must not be affected by PG's drift.
    place_reddit_fixtures("reddit", data_types=["comments"])
    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    # 5. The mount-drift bug surfaces here. The validator must:
    #    - exit 1 at the CLI (not deep inside docker compose run)
    #    - name the missing destination
    #    - print the exact recovery command
    result = run_sdp("run postgres_ingest --source reddit")
    assert result.returncode == 1, (
        f"expected exit 1 from mount-drift guard, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "/data/parsed/reddit" in combined, (
        f"missing-mount destination not surfaced:\n{combined}"
    )
    assert (
        "db stop postgres" in combined and "db start postgres" in combined
    ), f"recovery hint missing:\n{combined}"

    # 6. Run the recovery — the existing two-step regenerates the override
    #    and restarts PG with the per-source mount in place.
    result = run_sdp("db stop postgres")
    assert result.returncode == 0, f"db stop failed:\n{result.stderr}"
    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start (recovery) failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    # 7. Ingest now succeeds.
    result = run_sdp("run postgres_ingest --source reddit")
    assert result.returncode == 0, (
        f"ingest after recovery still failing:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 8. Sanity-check the data landed.
    conn = pg_connect()
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows after recovery, got {count}"
    finally:
        conn.close()

    run_sdp("db stop postgres")
