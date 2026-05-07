"""E2E: source-add ↔ DB-server mount lifecycle.

Two contracts are pinned here:

1. **In-parent source add doesn't need restart.** With the dual-mount
   design (docker-compose.yml binds ``${PARSED_PATH}`` / ``${OUTPUT_PATH}``
   on the postgres + starrocks server blocks), a source whose paths fall
   under those parents is visible to a running DB the moment its
   directories appear on disk. ``source add`` on a default-path source
   does not warn, and ``run postgres_ingest`` succeeds without a
   restart.
2. **Out-of-parent source add does need restart.** A source whose paths
   live outside ``${PARSED_PATH}`` / ``${OUTPUT_PATH}`` (the multi-disk
   case) is invisible to the running container's parent mount. ``source
   add`` warns about the drift, ``run postgres_ingest`` refuses with a
   ``db stop && db start`` recovery hint, and after the restart the
   regenerated override lets the same ingest succeed.

This test exercises the postgres path. The starrocks path is structurally
identical (same dual-mount pattern in docker-compose.yml) and is covered
indirectly through the existing ``test_sr_flow.py`` E2E.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import pg_connect, pg_table_exists, pg_row_count


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",         # postgres only
    "db_pgdata_path": "",
    "db_export_path": "",
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
    "src_parsed_path": "",        # default → ./data/parsed/reddit (in-parent)
    "src_output_path": "",        # default → ./data/output/reddit (in-parent)
    "src_file_format": "1",       # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_in_parent_source_add_does_not_need_restart(workspace):
    """db start (no sources) → source add (default paths) → run ingest, no restart.

    Default-path source's parsed/output dirs live under ``${PARSED_PATH}`` /
    ``${OUTPUT_PATH}``, which the postgres compose block already binds. The
    running container can see the new source's files immediately; the source-
    add warning stays silent and the ingest validator green-lights the run.
    """
    # 1. db setup, no sources yet.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Start postgres BEFORE adding any source.
    result = run_sdp("db start postgres")
    assert result.returncode == 0, (
        f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    # 3. Add the source AFTER PG is running. With default paths (in-parent),
    #    no override regen is required, and source add MUST NOT warn.
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"
    assert "[WARN] Mount drift" not in output, (
        f"source add unexpectedly warned for in-parent source:\n{output}"
    )

    # 4. db verify reports clean — no mount-related findings.
    result = run_sdp("db verify")
    assert result.returncode == 0, (
        f"db verify reported drift after in-parent source add:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 5. Place fixtures and run parse → ingest, all in one go (no db restart).
    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, (
        f"postgres_ingest failed without an intervening db restart "
        f"(this is the parent-mount fix's whole point):\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 6. Sanity-check the data landed via the live (un-restarted) container.
    conn = pg_connect()
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"
    finally:
        conn.close()

    run_sdp("db stop postgres")


PG_SOURCE_ADD_OUT_OF_PARENT = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "/workspace/extra/parsed/reddit",   # outside ${PARSED_PATH}=/workspace/data/parsed
    "src_output_path": "/workspace/extra/output/reddit",   # outside ${OUTPUT_PATH}=/workspace/data/output
    "src_file_format": "1",        # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",         # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_out_of_parent_source_still_drifts_until_restart(workspace):
    """source add (out-of-parent paths) → ingest blocks until db restart.

    Out-of-parent sources are not covered by the postgres compose block's
    parent ``${PARSED_PATH}`` / ``${OUTPUT_PATH}`` mounts, so the running
    container is genuinely missing the source's mounts. ``source add``
    must warn; the next ``run postgres_ingest`` must refuse with the
    ``db stop && db start`` hint; and after the restart, override regen
    must close the gap and let the same ingest go green.
    """
    # 1. db setup, no sources.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Start postgres BEFORE adding any source. The compose block binds
    #    ${PARSED_PATH} (=/workspace/data/parsed) and ${OUTPUT_PATH} as parent
    #    mounts; an out-of-parent source's paths fall outside both.
    result = run_sdp("db start postgres")
    assert result.returncode == 0, (
        f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    # 3. source add with explicit out-of-parent absolute paths. The
    #    post-add drift check must fire because the running container
    #    has no per-source mount and the parent mount can't cover a
    #    path outside ${PARSED_PATH}/${OUTPUT_PATH}.
    rc, output = SDPSession(PG_SOURCE_ADD_OUT_OF_PARENT).run_interactive(
        "source add reddit",
    )
    assert rc == 0, f"source add failed:\n{output}"
    assert "[WARN] Mount drift" in output, (
        f"source add did NOT warn for out-of-parent source — drift detection "
        f"is silently broken:\n{output}"
    )
    # Recovery hint format pinned in sdp.py:_warn_mount_drift_after_source_change
    assert "db stop postgres && python sdp.py db start postgres" in output, (
        f"source add warning is missing the copy-paste recovery line:\n{output}"
    )

    # 4. Place fixtures and try to ingest WITHOUT restarting. The
    #    cmd_run validator must refuse with the matching recovery hint.
    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, (
        f"run parse should succeed regardless of mount drift "
        f"(parse doesn't touch the DB container):\n{result.stderr}"
    )

    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode != 0, (
        f"postgres_ingest should have refused on out-of-parent drift "
        f"but exited 0:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "missing mount(s) for source 'reddit'" in combined, (
        f"validator did not surface the missing-mount error:\n{combined}"
    )
    assert "db stop postgres && python sdp.py db start postgres" in combined, (
        f"validator error did not name the db stop/start recovery line:\n{combined}"
    )

    # 5. Restart postgres. cmd_db_start regenerates the override with
    #    per-source mount entries (out-of-parent sources are NOT skipped
    #    by expected_source_mounts), so the new container has the mount.
    result = run_sdp("db stop postgres")
    assert result.returncode == 0, f"db stop failed:\n{result.stderr}"
    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start (post-restart) failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    # 6. db verify reports clean.
    result = run_sdp("db verify")
    assert result.returncode == 0, (
        f"db verify still reports drift after restart:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 7. Same ingest now succeeds.
    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, (
        f"postgres_ingest still failing after the restart that's supposed "
        f"to fix the drift:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 8. Sanity-check rows landed.
    conn = pg_connect()
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"
    finally:
        conn.close()

    run_sdp("db stop postgres")
