"""E2E: canonical smoke run across the operator's hot path.

Parametrized on ``workspace_via`` so the same pipeline runs through:

  - ``direct`` — sdp.py invoked with cwd=WORKSPACE (the canonical path).
  - ``symlink`` — sdp.py invoked with cwd=/tmp/sdp-link-* (a symlink
    pointing at WORKSPACE). Catches regressions in ROOT-resolution or
    mount-coverage code that compare paths string-wise without
    ``os.path.realpath``.

Three failure shapes pinned:

  - Bind-mount source dirs auto-created as root when the host dir is
    absent at ``db start`` time. Caught by uid assertions on
    ${PARSED_PATH} / ${OUTPUT_PATH} after the start.

  - Symlinked workspace breaking string-based mount comparison in
    ``_validate_run_mounts`` / ``_warn_mount_drift_after_source_change``.
    Caught by the ``symlink`` parametrization: a regression that drops
    realpath canonicalization false-flags drift on every in-parent
    source, which fails the post-add no-warn assertion.

  - Stale-mount-shaped failures in a running container. Root cause is
    diagnosed at the symptom level only; pinning the symptom (full
    parse → ingest pipeline must land rows end-to-end) means a future
    recurrence fails this test before reaching the operator.
"""

import os

import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.workspace import symlinked_workspace
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
    "src_parsed_path": "",       # default → in-parent
    "src_output_path": "",       # default → in-parent
    "src_file_format": "1",      # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",       # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


@pytest.mark.smoke
@pytest.mark.parametrize("workspace_via", ["direct", "symlink"])
def test_full_smoke(workspace, workspace_via):
    """db setup → start → source add → parse → ingest, end-to-end.

    Parametrization runs the same pipeline twice — once with sdp.py
    invoked at the canonical workspace path, once through a /tmp
    symlink. The symlink path exercises the realpath-canonicalization
    contract that ``_validate_run_mounts`` relies on; without it, the
    parent-mount coverage check false-flags drift on every in-parent
    source and ingest validation refuses to run.
    """
    if workspace_via == "symlink":
        ctx = symlinked_workspace()
    else:
        # contextlib.nullcontext-equivalent that yields WORKSPACE itself,
        # so the unified code path below stays simple.
        from contextlib import contextmanager

        @contextmanager
        def _direct():
            yield WORKSPACE
        ctx = _direct()

    with ctx as invoke_path:
        # 1. db setup PG-only, no auth.
        rc, output = SDPSession(PG_DB_SETUP).run_interactive(
            "db setup", cwd=invoke_path,
        )
        assert rc == 0, f"db setup failed:\n{output}"

        # 2. db start postgres BEFORE adding any source. Triggers
        #    cmd_db_start's parent-dir pre-create.
        result = run_sdp("db start postgres", cwd=invoke_path)
        assert result.returncode == 0, (
            f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        wait_for_healthy("postgres")

        # 3. Parent dirs must be host-user-owned, not root. Docker
        #    auto-creates missing bind-mount sources as root, which
        #    blocks any subsequent `mkdir` under them; pre-create in
        #    cmd_db_start guards against this. Assert against the
        #    CANONICAL workspace, not the symlink path — assertions
        #    side stays canonical, sdp.py side exercises the symlink.
        for sub in ("data/parsed", "data/output"):
            host_dir = WORKSPACE / sub
            assert host_dir.exists(), f"{host_dir} should be pre-created by db start"
            uid = host_dir.stat().st_uid
            assert uid == os.getuid(), (
                f"{host_dir} owned by uid={uid}, expected host uid={os.getuid()}. "
                f"docker auto-created it as root — pre-create regressed."
            )

        # 4. source add on default (in-parent) paths must NOT warn.
        #    A regression in the realpath canonicalization in
        #    _warn_mount_drift_after_source_change false-flags every
        #    in-parent source under symlinked invocation; this
        #    parametrization is the load-bearing pin for that path.
        rc, output = SDPSession(PG_SOURCE_ADD).run_interactive(
            "source add reddit", cwd=invoke_path,
        )
        assert rc == 0, f"source add failed:\n{output}"
        assert "[WARN] Mount drift" not in output, (
            f"source add false-flagged drift on in-parent paths "
            f"({workspace_via} workspace) — likely a regression in path "
            f"canonicalization:\n{output}"
        )

        # 5. Full pipeline must go green. parse → lingua isn't selected
        #    in PG_SOURCE_ADD (profiles 1,4 = parse + postgres_ingest
        #    only), so this sequence is parse → ingest.
        place_reddit_fixtures("reddit", data_types=["comments"])

        result = run_sdp("run parse --source reddit --build", cwd=invoke_path)
        assert result.returncode == 0, (
            f"run parse failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        result = run_sdp(
            "run postgres_ingest --source reddit --build", cwd=invoke_path,
        )
        assert result.returncode == 0, (
            f"postgres_ingest failed end-to-end ({workspace_via} workspace):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        # 6. Sanity-check rows actually landed (not just exit-code-zero).
        conn = pg_connect()
        try:
            assert pg_table_exists(conn, "reddit", "comments")
            count = pg_row_count(conn, "reddit", "comments")
            assert count > 0, f"expected rows ingested, got {count}"
        finally:
            conn.close()

        run_sdp("db stop postgres", cwd=invoke_path)
