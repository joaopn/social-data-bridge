"""E2E: db unsetup --db <name> symmetry with db setup.

Two contracts are pinned here, both surfaced as bug classes by the
auth-robustness sweep where each had unit-test coverage but no
end-to-end proof:

H5 — StarRocks ``storage_paths`` cleanup. The chown + rmtree dance via
``docker run`` happens behind ``input()`` prompts and isn't unit-testable
cleanly. The unit tests pin ``_read_sr_storage_paths`` (the helper that
reads the yaml); they do not pin that the actual cleanup fires.

H6 — Jobs scheduler orphaned-target warning. The warning text is stdout
only and easy to silently regress when refactoring ``_unsetup_single_db``.
"""

import shutil

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE


# ---------------------------------------------------------------------------
# H5 — SR storage_paths chown + rmtree on `db unsetup --db starrocks`
# ---------------------------------------------------------------------------

# Single extra storage path. The SDPSession answer dict is keyed by tag,
# so a multi-path setup (which loops on the same `db_sr_storage_path` /
# `db_sr_more_paths` tags) can't deliver distinct values per iteration —
# one path exercises the chown + rmtree dance just as well, and the bug
# class is "cleanup runs at all", not "cleanup runs N times".
SR_EXTRA_DISK = "/workspace/extra/sr_disk"

SR_ONLY_SETUP = {
    "db_data_path": "",
    "db_databases": "3",                     # starrocks only
    "db_sr_data_path": "",
    "db_export_path": "",
    "db_sr_port": "",
    "db_sr_fe_http_port": "",
    "db_sr_fe_heap": "",
    "db_sr_mem_limit": "0",
    "db_sr_be_mem": "",
    "db_sr_alter_workers": "",
    "db_sr_multidisk": "y",
    "db_sr_storage_path": SR_EXTRA_DISK,
    "db_sr_more_paths": "",                  # accept default (no)
    "db_auth": "",
    "db_write_files": "",
}


def test_sr_storage_paths_removed_on_db_unsetup(workspace):
    """`db unsetup --db starrocks` chowns + rmtrees the extra storage path.

    Without this contract, the cleanup arm regresses silently — the unit
    tests would still pass (they only cover the path-reading helper) and
    a future refactor of ``_unsetup_single_db`` could drop the SR side
    of the chown + rmtree dance without anything failing in CI.
    """
    # Pre-create the storage path as the host user. Without this, docker
    # auto-creates the bind-mount source dir as root when SR starts (H1
    # failure shape), and the test would conflate H1 with H5.
    extra_dir = WORKSPACE / "extra" / "sr_disk"
    extra_dir.mkdir(parents=True, exist_ok=True)

    rc, output = SDPSession(SR_ONLY_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    sr_yaml = WORKSPACE / "config" / "db" / "starrocks.yaml"
    assert sr_yaml.exists(), "starrocks.yaml should exist after setup"
    assert SR_EXTRA_DISK in sr_yaml.read_text(), (
        f"storage_paths not written to starrocks.yaml:\n{sr_yaml.read_text()}"
    )

    # Bring StarRocks up so the chown + rmtree path actually has SR-written
    # contents to clean (rather than vacuously succeeding on empty dirs).
    result = run_sdp("db start starrocks")
    assert result.returncode == 0, (
        f"db start starrocks failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("starrocks", timeout=180)

    # SR must have written something into the extra storage path while
    # running — otherwise the chown step is a no-op and the test doesn't
    # exercise the bug class.
    assert any(extra_dir.iterdir()), (
        f"SR did not populate {extra_dir} during start; chown+rmtree "
        f"would be a no-op and the test wouldn't pin H5."
    )

    # Single-DB unsetup: yes/yes through both confirmation prompts.
    result = run_sdp("db unsetup --db starrocks", input_text="y\ny\n")
    assert result.returncode == 0, (
        f"db unsetup --db starrocks failed:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    assert not extra_dir.exists(), (
        f"SR extra storage path {extra_dir} should be removed after "
        f"`db unsetup --db starrocks`. The chown + rmtree dance for "
        f"`storage_paths` regressed."
    )
    assert not sr_yaml.exists(), "starrocks.yaml should be gone after unsetup"

    env_text = (WORKSPACE / ".env").read_text()
    leftover = [
        line for line in env_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
        and line.split("=", 1)[0].strip().startswith("STARROCKS_")
    ]
    assert not leftover, (
        f"STARROCKS_* keys still present in .env after unsetup:\n"
        f"{chr(10).join(leftover)}"
    )

    # Best-effort cleanup of the storage parent in case of partial fail.
    shutil.rmtree(WORKSPACE / "extra", ignore_errors=True)


# ---------------------------------------------------------------------------
# H6 — Jobs scheduler orphaned-target warning on `db unsetup --db <name>`
# ---------------------------------------------------------------------------

PG_SR_SETUP_NO_AUTH = {
    "db_data_path": "",
    "db_databases": "1,3",                   # postgres + starrocks
    "db_pgdata_path": "",
    "db_sr_data_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",                 # skip
    "db_pg_mem_limit": "0",
    "db_sr_port": "",
    "db_sr_fe_http_port": "",
    "db_sr_fe_heap": "",
    "db_sr_mem_limit": "0",
    "db_sr_be_mem": "",
    "db_sr_alter_workers": "",
    "db_sr_multidisk": "",                   # default no
    "db_auth": "",
    "db_write_files": "",
}

JOBS_SETUP_PG_SR = {
    "jobs_port": "",
    "jobs_result_root": "",
    "jobs_max_concurrent": "",
    "jobs_history_retention": "",
    "jobs_pg_target_enable": "y",
    "jobs_pg_target_name": "pg_main",
    "jobs_pg_target_db": "",
    "jobs_sr_target_enable": "y",
    "jobs_sr_target_name": "sr_main",
    "jobs_pg_timeout_hours": "",
    "jobs_sr_timeout_hours": "",
    "jobs_ui_auth_enable": "",
    "jobs_write_files": "",
}


def test_jobs_target_warning_on_db_unsetup(workspace):
    """`db unsetup --db postgres` warns about orphaned jobs targets.

    The warning is stdout only and advisory, not auto-remediation. We
    assert the text shape (label + target name + recovery hint) without
    asserting that the targets are auto-removed — auto-remediation is a
    separate feature and pinning it would scope-creep beyond H6.
    """
    rc, output = SDPSession(PG_SR_SETUP_NO_AUTH).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    assert (WORKSPACE / "config" / "db" / "postgres.yaml").exists()
    assert (WORKSPACE / "config" / "db" / "starrocks.yaml").exists()

    rc, output = SDPSession(JOBS_SETUP_PG_SR).run_interactive("db setup-jobs")
    assert rc == 0, f"db setup-jobs failed:\n{output}"
    jobs_yaml = WORKSPACE / "config" / "jobs" / "config.local.yaml"
    assert jobs_yaml.exists()
    jobs_text = jobs_yaml.read_text()
    assert "pg_main" in jobs_text, (
        f"pg_main target missing from {jobs_yaml}:\n{jobs_text}"
    )
    assert "sr_main" in jobs_text, (
        f"sr_main target missing from {jobs_yaml}:\n{jobs_text}"
    )

    # Decline both confirmations: data path was never created (no
    # `db start`), so no chown / rmtree work happens regardless. The
    # warning text fires before the confirmations either way; declining
    # leaves the rest of the configuration intact, which keeps the test
    # focused on H6 (warning shape) without dragging in cleanup paths.
    result = run_sdp("db unsetup --db postgres", input_text="n\n")
    assert result.returncode == 0, (
        f"db unsetup --db postgres exit code unexpected:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    combined = result.stdout + result.stderr
    assert "[WARN] Jobs scheduler has target(s) pointing at PostgreSQL" in combined, (
        f"orphaned-target warning header missing from output:\n{combined}"
    )
    assert "pg_main" in combined, (
        f"orphaned target name 'pg_main' missing from warning:\n{combined}"
    )
    # Recovery hint must name at least one of the unsetup-jobs / setup-jobs
    # remediation commands.
    assert ("unsetup-jobs" in combined) or ("setup-jobs" in combined), (
        f"recovery hint substring missing from warning:\n{combined}"
    )

    # Configuration still intact (we declined).
    assert (WORKSPACE / "config" / "db" / "postgres.yaml").exists(), (
        "postgres.yaml should still exist after declined unsetup"
    )
    assert (WORKSPACE / "config" / "jobs" / "config.local.yaml").exists()
