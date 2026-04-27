"""E2E: PostgreSQL authentication — fresh install + migration path.

Two scenarios:

  fresh    sdp db setup (auth=y) → start → admin pw works, RO pw works,
           unauth rejected.

  migrate  sdp db setup (auth=n) → start → stop → sdp db setup (auth=y) →
           start → entrypoint-wrapper trust-auth swap migrates the existing
           cluster → same auth assertions as fresh install.

Migration flow exercises [config/postgres/entrypoint-wrapper.sh:33-48]
(the `[ -f $PGDATA/PG_VERSION ]` branch). pg_hba.local.conf forces
scram-sha-256 for all TCP connections, so unauth `localhost` connections
must fail with auth error.
"""

import psycopg
import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.db import pg_connect, pg_query_scalar, read_ro_credentials


ADMIN_PASSWORD = "TestAdmin!Pw0"

# PG-only setup, no auth.
DB_SETUP_NO_AUTH = {
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
    "db_auth": "",            # no
    "db_write_files": "",
}

# PG-only setup, auth enabled, RO user with auto-generated password.
# db_password / db_password_confirm are the getpass tags emitted by
# ask_password; SDPSession matches them via the [tag] prefix.
DB_SETUP_AUTH = {
    **DB_SETUP_NO_AUTH,
    "db_auth": "y",
    "db_password": ADMIN_PASSWORD,
    "db_password_confirm": ADMIN_PASSWORD,
    "db_ro_user": "",          # yes (default)
    "db_ro_username": "",
    "db_ro_auto_password": "", # yes (default)
}

PG_DATA_DIR = WORKSPACE / "data" / "database" / "postgres"


def _start_pg_with_auth():
    """Start the postgres service feeding the admin password via stdin.

    Without SDP_TAGGED_MODE the prompt is plain `Database admin password: `;
    getpass falls back to sys.stdin when the controlling tty is a pipe.
    """
    result = run_sdp(
        "db start postgres",
        input_text=f"{ADMIN_PASSWORD}\n",
    )
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")


def _assert_auth_enforced():
    """Admin works, RO works, unauth rejected."""
    # Admin connects with password.
    conn = pg_connect(password=ADMIN_PASSWORD)
    try:
        ver = pg_query_scalar(conn, "SHOW server_version_num")
        assert ver and int(ver) >= 180000, f"unexpected server version: {ver}"
    finally:
        conn.close()

    # Read RO credentials written by setup; connect as RO user.
    ro_user, ro_pass = read_ro_credentials(PG_DATA_DIR)
    assert ro_user, "RO username not found in .ro_credentials"
    assert ro_pass, "RO password not found in .ro_credentials"
    conn = pg_connect(user=ro_user, password=ro_pass)
    try:
        # pg_read_all_data role lets RO read system catalog.
        n = pg_query_scalar(conn, "SELECT count(*) FROM pg_database")
        assert n and n >= 1
    finally:
        conn.close()

    # Unauth connection must be rejected by pg_hba scram-sha-256 rule.
    with pytest.raises(psycopg.OperationalError) as excinfo:
        # No password param → libpq sends none → scram-sha-256 demand fails.
        pg_connect()
    msg = str(excinfo.value).lower()
    assert "password" in msg or "authentication" in msg, (
        f"expected auth-failure message, got: {excinfo.value}"
    )


def test_postgres_auth_fresh_install(workspace):
    """Fresh PG init with auth enabled → admin/RO/unauth checks pass."""
    session = SDPSession(DB_SETUP_AUTH)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    _start_pg_with_auth()
    try:
        _assert_auth_enforced()
    finally:
        run_sdp("db stop postgres", input_text=f"{ADMIN_PASSWORD}\n")


def test_postgres_auth_migration(workspace):
    """Existing no-auth cluster → enable auth → entrypoint trust-auth swap migrates it."""
    # 1. Initial no-auth setup + start to create the cluster.
    rc, output = SDPSession(DB_SETUP_NO_AUTH).run_interactive("db setup")
    assert rc == 0, f"db setup (no-auth) failed:\n{output}"

    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start (no-auth) failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    # Sanity: confirm cluster is reachable without password.
    conn = pg_connect()
    try:
        pg_query_scalar(conn, "SELECT 1")
    finally:
        conn.close()

    rc = run_sdp("db stop postgres").returncode
    assert rc == 0, "db stop (no-auth) failed"

    # 2. Reconfigure with auth enabled. db setup re-run picks up existing
    #    pgdata_path; the cluster's PG_VERSION exists, triggering the
    #    migration branch on next start.
    rc, output = SDPSession(DB_SETUP_AUTH).run_interactive("db setup")
    assert rc == 0, f"db setup (auth) failed:\n{output}"

    _start_pg_with_auth()
    try:
        _assert_auth_enforced()
    finally:
        run_sdp("db stop postgres", input_text=f"{ADMIN_PASSWORD}\n")
