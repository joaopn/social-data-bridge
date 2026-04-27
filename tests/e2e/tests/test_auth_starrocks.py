"""E2E: StarRocks authentication — fresh install.

Flow:
  sdp db setup (auth=y) → start → assert root authenticates with the password,
  the RO user from .ro_credentials authenticates and has SELECT-only access,
  unauth (no password) connections are rejected.

Migration is not covered here — SR's auth migration in
[config/starrocks/entrypoint-wrapper.sh] (skip-password fall-through that
sets the root password the first time around) differs structurally from
PG's trust-auth swap and is exercised manually.
"""

import mysql.connector
import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.db import sr_connect, sr_query_scalar, read_ro_credentials


ADMIN_PASSWORD = "TestAdmin!Pw0"

DB_SETUP_AUTH = {
    "db_data_path": "",
    "db_databases": "3",          # starrocks
    "db_sr_data_path": "",
    "db_export_path": "",
    "db_sr_port": "",
    "db_sr_fe_http_port": "",
    "db_sr_fe_heap": "",
    "db_sr_mem_limit": "0",
    "db_sr_be_mem": "",
    "db_sr_alter_workers": "",
    "db_sr_multidisk": "",
    "db_auth": "y",
    "db_password": ADMIN_PASSWORD,
    "db_password_confirm": ADMIN_PASSWORD,
    "db_ro_user": "",
    "db_ro_username": "",
    "db_ro_auto_password": "",
    "db_write_files": "",
}

SR_DATA_DIR = WORKSPACE / "data" / "database" / "starrocks"
SR_HEALTH_TIMEOUT = 180


def test_starrocks_auth_fresh_install(workspace):
    """Fresh SR init with auth → root works, RO works, unauth rejected."""
    rc, output = SDPSession(DB_SETUP_AUTH).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    result = run_sdp("db start starrocks", input_text=f"{ADMIN_PASSWORD}\n")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("starrocks", timeout=SR_HEALTH_TIMEOUT)

    try:
        # Admin connects as root with the password set by the entrypoint wrapper.
        conn = sr_connect(password=ADMIN_PASSWORD)
        try:
            v = sr_query_scalar(conn, "SELECT VERSION()")
            assert v, "empty version string"
        finally:
            conn.close()

        # RO user from .ro_credentials. sdp_readonly role grants SELECT on all
        # databases — pick a system DB known to exist on every install.
        ro_user, ro_pass = read_ro_credentials(SR_DATA_DIR)
        assert ro_user, "RO username not found"
        assert ro_pass, "RO password not found"
        conn = sr_connect(user=ro_user, password=ro_pass)
        try:
            n = sr_query_scalar(
                conn,
                "SELECT COUNT(*) FROM information_schema.schemata",
            )
            assert n is not None and n >= 1
        finally:
            conn.close()

        # Unauth connection rejected. mysql.connector raises ProgrammingError
        # (code 1045) when access is denied.
        with pytest.raises(mysql.connector.Error) as excinfo:
            sr_connect()  # no password
        msg = str(excinfo.value).lower()
        assert "access denied" in msg or "1045" in str(excinfo.value), (
            f"expected access-denied, got: {excinfo.value}"
        )
    finally:
        run_sdp("db stop starrocks", input_text=f"{ADMIN_PASSWORD}\n")
