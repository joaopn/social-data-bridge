"""E2E: MongoDB authentication — fresh install.

Flow:
  sdp db setup (auth=y) → start → assert admin can authenticate, RO user
  authenticates with the password from .ro_credentials, and an unauth
  client is rejected when it tries to read.

Migration is not covered here — Mongo's localhost-exception migration in
[config/mongo/entrypoint-wrapper.sh:20-63] is structurally different from
PG's trust-auth swap and is exercised manually.
"""

import pymongo
import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.db import mongo_connect, read_ro_credentials


ADMIN_PASSWORD = "TestAdmin!Pw0"

DB_SETUP_AUTH = {
    "db_data_path": "",
    "db_databases": "2",          # mongo only
    "db_mongo_data_path": "",
    "db_export_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",        # 1 GB (small for tests)
    "db_mongo_mem_limit": "0",
    "db_mongo_validate": "",
    "db_auth": "y",
    "db_password": ADMIN_PASSWORD,
    "db_password_confirm": ADMIN_PASSWORD,
    "db_ro_user": "",
    "db_ro_username": "",
    "db_ro_auto_password": "",
    "db_write_files": "",
}

MONGO_DATA_DIR = WORKSPACE / "data" / "database" / "mongo"


def test_mongo_auth_fresh_install(workspace):
    """Fresh Mongo init with auth → admin works, RO works, unauth read rejected."""
    rc, output = SDPSession(DB_SETUP_AUTH).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    result = run_sdp("db start mongo", input_text=f"{ADMIN_PASSWORD}\n")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    try:
        # Admin connects with password (root role on admin db).
        client = mongo_connect(username="admin", password=ADMIN_PASSWORD)
        try:
            dbs = client.list_database_names()
            assert "admin" in dbs, f"admin db missing: {dbs}"
        finally:
            client.close()

        # Read-only user from .ro_credentials. readAnyDatabase covers list_database_names.
        ro_user, ro_pass = read_ro_credentials(MONGO_DATA_DIR)
        assert ro_user, "RO username not found"
        assert ro_pass, "RO password not found"
        client = mongo_connect(username=ro_user, password=ro_pass)
        try:
            dbs = client.list_database_names()
            assert isinstance(dbs, list)
        finally:
            client.close()

        # Unauthenticated client must be rejected when it tries to do anything
        # that requires auth. list_database_names is auth-required.
        client = mongo_connect()
        try:
            with pytest.raises(pymongo.errors.OperationFailure) as excinfo:
                client.list_database_names()
            msg = str(excinfo.value).lower()
            assert "auth" in msg or "unauthorized" in msg, (
                f"expected auth-failure message, got: {excinfo.value}"
            )
        finally:
            client.close()
    finally:
        run_sdp("db stop mongo", input_text=f"{ADMIN_PASSWORD}\n")
