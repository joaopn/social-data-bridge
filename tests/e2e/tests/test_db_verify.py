"""E2E: `sdp db verify` exit code + JSON shape against a real workspace.

`db verify` is the operator preflight that scans every drift surface
(auth coherence, cred file mode/owner, container env / health, per-source
mounts, MCP / jobs cross-references). The unit tests pin the per-finding
logic with mocked ctx; this E2E test exercises the real CLI against a
freshly-set-up workspace, including the cred-file probing the unit
tests can't reasonably mock.

Two scenarios on a postgres-only setup, no containers started (verify
is supposed to work without a running DB so it's a useful preflight
*before* `db start`):

1. Right after `db setup` with auth disabled → coherent → exit 0.
2. After deleting the `.ro_credentials` while auth is enabled → drift
   surfaces, ``--json`` payload has the expected shape, exit code is 1.
"""

import json

from tests.e2e.helpers.sdp import SDPSession, run_sdp, WORKSPACE


PG_NO_AUTH_SETUP = {
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
    "db_auth": "",                # default = no
    "db_write_files": "",
}

PG_AUTH_SETUP = {
    **PG_NO_AUTH_SETUP,
    "db_auth": "y",
    "db_password": "verifyTestPw1!",
    "db_password_confirm": "verifyTestPw1!",
    "db_ro_user": "",            # default = yes
    "db_ro_username": "",
    "db_ro_auto_password": "",   # default = yes (auto-generate)
}


def test_db_verify_clean_exits_zero(workspace):
    """No-auth postgres install with no sources → verify is clean, exit 0."""
    rc, output = SDPSession(PG_NO_AUTH_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    result = run_sdp("db verify")
    assert result.returncode == 0, (
        f"verify on clean install failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "OK" in result.stdout

    # JSON mode mirrors the same exit code and structure.
    result = run_sdp("db verify --json")
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["exit_code"] == 0
    assert "postgres" in payload["results"]
    assert payload["results"]["postgres"]["ok"] is True


def test_db_verify_drift_exits_one(workspace):
    """Delete .ro_credentials while auth is enabled → verify exits 1 with shape."""
    rc, output = SDPSession(PG_AUTH_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    cred_file = WORKSPACE / "data" / "database" / "postgres" / ".ro_credentials"
    assert cred_file.exists(), ".ro_credentials should exist after setup with auth"
    cred_file.unlink()

    # Text mode: exit 1, message names the missing file and the recovery line.
    result = run_sdp("db verify")
    assert result.returncode == 1, (
        f"expected exit 1 from missing creds, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert ".ro_credentials" in result.stdout
    assert "recover-password --regenerate-ro" in result.stdout

    # JSON mode: same exit code, payload has a creds finding for postgres.
    result = run_sdp("db verify --json")
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["exit_code"] == 1
    pg = payload["results"]["postgres"]
    assert pg["ok"] is False
    cats = [f["category"] for f in pg["findings"]]
    assert "creds" in cats
