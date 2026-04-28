"""E2E: MCP server lifecycle for PostgreSQL and MongoDB.

Bug class: the MCP entrypoint scripts in [config/mcp/] read `.ro_credentials`
from the database data volume and build authenticated connection URIs.
If credential plumbing breaks or the container fails to bind its port, MCP
clients silently can't reach the DB. We verify by starting the parent DB
(which auto-bundles the MCP container per `sdp db start` semantics), then
hitting the MCP HTTP endpoint and asserting it responds.

StarRocks MCP liveness is folded into [test_sr_flow.py](test_sr_flow.py)
to avoid paying SR's ~7-min cold-boot cost a second time.

Both tests use auth + RO user so the entrypoint scripts exercise the
credential-reading code path; an unauthed MCP setup would skip it.
"""

import pytest

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.db import wait_mcp_alive


ADMIN_PASSWORD = "TestAdmin!Pw0"


PG_DB_SETUP = {
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
    "db_auth": "y",
    "db_password": ADMIN_PASSWORD,
    "db_password_confirm": ADMIN_PASSWORD,
    "db_ro_user": "",
    "db_ro_username": "",
    "db_ro_auto_password": "",
    "db_write_files": "",
}

MONGO_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "2",
    "db_mongo_data_path": "",
    "db_export_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",
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


def _pg_mcp_answers():
    return {
        "mcp_pg_enable": "",
        "mcp_pg_port": "",
        "mcp_pg_write_access": "",  # default: no (read-only)
        "mcp_write_files": "",
    }


def _mongo_mcp_answers():
    return {
        "mcp_mongo_enable": "",
        "mcp_mongo_port": "",
        "mcp_mongo_write_access": "",
        "mcp_write_files": "",
    }


@pytest.mark.parametrize(
    "service,db_setup,mcp_answers,url",
    [
        ("postgres", PG_DB_SETUP, _pg_mcp_answers(), "http://localhost:8000/sse"),
        ("mongo", MONGO_DB_SETUP, _mongo_mcp_answers(), "http://localhost:3000/mcp"),
    ],
    ids=["postgres", "mongo"],
)
def test_mcp_lifecycle(workspace, service, db_setup, mcp_answers, url):
    """db setup (auth) → setup-mcp → start → MCP endpoint responds."""
    rc, output = SDPSession(db_setup).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    rc, output = SDPSession(mcp_answers).run_interactive("db setup-mcp")
    assert rc == 0, f"db setup-mcp failed:\n{output}"

    # `sdp db start <service>` auto-bundles the MCP profile alongside the
    # parent DB profile (per sdp db start semantics).
    result = run_sdp(f"db start {service}", input_text=f"{ADMIN_PASSWORD}\n")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy(service)

    try:
        # MCP container has no healthcheck — poll the HTTP port directly.
        wait_mcp_alive(url, timeout=60)
    finally:
        run_sdp(f"db stop {service}", input_text=f"{ADMIN_PASSWORD}\n")
