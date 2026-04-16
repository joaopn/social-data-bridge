#!/bin/sh
# Entrypoint for StarRocks MCP server (mcp-server-starrocks).
# Builds the command from environment variables:
#   STARROCKS_MCP_USER      - MCP user (when auth enabled, reads password from credentials file)
#   STARROCKS_HOST           - StarRocks host (default: starrocks)
#   STARROCKS_PORT           - StarRocks MySQL port (default: 9030)
#   MCP_PORT                 - HTTP port (default: 9000)
#
# Read-only enforcement: database-level only, via the RO user's sdp_readonly role
# (SELECT-only grants). The official mcp-server-starrocks has no application-level
# read-only flag — write operations will fail with SQL permission errors.

set -e

PORT="${MCP_PORT:-9000}"

# Build credentials: use credentials file if MCP user is set, otherwise root with no password
if [ -n "${STARROCKS_MCP_USER:-}" ]; then
    # Read password from credentials file in mounted data volume
    CRED_FILE="/data/starrocks/.ro_credentials"
    if [ -f "$CRED_FILE" ]; then
        STARROCKS_MCP_PASSWORD=$(cut -d: -f2- "$CRED_FILE")
    fi

    if [ -n "${STARROCKS_MCP_PASSWORD:-}" ]; then
        export STARROCKS_USER="${STARROCKS_MCP_USER}"
        export STARROCKS_PASSWORD="${STARROCKS_MCP_PASSWORD}"
    else
        echo "[ERROR] STARROCKS_MCP_USER set but no password found (checked $CRED_FILE)"
        exit 1
    fi
else
    export STARROCKS_USER="root"
    export STARROCKS_PASSWORD=""
fi

exec mcp-server-starrocks --mode streamable-http --host 0.0.0.0 --port "${PORT}"
