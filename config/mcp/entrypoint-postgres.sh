#!/bin/sh
# Entrypoint for PostgreSQL MCP server (crystaldba/postgres-mcp).
# Reads MCP credentials from the data volume when auth is enabled,
# then delegates to the default entrypoint.
#
# Environment:
#   POSTGRES_MCP_USER  - MCP user (triggers credential file lookup)
#   DATABASE_URI       - Fallback connection string (when no auth)
#   POSTGRES_PORT      - PostgreSQL port (default: 5432)
#   DB_NAME            - Database name (default: datasets)

set -e

if [ -n "${POSTGRES_MCP_USER:-}" ]; then
    # Read password from credentials file in mounted data volume.
    # File format: single-line {password}\n (chmod 600). Username is
    # authoritative in config/db/postgres.yaml, mirrored to POSTGRES_MCP_USER.
    CRED_FILE="/data/database/.ro_credentials"
    if [ -f "$CRED_FILE" ]; then
        MCP_PASSWORD=$(cat "$CRED_FILE")
    fi

    if [ -n "${MCP_PASSWORD:-}" ]; then
        export DATABASE_URI="postgresql://${POSTGRES_MCP_USER}:${MCP_PASSWORD}@postgres:${POSTGRES_PORT:-5432}/${DB_NAME:-datasets}"
    else
        echo "[ERROR] POSTGRES_MCP_USER set but no password found (checked $CRED_FILE)"
        exit 1
    fi
fi

# Delegate to the default image entrypoint
exec /app/docker-entrypoint.sh postgres-mcp "$@"
