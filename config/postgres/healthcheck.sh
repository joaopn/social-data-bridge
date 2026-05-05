#!/bin/bash
# PostgreSQL container healthcheck.
#
# Two-stage probe:
#   1. Liveness: pg_isready against the admin role / database.
#   2. RO probe (only when AUTH_ENABLED + RO_USER configured + cred file
#      present): connect as the RO user with the password from
#      /data/database/.ro_credentials and run `SELECT 1`.
#
# The RO probe makes `service_healthy` reflect actual auth state, so
# `depends_on: service_healthy` on the MCP service refuses to green-light
# an MCP boot against a DB whose RO user is missing or has the wrong
# password. Keeps the healthcheck honest; avoids "DB up healthy, MCP
# silently broken" failure shapes.

set -e

PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_DB:-${DB_NAME:-datasets}}"

pg_isready -U postgres -d "$DB" -p "$PORT"

if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ] \
        && [ -n "${POSTGRES_RO_USER:-}" ] \
        && [ -f /data/database/.ro_credentials ]; then
    PGPASSWORD=$(cat /data/database/.ro_credentials) \
        psql -h 127.0.0.1 -p "$PORT" -U "$POSTGRES_RO_USER" -d "$DB" \
        -c 'SELECT 1' >/dev/null
fi
