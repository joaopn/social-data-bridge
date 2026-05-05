#!/bin/bash
# Create read-only user on fresh database init if credentials exist.
# Runs once via PostgreSQL's docker-entrypoint initdb mechanism.
#
# Reads the RO password from $SDP_RO_PASSWORD (exported by the entrypoint
# wrapper after reading the host-owned .ro_credentials file as root). The
# RO username comes from $POSTGRES_RO_USER (mirrored from the yaml via .env).

if [ -z "${SDP_RO_PASSWORD:-}" ] || [ -z "${POSTGRES_RO_USER:-}" ]; then
    echo '[INITDB] No RO password/username in env — skipping RO user creation'
    exit 0
fi

RO_USER="$POSTGRES_RO_USER"
RO_PWD="$SDP_RO_PASSWORD"

echo "[INITDB] Creating read-only user: $RO_USER"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE $RO_USER LOGIN PASSWORD '$RO_PWD';
    GRANT pg_read_all_data TO $RO_USER;
EOSQL
echo '[INITDB] Read-only user created'
