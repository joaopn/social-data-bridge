#!/bin/bash
# Create read-only user on fresh database init if credentials exist.
# Runs once via PostgreSQL's docker-entrypoint initdb mechanism.
#
# Reads the RO password from $SDP_RO_PASSWORD (exported by the entrypoint
# wrapper after reading the host-owned .ro_credentials file as root). The
# RO username comes from $POSTGRES_RO_USER (mirrored from the yaml via .env).

# RO user is optional under auth — when the username is empty, no RO user
# was configured at setup time, so skip silently. When it IS set under auth,
# the password MUST be present — otherwise that's setup/state drift.
if [ -z "${POSTGRES_RO_USER:-}" ]; then
    echo '[INITDB] No RO username in env — skipping RO user creation'
    exit 0
fi

if [ -z "${SDP_RO_PASSWORD:-}" ]; then
    if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ]; then
        echo "[ERROR] POSTGRES_RO_USER='${POSTGRES_RO_USER}' but no password in /data/database/.ro_credentials. Re-run 'sdp db setup --add postgres' or 'sdp db recover-password' to regenerate." >&2
        exit 1
    fi
    echo '[INITDB] No RO password in env — skipping RO user creation'
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
