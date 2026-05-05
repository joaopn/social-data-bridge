#!/bin/bash
set -e

# --- Config file selection ---
CFG=/etc/postgresql/config
PG_CONF=$CFG/postgresql.conf
[ -f $CFG/postgresql.local.conf ] && PG_CONF=$CFG/postgresql.local.conf && \
  echo '[CONFIG] Using local override: postgresql.local.conf'
HBA_CONF=$CFG/pg_hba.conf
[ -f $CFG/pg_hba.local.conf ] && HBA_CONF=$CFG/pg_hba.local.conf && \
  echo '[CONFIG] Using local override: pg_hba.local.conf'

# --- Tablespace directory permissions ---
if [ -d /data/tablespace ]; then
    for dir in /data/tablespace/*/; do
        [ -d "$dir" ] && chown postgres:postgres "$dir"
    done
fi

# --- Export directory permissions ---
[ -d /export ] && chown -R postgres:postgres /export

# --- RO credentials passthrough ---
# The credentials file is created on the host (chmod 600, host uid) and stays
# host-owned. The wrapper runs as root in the container, so it can read the
# file via the bind mount and pass the password to downstream init scripts
# (which run as the postgres user) via an env var. This avoids chowning the
# file to a container UID, which would make the host unable to read or
# refresh it on subsequent setup/recover-password runs.
RO_CRED_FILE="/data/database/.ro_credentials"
if [ -f "$RO_CRED_FILE" ]; then
    SDP_RO_PASSWORD=$(cat "$RO_CRED_FILE")
    export SDP_RO_PASSWORD
fi

# --- PostgreSQL 18 PGDATA path ---
PGDATA=/var/lib/postgresql/18/docker

# --- Auth migration for existing databases ---
if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ]; then
    # Check if this is an existing database (not first init)
    if [ -f "$PGDATA/PG_VERSION" ]; then
        echo '[CONFIG] Auth enabled on existing database — setting password'
        # Start postgres temporarily with trust auth on a different port to set password.
        # Uses port 54321 and localhost-only to avoid tripping the healthcheck.
        AUTH_INIT_PORT=54321
        su postgres -c "pg_ctl start -D $PGDATA \
            -o \"-c hba_file=$CFG/pg_hba.conf -c port=$AUTH_INIT_PORT -c listen_addresses=127.0.0.1\" \
            -w -l /tmp/pg_auth_init.log"
        su postgres -c "psql -p $AUTH_INIT_PORT -c \
            \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}'\""
        su postgres -c "pg_ctl stop -D $PGDATA -w"
        echo '[CONFIG] Password set successfully'
    fi
fi

# --- Read-only user sync (every start) ---
if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ] && [ -n "${SDP_RO_PASSWORD:-}" ] \
        && [ -n "${POSTGRES_RO_USER:-}" ] && [ -f "$PGDATA/PG_VERSION" ]; then
    RO_USER="$POSTGRES_RO_USER"
    RO_PWD="$SDP_RO_PASSWORD"
    echo "[CONFIG] Syncing read-only user: $RO_USER"
    RO_INIT_PORT=54321
    su postgres -c "pg_ctl start -D $PGDATA \
        -o \"-c hba_file=$CFG/pg_hba.conf -c port=$RO_INIT_PORT -c listen_addresses=127.0.0.1\" \
        -w -l /tmp/pg_ro_init.log"
    su postgres -c "psql -p $RO_INIT_PORT -c \
        \"DO \\\$\\\$ BEGIN
          IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$RO_USER') THEN
            CREATE ROLE $RO_USER LOGIN PASSWORD '$RO_PWD';
            GRANT pg_read_all_data TO $RO_USER;
            RAISE NOTICE 'Created RO user: %', '$RO_USER';
          ELSE
            ALTER ROLE $RO_USER PASSWORD '$RO_PWD';
            RAISE NOTICE 'RO user password synced: %', '$RO_USER';
          END IF;
        END \\\$\\\$;\""
    su postgres -c "pg_ctl stop -D $PGDATA -w"
    echo '[CONFIG] Read-only user ready'
fi

# --- Start PostgreSQL ---
chown -R postgres:postgres /var/lib/postgresql
exec docker-entrypoint.sh postgres \
    -c config_file=$PG_CONF \
    -c hba_file=$HBA_CONF \
    -c port="${POSTGRES_PORT:-5432}"
