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

# --- Auth migration for existing databases ---
if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ]; then
    # Check if this is an existing database (not first init)
    if [ -f "/var/lib/postgresql/data/PG_VERSION" ]; then
        echo '[CONFIG] Auth enabled on existing database — setting password'
        # Start postgres temporarily with trust auth on a different port to set password.
        # Uses port 54321 and localhost-only to avoid tripping the healthcheck.
        AUTH_INIT_PORT=54321
        su postgres -c "pg_ctl start -D /var/lib/postgresql/data \
            -o \"-c hba_file=$CFG/pg_hba.conf -c port=$AUTH_INIT_PORT -c listen_addresses=127.0.0.1\" \
            -w -l /tmp/pg_auth_init.log"
        su postgres -c "psql -p $AUTH_INIT_PORT -c \
            \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}'\""
        su postgres -c "pg_ctl stop -D /var/lib/postgresql/data -w"
        echo '[CONFIG] Password set successfully'
    fi
fi

# --- Read-only user sync (every start) ---
RO_CRED_FILE="/data/database/.ro_credentials"
if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ] && [ -f "$RO_CRED_FILE" ] && [ -f "/var/lib/postgresql/data/PG_VERSION" ]; then
    RO_USER=$(cut -d: -f1 "$RO_CRED_FILE")
    RO_PWD=$(cut -d: -f2- "$RO_CRED_FILE")
    echo "[CONFIG] Syncing read-only user: $RO_USER"
    RO_INIT_PORT=54321
    su postgres -c "pg_ctl start -D /var/lib/postgresql/data \
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
    su postgres -c "pg_ctl stop -D /var/lib/postgresql/data -w"
    echo '[CONFIG] Read-only user ready'
fi

# --- Start PostgreSQL ---
chown -R postgres:postgres /var/lib/postgresql
exec docker-entrypoint.sh postgres \
    -c config_file=$PG_CONF \
    -c hba_file=$HBA_CONF \
    -c port="${POSTGRES_PORT:-5432}"
