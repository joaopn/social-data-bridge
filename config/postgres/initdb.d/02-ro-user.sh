#!/bin/bash
# Create read-only user on fresh database init if credentials exist.
# Runs once via PostgreSQL's docker-entrypoint initdb mechanism.

RO_CRED_FILE="/data/database/.ro_credentials"
if [ ! -f "$RO_CRED_FILE" ]; then
    echo '[INITDB] No .ro_credentials — skipping RO user creation'
    exit 0
fi

RO_USER=$(cut -d: -f1 "$RO_CRED_FILE")
RO_PWD=$(cut -d: -f2- "$RO_CRED_FILE")

echo "[INITDB] Creating read-only user: $RO_USER"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE $RO_USER LOGIN PASSWORD '$RO_PWD';
    GRANT pg_read_all_data TO $RO_USER;
EOSQL
echo '[INITDB] Read-only user created'
