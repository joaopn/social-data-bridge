#!/bin/bash
set -e

# --- Start StarRocks (original entrypoint) in background ---
# The allin1-ubuntu image default: ENTRYPOINT [tini --] CMD [./entrypoint.sh]
# Since we override entrypoint, docker-compose sets init: true for tini.
/data/deploy/entrypoint.sh &
SR_PID=$!

cleanup() {
    echo "[sdp] Shutting down StarRocks..."
    kill -TERM "$SR_PID" 2>/dev/null
    wait "$SR_PID" 2>/dev/null
}
trap cleanup TERM INT

# --- Wait for StarRocks FE to be ready ---
echo "[sdp] Waiting for StarRocks FE to be ready..."
MAX_RETRIES=60
RETRY_INTERVAL=5
for i in $(seq 1 $MAX_RETRIES); do
    if mysql -h 127.0.0.1 -P 9030 -u root --skip-password -e "SELECT 1" >/dev/null 2>&1; then
        echo "[sdp] StarRocks FE is ready"
        break
    fi
    if ! kill -0 "$SR_PID" 2>/dev/null; then
        echo "[sdp] ERROR: StarRocks process exited unexpectedly"
        exit 1
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "[sdp] ERROR: StarRocks FE failed to start within $((MAX_RETRIES * RETRY_INTERVAL))s"
        exit 1
    fi
    sleep $RETRY_INTERVAL
done

# --- Wait for StarRocks BE to register ---
echo "[sdp] Waiting for Backend to register..."
for i in $(seq 1 $MAX_RETRIES); do
    if mysql -h 127.0.0.1 -P 9030 -u root --skip-password -e "SHOW BACKENDS\G" 2>/dev/null | grep -q "Alive: true"; then
        echo "[sdp] Backend is registered and alive"
        break
    fi
    if ! kill -0 "$SR_PID" 2>/dev/null; then
        echo "[sdp] ERROR: StarRocks process exited unexpectedly"
        exit 1
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "[sdp] ERROR: StarRocks BE failed to register within $((MAX_RETRIES * RETRY_INTERVAL))s"
        exit 1
    fi
    sleep $RETRY_INTERVAL
done

# --- Set root password if provided ---
if [ -n "$STARROCKS_ROOT_PASSWORD" ]; then
    # Escape single quotes in password for SQL
    ESCAPED_PW="${STARROCKS_ROOT_PASSWORD//\'/\'\'}"

    if mysql -h 127.0.0.1 -P 9030 -u root --skip-password -e "SELECT 1" >/dev/null 2>&1; then
        # Passwordless access works — fresh install or password not yet set
        echo "[sdp] Setting root password..."
        mysql -h 127.0.0.1 -P 9030 -u root --skip-password -e \
            "ALTER USER root IDENTIFIED BY '${ESCAPED_PW}'" 2>/dev/null
        echo "[sdp] Root password set"
    fi
    MYSQL_AUTH="-u root -p${STARROCKS_ROOT_PASSWORD}"
else
    MYSQL_AUTH="-u root --skip-password"
fi

# --- Create/sync read-only user from .ro_credentials ---
RO_CREDS_FILE="/data/starrocks/.ro_credentials"
if [ -f "$RO_CREDS_FILE" ]; then
    RO_USER=$(cut -d: -f1 "$RO_CREDS_FILE")
    RO_PASS=$(cut -d: -f2- "$RO_CREDS_FILE")

    echo "[sdp] Syncing read-only user: $RO_USER"
    # shellcheck disable=SC2086
    mysql -h 127.0.0.1 -P 9030 $MYSQL_AUTH -e "
        CREATE USER IF NOT EXISTS '${RO_USER}' IDENTIFIED BY '${RO_PASS}';
        ALTER USER '${RO_USER}' IDENTIFIED BY '${RO_PASS}';
        CREATE ROLE IF NOT EXISTS 'sdp_readonly';
        GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE 'sdp_readonly';
        GRANT 'sdp_readonly' TO '${RO_USER}';
        SET DEFAULT ROLE 'sdp_readonly' TO '${RO_USER}';
    " 2>/dev/null && echo "[sdp] Read-only user synced: $RO_USER" \
                  || echo "[sdp] WARNING: Failed to sync read-only user"
fi

echo "[sdp] Auth setup complete"

# --- Wait for StarRocks process ---
wait "$SR_PID"
