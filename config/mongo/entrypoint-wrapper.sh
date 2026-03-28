#!/bin/bash
set -e

# MongoDB entrypoint wrapper for Social Data Pipeline.
# Delegates to the official docker-entrypoint.sh for user creation on fresh DBs.
# Handles auth migration for existing databases.
# Falls back to standard mongod when auth is not enabled.

# JSON-escape a string value (same approach as official mongo docker-entrypoint)
_js_escape() {
    jq --null-input --arg 'str' "$1" '$str'
}

MONGOD_ARGS=(mongod --config /etc/mongo/config/mongod.conf --wiredTigerCacheSizeGB "${MONGO_CACHE_SIZE_GB:-2}")

if [ "${MONGO_AUTH_ENABLED:-}" = "true" ]; then
    # Handle migration: existing database that hasn't been initialized with auth
    # The official entrypoint skips init when data files exist, so we must
    # handle this case ourselves before delegating.
    if [ -e "/data/db/WiredTiger" ] && [ ! -f "/data/db/.sdb_auth_initialized" ]; then
        echo '[CONFIG] Auth enabled — migrating existing database'

        # Use the official docker-entrypoint.sh to create the admin user.
        # It detects data files and skips init, so we need to temporarily
        # remove the data marker to trick it into running init... EXCEPT
        # the official entrypoint checks multiple files. Instead, create
        # users via the localhost exception: start WITH --auth, connect
        # locally, create the first user (localhost exception allows this).
        # Use a different port for the temporary start to avoid tripping the healthcheck
        AUTH_INIT_PORT=27018
        mongod --fork --logpath /tmp/mongod_auth_init.log \
            --config /etc/mongo/config/mongod.conf \
            --bind_ip 127.0.0.1 \
            --port $AUTH_INIT_PORT \
            --auth \
            --wiredTigerCacheSizeGB "${MONGO_CACHE_SIZE_GB:-2}" \
        || { echo '[ERROR] mongod fork failed — log:'; cat /tmp/mongod_auth_init.log; exit 1; }

        # Wait for mongod
        for i in $(seq 1 30); do
            if mongosh --host 127.0.0.1 --port $AUTH_INIT_PORT --quiet --eval 'quit(0)' >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done

        # Localhost exception allows creating the first user without auth
        ADMIN_USER="${MONGO_ADMIN_USER:-admin}"
        ADMIN_PWD="${MONGO_ADMIN_PASSWORD}"
        mongosh --host 127.0.0.1 --port $AUTH_INIT_PORT --quiet admin <<EOJS
db.createUser({
    user: $(_js_escape "$ADMIN_USER"),
    pwd: $(_js_escape "$ADMIN_PWD"),
    roles: [ { role: 'root', db: 'admin' } ]
})
EOJS
        echo '[CONFIG] Admin user created (migration)'

        mongod --shutdown --dbpath /data/db 2>/dev/null || true
        sleep 2
        touch /data/db/.sdb_auth_initialized
        echo '[CONFIG] Migration complete'
    fi

    # --- Read-only user sync (every start of existing DB) ---
    RO_CRED_FILE="/data/mongo/.ro_credentials"
    if [ -e "/data/db/WiredTiger" ] && [ -f "$RO_CRED_FILE" ]; then
        RO_USER=$(cut -d: -f1 "$RO_CRED_FILE")
        RO_PWD=$(cut -d: -f2- "$RO_CRED_FILE")
        ADMIN_USER="${MONGO_ADMIN_USER:-admin}"
        ADMIN_PWD="${MONGO_ADMIN_PASSWORD}"
        echo "[CONFIG] Syncing read-only user: $RO_USER"
        RO_INIT_PORT=27018
        mongod --fork --logpath /tmp/mongod_ro_init.log \
            --config /etc/mongo/config/mongod.conf \
            --bind_ip 127.0.0.1 \
            --port $RO_INIT_PORT \
            --auth \
            --wiredTigerCacheSizeGB "${MONGO_CACHE_SIZE_GB:-2}" \
        || { echo '[ERROR] mongod fork failed — log:'; cat /tmp/mongod_ro_init.log; exit 1; }
        for i in $(seq 1 30); do
            if mongosh --host 127.0.0.1 --port $RO_INIT_PORT --quiet --eval 'quit(0)' >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        mongosh --host 127.0.0.1 --port $RO_INIT_PORT --quiet \
            -u "$ADMIN_USER" -p "$ADMIN_PWD" --authenticationDatabase admin admin <<EOJS
try {
    db.createUser({
        user: $(_js_escape "$RO_USER"),
        pwd: $(_js_escape "$RO_PWD"),
        roles: [{role: 'readAnyDatabase', db: 'admin'}]
    });
    print('[CONFIG] Created read-only user: $RO_USER');
} catch (e) {
    if (e.codeName === 'DuplicateKey' || e.code === 51003) {
        db.updateUser($(_js_escape "$RO_USER"), { pwd: $(_js_escape "$RO_PWD") });
        print('[CONFIG] Read-only user password synced: $RO_USER');
    } else {
        throw e;
    }
}
EOJS
        mongod --shutdown --dbpath /data/db 2>/dev/null || true
        sleep 2
        echo '[CONFIG] Read-only user ready'
    fi

    # For fresh DBs, the official docker-entrypoint.sh handles everything:
    # it detects MONGO_INITDB_ROOT_USERNAME/PASSWORD, creates the user,
    # and adds --auth automatically. We just need to mark it done afterward.
    if [ ! -e "/data/db/WiredTiger" ] && [ ! -f "/data/db/.sdb_auth_initialized" ]; then
        # Write post-init scripts for RO user and marker
        mkdir -p /docker-entrypoint-initdb.d
        if [ -f "$RO_CRED_FILE" ]; then
            RO_USER=$(cut -d: -f1 "$RO_CRED_FILE")
            RO_PWD=$(cut -d: -f2- "$RO_CRED_FILE")
            cat > /docker-entrypoint-initdb.d/01-sdp-ro-user.js <<EOJS
db = db.getSiblingDB('admin');
try {
    db.createUser({
        user: $(_js_escape "$RO_USER"),
        pwd: $(_js_escape "$RO_PWD"),
        roles: [{role: 'readAnyDatabase', db: 'admin'}]
    });
    print('[CONFIG] Read-only user created');
} catch (e) {
    print('[CONFIG] Warning: RO user: ' + e.message);
}
EOJS
        fi
        cat > /docker-entrypoint-initdb.d/99-sdp-marker.sh <<'EOSH'
#!/bin/bash
touch /data/db/.sdb_auth_initialized
echo '[CONFIG] Auth initialization complete'
EOSH
        chmod +x /docker-entrypoint-initdb.d/99-sdp-marker.sh
    fi

    # Delegate to official docker-entrypoint.sh
    exec /usr/local/bin/docker-entrypoint.sh "${MONGOD_ARGS[@]}"
else
    # No auth — start mongod directly
    exec "${MONGOD_ARGS[@]}"
fi
