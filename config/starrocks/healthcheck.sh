#!/bin/bash
# StarRocks container healthcheck.
#
# Two-stage probe:
#   1. Liveness: connect via mysql client (passwordless first, fallback
#      with STARROCKS_ROOT_PASSWORD), run SHOW BACKENDS, ensure at least
#      one BE is `Alive: true`. Auth state varies across restart vs fresh
#      install — the dual mysql attempt covers both.
#   2. RO probe (only when AUTH_ENABLED + RO_USER configured + cred file
#      present): connect as the RO user with the password from
#      /data/starrocks/.ro_credentials and run `SELECT 1`.
#
# The RO probe makes `service_healthy` reflect actual auth state, so
# `depends_on: service_healthy` on starrocks-mcp refuses to green-light an
# MCP boot against a DB whose RO user is missing or has the wrong password.

set -e

# Liveness: BE alive via either passwordless or password-auth root.
(mysql -h 127.0.0.1 -P 9030 -u root --skip-password \
        -e 'SHOW BACKENDS\G' 2>/dev/null \
    || mysql -h 127.0.0.1 -P 9030 -u root -p"${STARROCKS_ROOT_PASSWORD:-}" \
        -e 'SHOW BACKENDS\G' 2>/dev/null) \
    | grep -q 'Alive: true'

if [ "${STARROCKS_AUTH_ENABLED:-}" = "true" ] \
        && [ -n "${STARROCKS_RO_USER:-}" ] \
        && [ -f /data/starrocks/.ro_credentials ]; then
    RO_PWD=$(cat /data/starrocks/.ro_credentials)
    mysql -h 127.0.0.1 -P 9030 -u "$STARROCKS_RO_USER" -p"$RO_PWD" \
        -e 'SELECT 1' >/dev/null 2>&1
fi
