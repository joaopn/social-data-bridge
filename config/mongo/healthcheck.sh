#!/bin/bash
# MongoDB container healthcheck.
#
# Two-stage probe:
#   1. Liveness: mongosh adminCommand("ping") (auth-anonymous; works under
#      both auth-on and auth-off).
#   2. RO probe (only when AUTH_ENABLED + RO_USER configured + cred file
#      present): authenticate as the RO user with the password from
#      /data/mongo/.ro_credentials and ping again.
#
# The RO probe makes `service_healthy` reflect actual auth state, so
# `depends_on: service_healthy` on mongo-mcp refuses to green-light an
# MCP boot against a DB whose RO user is missing or has the wrong
# password. Keeps the healthcheck honest.

set -e

mongosh --quiet --eval 'db.adminCommand("ping")' >/dev/null

if [ "${MONGO_AUTH_ENABLED:-}" = "true" ] \
        && [ -n "${MONGO_RO_USER:-}" ] \
        && [ -f /data/mongo/.ro_credentials ]; then
    RO_PWD=$(cat /data/mongo/.ro_credentials)
    mongosh --quiet \
        -u "$MONGO_RO_USER" -p "$RO_PWD" --authenticationDatabase admin \
        --eval 'db.adminCommand("ping")' >/dev/null
fi
