#!/bin/sh
# Jobs service entrypoint.
#
# Runs the FastAPI + MCP + runner process. Admin passwords flow from the
# parent `sdp db start` via the container environment
# (POSTGRES_PASSWORD / STARROCKS_ROOT_PASSWORD) and never touch disk.
#
# umask 0000 keeps result files world-writable so the PostgreSQL and
# StarRocks server containers (running as different uids) can write into
# job folders created by this container.

set -eu
umask 0000

exec python -m social_data_pipeline.jobs
