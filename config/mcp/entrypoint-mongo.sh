#!/bin/sh
# Entrypoint for MongoDB MCP server (kiliczsh/mcp-mongo-server).
# Builds the command from environment variables:
#   MCP_MONGODB_URI       - MongoDB connection string (required)
#   MCP_MONGODB_READONLY  - "true" to enable read-only mode (default: true)
#   MCP_PORT              - HTTP port (default: 3000)

set -e

URI="${MCP_MONGODB_URI:?MCP_MONGODB_URI is required}"
PORT="${MCP_PORT:-3000}"
READONLY="${MCP_MONGODB_READONLY:-true}"

ARGS="build/index.js ${URI} --transport http --port ${PORT}"

if [ "${READONLY}" = "true" ]; then
    ARGS="${ARGS} --read-only"
fi

exec node ${ARGS}
