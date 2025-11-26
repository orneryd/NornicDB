#!/bin/sh
set -e

# NornicDB Docker Entrypoint
# Handles environment variable expansion and startup options

# Build command arguments
CMD_ARGS="serve"
CMD_ARGS="$CMD_ARGS --data-dir ${NORNICDB_DATA_DIR:-/data}"
CMD_ARGS="$CMD_ARGS --http-port ${NORNICDB_HTTP_PORT:-7474}"
CMD_ARGS="$CMD_ARGS --bolt-port ${NORNICDB_BOLT_PORT:-7687}"
CMD_ARGS="$CMD_ARGS --embedding-url ${NORNICDB_EMBEDDING_URL:-http://localhost:11434}"
CMD_ARGS="$CMD_ARGS --embedding-model ${NORNICDB_EMBEDDING_MODEL:-mxbai-embed-large}"
CMD_ARGS="$CMD_ARGS --embedding-dim ${NORNICDB_EMBEDDING_DIM:-1024}"

# Authentication
if [ "${NORNICDB_NO_AUTH:-false}" = "true" ]; then
  CMD_ARGS="$CMD_ARGS --no-auth"
else
  CMD_ARGS="$CMD_ARGS --admin-password ${NORNICDB_ADMIN_PASSWORD:-password}"
fi

# Load export data on startup (if specified)
if [ -n "${NORNICDB_LOAD_EXPORT}" ]; then
  CMD_ARGS="$CMD_ARGS --load-export ${NORNICDB_LOAD_EXPORT}"
fi

echo "🚀 Starting NornicDB..."
echo "   Data directory: ${NORNICDB_DATA_DIR:-/data}"
echo "   HTTP port: ${NORNICDB_HTTP_PORT:-7474}"
echo "   Bolt port: ${NORNICDB_BOLT_PORT:-7687}"

# Execute nornicdb with built arguments
exec /app/nornicdb $CMD_ARGS "$@"
