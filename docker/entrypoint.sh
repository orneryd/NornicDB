#!/bin/sh
# NornicDB Docker Entrypoint

set -e

# Check for GPU availability
if [ -f "/usr/bin/nvidia-smi" ] || [ -f "/usr/local/cuda/bin/nvidia-smi" ]; then
    if nvidia-smi >/dev/null 2>&1; then
        echo "ÃƒÂ¢Ã…â€œÃ¢â‚¬Å“ CUDA GPU detected"
        export LD_PRELOAD=""
    else
        echo "ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â  CUDA libraries found but no GPU detected - disabling CUDA"
        # Prevent CUDA library loading by unsetting CUDA variables
        unset CUDA_VISIBLE_DEVICES
        unset NVIDIA_VISIBLE_DEVICES
        # Set flag to disable local embeddings if built with CUDA
        export NORNICDB_EMBEDDING_PROVIDER="${NORNICDB_EMBEDDING_PROVIDER:-openai}"
    fi
else
    echo "ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â  No CUDA libraries detected - running in CPU mode"
fi

# Build command line args from environment
ARGS="serve"
ARGS="$ARGS --data-dir=${NORNICDB_DATA_DIR:-/data}"
ARGS="$ARGS --http-port=${NORNICDB_HTTP_PORT:-7474}"
ARGS="$ARGS --bolt-port=${NORNICDB_BOLT_PORT:-7687}"

# IMPORTANT: In Docker, we must bind to 0.0.0.0 to accept external connections
# The default changed to 127.0.0.1 for security (localhost-only outside containers)
ARGS="$ARGS --address=${NORNICDB_ADDRESS:-0.0.0.0}"

[ "${NORNICDB_NO_AUTH:-false}" = "true" ] && ARGS="$ARGS --no-auth"

# Embedding config
[ -n "$NORNICDB_EMBEDDING_PROVIDER" ] && ARGS="$ARGS --embedding-provider=$NORNICDB_EMBEDDING_PROVIDER"
[ -n "$NORNICDB_EMBEDDING_URL" ] && ARGS="$ARGS --embedding-url=$NORNICDB_EMBEDDING_URL"
[ -n "$NORNICDB_EMBEDDING_MODEL" ] && ARGS="$ARGS --embedding-model=$NORNICDB_EMBEDDING_MODEL"
[ -n "$NORNICDB_EMBEDDING_DIM" ] && ARGS="$ARGS --embedding-dim=$NORNICDB_EMBEDDING_DIM"
[ -n "$NORNICDB_EMBEDDING_DIMENSIONS" ] && ARGS="$ARGS --embedding-dim=$NORNICDB_EMBEDDING_DIMENSIONS"
[ -n "$NORNICDB_EMBEDDING_GPU_LAYERS" ] && ARGS="$ARGS --embedding-gpu-layers=$NORNICDB_EMBEDDING_GPU_LAYERS"

# Base path for reverse proxy deployment
if [ -n "$NORNICDB_BASE_PATH" ]; then
    ARGS="$ARGS --base-path=$NORNICDB_BASE_PATH"
fi

# Per-DB search index master switches. The Go binary already reads
# NORNICDB_SEARCH_* directly via pkg/config.LoadFromEnv, but propagating
# them as CLI flags makes them visible in `ps`, in container inspect
# output, and in tests that grep --search-* in startup logs. Both paths
# converge on the same cfg.Memory.Search* fields downstream.
[ -n "$NORNICDB_SEARCH_BM25_ENABLED" ] && ARGS="$ARGS --search-bm25-enabled=$NORNICDB_SEARCH_BM25_ENABLED"
[ -n "$NORNICDB_SEARCH_BM25_WARMING" ] && ARGS="$ARGS --search-bm25-warming=$NORNICDB_SEARCH_BM25_WARMING"
[ -n "$NORNICDB_SEARCH_VECTOR_ENABLED" ] && ARGS="$ARGS --search-vector-enabled=$NORNICDB_SEARCH_VECTOR_ENABLED"
[ -n "$NORNICDB_SEARCH_VECTOR_WARMING" ] && ARGS="$ARGS --search-vector-warming=$NORNICDB_SEARCH_VECTOR_WARMING"

# NORNICDB_BIN lets test harnesses (and unusual layouts) substitute a
# different binary path. Production images install at /app/nornicdb, so
# the default keeps the original behavior intact.
exec "${NORNICDB_BIN:-/app/nornicdb}" $ARGS "$@"
