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

# The Go command reads NORNICDB_* directly. Do not translate environment values
# into explicit flags here: explicit flags outrank YAML and protocol-specific
# variables such as NORNICDB_HTTP_ADDRESS and NORNICDB_HTTPS_PORT.
if [ "${NORNICDB_NO_AUTH:-false}" = "true" ]; then
    printf '%s\n' '{"level":"ERROR","event_id":"security.insecure_no_auth.enabled","component":"entrypoint","message":"Authentication explicitly disabled for container startup"}' >&2
    exec "${NORNICDB_BIN:-/app/nornicdb}" serve --no-auth "$@"
fi

# NORNICDB_BIN lets test harnesses (and unusual layouts) substitute a
# different binary path. Production images install at /app/nornicdb, so
# the default keeps the original behavior intact.
exec "${NORNICDB_BIN:-/app/nornicdb}" serve "$@"
