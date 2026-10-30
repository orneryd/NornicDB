#!/bin/sh

set -eu

is_true() {
    case "$1" in
        1|t|T|true|TRUE|True) return 0 ;;
        *) return 1 ;;
    esac
}

if ! is_true "${NORNICDB_HTTP_ENABLED:-true}" && ! is_true "${NORNICDB_HTTPS_ENABLED:-false}"; then
    exit 0
fi

address="${NORNICDB_HTTP_ADDRESS:-${NORNICDB_ADDRESS:-127.0.0.1}}"
case "$address" in
    0.0.0.0) address=127.0.0.1 ;;
    ::|\[::\]) address='[::1]' ;;
    *:*) address="[$address]" ;;
esac

wget_args="--spider -q"
if is_true "${NORNICDB_HTTPS_ENABLED:-false}"; then
    scheme=https
    port="${NORNICDB_HTTPS_PORT:-7473}"
    wget_args="$wget_args --no-check-certificate"
else
    scheme=http
    port="${NORNICDB_HTTP_PORT:-7474}"
fi

base_path="${NORNICDB_BASE_PATH:-}"
if [ -n "$base_path" ]; then
    case "$base_path" in
        /*) ;;
        *) base_path="/$base_path" ;;
    esac
    base_path="${base_path%/}"
fi

# Certificate trust is checked at server startup; this probe only verifies local liveness.
# shellcheck disable=SC2086
exec wget $wget_args "${scheme}://${address}:${port}${base_path}/health"
