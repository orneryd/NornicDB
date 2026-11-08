#!/usr/bin/env bash
# Shared environment policy for verification and direct CI coverage entry points.
# Covered Cypher compilation alone can consume most of a hosted runner. Keep
# Go-managed memory below that host budget; this soft GC target is inherited
# by compiler/test children and does not change production server defaults.
# Explicit operator settings (including "off") always win.
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  export GOMEMLIMIT="${GOMEMLIMIT:-3GiB}"
fi
