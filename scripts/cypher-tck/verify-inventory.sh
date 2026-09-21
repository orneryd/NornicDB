#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

go run ./testing/cypher/tck/cmd/inventory \
  -check testing/cypher/tck/testdata/inventory.json
