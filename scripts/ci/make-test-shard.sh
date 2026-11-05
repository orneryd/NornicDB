#!/usr/bin/env bash
# Partition the exact go test ./... package set across independent runners.
set -euo pipefail
shard="${1:?usage: make-test-shard.sh cypher|storage|server|storagecensus|capturecensus|command|other}"
case "$shard" in cypher|storage|server|storagecensus|capturecensus|command|other) ;; *) echo "Unknown shard: $shard" >&2; exit 2;; esac
packages=()
package_list="$(go list ./...)"
[[ -n "$package_list" ]] || { echo "go list returned no packages" >&2; exit 1; }
while IFS= read -r pkg; do
  [[ -n "$pkg" ]] || continue
  case "$pkg" in
    */pkg/cypher|*/pkg/cypher/*) group=cypher ;;
    */pkg/storage|*/pkg/storage/*) group=storage ;;
    */pkg/server|*/pkg/server/*) group=server ;;
    */testing/storagecensus|*/testing/storagecensus/*) group=storagecensus ;;
    */testing/capturecensus|*/testing/capturecensus/*) group=capturecensus ;;
    */cmd/nornicdb|*/cmd/nornicdb/*|*/pkg/nornicdb|*/pkg/nornicdb/*) group=command ;;
    *) group=other ;;
  esac
  [[ "$group" != "$shard" ]] || packages+=("$pkg")
done <<< "$package_list"
if (( ${#packages[@]} == 0 )); then
  echo "No packages in shard $shard"
  exit 0
fi
printf 'Shard %s packages:\n' "$shard"
printf '  %s\n' "${packages[@]}"
go test -p 1 -parallel 4 -timeout 30m "${packages[@]}"
