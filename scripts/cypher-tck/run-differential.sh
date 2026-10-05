#!/usr/bin/env bash
# usage: run-differential.sh [fixed|ratchet]
#   fixed (default, every change): the fixed corpus and the route checks
#     against the pinned Neo4j.
#   ratchet (nightly): the differential ratchet (#754), the sweep and the issue
#     reproductions through Bolt auto-commit, Bolt explicit transactions and
#     HTTP, checked against testdata/differential/known_mismatches.jsonl.
set -euo pipefail

mode="${1:-fixed}"
case "${mode}" in
  fixed | ratchet) ;;
  *)
    echo "usage: $0 [fixed|ratchet]" >&2
    exit 2
    ;;
esac

neo4j_image="neo4j:5.26.30-community@sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c"
container_name="nornicdb-cypher-reference-$$"

cleanup() {
  docker rm --force "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --rm \
  --name "${container_name}" \
  --env NEO4J_AUTH=none \
  --publish 127.0.0.1::7687 \
  --publish 127.0.0.1::7474 \
  "${neo4j_image}" >/dev/null

published_address="$(docker port "${container_name}" 7687/tcp)"
if [[ -z "${published_address}" ]]; then
  echo "Neo4j reference did not publish Bolt port 7687" >&2
  exit 1
fi
published_http_address="$(docker port "${container_name}" 7474/tcp)"

if [[ "${mode}" == "ratchet" ]]; then
  # -timeout bounds each route's whole run; every statement also has its own
  # deadline (differential.DefaultOptions).
  NORNICDB_NEO4J_REFERENCE_URI="bolt://${published_address}" \
    go test -tags 'noui,nolocalllm' ./testing/cypher/tck -run '^TestDifferentialRatchetBoltMatchesPinnedNeo4j$' -count=1 -timeout 45m -v
  NORNICDB_NEO4J_REFERENCE_HTTP_URI="http://${published_http_address}" \
    go test -tags 'noui,nolocalllm' ./pkg/server -run '^TestDifferentialRatchetHTTPMatchesPinnedNeo4j$' -count=1 -timeout 45m -v
  exit 0
fi

NORNICDB_NEO4J_REFERENCE_URI="bolt://${published_address}" \
  go test -tags 'noui,nolocalllm' ./testing/cypher/tck -run '^(TestFixedDifferentialCorpusMatchesPinnedNeo4j|TestGh809_BoltTransactionIndexVisibility|TestGh810_NullPropertyMapsMatchPinnedNeo4j)$' -count=1 -v

NORNICDB_NEO4J_REFERENCE_HTTP_URI="http://${published_http_address}" \
  go test -tags 'noui,nolocalllm' ./pkg/server -run '^(TestHTTPFixedDifferentialCorpusMatchesPinnedNeo4j|TestHTTPDifferentialResetClearsSchemaArtifacts|TestRemoteHTTPGraphRoundTrip|TestRemoteHTTPPinnedNeo4j|TestGh776_HTTPMalformedCommitRollback|TestGh809_HTTPTransactionIndexVisibility|TestGh810_HTTPNullPropertyMaps)$' -count=1 -v
