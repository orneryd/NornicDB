#!/usr/bin/env bash
# Focused NOR-12 Neo4j Enterprise FGAC comparison harness.
#
# The comparison is opt-in and never runs during ordinary tests. It compares
# documented, normative authorization flows on synthetic data between a pinned
# Neo4j Enterprise reference and a NornicDB candidate.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../../.." && pwd)"
references_path="${script_dir}/references.json"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
usage: run-fgac-comparison.sh

Environment:
  FGAC_EE_EVALUATION=1                 Required explicit Enterprise evaluation opt-in.
  NORNICDB_FGAC_CANDIDATE_URI          Candidate Bolt URI.
  NORNICDB_FGAC_CANDIDATE_USER         Candidate admin user (default: admin).
  NORNICDB_FGAC_CANDIDATE_PASSWORD     Candidate admin password (required).
  NORNICDB_FGAC_CANDIDATE_DATABASE     Candidate target database (default: nornic).
  NORNICDB_NEO4J_REFERENCE_URI         Reference Bolt URI when not starting a container.
  NORNICDB_NEO4J_REFERENCE_USER        Reference admin user (default: neo4j).
  NORNICDB_NEO4J_REFERENCE_PASSWORD    Reference admin password (required).
  NORNICDB_NEO4J_REFERENCE_DATABASE    Reference target database (default: neo4j).
  NORNICDB_FGAC_REFERENCE_VERSION      Optional pinned reference version check.

Reference container (optional):
  FGAC_START_REFERENCE=1               Start the pinned Enterprise reference image.
  FGAC_REFERENCE_VERSION               Required with FGAC_START_REFERENCE=1; must be pinned.
  FGAC_REFERENCE_PASSWORD              Initial reference admin password.
  FGAC_ALLOW_IMAGE_PULL=1              Allow an explicit docker pull of the pinned image.

The pinned image list is testing/cypher/fgac/references.json. When
FGAC_START_REFERENCE=1 the image must already be present locally unless
FGAC_ALLOW_IMAGE_PULL=1 is also set.
EOF
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
  "")
    ;;
  *)
    die "unknown argument: $1"
    ;;
esac

if [[ "${FGAC_EE_EVALUATION:-}" != "1" ]]; then
  cat >&2 <<'EOF'
error: the Neo4j Enterprise comparison is opt-in.
Set FGAC_EE_EVALUATION=1 only when you intend to run the pinned Enterprise
evaluation images or an operator-supplied Enterprise reference endpoint.
EOF
  exit 2
fi

command -v python3 >/dev/null 2>&1 || die "python3 is required to read the pinned references"
[[ -f "${references_path}" ]] || die "missing pinned reference list: ${references_path}"

pinned_reference() {
  local version="$1"
  python3 - "${references_path}" "${version}" <<'PY'
import json
import sys

path, version = sys.argv[1], sys.argv[2]
try:
    with open(path, encoding="utf-8") as handle:
        references = json.load(handle)["references"]
except (OSError, ValueError, KeyError) as exc:
    sys.exit(f"error: read pinned references {path}: {exc}")

for pin in references:
    if pin.get("version") == version:
        print(f'{pin["image"]}@{pin["digest"]}')
        break
else:
    pinned = ", ".join(pin.get("version", "?") for pin in references)
    sys.exit(f"error: reference version {version!r} is not pinned ({pinned})")
PY
}

if [[ "${FGAC_START_REFERENCE:-0}" == "1" ]]; then
  reference_version="${FGAC_REFERENCE_VERSION:-}"
  [[ -n "${reference_version}" ]] || die "FGAC_REFERENCE_VERSION is required with FGAC_START_REFERENCE=1"
  reference_image="$(pinned_reference "${reference_version}")" || exit 1

  reference_password="${FGAC_REFERENCE_PASSWORD:-${NORNICDB_NEO4J_REFERENCE_PASSWORD:-}}"
  [[ -n "${reference_password}" ]] || die "set FGAC_REFERENCE_PASSWORD for the reference container"

  command -v docker >/dev/null 2>&1 || die "docker is required with FGAC_START_REFERENCE=1"
  if ! docker image inspect "${reference_image}" >/dev/null 2>&1; then
    if [[ "${FGAC_ALLOW_IMAGE_PULL:-0}" == "1" ]]; then
      printf 'Pulling pinned reference %s\n' "${reference_image}" >&2
      docker pull "${reference_image}" >&2
    else
      die "pinned reference is not present locally; run 'docker pull ${reference_image}' or set FGAC_ALLOW_IMAGE_PULL=1"
    fi
  fi

  container="nornicdb-fgac-reference-$$"
  cleanup() {
    docker rm --force "${container}" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT INT TERM

  docker run --detach --rm \
    --name "${container}" \
    --env NEO4J_ACCEPT_LICENSE_AGREEMENT=eval \
    --env NEO4J_AUTH="neo4j/${reference_password}" \
    --publish 127.0.0.1::7687 \
    "${reference_image}" >/dev/null

  published_address="$(docker port "${container}" 7687/tcp | head -n 1)"
  [[ -n "${published_address}" ]] || die "reference container did not publish Bolt port 7687"

  export NORNICDB_NEO4J_REFERENCE_URI="bolt://${published_address}"
  export NORNICDB_NEO4J_REFERENCE_USER="neo4j"
  export NORNICDB_NEO4J_REFERENCE_PASSWORD="${reference_password}"
  export NORNICDB_NEO4J_REFERENCE_DATABASE="${NORNICDB_NEO4J_REFERENCE_DATABASE:-neo4j}"
  export NORNICDB_FGAC_REFERENCE_VERSION="${reference_version}"
  printf 'Started pinned reference %s at bolt://%s\n' "${reference_image}" "${published_address}" >&2
else
  [[ -n "${NORNICDB_NEO4J_REFERENCE_URI:-}" ]] || die "set NORNICDB_NEO4J_REFERENCE_URI or FGAC_START_REFERENCE=1"
  [[ -n "${NORNICDB_NEO4J_REFERENCE_PASSWORD:-}" ]] || die "set NORNICDB_NEO4J_REFERENCE_PASSWORD for the reference admin"
fi

[[ -n "${NORNICDB_FGAC_CANDIDATE_URI:-}" ]] || die "set NORNICDB_FGAC_CANDIDATE_URI for the NornicDB candidate"
[[ -n "${NORNICDB_FGAC_CANDIDATE_PASSWORD:-}" ]] || die "set NORNICDB_FGAC_CANDIDATE_PASSWORD for the candidate admin"

export NORNICDB_FGAC_EE_EVALUATION=1
cd "${repo_root}"
# Do not exec: the EXIT trap must remove the reference container after Go exits.
go test -tags noui ./testing/cypher/fgac -run '^TestFGACComparison$' -count=1 -v -timeout 15m
