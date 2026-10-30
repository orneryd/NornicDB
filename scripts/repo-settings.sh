#!/usr/bin/env bash
#
# Apply the repository settings kept in .github/rulesets/*.json to GitHub and
# create the `ci:run` label. Idempotent: a ruleset with the same name is
# updated, otherwise created. Rulesets that are not in the directory (such as
# "main branch") are left alone. Needs gh logged in as a repository admin.
#
#   scripts/repo-settings.sh            apply
#   scripts/repo-settings.sh --diff     show what would change, change nothing
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="${RELEASE_REPO:-orneryd/NornicDB}"
MODE="${1:-apply}"

existing="$(gh api "repos/${REPO}/rulesets" --jq '.[] | "\(.id)\t\(.name)"')"
for file in "${ROOT}"/.github/rulesets/*.json; do
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "${file}")"
  id="$(printf '%s\n' "${existing}" | awk -F'\t' -v n="${name}" '$2 == n { print $1 }')"
  if [[ "${MODE}" == "--diff" ]]; then
    if [[ -z "${id}" ]]; then
      echo "would create ruleset '${name}'"
    else
      current="$(gh api "repos/${REPO}/rulesets/${id}" --jq '{name, target, enforcement, conditions, bypass_actors, rules}')"
      if diff <(printf '%s' "${current}" | python3 -m json.tool --sort-keys) <(python3 -m json.tool --sort-keys "${file}") >/dev/null; then
        echo "ruleset '${name}' is up to date"
      else
        echo "would update ruleset '${name}' (#${id}):"
        diff <(printf '%s' "${current}" | python3 -m json.tool --sort-keys) <(python3 -m json.tool --sort-keys "${file}") || true
      fi
    fi
    continue
  fi
  if [[ -z "${id}" ]]; then
    gh api -X POST "repos/${REPO}/rulesets" --input "${file}" --jq '"created ruleset \(.name) (#\(.id))"'
  else
    gh api -X PUT "repos/${REPO}/rulesets/${id}" --input "${file}" --jq '"updated ruleset \(.name) (#\(.id))"'
  fi
done

if [[ "${MODE}" != "--diff" ]]; then
  gh label create "ci:run" --repo "${REPO}" --color 0E8A16 --force \
    --description "Run the CI and Cypher conformance checks on this pull request" >/dev/null
  echo "label ci:run is present"
fi
