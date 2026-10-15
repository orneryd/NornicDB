#!/usr/bin/env bash
#
# Release branches and milestones (see CONTRIBUTING.md, "Development workflow").
#
#   release.sh start VERSION            create milestone vVERSION and branch release/vVERSION from main
#   release.sh pr VERSION [HEAD]        open the PR HEAD (default develop) -> release/vVERSION, on the milestone
#   release.sh milestone VERSION N...   put issues or pull requests N... on milestone vVERSION
#   release.sh status VERSION           milestone progress, open PRs, and how far the branch is from main
#   release.sh finish VERSION           tag vVERSION, fast-forward main, close the milestone, update develop
#
# VERSION is x.y.z (a leading v is accepted). DRY_RUN=1 prints what would change.
# Needs gh logged in as a maintainer; start and finish need admin (the branch rules).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
REPO="${RELEASE_REPO:-orneryd/NornicDB}"
REMOTE="${RELEASE_REMOTE:-origin}"
DEVELOP="${RELEASE_DEVELOP:-develop}"
DRY_RUN="${DRY_RUN:-0}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
say() { printf '%s\n' "$*"; }
run() {
  if [[ "${DRY_RUN}" == "1" ]]; then printf '[dry run] %s\n' "$*"; else "$@"; fi
}

version() {
  local raw="${1:-}"
  raw="${raw#v}"
  [[ "${raw}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "VERSION must look like 1.4.3 (got '${1:-}')"
  printf '%s' "${raw}"
}

milestone_number() {  # prints the milestone number for title $1 (with or without the leading v), or nothing
  local bare="${1#v}"
  gh api "repos/${REPO}/milestones?state=all&per_page=100" \
    --jq ".[] | select(.title == \"v${bare}\" or .title == \"${bare}\") | .number" | head -n1
}

remote_branch_exists() {
  git ls-remote --exit-code --heads "${REMOTE}" "$1" >/dev/null 2>&1
}

cmd_start() {
  local v; v="$(version "${1:-}")"
  local tag="v${v}" branch="release/v${v}"
  git fetch --quiet "${REMOTE}" main
  remote_branch_exists "${branch}" && die "${branch} already exists"
  local number; number="$(milestone_number "${tag}")"
  if [[ -z "${number}" ]]; then
    say "creating milestone ${tag}"
    run gh api -X POST "repos/${REPO}/milestones" -f title="${tag}" \
      -f description="Release ${tag}. Work merges into ${branch} through pull requests; the release is tagged from that branch and merged into main." >/dev/null
  else
    say "milestone ${tag} already exists (#${number})"
  fi
  say "creating ${branch} from main ($(git rev-parse --short "${REMOTE}/main"))"
  run git push --quiet "${REMOTE}" "${REMOTE}/main:refs/heads/${branch}"
  if ! remote_branch_exists "${DEVELOP}"; then
    say "creating ${DEVELOP} from main"
    run git push --quiet "${REMOTE}" "${REMOTE}/main:refs/heads/${DEVELOP}"
  fi
  say ""
  say "Next:"
  say "  make release-pr VERSION=${v}            # open ${DEVELOP} -> ${branch}"
  say "  make milestone VERSION=${v} ISSUES=\"12 34\"   # attach work to ${tag}"
}

cmd_pr() {
  local v; v="$(version "${1:-}")"
  local head="${2:-${DEVELOP}}" tag="v${v}" branch="release/v${v}"
  remote_branch_exists "${branch}" || die "${branch} does not exist; run: make release-start VERSION=${v}"
  [[ -n "$(milestone_number "${tag}")" ]] || die "milestone ${tag} does not exist; run: make release-start VERSION=${v}"
  local existing
  existing="$(gh pr list --repo "${REPO}" --base "${branch}" --head "${head}" --state open --json url --jq '.[0].url')"
  if [[ -n "${existing}" ]]; then say "already open: ${existing}"; return; fi
  run gh pr create --repo "${REPO}" --base "${branch}" --head "${head}" --milestone "${tag}" \
    --title "Release ${tag}: ${head} -> ${branch}" \
    --body "Work from \`${head}\` for ${tag}. Milestone: ${tag}.

Checks do not run automatically. Before merging, run \`make verify BRANCH=${head} BASE=${REMOTE}/${branch}\` locally. For GitHub checks, manually dispatch the workflow and obtain administrator approval through \`admin-approved-actions\`. PR events and labels do not start checks."
}

cmd_milestone() {
  local v; v="$(version "${1:-}")"; shift || true
  [[ $# -gt 0 ]] || die "give issue or pull request numbers, e.g. ISSUES=\"12 34\""
  local tag="v${v}" number
  number="$(milestone_number "${tag}")"
  [[ -n "${number}" ]] || die "milestone ${tag} does not exist; run: make release-start VERSION=${v}"
  for item in "$@"; do
    item="${item#\#}"
    [[ "${item}" =~ ^[0-9]+$ ]] || die "not an issue number: ${item}"
    run gh api -X PATCH "repos/${REPO}/issues/${item}" -F milestone="${number}" --jq '"#\(.number) \(.title) -> \(.milestone.title)"'
  done
}

cmd_status() {
  local v; v="$(version "${1:-}")"
  local tag="v${v}" branch="release/v${v}" number
  number="$(milestone_number "${tag}")"
  [[ -n "${number}" ]] || die "milestone ${tag} does not exist"
  gh api "repos/${REPO}/milestones/${number}" --jq '"milestone \(.title) (\(.state)): \(.closed_issues) closed, \(.open_issues) open  \(.html_url)"'
  say ""
  say "open on the milestone:"
  gh api "repos/${REPO}/issues?milestone=${number}&state=open&per_page=100" \
    --jq '.[] | "  #\(.number) \(if .pull_request then "PR   " else "issue" end) \(.title)"'
  say ""
  say "open pull requests into ${branch}:"
  gh pr list --repo "${REPO}" --base "${branch}" --state open --json number,title,headRefName \
    --jq '.[] | "  #\(.number) \(.headRefName): \(.title)"'
  if remote_branch_exists "${branch}"; then
    git fetch --quiet "${REMOTE}" main "${branch}"
    say ""
    say "${branch}: $(git rev-list --count "${REMOTE}/main..${REMOTE}/${branch}") commits ahead of main," \
      "$(git rev-list --count "${REMOTE}/${branch}..${REMOTE}/main") behind"
  fi
}

cmd_finish() {
  local v; v="$(version "${1:-}")"
  local tag="v${v}" branch="release/v${v}" number
  git fetch --quiet --tags "${REMOTE}" main "${branch}" "${DEVELOP}" 2>/dev/null || git fetch --quiet --tags "${REMOTE}" main "${branch}"
  git rev-parse --verify --quiet "refs/tags/${tag}" >/dev/null && die "tag ${tag} already exists"
  git merge-base --is-ancestor "${REMOTE}/main" "${REMOTE}/${branch}" ||
    die "main has commits that ${branch} lacks; rebase ${branch} onto main first (main keeps a linear history)"
  number="$(milestone_number "${tag}")"
  local open=0
  [[ -n "${number}" ]] && open="$(gh api "repos/${REPO}/milestones/${number}" --jq .open_issues)"
  local file_version
  file_version="$(git show "${REMOTE}/${branch}:pkg/buildinfo/VERSION" 2>/dev/null | tr -d '[:space:]' || true)"
  say "release ${tag} from ${branch} @ $(git rev-parse --short "${REMOTE}/${branch}")" \
    "($(git rev-list --count "${REMOTE}/main..${REMOTE}/${branch}") commits ahead of main)"
  [[ "${open}" != "0" ]] && say "warning: milestone ${tag} still has ${open} open issue(s) or PR(s)"
  [[ -n "${file_version}" && "${file_version}" != "${v}" ]] &&
    say "warning: pkg/buildinfo/VERSION on ${branch} is ${file_version}, not ${v}"
  say "this will: tag ${tag} (starts the image and macOS release builds), fast-forward main," \
    "close the milestone, and merge main into ${DEVELOP}"
  if [[ "${DRY_RUN}" != "1" ]]; then
    read -r -p "continue? [y/N] " answer
    [[ "${answer}" == "y" || "${answer}" == "Y" ]] || die "stopped"
  fi
  run git tag -a "${tag}" -m "NornicDB ${tag}" "${REMOTE}/${branch}"
  run git push --quiet "${REMOTE}" "${REMOTE}/${branch}:refs/heads/main"
  run git push --quiet "${REMOTE}" "refs/tags/${tag}"
  [[ -n "${number}" ]] && run gh api -X PATCH "repos/${REPO}/milestones/${number}" -f state=closed --jq '"closed milestone \(.title)"'
  if remote_branch_exists "${DEVELOP}"; then
    local scratch="${ROOT}/.verify/worktrees/release-develop"
    rm -rf "${scratch}"; git worktree prune
    run git worktree add --quiet --detach "${scratch}" "${REMOTE}/${DEVELOP}"
    if [[ "${DRY_RUN}" != "1" ]]; then
      if git -C "${scratch}" merge --quiet --no-edit "${REMOTE}/main" -m "Merge ${tag} (main) into ${DEVELOP}"; then
        git -C "${scratch}" push --quiet "${REMOTE}" "HEAD:refs/heads/${DEVELOP}"
        say "${DEVELOP} now includes ${tag}"
      else
        git -C "${scratch}" merge --abort || true
        say "warning: merging main into ${DEVELOP} conflicts; resolve it on ${DEVELOP} by hand"
      fi
      git worktree remove --force "${scratch}"
    fi
  fi
  say "done: ${tag} is tagged and main is at ${branch}. Delete ${branch} when the release builds are published."
}

case "${1:-}" in
  start)     shift; cmd_start "$@" ;;
  pr)        shift; cmd_pr "$@" ;;
  milestone) shift; cmd_milestone "$@" ;;
  status)    shift; cmd_status "$@" ;;
  finish)    shift; cmd_finish "$@" ;;
  *) sed -n '3,13p' "$0"; exit 2 ;;
esac
