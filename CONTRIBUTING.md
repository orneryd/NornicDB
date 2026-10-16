# Contributing to NornicDB

## Quick Links

- [Development workflow](#development-workflow): branches, releases, milestones and checks
- [AI Agent Development Guide](AGENTS.md)
- [Development overview](docs/development/README.md) and [Testing guide](docs/development/testing.md)
- [Contributors](CONTRIBUTORS.md)

## Before You Start

1. Read [AGENTS.md](AGENTS.md), the guide for AI coding agents and contributors.
2. Read the [development workflow](#development-workflow) below.
3. Check existing [issues](https://github.com/orneryd/NornicDB/issues) and [pull requests](https://github.com/orneryd/NornicDB/pulls).

## Development workflow

NornicDB follows **Git Flow**: work is integrated on `develop`, stabilized on a `release/vX.Y.Z` branch, then
tagged and merged into `main`. Maintainers may also merge scoped operational and documentation PRs directly
into `main`, using the path described below.

```
feature/<topic> ──┐  (no checks; merge freely)
feature/<topic> ──┼──► develop ──── manual admin-approved checks ────┐
fix/<topic>     ──┘                                     │ pull request (milestone vX.Y.Z)
                                                        ▼
                                              release/vX.Y.Z ──► tag vX.Y.Z ──► main
                                                                  (images, macOS release)
```

| Branch | Who | How changes land | Checks on GitHub |
|---|---|---|---|
| `feature/*`, `fix/*` | anyone with write access | push freely; open PRs into `develop` if you want a review | none |
| `develop` | integration branch | push or merge freely (no force-push, no deletion) | manual, admin-approved |
| `release/vX.Y.Z` | created and merged by the maintainer | pull request from `develop` (or a fix branch), on milestone `vX.Y.Z` | manual, admin-approved |
| `main` | maintainers only | fast-forward from a finished release branch, or a scoped operational/documentation PR | local verification; manual, admin-approved GitHub checks |

- **Experiment on `feature/<topic>` branches** cut from `develop`, iterate as fast as you like, and merge them into
  `develop`. Nothing runs on GitHub for these pushes or pull requests.
- **Ship through a release branch.** When `develop` has what a release needs, open the pull request from `develop`
  into `release/vX.Y.Z` (`make release-pr VERSION=X.Y.Z`). The maintainer reviews, verifies and merges it.
  Follow-up fixes for that release go to the release branch the same way.
- **Only the maintainer** creates release branches, merges into them, tags releases and updates `main`.
  The checked-in rulesets for `develop` and release branches live in [`.github/rulesets/`](.github/rulesets/)
  (applied with `make repo-settings`).
- After a release, `main` is merged back into `develop` (`make release-finish` does it), so `develop` always contains
  every release.

### Maintainer operational and documentation PRs into `main`

Maintainers can use a short-lived branch based on the latest `main` for documentation, contributor/agent
instructions, repository administration, and scoped CI/CD or operational maintenance. This path does not require
waiting for a product release. Product features, query/storage behavior changes, runtime bug fixes, and performance
changes follow `develop` and the release process. If a change mixes both scopes, split the PRs or use `develop`.

1. Create the branch from `origin/main` (for example, `docs/contributor-workflow` or `chore/ci-maintenance`).
   An existing feature branch is suitable only if its diff against `main` contains solely the intended maintenance;
   otherwise, create a clean branch and cherry-pick the relevant commits.
2. Open a PR targeting `main`, explain why it qualifies for this path, and run
   `make verify BASE=origin/main`. Record the checks and any skips in the PR. Review CI/CD changes for their
   deployment and publishing effects. A release milestone is optional unless the work belongs to a release.
3. A maintainer reviews and merges using a method permitted by the repository's current `main` protections.
   This is a PR workflow; it does not authorize direct pushes, protection bypasses, or product release tags.
4. Promptly merge the updated `origin/main` into `develop` so future releases retain the change. If a release
   branch is already open, bring it up to date with `main` before final verification and tagging; `release-finish`
   refuses to run unless `main` is an ancestor of the release branch. Coordinate any release-branch rebase with
   the maintainer; do not rewrite shared history without coordination. Re-run verification after reconciliation.

Docs changes on `main` can publish the documentation site immediately. CI/CD changes can affect publishing too;
inspect the relevant workflow triggers before merging.

### Checks: run them locally, spend Actions minutes deliberately

Pull requests do **not** start GitHub Actions automatically; opening many pull requests in a short time used to
exhaust the Actions allowance. Instead:

1. **Locally, before asking for a merge:** `make verify BASE=origin/develop` (use the actual PR base) runs every check CI runs (localization, UI build, build,
   all test groups, allocation ratchet, Cypher conformance, the Neo4j differential, docs) for the files your branch
   changes, with one summary at the end. `make verify-list` shows what would run, `make verify-help` lists the
   options, and `make verify BRANCH=<branch>` verifies another branch without touching your checkout.
   The default base is `origin/develop`; path filters and allocation comparisons use the branch's merge base.
   For a release branch, compare with main using `VERIFY_BASE=origin/main make verify` (or
   `make verify BASE=origin/main`).
2. **On GitHub, when needed:** use *Run workflow* to select a branch/ref, then an administrator approves
   the waiting jobs through the `admin-approved-actions` environment. PR events and labels, including
   `ci:run`, never start checks. Check the exact ref before approving; approval permits that run's jobs.
3. **Automatic develop runs are paused:** both nightly verification and weekly canary publishing are
   manual-only until the metered Actions budget policy changes.

Existing tag pushes still build/publish images and macOS releases. Existing pushes to `main` can publish
GitHub Pages and rebuild llama libraries when their path filters match. These push jobs use the
`automatic-publishing` environment without an approval requirement. All manual runs, including runs
selected on `main`, and reusable runs from manual callers require admin approval before runner allocation.

#### Administrator approval setting

In **Settings → Environments → admin-approved-actions**, keep **Required reviewers** limited to repository
administrators (currently `orneryd`) and disable **Allow administrators to bypass configured protection
rules**. Self-review is allowed so the sole administrator can approve their own manual dispatch. Keep
this reviewer list current when admin membership changes; GitHub does not automatically bind it to the
admin role. Configure this environment before enabling these workflows: merely naming an environment
in YAML creates an unprotected environment if it does not exist.

Repository writers can request manual runs; they cannot approve them unless named as reviewers.
The repository's separate fork-PR approval setting is not an admin-only gate for collaborator PRs.
Workflow editors can remove YAML gates, so protect workflow changes through repository review policy.
Existing branches retain their old workflow definitions until updated; disable their live workflows during
rollout and only re-enable them after merging and syncing this policy to `develop` and release branches.

Run `python3 scripts/check-actions-policy.py` to check trigger and runner approval policy locally.

### Temporary local PR verification policy (through October 31, 2026)

Through October 31, 2026 inclusive (America/Phoenix), run `make verify BASE=origin/<pr-base>` locally
for every PR created. Post a result comment on the PR, not just a description: include the checked head SHA,
base SHA, exact command, results, failures, and any skipped or unexecuted checks with reasons. Re-run after
changes or rebases and post the updated result. Path-filtered no-op runs must be reported as such, not as a
full-suite pass. GitHub Actions remains manual and administrator-approved.

For a maintainer-approved release batch, focused-validated fixes that merge cleanly may be integrated before
full verification. Run one combined `make verify ALL=1 BASE=<immutable-base>` on the frozen release head,
and post its checked head, base, command, result, and skips on every participating PR. Clearly identify this
as shared release verification, not an individual PR-head pass. Cancel redundant individual full runs, preserve
their logs, and open follow-up PRs against the release branch for any integration regressions. Re-run the
combined checks after fixes; do not declare the release verified while failures remain.

Fix every regression found before declaring the PR or combined release verified. If several PRs share a root cause, open a dedicated
fix PR against their common integration base, link it from affected PRs, and propagate it into each affected
branch before re-running verification. Do not waive regressions or claim a blocked check passed. Report local
toolchain blockers separately, resolve them where possible, and rerun the affected checks.

### Releases and milestones

Every release has a milestone with the same name (`v1.4.3`); features and fixes for that release are assigned to it.

```sh
make release-start  VERSION=1.4.3                 # maintainer: milestone v1.4.3 + branch release/v1.4.3 from main
make release-pr     VERSION=1.4.3                 # open develop -> release/v1.4.3, on the milestone
make milestone      VERSION=1.4.3 ISSUES="12 34"  # put issues or pull requests #12 and #34 on v1.4.3
make release-status VERSION=1.4.3                 # milestone progress, open PRs, distance from main
make release-finish VERSION=1.4.3                 # maintainer: tag, fast-forward main, close milestone, sync develop
```

All of them accept `DRY_RUN=1` to show what they would do. They use the `gh` CLI. Before `release-finish`, bump
`pkg/buildinfo/VERSION` and move the `[Unreleased]` section of `CHANGELOG.md` to the new version on the release
branch.

### Pull requests

- Target `develop` for product work, `release/vX.Y.Z` for a release fix, or `main` for the scoped maintainer
  operational/documentation path above.
- Put release work on its release milestone; the maintainer maintenance path may omit one. Link the issues
  the PR fixes (`Fixes #123`).
- Run `make verify` and say so in the description, with anything you skipped.

## Need Help?

- Open an [issue](https://github.com/orneryd/NornicDB/issues)
- Join our community (links in README.md)
