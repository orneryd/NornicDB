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

## Local setup

Use the Go toolchain declared in `go.mod` (currently 1.27.3), Node.js 24 for the documentation portal, npm, Git, and Make. CGO/native build paths require a supported C/C++ toolchain and their matching libraries. Docker is needed for container builds and the Neo4j differential checks; `gh` is needed for repository/release helpers.

```bash
git clone https://github.com/orneryd/NornicDB.git
cd NornicDB
go mod download
npm ci --prefix ui
npm ci --prefix docs-site
```

Build the server with `make build` (includes UI) or `make build-headless`. Native inference builds need the prerequisites described in [build setup](docs/development/setup.md); do not assume a downloaded model alone supplies the native library. Embedding a database in another application starts with the [Go embedding guide](docs/development/embedding.md).

## Validate a change

Run the affected checks before review and record the exact head, commands, outcomes, and skipped dependencies. Use the canonical verifier to discover the required checks:

```bash
make verify-list BASE=origin/main
make verify BASE=origin/main
```

For documentation changes:

```bash
npm run build --prefix docs-site
npm test --prefix docs-site
npm run test:browser --prefix docs-site
```

The browser command builds current docs and a tagged bundle before running journeys. Local tests use installed Chrome; CI installs Playwright Chromium. Build failures and dead links must be fixed, not suppressed. The corpus checks preserve retained documentation and enforce the declared maintenance/release scope.

For product changes, add a regression that demonstrates the reported problem and exercise the public package/transport path it affects. `make test` runs the complete package set and lint checks; `make verify ALL=1 BASE=<immutable-base>` also covers the release verification owners. Resource-limited machines can start with focused package checks, but a focused pass is not a full-suite pass. Do not present missing models, unavailable backends, canceled jobs, or old cached results as fresh qualification.

Git hooks are optional local tooling, not repository push gates. Do not install/enable Graphify hooks to replace the trusted GitHub advisory workflow. See [testing](docs/development/testing.md) for fixtures and [coding conventions](docs/development/code-style.md) for formatting and ownership rules.

## Documentation changes

Edit runtime facts in their canonical guide and link to them from related pages. Verify defaults, units, precedence, lifecycle behavior, and examples against the current implementation. Keep historical measurements scoped to their workload; do not turn them into RAM, durability, or throughput guarantees.

The portal renders this file as its contribution guide; do not maintain a second copy. Durability, memory, and embedding guidance lives in `docs/operations` and `docs/development`. Check both current and tagged builds after changing publication rules or navigation. Documentation-only maintenance must not modify production behavior or changelogs.

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
| `feature/*`, `fix/*` | anyone with write access | push freely; target `release/vX.Y.Z` for release fixes | none |
| `develop` | integration branch | direct integration only; pull requests targeting develop are closed (no force-push, no deletion) | manual, admin-approved |
| `release/vX.Y.Z` | created and merged by the maintainer | pull request from `develop` (or a fix branch), on milestone `vX.Y.Z` | record focused/local or approved CI evidence; no automatic CI on release pushes |
| `main` | maintainers only | release PR or scoped operational/documentation PR | local verification; manual, admin-approved GitHub checks |

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

### Checks: local, release, and administrator-approved runs

Full CI is requested automatically only for a PR from a `release/**` branch in this repository targeting `main`. Opening, reopening, or updating such a PR to `main` automatically runs the full `make verify ALL=1` suite, the full `make test` package set, and the Cypher differential ratchet without administrator approval. Independent verification checks and the existing coverage groups run on separate runners; a single required release check fails if any group fails or is skipped. After retargeting a PR to `main`, close and reopen it to request verification. It checks out the event's exact release head, requires the event's main base SHA to be an ancestor, and uses that base as the allocation baseline; each new head starts its own run and cancels any earlier release-verification run for the same PR. Other workflows are not canceled by this concurrency group. Release branch pushes alone do not run CI. Direct updates to release branches are blocked; all changes must be merged through PRs. Candidates may be pushed for review before a local full-suite run. For a reviewed same-repository PR workflow, dispatch the exact candidate SHA with `gh workflow run ci.yml --ref <pr-branch> -f branch=<full-head-sha> -f test_only=true` and approve its `admin-approved-actions` environment. This mode runs the complete `make test` package set in resource-bounded shards. Record the checked SHA, failures and skips; a queued, skipped or superseded run is not a pass. Finish any discovered regressions before declaring merge readiness. Local full-suite execution and Codex Git hooks are not required push gates.

Release PR duplicate analysis is advisory. Its trusted default-branch workflow can be requested by a maintainer commenting `/graphify approve <full-head-sha>` on the PR. Report the actual findings, indexed revision and incomplete searches; do not equate a successful advisory wrapper with a completed or empty analysis. Review candidates in current source and document reuse, consolidation or evidence-backed separation.

PRs targeting `develop` are closed by a governance-only workflow without executing PR code or tests; GitHub may briefly show them open before that workflow runs. Other PRs do not automatically run CI. When needed, dispatch existing CI workflows from a trusted workflow ref, specify the intended target SHA, and have an administrator approve the waiting job. Check the workflow definition and target before approval. Workflow editors can remove YAML gates in their own branches; do not dispatch untrusted workflow definitions.

Existing tag pushes still build and publish images and macOS releases. Existing pushes to `main` may publish GitHub Pages and rebuild llama libraries when their path filters match. Automatic develop nightly and canary jobs remain paused.

#### Administrator approval setting

In **Settings → Environments → admin-approved-actions**, keep **Required reviewers** limited to repository
administrators (currently `orneryd`) and disable **Allow administrators to bypass configured protection
rules**. Self-review is allowed so the sole administrator can approve their own manual dispatch. Keep
this reviewer list current when admin membership changes; GitHub does not automatically bind it to the
admin role. Configure this environment before enabling these workflows: merely naming an environment
in YAML creates an unprotected environment if it does not exist.

Repository writers can request manual runs; they cannot approve them unless named as reviewers.
The repository's separate fork-PR approval setting is not an admin-only gate for collaborator PRs.
The `main` ruleset requires a code-owner approval from the administrator; administrators retain a bypass for their own PRs, which cannot be self-approved. The release pull request safeguards also permit the administrator role to bypass the ruleset for pull requests; the bypass does not apply to direct pushes through that ruleset. GitHub grants that bypass by role rather than PR author, so it must be used deliberately. Workflow editors can remove YAML gates, including in a manually selected branch. Only dispatch trusted workflow definitions; check the workflow ref as well as its target before approval, and protect workflow changes through repository review policy.
For an administrator-approved CI dispatch, the administrator approves the plan before its dependent runners start, and approves the final summary job. Select `test_only` to run the complete `make test` package set across independent runners. Normal CI runs the existing coverage groups in parallel and combines their profiles before uploading coverage. CI tests use CGO with the local llama library enabled; Heimdall tests that require a real GGUF model retain their existing CI skip. `make test` and `make verify` remain available locally.

Existing branches retain their old workflow definitions until updated. Merge this policy into an active release branch before relying on its automatic verification; sync maintenance changes from `main` into `develop` as documented above.

Run `python3 scripts/check-actions-policy.py` to check trigger and runner approval policy locally.

### Temporary local PR verification policy (through October 31, 2026)

Through October 31, 2026 inclusive (America/Phoenix), run `make verify BASE=origin/<pr-base>` locally
for every PR created. Post a result comment on the PR, not just a description: include the checked head SHA,
base SHA, exact command, results, failures, and any skipped or unexecuted checks with reasons. Re-run after
changes or rebases and post the updated result. Path-filtered no-op runs must be reported as such, not as a
full-suite pass. Non-release workflow dispatches remain administrator-approved.

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

- Integrate product work directly into `develop`; pull requests targeting `develop` are not accepted. Target `release/vX.Y.Z` for a release fix, or `main` for the scoped maintainer operational/documentation path above.
- Put release work on its release milestone; the maintainer maintenance path may omit one. Link the issues
  the PR fixes (`Fixes #123`).
- Record validation for each reviewed head. Candidates may be pushed for review before a local full-suite run; use the exact-head approved CI path above when full local qualification is deferred. For maintainer PRs to `main`, run the applicable `make verify` checks and report every skip.

## Need Help?

- Open an [issue](https://github.com/orneryd/NornicDB/issues)
- Join our community (links in README.md)
