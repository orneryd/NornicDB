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
tagged and merged into `main`.

```
feature/<topic> ──┐  (no checks; merge freely)
feature/<topic> ──┼──► develop ──── nightly checks ────┐
fix/<topic>     ──┘                                     │ pull request (milestone vX.Y.Z)
                                                        ▼
                                              release/vX.Y.Z ──► tag vX.Y.Z ──► main
                                                                  (images, macOS release)
```

| Branch | Who | How changes land | Checks on GitHub |
|---|---|---|---|
| `feature/*`, `fix/*` | anyone with write access | push freely; open PRs into `develop` if you want a review | none |
| `develop` | integration branch | push or merge freely (no force-push, no deletion) | nightly, when `develop` changed |
| `release/vX.Y.Z` | created and merged by the maintainer | pull request from `develop` (or a fix branch), on milestone `vX.Y.Z` | on request (`ci:run` label) |
| `main` | the maintainer only | fast-forward from a finished release branch | none (already verified) |

- **Experiment on `feature/<topic>` branches** cut from `develop`, iterate as fast as you like, and merge them into
  `develop`. Nothing runs on GitHub for these pushes or pull requests.
- **Ship through a release branch.** When `develop` has what a release needs, open the pull request from `develop`
  into `release/vX.Y.Z` (`make release-pr VERSION=X.Y.Z`). The maintainer reviews, verifies and merges it.
  Follow-up fixes for that release go to the release branch the same way.
- **Only the maintainer** creates release branches, merges into them, tags releases and updates `main`. These rules
  are repository rulesets kept in [`.github/rulesets/`](.github/rulesets/) (applied with `make repo-settings`).
- After a release, `main` is merged back into `develop` (`make release-finish` does it), so `develop` always contains
  every release.

### Checks: run them locally, spend Actions minutes deliberately

Pull requests do **not** start GitHub Actions automatically; opening many pull requests in a short time used to
exhaust the Actions allowance. Instead:

1. **Locally, before asking for a merge:** `make verify` runs every check CI runs (localization, UI build, build,
   all test groups, allocation ratchet, Cypher conformance, the Neo4j differential, docs) for the files your branch
   changes, with one summary at the end. `make verify-list` shows what would run, `make verify-help` lists the
   options, and `make verify BRANCH=<branch>` verifies another branch without touching your checkout.
2. **On GitHub, when needed:** add the **`ci:run`** label to a pull request to run CI and Cypher conformance on it,
   or use *Run workflow* on the CI workflow for any branch.
3. **Nightly:** the full suite (CI, conformance, and the differential ratchet) runs against `develop` at 03:17 UTC
   when `develop` changed in the last 24 hours (`.github/workflows/nightly.yml`).

Tag pushes still build and publish the images and the macOS release, and pushes to `main` that touch `docs/` still
publish the documentation site. Every Monday at 04:23 UTC, when `develop` changed during the week, the release
images are also built from `develop` and published as **`:canary`** (`.github/workflows/canary.yml`); a canary
never moves `:latest` or a version tag.

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

- Base a pull request on `develop` (or, for a release fix, on `release/vX.Y.Z`), never on `main`.
- Put it on the release milestone, and link the issues it fixes (`Fixes #123`).
- Run `make verify` and say so in the description, with anything you skipped.

## Need Help?

- Open an [issue](https://github.com/orneryd/NornicDB/issues)
- Join our community (links in README.md)
