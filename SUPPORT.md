# Support

This document describes where to ask questions, where to report bugs, and how to route security-sensitive issues for NornicDB.

## Before Opening Anything

Please check these first:

- [README.md](README.md) for quick start, feature overview, and major docs links
- [docs](docs) for architecture, user guides, and compliance-oriented material
- [CHANGELOG.md](CHANGELOG.md) for recent behavior changes
- [CONTRIBUTING.md](CONTRIBUTING.md) for contribution expectations

## Where To Ask For Help

### GitHub Issues

Use GitHub Issues for:

- reproducible bugs
- regressions
- incorrect behavior
- documentation problems
- specific feature requests

Please use the existing issue templates when possible.

### Discord

Use the community Discord linked from the README for:

- setup questions
- usage questions
- design discussion
- early feedback on ideas before filing an issue

Discord is the best place for exploratory questions. GitHub Issues are the best place for actionable defects and tracked work.

## What Not To Put In Public Channels

Do not post these publicly in Issues, Discussions, or Discord:

- vulnerability details
- secrets, tokens, or credentials
- production data
- private cluster topology or internal endpoints

For security issues, follow [SECURITY.md](SECURITY.md).

## What To Include In Bug Reports

Reports are much easier to act on when they include:

- NornicDB version, tag, or commit SHA
- how you are running it: source build, Docker image, platform, GPU mode, headless/UI, clustered/standalone
- the exact command, API request, Cypher query, or reproduction steps
- expected result versus actual result
- relevant logs, panic output, stack traces, or screenshots
- whether the issue is reproducible on `main` or only on a release tag

## Support Expectations

NornicDB is maintained as an active open-source project. Support is best-effort and priority usually goes to:

1. correctness bugs
2. security issues
3. regressions in documented functionality
4. build and compatibility breakage
5. feature requests and design discussions

## Commercial Or Pilot Interest

If you are evaluating NornicDB for an internal pilot, compliance-heavy workload, or a larger integration, open a GitHub issue only if the discussion can be public. Otherwise, reach out through the maintainer's GitHub contact path first so the conversation can be routed appropriately.