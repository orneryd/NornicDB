## macOS installer placement and ownership

- [x] Disable bundle relocation so the package targets `/Applications/NornicDB.app` instead of a matching bundle in build staging.
- [x] Remove the build script's privileged cleanup fallback; report inaccessible staging explicitly.
- [x] Write per-user install files as the user in both package variants, and report missing app or launch failures accurately.
- [x] Check shell syntax, package relocation metadata, and the missing-app failure path.
- [x] Remove root-owned staging from the earlier relocatable install (one-time administrator action).
- [x] Verify the package installs its app in `/Applications` and no build-staging files are root-owned; the upgrade postinstall then failed on a legacy root-owned file.
- [x] Repair legacy root-owned environment and LaunchAgent files during upgrades; check both rebuilt package scripts and metadata.
- [x] Isolate parallel build staging and publish completed package artifacts atomically.
- [ ] Install the rebuilt full package and verify postinstall completes and user files are no longer root-owned.