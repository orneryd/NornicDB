# Native Package Distribution Plan

**Status:** Proposed (single-pass)

## Scope

The pages in [`docs/packaging/`](../packaging/README.md) describe the desired end state for native package distribution on macOS, Windows, Linux, and Raspberry Pi. They describe target user experience and artifact contents but do not enumerate the build pipeline, CI wiring, signing posture, or release-cut workflow needed to actually ship them.

This plan is the missing execution surface for those doc pages. Each item below is either DONE (already shipping), IN-PROGRESS (partially wired), or TODO (no implementation yet).

The Docker channel is intentionally out of scope here — it is already available and uses a separate publish pipeline.

## Current State

### Cross-compiled binaries (DONE)

`make cross-all` produces:

- `bin/nornicdb-linux-amd64`
- `bin/nornicdb-linux-arm64`
- `bin/nornicdb-rpi64`
- `bin/nornicdb-rpi32`
- `bin/nornicdb-rpi-zero`
- `bin/nornicdb.exe`

Implementation: `Makefile` `cross-linux-amd64`, `cross-linux-arm64`, `cross-rpi`, `cross-rpi32`, `cross-rpi-zero`, `cross-windows`, `cross-windows-native`. Verified to compile from a single host without per-platform CI runners.

### macOS .pkg installer (IN-PROGRESS)

`make macos-package` and `macos-package-full` exist and produce:

- `dist/installer/component.pkg`
- `dist/installer/dmg/NornicDB-1.1.0-arm64-full.pkg`
- `dist/installer/{payload,resources,scripts,root}/`
- `dist/installer/distribution.xml`

This is a real shipping artifact today. The gap versus the macOS packaging doc is:

- **No notarized release flow.** The current pkg is not codesigned with a Developer ID Application certificate or notarized through `notarytool`. End users will hit Gatekeeper.
- **Homebrew tap scaffold exists.** `homebrew/` is structured as the future `orneryd/homebrew-nornicdb` repo with `Formula/nornicdb.rb`, tap CI, formula update automation, and README instructions.
- **Darwin Homebrew tarballs are wired.** `make homebrew-artifacts` and `.github/workflows/release-macos.yml` publish `nornicdb-darwin-arm64.tar.gz`, `nornicdb-darwin-amd64.tar.gz`, and `SHA256SUMS` for Homebrew.
- **Homebrew tarballs are not notarized.** Codesigning is supported by `scripts/build-homebrew-artifacts.sh` when `MACOS_CODESIGN_IDENTITY` is configured, but release notarization for tarball contents is still not complete.

### Windows builds (IN-PROGRESS)

`build.bat` exists at the repo root and produces five Windows variants per the [Windows packaging doc](../packaging/windows.md): `cpu`, `cpu-localllm`, `cpu-bge`, `cuda`, `cuda-bge`. The cross-compile path (`cross-windows`) also produces `bin/nornicdb.exe` from a non-Windows host.

The gap versus the Windows packaging doc:

- **No MSI installer.** No WiX or `go-msi` configuration, no Windows Service registration script, no Authenticode signing.
- **No winget manifest.** No `manifests/` entry submitted to `microsoft/winget-pkgs`.
- **No Chocolatey package.** No `nornicdb.nuspec` or chocolateyInstall script.

### Linux distribution (TODO)

The [Linux packaging doc](../packaging/linux.md) describes systemd, .deb, .rpm, and Snap distribution. None of those are wired:

- No `nornicdb.service` unit file in the repo.
- No `nfpm.yaml` or `debian/` directory for native package generation.
- No `snapcraft.yaml`.
- The "one-line install" curl-pipe URL `https://get.nornicdb.io/...` is not registered.

Cross-compiled binaries land in `bin/nornicdb-linux-amd64` and `bin/nornicdb-linux-arm64` so the underlying artifact exists; only the packaging metadata is missing.

### Raspberry Pi distribution (TODO)

Cross-compiled binaries (`rpi64`, `rpi32`, `rpi-zero`) exist. The [Raspberry Pi packaging doc](../packaging/raspberry-pi.md) describes a `get.nornicdb.io/pi` install script and a systemd unit. Neither exists in the repo. Pi distribution shares the Linux gap.

## Phased Plan

### Phase 1 — Honest signed releases (highest leverage)

Goal: turn `make cross-all` + `make macos-package` into a release that downstream installers can consume.

1. **Complete the broader release target** that:
    - Runs `make cross-all`.
    - Strips and codesigns the macOS binary with a Developer ID Application certificate (`codesign --sign "$DEV_ID" --options runtime`).
    - Reuses `make homebrew-artifacts` for darwin Homebrew tarballs.
    - Tars each linux binary into `nornicdb-linux-amd64.tar.gz` and `nornicdb-linux-arm64.tar.gz`.
    - Tars each Pi binary into `nornicdb-rpi{64,32,zero}.tar.gz`.
    - Computes SHA256 for every non-Homebrew tarball and writes `dist/release/SHA256SUMS`.
    - Notarizes the macOS .pkg via `notarytool submit ... --wait` and staples the ticket.

2. **Extend the existing GitHub Actions release workflow** triggered on `v*` tags that:
    - Already builds macOS pkg/dmg assets.
    - Already builds and attaches Homebrew tarballs plus `SHA256SUMS`.
    - Still needs notarized `.pkg` publishing and broader Linux/Pi release tarballs.
    - Surfaces the SHA256s in the action output for downstream consumers.

3. **Document the release contract** so other tap/manifest pipelines can rely on stable artifact names and locations.

Acceptance: `git tag v1.2.0 && git push --tags` produces a GitHub Release with seven artifact tarballs, a notarized .pkg, and a `SHA256SUMS` file, with no manual steps.

### Phase 2 — Homebrew tap

Depends on Phase 1.

1. **Create the `orneryd/homebrew-nornicdb` repo** from the tracked `homebrew/` scaffold.

2. **Maintain `Formula/nornicdb.rb`** matching the macOS packaging doc but using:
    - `service do ... end` (not the deprecated `plist do`) for `brew services start/stop nornicdb` LaunchAgent integration.
    - Architecture-split `on_macos do on_arm do ... on_intel do ... end end` blocks.
    - SHA256s pulled from release `SHA256SUMS`.
    - A `test do` block that runs `bin/nornicdb version`.
    - A Homebrew `post_install` first-run setup wizard that writes `etc/nornicdb/config.yaml`.

3. **Configure the tap-bump handoff**:
    - Main repo variable: `HOMEBREW_TAP_REPOSITORY=orneryd/homebrew-nornicdb`.
    - Main repo secret: `HOMEBREW_TAP_TOKEN`.
    - The main release workflow sends a `nornicdb-release` repository dispatch.
    - The tap workflow downloads `SHA256SUMS`, runs `scripts/update-formula.sh`, and opens a PR.

4. **Smoke test**: `brew tap orneryd/nornicdb && brew install nornicdb && brew services start nornicdb && curl http://localhost:7474/health`.

Acceptance: a fresh macOS host installs and runs NornicDB through Homebrew with no manual configuration.

### Phase 3 — Linux .deb / .rpm via nfpm

Depends on Phase 1.

1. **Add `nfpm.yaml`** at the repo root describing:
    - Binary install path: `/usr/local/bin/nornicdb`.
    - systemd unit install path: `/lib/systemd/system/nornicdb.service`.
    - Default config dir: `/etc/nornicdb/`.
    - Data dir: `/var/lib/nornicdb/`.
    - Log dir: `/var/log/nornicdb/`.
    - postinst hook: `useradd -r -s /usr/sbin/nologin nornicdb`, `systemctl daemon-reload`.
    - prerm hook: `systemctl stop nornicdb || true`.

2. **Add `nornicdb.service`** systemd unit file in `dist/linux/` matching the Linux packaging doc.

3. **Extend `make release`** to call `nfpm pkg --packager deb` and `nfpm pkg --packager rpm` for both arm64 and amd64. Output: `dist/release/nornicdb_<version>_<arch>.deb` and `nornicdb-<version>-<arch>.rpm`.

4. **Sign packages** (deb: `dpkg-sig`; rpm: `rpmsign --addsign`) using the project's Linux release key.

5. **Publish to a hosted repository**. Initial scope is "GitHub Release attachments installable via `dpkg -i`/`rpm -i`"; APT/YUM repos are deferred to a follow-up phase.

Acceptance: `dpkg -i nornicdb_*.deb && systemctl start nornicdb` works on a clean Ubuntu 24.04 image.

### Phase 4 — Raspberry Pi install path

Depends on Phase 3.

The Pi binaries ride on the Linux .deb pipeline. The remaining work is platform-specific UX:

1. **Add `dist/scripts/install-pi.sh`** that:
    - Detects the running kernel (`uname -m`) and selects `nornicdb-rpi{64,32,zero}` accordingly.
    - Downloads the right tarball from the latest GitHub Release.
    - Installs the binary, systemd unit, and a low-memory default config.
    - Enables and starts the service.

2. **Decide hosting for `get.nornicdb.io/pi`.** Either register the domain and serve `install-pi.sh` from object storage, or change the Raspberry Pi packaging doc to use the GitHub raw URL (`https://raw.githubusercontent.com/orneryd/nornicdb/main/dist/scripts/install-pi.sh`).

3. **Smoke test** on a real Pi 4 and Pi Zero 2 W.

Acceptance: `curl -sSL https://<install-host>/pi | bash` works on a fresh Raspberry Pi OS image.

### Phase 5 — Windows MSI + winget

Depends on Phase 1.

1. **Add WiX or `go-msi` configuration** in `dist/windows/` covering:
    - Binary install (`%ProgramFiles%\NornicDB\nornicdb.exe`).
    - Windows Service registration via `sc create` postinstall (or `[ServiceInstall]` in WiX).
    - Default data dir `%ProgramData%\NornicDB\data`.
    - Start menu shortcuts for the dashboard URL.

2. **Authenticode-sign** the resulting MSI with the project's code-signing certificate.

3. **Add a winget manifest** under a `manifests/` directory and submit a PR to `microsoft/winget-pkgs`. Each tagged release submits an updated manifest.

4. **Add a Chocolatey nuspec** and `chocolateyInstall.ps1` that downloads the MSI and runs `Install-ChocolateyPackage`.

Acceptance: `winget install nornicdb` and `choco install nornicdb` both produce a running Windows Service.

## Cross-Cutting Work

### Codesigning posture

- macOS: Developer ID Application certificate stored in GitHub Actions encrypted secrets. Notarization uses `notarytool` with an app-specific Apple ID password.
- Windows: EV or OV code-signing certificate, also via Actions secrets, signed with `signtool`.
- Linux: GPG key for `dpkg-sig` and `rpmsign`. Public key published at a stable URL so users can verify.

### `nornicdb install` subcommand (referenced by the Linux/Pi packaging docs)

The Linux and Pi packaging docs describe a `nornicdb install` subcommand that registers the systemd service. That subcommand does not exist in `cmd/nornicdb/main.go` today. Either:

- Add the subcommand (writes the unit file and calls `systemctl daemon-reload`), **or**
- Update the packaging docs to describe the `dpkg -i`/`rpm -i` flow that comes for free with the .deb/.rpm packages from Phase 3.

The second option is cheaper and matches what most users actually do; recommend deleting the `nornicdb install` claim from the packaging docs once Phase 3 lands.

### Reuse of existing Go-API behavior

None of the packaging work changes the runtime. All targets ultimately invoke `nornicdb serve` with a data directory, so `db.Open()` / shutdown-and-recovery semantics are unchanged. WAL retention and MVCC lifecycle behavior are platform-agnostic and need no per-package customization.

## Out of Scope for This Plan

- Cloud-hosted install services (e.g. NornicDB Cloud).
- Signed APT/YUM repositories (deferred follow-up after Phase 3).
- macOS App Store submission (sandbox restrictions make a service-style binary infeasible).
- Snap (the Linux packaging doc lists it; Snap's auto-update model conflicts with NornicDB's data-directory layout — recommend dropping Snap from scope unless a specific user need surfaces).

## Related Documentation

- [Packaging README](../packaging/README.md) — desired user experience per platform.
- [macOS Deployment](../packaging/macos.md), [Windows](../packaging/windows.md), [Linux](../packaging/linux.md), [Raspberry Pi](../packaging/raspberry-pi.md) — per-platform packaging targets.
- [Configuration](../operations/configuration.md) — runtime config consumed by every package.
- [Backup & Restore](../operations/backup-restore.md) — admin actions independent of the packaging channel.
