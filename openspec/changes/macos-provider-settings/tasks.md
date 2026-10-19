- [x] Create fix/macos-provider-settings from origin/release/v1.4.4; preserve paused #1487.
- [x] Identify installer environment/first-run resets and settings provider/model coercion.
- [x] Confirm three-step a-la-carte layout and loading existing settings on install.
- [x] Reproduce config loss with focused tests before changing its owner.
- [x] Preserve upgrade config/environment/LaunchAgent and unify installer postinstall logic.
- [x] Support and display existing per-service providers/settings without remote GGUF coercion.
- [x] Share independent AI controls in Settings/setup; implement three-step review and confirmations.
- [x] Add shared optional Advanced tab in setup/Settings; load and preserve existing environment/runtime arguments, validate and apply at startup.
- [x] Nine focused Swift tests, two isolated installer tests, shell syntax checks, native app/server builds, Lite/Full packages.
- [x] Install rebuilt app/server; verify restored config/environment/plist hashes unchanged and live OpenAI configuration.
- [ ] Complete required verification. After pausing all hooks per maintainer direction, verify-list passed; full verification is next. Earlier failed/canceled attempts and fixture cleanup remain recorded.
- [ ] Validate latest packaged postinstall GUI-domain correction on a new privileged install (second authentication canceled).
- [ ] Rebuild/reinstall, restore local OpenAI settings, re-embed nornicdbcode.
- [ ] Publish separate release PR and verification evidence; resume #1487.

- [x] Maintainer clarified: no additional feature scope; pause all installed duplication/ingest hooks and continue this installer work.

- [x] Reproduce missing upgrade review marker against previous postinstall.
- [x] Stop both agents and retire old installed executable payloads; reject replacement while an old process remains alive. Isolated tests preserve config/data/models.
- [x] Add per-user app lock, legacy-copy retirement, exact canonical launch path and reopen routing for pending setup.
- [x] Thirteen Swift regressions and seven isolated installer tests pass on the corrected lifecycle source.
- [ ] Build corrected lifecycle package and verify actual replacement, one tray owner and visible review wizard on local reinstall.

- [x] Restore shared K-Means, memory decay and Auto-TLP switches in setup AI page and Settings AI features; show choices on Review.
- [x] Close setup after launch submission and keep startup status yellow until actual readiness or process failure.

- [x] Restore default local model download action directly in setup and Settings AI controls using the existing shared downloader.
