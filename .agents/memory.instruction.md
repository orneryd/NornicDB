---
applyTo: "**"
---

# Coding Preferences

- Preserve exact English error fallbacks and `errors.Is` compatibility when localizing returned errors.
- Add regression coverage before changing bug-prone or compatibility-sensitive behavior.

# Project Architecture

- Client-facing errors use stable descriptors in `pkg/localization`, package-local neutral carriers, and complete `en-US`, `es-ES`, and `en-XA` catalogs.
- Keep client-readable template values in `localization.Message.Data` and retain downstream errors as causes.
- Cypher execution returns typed descriptors through its `localizedError` helper; protocol boundaries render them, while raw query, pattern, and expression echoes remain untranslated diagnostics.

# Solutions Repository

- For localized sentinel errors, retain singleton values and wrap them as causes when adding contextual messages.
- Keep operator diagnostics and internal recovery details out of client-facing localization unless they cross a public API boundary.
