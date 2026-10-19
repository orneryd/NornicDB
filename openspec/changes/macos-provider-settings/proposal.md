# Preserve macOS configuration and offer independent AI services

Maintainer request: a separate PR from release/v1.4.4. Installer upgrades and
Settings must preserve existing YAML, environment and provider settings, and
show them accurately. Support independent external/local Embeddings, Reranking
and Heimdall configuration. Replace feature bundles with three short setup steps:
Server/Security, optional AI services, Review. Reuse the same AI controls in
Settings. Disabled features retain their configuration. Only local models use
GGUF names/download requirements. Stop and Restart buttons require confirmation.

After verification, rebuild/reinstall locally, restore the intended OpenAI
text-embedding-3-small configuration with ten workers without exposing secrets,
then re-embed the existing nornicdbcode graph. Resume paused #1487 afterward.
On October 18 the maintainer explicitly requested temporarily disabling all
installed duplication and automatic-ingestion Git hooks. Their entries are
preserved as .disabled files; do not restore them without maintainer direction.
This is a local operational change, not a permanent bypass in this PR.

An optional Advanced tab in setup and Settings edits persistent server environment
variables and runtime arguments. Load the existing LaunchAgent environment and
argument array; preserve argument boundaries (no shell evaluation). Show secrets
as protected values. Preserve custom settings on upgrade and use the same launch
configuration whenever the server starts. Advanced overrides must have explicit
precedence over ordinary settings, with conflicting values visible to the user.
The three-step setup remains the primary flow; Advanced is optional.
