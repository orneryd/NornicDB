#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
git config --local codeIntelligence.hookRunner "$repo_root/scripts/code-intelligence-hook.sh"
case "${1:-}" in
    "") ;;
    --with-code-intelligence) git config --local codeIntelligence.enabled true ;;
    --without-code-intelligence) git config --local codeIntelligence.enabled false ;;
    *) echo "usage: scripts/install-git-hooks.sh [--with-code-intelligence|--without-code-intelligence]" >&2; exit 2 ;;
esac
# git-path honors linked worktrees and a configured core.hooksPath.
for hook in pre-commit post-merge post-rewrite; do
    target="$(git rev-parse --git-path "hooks/$hook")"
    mkdir -p "$(dirname "$target")"
    cp "$repo_root/.githooks/$hook" "$target"
    chmod +x "$target"
    echo "installed git hook: $target"
done
if [ "$(git config --bool --get codeIntelligence.enabled || true)" = "true" ]; then
    echo "Code intelligence enabled: exports NORNICDB_URI and credentials must be available to terminal/IDE Git processes."
    echo "Setup: docs/user-guides/graphify-local.md"
fi
