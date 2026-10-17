#!/usr/bin/env bash
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(git config --bool --get codeIntelligence.enabled || true)" != "true" ]; then
    exit 0
fi
if [ "${1:-}" = "post-update" ] && [ "$(git branch --show-current)" != "main" ]; then
    exit 0
fi
python="$(git config --get codeIntelligence.python || true)"
python="${python:-${CODE_INTEL_PYTHON:-$repo_root/.venv-code-intelligence/bin/python}}"
if [ ! -x "$python" ]; then
    echo "code-intelligence: Python environment missing: $python" >&2
    echo "Create a venv, install scripts/requirements-code-intelligence.txt, and set git config codeIntelligence.python /absolute/path/to/python." >&2
    exit 1
fi
command="${1:?hook command required}"
shift
exec "$python" "$script_dir/code_intelligence.py" "$command" --repo-root "$repo_root" "$@"
