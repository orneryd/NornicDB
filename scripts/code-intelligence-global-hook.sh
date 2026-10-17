#!/usr/bin/env bash
set -euo pipefail
hook="${1:?hook name required}"
shift
runtime="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
previous="$(git config --global --get codeIntelligence.previousHooksPath || true)"
if [ -n "$previous" ]; then
    case "$previous" in '~/'*) previous="$HOME/${previous#\~/}" ;; esac
    native="$previous/$hook"
else
    native="$(git rev-parse --git-common-dir)/hooks/$hook"
fi
# A reference update's stdin is consumed independently by both chained hooks.
input=""
if [ "$hook" = reference-transaction ]; then
    input="$(mktemp "${TMPDIR:-/tmp}/nornic-ref-hook.XXXXXX")"
    trap 'rm -f "$input"' EXIT
    cat > "$input"
fi
if [ -x "$native" ]; then
    if [ -n "$input" ]; then "$native" "$@" < "$input"; else "$native" "$@"; fi
    # Repository-managed hooks already call the same runner.
    if grep -q '^# nornic-code-intelligence-managed$' "$native" 2>/dev/null; then exit 0; fi
fi
case "$hook" in
    pre-commit) "$runtime/code-intelligence-hook.sh" check-staged ;;
    reference-transaction)
        if [ "${1:-}" = committed ]; then
            while read -r old new ref; do
                if [ "$ref" = refs/heads/main ] && [ "$new" != "$old" ] && [ -n "${new//0/}" ]; then
                    "$runtime/code-intelligence-hook.sh" main-updated --revision "$new"
                fi
            done < "$input"
        fi ;;
esac
