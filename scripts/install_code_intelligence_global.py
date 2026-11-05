#!/usr/bin/env python3
"""Install a standalone user-wide code-intelligence runtime and chained Git hooks."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess

HOOKS = "applypatch-msg pre-applypatch post-applypatch pre-commit pre-merge-commit prepare-commit-msg commit-msg post-commit pre-rebase post-checkout post-merge pre-push pre-receive update post-receive post-update push-to-checkout pre-auto-gc post-rewrite sendemail-validate reference-transaction".split()


def config(*args):
    return subprocess.run(["git", "config", "--global", *args], capture_output=True, text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--destination", type=Path, default=Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "nornicdb-code-intelligence")
    args = parser.parse_args()
    python = args.python.absolute()
    if not os.access(python, os.X_OK):
        parser.error("--python must identify an executable environment with requirements-code-intelligence.txt installed")
    source = Path(__file__).resolve().parent
    home = args.destination.absolute()
    runtime, hooks = home / "runtime", home / "hooks"
    runtime.mkdir(parents=True, exist_ok=True)
    hooks.mkdir(parents=True, exist_ok=True)
    for name in ["code_intelligence.py", "duplicate_check_cache.py", "duplicate_ownership.py", "graphify_local.py", "pr_duplicates.py", "code-intelligence-hook.sh", "requirements-code-intelligence.txt"]:
        shutil.copy2(source / name, runtime / name)
    binding_runtime = runtime / "go_owner_bindings"
    binding_runtime.mkdir(exist_ok=True)
    binding_sources = {p.name: p for p in (source / "go_owner_bindings").glob("*.go") if not p.name.endswith("_test.go")}
    for name, path in binding_sources.items():
        shutil.copy2(path, binding_runtime / name)
    for path in binding_runtime.glob("*.go"):
        if path.name not in binding_sources:
            path.unlink()
    shutil.copy2(source / "code-intelligence-global-hook.sh", runtime / "code-intelligence-global-hook.sh")
    previous = config("--get", "core.hooksPath").stdout.strip()
    if previous and Path(previous).expanduser().absolute() != hooks:
        config("codeIntelligence.previousHooksPath", previous).check_returncode()
    for name in HOOKS:
        # No shell interpolation of installation paths: the wrapper resolves its own location.
        (hooks / name).write_text('#!/usr/bin/env bash\nexec "$(dirname "$0")/../runtime/code-intelligence-global-hook.sh" "$(basename "$0")" "$@"\n')
        (hooks / name).chmod(0o755)
    for key, value in [("codeIntelligence.python", str(python)),
                       ("codeIntelligence.hookRunner", str(runtime / "code-intelligence-hook.sh")),
                       ("codeIntelligence.enabled", "true"),
                       ("codeIntelligence.databaseFromRepo", "true"),
                       ("core.hooksPath", str(hooks))]:
        config(key, value).check_returncode()
    print(f"Global hooks installed at {hooks}; runtime at {runtime}.")
    print("URLs and credentials must be exported to Git processes. Repository core.hooksPath overrides take precedence.")


if __name__ == "__main__":
    main()
