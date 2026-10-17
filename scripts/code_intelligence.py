#!/usr/bin/env python3
"""Local action-format ingestion and staged duplicate checks; configuration is ENV-only."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from urllib.parse import urlsplit

SETUP = ("Set NORNICDB_URI and either NORNICDB_TOKEN or both NORNICDB_USER and "
         "NORNICDB_PASSWORD in the environment of your terminal/IDE. "
         "See docs/user-guides/graphify-local.md#configuration. No credentials belong in Git.")
CODE_SUFFIXES = set(".go .py .js .jsx .ts .tsx .java .rb .rs .c .h .cpp .cc .hpp .cs .swift .kt .kts .php .scala .sh .bash .zsh .lua .ex .exs .sql .m .mm .vue .svelte".split())


class CheckError(Exception):
    pass


def git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
    if result.returncode:
        raise CheckError(f"git {args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def connection():
    uri = os.environ.get("NORNICDB_URI", "").rstrip("/")
    token = os.environ.get("NORNICDB_TOKEN", "")
    user, password = os.environ.get("NORNICDB_USER", ""), os.environ.get("NORNICDB_PASSWORD", "")
    if not uri or not (token or (user and password)):
        raise CheckError(SETUP)
    parsed = urlsplit(uri)
    if parsed.username or parsed.password:
        raise CheckError("Keep credentials out of NORNICDB_URI. " + SETUP)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise CheckError("The duplicate check needs an HTTP(S) NORNICDB_URI. " + SETUP)
    if parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
        raise CheckError("Use HTTPS for a remote NORNICDB_URI, or HTTP on loopback for local development.")
    return uri, user, password, token


def identity(root):
    import graphify_local as gi
    return (os.environ.get("CODE_INTEL_REPO") or gi.repo_from_git(root),
            os.environ.get("NORNICDB_DATABASE") or gi.DEFAULT_DATABASE)


def extract(root, output):
    executable = os.environ.get("CODE_INTEL_GRAPHIFY") or str(Path(sys.executable).parent / "graphify")
    if not Path(executable).is_file():
        executable = shutil.which("graphify")
    if not executable:
        raise CheckError("Graphify is missing. Install scripts/requirements-code-intelligence.txt "
                         "in a virtual environment and configure codeIntelligence.python; see the local Graphify guide.")
    result = subprocess.run([executable, "extract", ".", "--code-only", "--no-cluster", "--out", str(output)], cwd=root)
    if result.returncode:
        raise CheckError("Graphify extraction failed; no successful check or ingest was recorded.")
    graph = output / "graphify-out" / "graph.json"
    if not graph.is_file():
        raise CheckError("Graphify did not produce graphify-out/graph.json.")
    return graph


def snapshot(root, tree, directory):
    archive = directory / "source.tar"
    git(root, "archive", "--format=tar", f"--output={archive}", tree)
    source = directory / "source"
    source.mkdir()
    with tarfile.open(archive) as contents:
        contents.extractall(source, filter="data")
    return source


def ingest(root, revision="HEAD", force=False):
    import graphify_local as gi
    uri, user, password, token = connection()
    repo, database = identity(root)
    commit = git(root, "rev-parse", "--verify", f"{revision}^{{commit}}")
    branch = git(root, "branch", "--show-current")
    main = os.environ.get("CODE_INTEL_MAIN", gi.DEFAULT_MAIN)
    cap = int(os.environ.get("CODE_INTEL_MAX_EMBED_CHARS", "0"))
    if cap and cap < gi.MIN_EMBED_CHARS:
        raise CheckError(f"CODE_INTEL_MAX_EMBED_CHARS must be 0 or at least {gi.MIN_EMBED_CHARS}.")
    state = None if force else gi.last_ingested_state(uri, user, password, database, repo, token)
    if state and state.get("commit") == commit and state.get("main_spec") == main and int(state.get("max_embed_chars") or 0) == cap:
        print(f"Code graph is current at {commit[:12]}.")
        return
    with tempfile.TemporaryDirectory(prefix="nornic-ingest-") as temp:
        directory = Path(temp)
        source = snapshot(root, commit, directory)
        # Preserve source-history times even though the immutable export has no .git directory.
        for path, mtime in gi.git_file_times(root).items():
            target = source / path
            if target.is_file() and not target.is_symlink():
                os.utime(target, (mtime, mtime))
        graph = extract(source, directory / "extract")
        chosen = None if main.lower() == gi.NO_MAIN else gi.choose_main(graph, main)
        gi.import_graph(graph, uri, user, password, 2000, str(source), database, repo,
                        commit=commit, branch=branch, main=chosen,
                        main_spec=main if chosen else gi.NO_MAIN, token=token, max_embed_chars=cap)


def check_staged(root):
    import pr_duplicates as pd
    raw = git(root, "diff", "--cached", "--unified=0", "--no-renames", "--no-color")
    diff = pd.parse_diff(raw)
    if not any(Path(path).suffix.lower() in CODE_SUFFIXES and data.added for path, data in diff.items()):
        print("Code intelligence: no staged source additions to check.")
        return
    uri, user, password, token = connection()
    repo, database = identity(root)
    head = git(root, "rev-parse", "HEAD")
    staged = git(root, "write-tree")
    with tempfile.TemporaryDirectory(prefix="nornic-staged-") as temp:
        directory = Path(temp)
        source = snapshot(root, staged, directory)
        graph = extract(source, directory / "extract")
        db = pd.NornicDB(uri, user, password, token, database)
        report = pd.analyze(db, repo, graph, source, diff, head_sha=head, base_sha=head,
                            options={"max_candidates": sys.maxsize,
                                     "deadline_seconds": int(os.environ.get("CODE_INTEL_DEADLINE_SECONDS", "300"))},
                            ignore=pd.DEFAULT_IGNORE)
        print(pd.render_summary(report, max_findings=sys.maxsize))
        if report.nothing_compared:
            raise CheckError(report.nothing_compared + " Run scripts/code_intelligence.py ingest first.")
        if report.skipped_by_cap:
            raise CheckError("Duplicate analysis was incomplete. Increase CODE_INTEL_DEADLINE_SECONDS and retry.")
        if report.reuse_candidates or report.refactor_targets:
            raise CheckError("Commit blocked: fold the reported reuse/refactor candidates into their existing owner before retrying.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["ingest", "check-staged", "post-update"])
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--revision", default="HEAD", help="immutable commit to ingest")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    root = args.repo_root.resolve()
    try:
        if args.command == "post-update":
            if git(root, "branch", "--show-current") != "main":
                return 0
            try:
                ingest(root)
            except Exception:
                print("main was updated, but its graph refresh failed. Fix configuration/service availability "
                      "and run scripts/code_intelligence.py ingest. The Git update is not rolled back.", file=sys.stderr)
                raise
        elif args.command == "ingest":
            ingest(root, args.revision, args.force)
        else:
            check_staged(root)
    except ImportError:
        print("code-intelligence: Python dependencies are missing. Install "
              "scripts/requirements-code-intelligence.txt into the configured Python environment.", file=sys.stderr)
        return 1
    except (CheckError, OSError, ValueError, RuntimeError) as error:
        print(f"code-intelligence: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        # HTTP client and extraction dependencies have their own exception types.
        # Never treat an unavailable/failed check as a successful commit check.
        print(f"code-intelligence: {type(error).__name__}: analysis/ingestion failed. "
              "Verify NORNICDB_URI, credentials and service readiness; see the local Graphify guide.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
