#!/usr/bin/env python3
"""Local action-format ingestion and staged duplicate checks; connection credentials use ENV configuration."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import urlsplit

SETUP = ("Set NORNICDB_URI and either NORNICDB_TOKEN or both NORNICDB_USER and "
         "NORNICDB_PASSWORD in the environment of your terminal/IDE. "
         "See docs/user-guides/graphify-local.md#setup. No credentials belong in Git.")
MANDATORY = ("MANDATORY COMMIT CHECK: Do not override, bypass or disable this hook. "
             "Fix the reported database, ingestion, search-readiness or duplicate-code problem "
             "before proceeding. This prevents duplicate, divergent code from being checked in.")
REFACTOR_INSTRUCTIONS = """MANDATORY REFACTOR BEFORE CHECK-IN
This commit is blocked. Both reuse candidates and refactor targets require resolution.
1. Inspect every reported implementation in the current source and its callers.
   Establish whether they perform the same required behavior; a similarity score
   alone is not proof of equivalence. Confirm that the indexed revision is current.
2. For a reuse candidate, extend or call the existing owner instead of adding a
   parallel implementation. For refactor targets, converge all affected paths into
   one canonical owner and migrate every caller, then remove redundant logic.
   Delete redundant forwarding and compatibility wrappers, aliases and shims.
   Migrate every caller to the canonical API, then remove the obsolete entry
   points. Do not leave a tiny delegating wrapper as a substitute for completing
   the refactor; preserve required behavior by updating the actual consumers.
   When two command constructors mix shared root setup with different subcommands,
   move root setup into one shared owner, keep each binary's command registration
   separate, migrate entry points and tests, then delete the old constructors.
   Rewriting a function does not retire its indexed implementation: only code
   actually deleted in this check-in is excluded from duplicate findings.
   If a deleted owner is still reported, verify its declaration is removed in
   this staged diff and absent from the staged graph; correct declaration
   detection with a regression test. Any other live match still requires reuse.
3. Do not leaf-patch divergent paths, mask failures, delete required behavior,
   suppress findings, lower thresholds, or bypass/disable this hook to commit.
4. Verify the complete affected behavior with focused regression tests and the
   required package checks. For a proven incorrect finding, correct its causal
   detector/indexing defect with regression evidence; do not refactor unrelated
   behaviors merely to silence similarity. No unresolved finding permits check-in.
5. Stage the complete correction and rerun this check. Check-in may proceed only
   after the mandatory check succeeds against the corrected source.
"""
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
    repo = os.environ.get("CODE_INTEL_REPO") or gi.repo_from_git(root)
    per_repo = gi.git_output(root, "config", "--get", "codeIntelligence.database")
    global_mode = gi.git_output(root, "config", "--bool", "--get", "codeIntelligence.databaseFromRepo") == "true"
    database = per_repo or os.environ.get("NORNICDB_DATABASE") or (repo.rsplit("/", 1)[-1] if global_mode else gi.DEFAULT_DATABASE)
    if not database.strip() or ":" in database or database.startswith("_"):
        raise CheckError("Invalid database name; set NORNICDB_DATABASE or git config codeIntelligence.database.")
    return repo, database


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


def ingest(root, revision="HEAD", force=False, branch_override=None):
    import graphify_local as gi
    uri, user, password, token = connection()
    repo, database = identity(root)
    commit = git(root, "rev-parse", "--verify", f"{revision}^{{commit}}")
    branch = branch_override or git(root, "branch", "--show-current")
    global_mode = gi.git_output(root, "config", "--bool", "--get", "codeIntelligence.databaseFromRepo") == "true"
    main = os.environ.get("CODE_INTEL_MAIN", gi.git_output(root, "config", "--get", "codeIntelligence.main") or ("" if global_mode else gi.DEFAULT_MAIN))
    cap = int(os.environ.get("CODE_INTEL_MAX_EMBED_CHARS", "0"))
    if cap and cap < gi.MIN_EMBED_CHARS:
        raise CheckError(f"CODE_INTEL_MAX_EMBED_CHARS must be 0 or at least {gi.MIN_EMBED_CHARS}.")
    state = None if force else gi.last_ingested_state(uri, user, password, database, repo, token)
    if state and state.get("commit") == commit and state.get("main_spec") == main and int(state.get("max_embed_chars") or 0) == cap:
        print(f"Code graph is current at {commit[:12]}.")
        return
    print(f"Updating {repo} at {commit} in database {database}.", flush=True)
    with tempfile.TemporaryDirectory(prefix="nornic-ingest-") as temp:
        directory = Path(temp)
        source = snapshot(root, commit, directory)
        # Preserve source-history times even though the immutable export has no .git directory.
        for path, mtime in gi.git_file_times(root, commit).items():
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
    if not any(Path(path).suffix.lower() in CODE_SUFFIXES and data.added and not pd.ignored(path, pd.DEFAULT_IGNORE) for path, data in diff.items()):
        print("Code intelligence: no staged source additions to check.")
        return
    uri, user, password, token = connection()
    repo, database = identity(root)
    print(f"Mandatory duplicate check: repository={repo}, database={database}, "
          f"endpoint={urlsplit(uri)._replace(query='', fragment='').geturl()}", flush=True)
    revision = subprocess.run(["git", "-C", str(root), "rev-parse", "--verify", "--quiet", "HEAD"],
                              capture_output=True, text=True)
    if revision.returncode not in (0, 1):
        raise CheckError(f"git rev-parse failed: {revision.stderr.strip()}")
    head = revision.stdout.strip() if revision.returncode == 0 else ""
    staged = git(root, "write-tree")
    import duplicate_check_cache as cache
    cache_db = pd.NornicDB(uri, user, password, token, database, require_scores=True, repo=repo,
                           deadline=time.monotonic()+30)
    baseline = cache.baseline(cache_db, repo)
    receipt_path, receipt_key = cache.receipt(root, staged, head,
        {'uri':uri,'repo':repo,'database':database}, baseline)
    if cache.passed(receipt_path, receipt_key):
        if git(root, "write-tree") != staged:
            raise CheckError("Index changed during cached-pass validation")
        print("Mandatory duplicate check: cached successful result for unchanged staged tree and graph baseline.")
        return
    with tempfile.TemporaryDirectory(prefix="nornic-staged-") as temp:
        directory = Path(temp)
        source = snapshot(root, staged, directory)
        graph = extract(source, directory / "extract")
        deadline_seconds = int(os.environ.get("CODE_INTEL_DEADLINE_SECONDS", "300"))
        db = pd.NornicDB(uri, user, password, token, database, require_scores=True, repo=repo,
                         deadline=time.monotonic() + deadline_seconds)
        analysis_started = time.monotonic()
        try:
            report = pd.analyze(db, repo, graph, source, diff, head_sha=head, base_sha=head,
                                options={"search_limit": 1000, "max_candidates": sys.maxsize, "min_added_fraction": 0.0, "max_matches": 0,
                                         "deadline_seconds": deadline_seconds,
                                         "search_workers": int(os.environ.get("CODE_INTEL_SEARCH_WORKERS", "4"))},
                                ignore=pd.DEFAULT_IGNORE)
        except pd.AnalysisError as error:
            raise CheckError(str(error)) from error
        print(f"Bounded-match analysis completed in {time.monotonic() - analysis_started:.2f}s "
              f"(limit 1000, RRF score floor {db.min_rrf_score:g}).")
        print(pd.render_summary(report, max_findings=sys.maxsize), flush=True)
        if report.nothing_compared:
            raise CheckError(report.nothing_compared + " Run scripts/code_intelligence.py ingest first.")
        if report.skipped_by_cap:
            raise CheckError("Duplicate analysis was incomplete. Increase CODE_INTEL_DEADLINE_SECONDS and retry.")
        if report.reuse_candidates or report.refactor_targets:
            print(REFACTOR_INSTRUCTIONS, flush=True)
            raise CheckError("Commit blocked: fold the reported reuse/refactor candidates into their existing owner before retrying.")
        if git(root, "write-tree") != staged:
            raise CheckError("Index changed during duplicate analysis; rerun check-in.")
        if cache.baseline(cache_db, repo) != baseline:
            raise CheckError("Graph baseline changed during duplicate analysis; rerun check-in.")
        cache.record(receipt_path, receipt_key)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["ingest", "check-staged", "post-update", "main-updated"])
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--revision", default="HEAD", help="immutable commit to ingest")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    root = args.repo_root.resolve()
    try:
        if args.command in ("main-updated", "post-update"):
            if args.command == "post-update" and git(root, "branch", "--show-current") != "main":
                return 0
            try:
                ingest(root, args.revision, branch_override="main")
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
    except (CheckError, OSError, ValueError, RuntimeError) as error:
        print(f"code-intelligence: {error}", file=sys.stderr)
    except Exception as error:
        # HTTP client and extraction dependencies have their own exception types.
        # Never treat an unavailable/failed check as a successful commit check.
        print(f"code-intelligence: {type(error).__name__}: analysis/ingestion failed. "
              "Verify NORNICDB_URI, credentials and service readiness; see the local Graphify guide.", file=sys.stderr)
    else:
        return 0
    if args.command == "check-staged":
        import duplicate_check_cache as cache
        cache.invalidate(root)
        print(MANDATORY, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
