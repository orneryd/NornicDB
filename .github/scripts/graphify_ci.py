"""Run trusted release-branch Graphify tooling against immutable PR data."""
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from graphify_gate import API

MARKER = "<!-- nornicdb-release-graphify -->"


def upsert(api, number, body):
    body = MARKER + "\n" + body
    for page in range(1, 101):
        comments = api.request("GET", f"/repos/{api.repo}/issues/{number}/comments?per_page=100&page={page}")
        for comment in comments:
            if MARKER in (comment.get("body") or "") and (comment.get("user") or {}).get("login") == "github-actions[bot]":
                if comment.get("body") != body:
                    api.request("PATCH", f"/repos/{api.repo}/issues/comments/{comment['id']}", {"body": body})
                return
        if len(comments) < 100:
            api.request("POST", f"/repos/{api.repo}/issues/{number}/comments", {"body": body})
            return
    raise RuntimeError("Too many PR comments to locate the Graphify report")


def tools():
    root = Path(os.environ["TRUSTED_RELEASE_ROOT"]).resolve()
    sys.path.insert(0, str(root / "scripts"))
    return root, importlib.import_module("code_intelligence"), importlib.import_module("pr_duplicates")


def snapshot():
    root, ci, pd = tools()
    number = int(os.environ["PR_NUMBER"])
    base, head = os.environ["PR_BASE_SHA"], os.environ["PR_HEAD_SHA"]
    for value in (base, head):
        if len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("invalid PR commit ID")
    # Fetch the PR head into a private ref. Its contents are extracted as data;
    # no checkout, executable, dependency file or script from the PR is run.
    subprocess.run(["git", "-C", str(root), "fetch", "--no-tags", "origin",
                    f"refs/pull/{number}/head:refs/graphify/pr-head"], check=True)
    fetched = ci.git(root, "rev-parse", "refs/graphify/pr-head")
    if fetched != head:
        raise RuntimeError("PR head changed after authorization; rerun the check for the new head")
    directory = Path(os.environ["RUNNER_TEMP"]) / "nornic-pr-graphify"
    directory.mkdir()
    snapshot = ci.snapshot(root, head, directory)
    return ci, snapshot, directory


def prepare():
    ci, source, directory = snapshot()
    ci.extract(source, directory / "extract")


def check():
    snapshot()
    root, ci, pd = tools()
    number = int(os.environ["PR_NUMBER"])
    base, head = os.environ["PR_BASE_SHA"], os.environ["PR_HEAD_SHA"]
    if ci.git(root, "rev-parse", "refs/graphify/pr-head") != head:
        raise RuntimeError("Prepared PR head no longer matches the authorized head")
    merge_base = ci.git(root, "merge-base", base, head)
    diff = pd.parse_diff(pd.git_diff(str(root), merge_base, head))
    uri, user, password, token = ci.connection()
    repo = os.environ["GITHUB_REPOSITORY"]
    database = os.environ.get("NORNICDB_DATABASE") or "nornicdbcode"
    directory = Path(os.environ["RUNNER_TEMP"]) / "nornic-pr-graphify"
    source = directory / "source"
    graph = Path(os.environ["RUNNER_TEMP"]) / "graphify-artifact" / "graph.json"
    db = pd.NornicDB(uri, user, password, token, database, require_scores=True,
                     repo=repo, deadline=time.monotonic() + 300)
    report = pd.analyze(db, repo, graph, str(source), diff, head_sha=head,
                        base_sha=merge_base, ignore=pd.DEFAULT_IGNORE)
    summary = pd.render_summary(report, server=os.environ.get("GITHUB_SERVER_URL", "https://github.com"))
    if report.nothing_compared:
        heading = "**Check incomplete:** no graph was available to compare."
    elif not report.findings and report.skipped_by_cap:
        heading = "**Check incomplete:** no findings among the functions analyzed; some were skipped."
    elif not report.findings:
        heading = "**No findings.**"
    else:
        heading = "**Advisory findings:** review the candidates below."
    body = heading + "\n\n" + summary + "\n\nThis check is informative and does not block the PR."
    api = API(os.environ["GITHUB_API_URL"], repo, os.environ["GITHUB_TOKEN"])
    current = api.request("GET", f"/repos/{repo}/pulls/{number}")
    if current.get("state") != "open" or current["head"]["sha"] != head or current["base"]["sha"] != base:
        print("PR changed while the check was running; discarding the stale report.")
        return
    upsert(api, number, body)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        Path(os.environ["GITHUB_STEP_SUMMARY"]).write_text(body + "\n", encoding="utf-8")


def failed():
    api = API(os.environ["GITHUB_API_URL"], os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    run = f"{os.environ['GITHUB_SERVER_URL']}/{api.repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    upsert(api, int(os.environ["PR_NUMBER"]),
           f"**Check incomplete.** Graphify could not finish; [inspect the workflow run]({run}). "
           "This advisory check does not block the PR.")


def ingest():
    root, ci, _ = tools()
    branch = os.environ["RELEASE_BRANCH"]
    if not branch.startswith("release/"):
        raise ValueError("a release branch is required")
    os.environ["CODE_INTEL_REPO"] = os.environ["GITHUB_REPOSITORY"]
    os.environ.setdefault("NORNICDB_DATABASE", "nornicdbcode")
    ci.ingest(root, "HEAD", branch_override=branch)


if __name__ == "__main__":
    {"prepare": prepare, "check": check, "failed": failed, "ingest": ingest}[sys.argv[1]]()
