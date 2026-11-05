"""Select the release branch for a merge-triggered or authorized manual refresh."""
import json
import os

from graphify_gate import API

event = json.loads(open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8").read())
if os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch":
    api = API(os.environ["GITHUB_API_URL"], os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    if not api.maintainer((event.get("sender") or {}).get("login")):
        raise SystemExit("Only maintainers and administrators may start a manual graph refresh")
    branch = (event.get("inputs") or {}).get("branch", "")
else:
    pr = event.get("pull_request") or {}
    if not pr.get("merged"):
        raise SystemExit("The pull request was not merged")
    branch = (pr.get("base") or {}).get("ref", "")
if not branch.startswith("release/") or any(c in branch for c in "\r\n"):
    raise SystemExit("A release branch is required")
with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
    output.write(f"branch={branch}\n")
