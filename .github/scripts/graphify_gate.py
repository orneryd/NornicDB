"""Authorize release PR graph checks without executing pull-request code."""
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

MARKER = "<!-- nornicdb-graphify-approval -->"


class API:
    def __init__(self, base, repo, token):
        self.base, self.repo, self.token = base.rstrip("/"), repo, token

    def request(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json",
            "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28"})
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else None

    def maintainer(self, login):
        if not login:
            return False
        try:
            result = self.request("GET", f"/repos/{self.repo}/collaborators/{urllib.parse.quote(login, safe='')}/permission")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return False
            raise
        return result.get("permission") == "admin" or result.get("role_name") in ("admin", "maintain")

    def notice(self, number, text, *, create):
        body = MARKER + "\n" + text
        for page in range(1, 101):
            comments = self.request("GET", f"/repos/{self.repo}/issues/{number}/comments?per_page=100&page={page}")
            for comment in comments:
                if MARKER in (comment.get("body") or "") and (comment.get("user") or {}).get("login") == "github-actions[bot]":
                    if comment.get("body") != body:
                        self.request("PATCH", f"/repos/{self.repo}/issues/comments/{comment['id']}", {"body": body})
                    return
            if len(comments) < 100:
                if create:
                    self.request("POST", f"/repos/{self.repo}/issues/{number}/comments", {"body": body})
                return
        raise RuntimeError("Too many PR comments to locate the Graphify approval notice")


def decide(event_name, event, api):
    if event_name == "pull_request_target":
        pr = event["pull_request"]
        if pr.get("state") != "open":
            return None
        author = (pr.get("user") or {}).get("login")
        actor = (event.get("sender") or {}).get("login")
        if not (api.maintainer(author) and api.maintainer(actor)):
            api.notice(pr["number"], "Graphify is awaiting approval. A repository maintainer or administrator "
                       f"can comment `/graphify approve {pr['head']['sha']}` to approve this exact commit. The check is advisory.", create=True)
            return None
    elif event_name == "issue_comment":
        issue = event.get("issue") or {}
        comment = event.get("comment") or {}
        match = re.fullmatch(r"/graphify approve ([0-9a-f]{7,40})", (comment.get("body") or "").strip())
        if not issue.get("pull_request") or not match:
            return None
        if not api.maintainer((comment.get("user") or {}).get("login")):
            return None
        pr = api.request("GET", f"/repos/{api.repo}/pulls/{int(issue['number'])}")
        if pr.get("state") != "open" or not pr["head"]["sha"].startswith(match.group(1)):
            return None
    else:
        return None
    branch = (pr.get("base") or {}).get("ref", "")
    if not branch.startswith("release/"):
        return None
    api.notice(pr["number"], f"Graphify was authorized for commit `{pr['head']['sha'][:12]}`. The check is advisory.", create=False)
    return {"number": str(int(pr["number"])), "base": pr["base"]["sha"],
            "head": pr["head"]["sha"], "branch": branch}


def main():
    event = json.loads(open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8").read())
    api = API(os.environ["GITHUB_API_URL"], os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    decision = decide(os.environ["GITHUB_EVENT_NAME"], event, api)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"authorized={'true' if decision else 'false'}\n")
        for key, value in (decision or {}).items():
            if "\n" in value or "\r" in value:
                raise ValueError("invalid PR metadata")
            output.write(f"{key}={value}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Graphify authorization failed: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)
