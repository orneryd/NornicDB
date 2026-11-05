import importlib.util
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import graphify_gate as gate
import graphify_ci as ci


def pr(author="writer", branch="release/v1.4.4"):
    return {"number": 17, "state": "open", "user": {"login": author},
            "base": {"ref": branch, "sha": "a" * 40}, "head": {"sha": "b" * 40, "repo": {"full_name": "org/repo"}}}


class FakeAPI:
    repo = "org/repo"
    def __init__(self, maintainers=()):
        self.maintainers = set(maintainers)
        self.notices = []
        self.pull = pr()
    def maintainer(self, login):
        return login in self.maintainers
    def notice(self, number, text, *, create):
        self.notices.append((number, text, create))
    def request(self, method, path, body=None):
        return self.pull


class GateTests(unittest.TestCase):
    def test_maintainer_pr_runs_automatically(self):
        api = FakeAPI({"maintainer"})
        event = {"pull_request": pr("maintainer"), "sender": {"login": "maintainer"}}
        result = gate.decide("pull_request_target", event, api)
        self.assertEqual(result["head"], "b" * 40)
        self.assertEqual(result["branch"], "release/v1.4.4")

    def test_maintainer_fork_requires_explicit_approval(self):
        api = FakeAPI({"maintainer"})
        fork = pr("maintainer")
        fork["head"]["repo"]["full_name"] = "maintainer/fork"
        result = gate.decide("pull_request_target", {"pull_request": fork, "sender": {"login": "maintainer"}}, api)
        self.assertIsNone(result)
        self.assertIn("/graphify approve", api.notices[0][1])
        api.pull = fork
        event = {"issue": {"number": 17, "pull_request": {"url": "present"}},
                 "comment": {"body": "/graphify approve " + "b" * 40, "user": {"login": "maintainer"}}}
        self.assertEqual(gate.decide("issue_comment", event, api)["head"], "b" * 40)

    def test_untrusted_author_or_sender_requires_approval(self):
        for author, actor in (("writer", "writer"), ("maintainer", "writer")):
            api = FakeAPI({"maintainer"})
            result = gate.decide("pull_request_target", {"pull_request": pr(author), "sender": {"login": actor}}, api)
            self.assertIsNone(result)
            self.assertIn("/graphify approve", api.notices[0][1])

    def test_only_maintainer_comment_approves_current_head(self):
        api = FakeAPI({"maintainer"})
        event = {"issue": {"number": 17, "pull_request": {}},
                 "comment": {"body": "/graphify approve " + "b" * 40, "user": {"login": "writer"}}}
        # GitHub includes a populated pull_request link in issue_comment events.
        event["issue"]["pull_request"] = {"url": "present"}
        self.assertIsNone(gate.decide("issue_comment", event, api))
        event["comment"]["user"]["login"] = "maintainer"
        self.assertEqual(gate.decide("issue_comment", event, api)["head"], "b" * 40)
        event["comment"]["body"] = "/graphify approve extra"
        self.assertIsNone(gate.decide("issue_comment", event, api))

    def test_approval_rejects_a_stale_commit(self):
        api = FakeAPI({"maintainer"})
        event = {"issue": {"number": 17, "pull_request": {"url": "present"}},
                 "comment": {"body": "/graphify approve " + "a" * 40, "user": {"login": "maintainer"}}}
        self.assertIsNone(gate.decide("issue_comment", event, api))

    def test_approval_does_not_enable_nonrelease_or_closed_pr(self):
        api = FakeAPI({"maintainer"})
        event = {"issue": {"number": 17, "pull_request": {"url": "present"}},
                 "comment": {"body": "/graphify approve " + "b" * 40, "user": {"login": "maintainer"}}}
        api.pull = pr(branch="main")
        self.assertIsNone(gate.decide("issue_comment", event, api))
        api.pull = pr()
        api.pull["state"] = "closed"
        self.assertIsNone(gate.decide("issue_comment", event, api))

    def test_permission_uses_maintain_role_not_generic_write(self):
        api = gate.API("https://example.test", "org/repo", "token")
        with mock.patch.object(api, "request", return_value={"permission": "write", "role_name": "maintain"}):
            self.assertTrue(api.maintainer("m"))
        with mock.patch.object(api, "request", return_value={"permission": "write", "role_name": "write"}):
            self.assertFalse(api.maintainer("w"))


class AdvisoryTests(unittest.TestCase):
    def test_comments_cover_no_findings_findings_and_incomplete(self):
        for findings, skipped, expected in (([], 0, "No findings"), ([object()], 0, "Advisory findings"), ([], 1, "Check incomplete")):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temp:
                report = SimpleNamespace(findings=findings, skipped_by_cap=skipped, nothing_compared="")
                engine = SimpleNamespace(git=lambda *args: "b" * 40, connection=lambda: ("https://example.test", "u", "p", ""))
                duplicates = SimpleNamespace(parse_diff=lambda diff: diff, git_diff=lambda *args: "diff", NornicDB=lambda *args, **kw: None,
                    analyze=lambda *args, **kw: report, render_summary=lambda *args, **kw: "summary", DEFAULT_IGNORE=())
                api = FakeAPI()
                env = {"PR_NUMBER": "17", "PR_BASE_SHA": "a" * 40, "PR_HEAD_SHA": "b" * 40,
                       "GITHUB_REPOSITORY": "org/repo", "GITHUB_API_URL": "https://example.test", "GITHUB_TOKEN": "test",
                       "RUNNER_TEMP": temp}
                with mock.patch.dict(os.environ, env), mock.patch.object(ci, "snapshot"), mock.patch.object(ci, "tools", return_value=(Path(temp), engine, duplicates)), mock.patch.object(ci, "API", return_value=api), mock.patch.object(ci, "upsert") as upsert:
                    ci.check()
                    self.assertIn(expected, upsert.call_args.args[2])

class CommentTests(unittest.TestCase):
    def test_upsert_ignores_marker_from_untrusted_user(self):
        api = FakeAPI()
        calls = []
        def request(method, path, body=None):
            calls.append((method, path, body))
            if method == "GET":
                return [{"id": 12, "body": ci.MARKER, "user": {"login": "other"}}]
        api.request = request
        ci.upsert(api, 17, "**No findings.**")
        self.assertEqual(calls[-1][0], "POST")
        self.assertIn("No findings", calls[-1][2]["body"])

    def test_upsert_updates_own_comment(self):
        api = FakeAPI()
        calls = []
        def request(method, path, body=None):
            calls.append((method, path, body))
            if method == "GET":
                return [{"id": 23, "body": ci.MARKER, "user": {"login": "github-actions[bot]"}}]
        api.request = request
        ci.upsert(api, 17, "**No findings.**")
        self.assertEqual(calls[-1][0], "PATCH")
        self.assertTrue(calls[-1][1].endswith("/comments/23"))


if __name__ == "__main__":
    unittest.main()
