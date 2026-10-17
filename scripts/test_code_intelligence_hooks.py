"""Exercise real Git commits/pulls with staged snapshots and the HTTP duplicate checker."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCRIPTS = Path(__file__).resolve().parent
BODY = '''def calculate_total(items, tax_rate, discount):
    subtotal = 0
    count = 0
    for item in items:
        subtotal += item.price * item.quantity
        count += item.quantity
    tax = subtotal * tax_rate
    shipping = count * 2
    adjusted = subtotal + tax + shipping
    if adjusted > 100:
        adjusted -= discount
    return max(0, adjusted)
'''


class HookTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nornic-hook-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("NORNICDB_", "CODE_INTEL_", "GIT_"))}
        self.env["GIT_CONFIG_GLOBAL"] = str(Path(self.temp.name) / "global.gitconfig")
        self.env.update(GIT_AUTHOR_NAME="Test", GIT_AUTHOR_EMAIL="test@example.invalid",
                        GIT_COMMITTER_NAME="Test", GIT_COMMITTER_EMAIL="test@example.invalid")
        self.git("init", "-b", "main")
        (self.root / "scripts").mkdir()
        (self.root / ".githooks").mkdir()
        for name in ["graphify_local.py", "pr_duplicates.py", "code_intelligence.py", "code-intelligence-hook.sh", "install-git-hooks.sh", "install_code_intelligence_global.py", "code-intelligence-global-hook.sh", "requirements-code-intelligence.txt"]:
            shutil.copy2(SCRIPTS / name, self.root / "scripts" / name)
        for name in ["pre-commit", "post-merge", "post-rewrite", "reference-transaction"]:
            shutil.copy2(SCRIPTS.parent / ".githooks" / name, self.root / ".githooks" / name)
        (self.root / "candidate.py").write_text("# baseline\n")
        self.git("add", ".")
        self.git("commit", "-m", "baseline")
        self.baseline = self.git("rev-parse", "HEAD").stdout.strip()
        self.searches = []
        self.hits = 0
        self.stored_commit = self.baseline
        self.auth = []
        self.paths = []
        self.search_databases = []
        self.truncate_commit_once = False
        self.search_pages = None
        self.search_requests = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                fixture.paths.append(self.path)
                fixture.auth.append(self.headers.get("Authorization"))
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/nornicdb/search":
                    fixture.searches.append(data.get("query", ""))
                    fixture.search_databases.append(data["database"])
                    fixture.search_requests.append(data)
                    payload = {"results": [{"node": {"properties": {
                        "id": f"existing-{i}", "label": "existing()", "body": BODY,
                        "source_file": f"existing{i}.py", "source_location": "L1",
                        "symbol_kind": "function", "repo": "test/repo"}}} for i in range(fixture.hits)]}
                    payload.update(has_more=False, collection_exhausted=True,
                                   completion="eligible_population_exhausted")
                    if data.get("discard"):
                        payload = {"results": [], "has_more": False, "collection_exhausted": True}
                    elif fixture.search_pages is not None:
                        index = int(data.get("qid", "page-0").split("-")[-1])
                        payload = fixture.search_pages[index]
                else:
                    statement = data["statements"][0]
                    query = statement["statement"]
                    columns, rows = [], []
                    if "r.last_commit AS commit" in query:
                        columns, rows = ["commit", "main_spec", "max_embed_chars"], [[fixture.stored_commit, "none", 0]]
                    elif "SHOW DATABASES" in query:
                        columns, rows = ["name"], [["system"], ["nornicdbcode"]]
                    elif "MERGE (r:CodeRepository" in query:
                        fixture.stored_commit = statement["parameters"]["commit"]
                    payload = {"results": [{"columns": columns, "data": [{"row": r} for r in rows]}], "errors": []}
                raw = json.dumps(payload).encode()
                self.send_response(payload.get("_http_status", 200) if isinstance(payload, dict) else 200)
                self.send_header("Content-Type", "application/json")
                truncate = fixture.truncate_commit_once and self.path.endswith("/tx/commit") and "MERGE (r:CodeRepository" in data["statements"][0]["statement"]
                if truncate:
                    fixture.truncate_commit_once = False
                    self.close_connection = True
                self.send_header("Content-Length", str(len(raw) + (10 if truncate else 0)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.env.update(NORNICDB_URI=f"http://127.0.0.1:{self.server.server_port}/",
                        NORNICDB_TOKEN="fixture-token", CODE_INTEL_REPO="test/repo", CODE_INTEL_MAIN="none")
        self.extractor = Path(self.temp.name) / "graphify"
        self.extractor.write_text(f'''#!{sys.executable}
import json,sys
from pathlib import Path
out=Path(sys.argv[sys.argv.index("--out")+1])/"graphify-out"
out.mkdir(parents=True)
body=Path("candidate.py").read_text()
nodes=[{{"id":"candidate","label":"calculate_total()","file_type":"code","source_file":"candidate.py","source_location":"L1","_callable":True}}] if "def " in body else []
(out/"graph.json").write_text(json.dumps({{"nodes":nodes,"links":[]}}))
''')
        self.extractor.chmod(0o755)
        self.env["CODE_INTEL_GRAPHIFY"] = str(self.extractor)
        self.git("config", "codeIntelligence.python", sys.executable)
        self.install("--with-code-intelligence")

    def git(self, *args, check=True, root=None):
        result = subprocess.run(["git", "-C", str(root or self.root), *args],
                                capture_output=True, text=True, env=self.env)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def install(self, option):
        result = subprocess.run(["bash", str(self.root / "scripts/install-git-hooks.sh"), option],
                                capture_output=True, text=True, env=self.env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def stage(self, body=BODY):
        (self.root / "candidate.py").write_text(body)
        self.git("add", "candidate.py")

    def test_blocks_one_reuse_and_multiple_refactor_matches(self):
        for hits in [1, 2]:
            with self.subTest(matches=hits):
                self.hits = hits
                self.stage()
                result = self.git("commit", "-m", "duplicate", check=False)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("Commit blocked", result.stderr)
                self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.baseline)
                self.assertEqual(self.auth[-1], "Bearer fixture-token")
                self.assertEqual(set(self.search_databases), {"nornicdbcode"})
                self.assertNotIn("/db/nornic/tx/commit", self.paths)

    def test_unstaged_edit_cannot_hide_staged_duplicate(self):
        self.hits = 1
        self.stage()
        unstaged = "def unique():\n    return 'different unstaged behavior'\n"
        (self.root / "candidate.py").write_text(unstaged)
        index_before = self.git("write-tree").stdout
        result = self.git("commit", "-m", "staged duplicate", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Commit blocked", result.stderr)
        self.assertIn("subtotal", self.searches[-1])
        self.assertEqual((self.root / "candidate.py").read_text(), unstaged)
        self.assertEqual(self.git("write-tree").stdout, index_before)

    def test_small_leaf_edit_is_checked_against_existing_owner(self):
        self.stage()
        self.git("commit", "-m", "baseline function")
        self.searches.clear()
        self.hits = 1
        self.stage(BODY.replace("subtotal = 0", "subtotal = 1"))
        result = self.git("commit", "-m", "small duplicate leaf patch", check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Commit blocked", result.stderr)
        self.assertTrue(self.searches)

    def test_duplicate_on_later_cursor_page_blocks_commit(self):
        self.search_pages = [
            {"results": [], "has_more": True, "qid": "page-1"},
            {"results": [{"node": {"properties": {"id": "later-copy", "body": BODY,
              "source_file": "lib/existing.py", "symbol_kind": "function", "repo": "test/repo"}}}],
             "has_more": False, "collection_exhausted": True, "completion": "eligible_population_exhausted"},
        ]
        self.stage()
        result = self.git("commit", "-m", "later-page duplicate", check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Commit blocked", result.stderr)
        self.assertEqual(self.search_requests[0]["mode"], "ranked_then_id")
        self.assertEqual(self.search_requests[1]["qid"], "page-1")
        self.assertEqual(self.search_requests[1]["database"], "nornicdbcode")
        self.assertNotIn("/db/nornic/tx/commit", self.paths)

    def test_incomplete_or_expired_cursor_never_passes_commit(self):
        cases = [
            [{"results": [], "has_more": False, "collection_exhausted": False, "completion": "candidate_pool_exhausted"}],
            [{"results": [], "has_more": True, "qid": "page-1"},
             {"_http_status": 410, "request_status": "continuation_gone", "results": []}],
            [{"results": [], "has_more": True, "qid": "page-1"},
             {"results": [], "has_more": True, "qid": "page-1"}],
        ]
        self.stage()
        for pages in cases:
            with self.subTest(pages=pages):
                self.search_pages = pages
                result = self.git("commit", "-m", "incomplete cursor", check=False)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.baseline)
                if pages[0].get("has_more"):
                    self.assertTrue(self.search_requests[-1].get("discard"))

    def test_ingest_retries_a_truncated_commit_response(self):
        self.truncate_commit_once = True
        result = subprocess.run([sys.executable, str(self.root / "scripts/code_intelligence.py"),
                                 "ingest", "--repo-root", str(self.root), "--force"],
                                env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.truncate_commit_once)
        self.assertEqual(self.stored_commit, self.baseline)

    def test_no_match_allows_commit(self):
        self.stage()
        self.git("commit", "-m", "unique addition")
        self.assertTrue(self.searches)
        self.assertNotEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.baseline)

    def test_missing_configuration_blocks_with_setup_guidance(self):
        self.stage()
        del self.env["NORNICDB_URI"]
        result = self.git("commit", "-m", "missing config", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Set NORNICDB_URI", result.stderr)
        self.assertNotIn("fixture-token", result.stdout + result.stderr)
        self.assertFalse(self.searches)

    def test_missing_ingest_is_incomplete_not_pass(self):
        self.stage()
        self.stored_commit = ""
        result = self.git("commit", "-m", "missing graph", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ingest first", result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.baseline)

    def test_disabled_hook_and_docs_commit_need_no_credentials(self):
        self.env.pop("NORNICDB_URI")
        self.env.pop("NORNICDB_TOKEN")
        (self.root / "README.md").write_text("Documentation only\n")
        self.git("add", "README.md")
        self.git("commit", "-m", "docs")
        self.install("--without-code-intelligence")
        self.stage()
        self.git("commit", "-m", "disabled check")
        self.assertFalse(self.searches)

    def test_main_pull_ingests_new_head_and_feature_branch_skips(self):
        remote = Path(self.temp.name) / "remote.git"
        peer = Path(self.temp.name) / "peer"
        self.git("clone", "--bare", str(self.root), str(remote))
        self.git("remote", "add", "origin", str(remote))
        self.git("clone", str(remote), str(peer))
        (peer / "README.md").write_text("New main\n")
        self.git("add", "README.md", root=peer)
        self.git("commit", "-m", "advance main", root=peer)
        self.git("push", "origin", "main", root=peer)
        self.git("pull", "--ff-only", "origin", "main")
        self.assertEqual(self.stored_commit, self.git("rev-parse", "HEAD").stdout.strip())
        self.assertIn("/db/nornicdbcode/tx/commit", self.paths)
        self.assertNotIn("/db/nornic/tx/commit", self.paths)
        # A post-rewrite from pull --rebase uses the same update command.
        result = subprocess.run([str(self.root / ".git/hooks/post-rewrite"), "rebase"],
                                input="", capture_output=True, text=True, env=self.env, cwd=self.root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("graph is current", result.stdout)
        self.git("checkout", "-b", "feature")
        del self.env["NORNICDB_URI"]
        result = subprocess.run([str(self.root / ".git/hooks/post-merge"), "0"],
                                capture_output=True, text=True, env=self.env, cwd=self.root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_global_install_checks_repo_database_chains_native_and_refreshes_main_reset(self):
        for hook in ["pre-commit", "post-merge", "post-rewrite", "reference-transaction"]:
            (self.root / ".git/hooks" / hook).unlink()
        native = self.root / ".git/hooks/pre-commit"
        native.write_text("#!/bin/sh\necho native-hook-ran\n")
        native.chmod(0o755)
        destination = Path(self.temp.name) / "standalone"
        result = subprocess.run([sys.executable, str(self.root / "scripts/install_code_intelligence_global.py"),
                                 "--python", sys.executable, "--destination", str(destination)],
                                env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.stage()
        self.hits = 1
        result = self.git("commit", "-m", "global duplicate", check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("native-hook-ran", result.stdout + result.stderr)
        self.assertEqual(set(self.search_databases), {"repo"})
        self.hits = 0
        self.git("commit", "-m", "global main update")
        updated = self.git("rev-parse", "HEAD").stdout.strip()
        self.assertEqual(self.stored_commit, updated)
        self.assertIn("/db/repo/tx/commit", self.paths)
        self.git("reset", "--hard", self.baseline)
        self.assertEqual(self.stored_commit, self.baseline)
        self.assertEqual(self.git("config", "--global", "core.hooksPath").stdout.strip(), str(destination / "hooks"))
        self.git("checkout", "-b", "feature")
        self.git("update-ref", "refs/heads/main", updated)
        self.assertEqual(self.stored_commit, updated)
        self.assertEqual(self.git("branch", "--show-current").stdout.strip(), "feature")
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.baseline)

    def test_global_repo_database_can_preserve_existing_nornicdbcode(self):
        self.git("config", "codeIntelligence.databaseFromRepo", "true")
        self.git("config", "codeIntelligence.database", "nornicdbcode")
        self.stage()
        self.git("commit", "-m", "keep existing database")
        self.assertEqual(set(self.search_databases), {"nornicdbcode"})
        self.assertNotIn("/db/repo/tx/commit", self.paths)


if __name__ == "__main__":
    unittest.main()
