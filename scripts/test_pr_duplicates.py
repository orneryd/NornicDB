"""PR-time duplicate detection: diff parsing, candidate selection, match rules, HTTP clients (stdlib unittest)."""
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import pr_duplicates as pd  # noqa: E402

FUNC_A = """def total_price(items, tax_rate):
    subtotal = 0
    for item in items:
        subtotal += item.price * item.quantity
    tax = subtotal * tax_rate
    return subtotal + tax
"""
# The same logic with different names: a renamed copy.
FUNC_B = FUNC_A.replace("total_price", "order_total").replace("items", "lines").replace("item", "line") \
    .replace("subtotal", "net").replace("tax_rate", "rate").replace("tax", "levy")
OTHER = """def greet(name):
    banner = "=" * 20
    print(banner)
    print("Hello, " + name + "!")
    print(banner)
"""


def node(node_id, label, path, line, **extra):
    return {"id": node_id, "label": label, "file_type": "code", "source_file": path,
            "source_location": f"L{line}", "_callable": True, **extra}


class FakeServer:
    """A NornicDB and GitHub stand-in that records every request."""

    def __init__(self, search_hits=None, cypher=None, status=200):
        self.requests = []
        self.search_hits = search_hits if search_hits is not None else []
        self.cypher = cypher or {}
        self.status = status
        self.comments = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _body(self):
                length = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(length) or b"null") if length else None

            def _send(self, payload, status=200):
                raw = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _record(self, body):
                outer.requests.append({"method": self.command, "path": self.path, "body": body,
                                       "auth": self.headers.get("Authorization")})

            def do_POST(self):
                body = self._body()
                self._record(body)
                if outer.status != 200:
                    return self._send({"errors": [{"code": "x", "message": "boom"}]}, outer.status)
                if self.path == "/nornicdb/search":
                    return self._send(outer.search_hits(body) if callable(outer.search_hits) else outer.search_hits)
                if self.path.endswith("/tx/commit"):
                    statement = body["statements"][0]["statement"]
                    for needle, rows in outer.cypher.items():
                        if needle in statement:
                            cols = list(rows[0]) if rows else []
                            return self._send({"results": [{"columns": cols, "data": [{"row": [r[c] for c in cols]}
                                                                                         for r in rows]}], "errors": []})
                    return self._send({"results": [{"columns": [], "data": []}], "errors": []})
                if "/issues/" in self.path and self.path.endswith("/comments"):
                    outer.comments.append({"id": len(outer.comments) + 1, "body": body["body"]})
                    return self._send(outer.comments[-1], 201)
                self._send({}, 404)

            def do_GET(self):
                self._record(None)
                if "/issues/" in self.path and "/comments" in self.path:
                    return self._send(outer.comments)
                self._send({}, 404)

            def do_PATCH(self):
                body = self._body()
                self._record(body)
                comment_id = int(self.path.rsplit("/", 1)[1])
                for comment in outer.comments:
                    if comment["id"] == comment_id:
                        comment["body"] = body["body"]
                self._send({}, 200)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def hit_json(hit_id, label, path, body, *, vector_rank=1, bm25_rank=1, repo="o/r", kind="function", line=1):
    return {"node": {"id": "internal-" + hit_id, "labels": ["Code"], "properties": {
        "id": hit_id, "label": label, "source_file": path, "source_location": f"L{line}", "symbol_kind": kind,
        "repo": repo, "body": body}}, "score": 0.0328, "rrf_score": 0.0328,
        "vector_rank": vector_rank, "bm25_rank": bm25_rank}


class DiffTests(unittest.TestCase):
    def test_hunks_additions_deletions_new_and_deleted_files(self):
        text = """diff --git a/a.py b/a.py
--- a/a.py
+++ b/a.py
@@ -3,2 +3,3 @@ context
-old1
-old2
+new1
+new2
+new3
@@ -10 +11,0 @@
-gone
\\ No newline at end of file
diff --git a/b.py b/b.py
deleted file mode 100644
--- a/b.py
+++ /dev/null
@@ -1,2 +0,0 @@
-x
-y
diff --git a/c.py b/c.py
new file mode 100644
--- /dev/null
+++ b/c.py
@@ -0,0 +1,2 @@
+p
+q
diff --git a/img.png b/img.png
Binary files a/img.png and b/img.png differ
"""
        files = pd.parse_diff(text)
        self.assertEqual(sorted(files["a.py"].added), [3, 4, 5])
        self.assertEqual(sorted(files["a.py"].removed), [3, 4, 10])
        self.assertTrue(files["b.py"].deleted)
        self.assertEqual(sorted(files["b.py"].removed), [1, 2])
        self.assertTrue(files["c.py"].created)
        self.assertEqual(sorted(files["c.py"].added), [1, 2])
        self.assertNotIn("img.png", files)

    def test_path_with_a_tab_suffix(self):
        files = pd.parse_diff("diff --git a/my file.py b/my file.py\n--- a/my file.py\t\n+++ b/my file.py\t\n@@ -1 +1 @@\n-a\n+b\n")
        self.assertIn("my file.py", files)


class SimilarityTests(unittest.TestCase):
    def test_a_renamed_copy_is_structurally_identical(self):
        sim = pd.similarity(FUNC_A, FUNC_B)
        self.assertGreater(sim["structural"], 0.95)
        self.assertGreater(sim["score"], 0.8)

    def test_unrelated_code_scores_low(self):
        self.assertLess(pd.similarity(FUNC_A, OTHER)["score"], 0.3)

    def test_a_method_rewritten_as_a_function_still_matches(self):
        # Seen in soraban-connect: a repository method re-derived as a free function that takes
        # the session as a parameter scored 0.77 structurally, under the 0.8 threshold.
        method = (
            "    async def get_heartbeats_by_firm_id(self, firm_id, seen_before=None):\n"
            "        stmt = select(AgentHeartbeat).where(AgentHeartbeat.firm_id == firm_id)\n\n"
            "        if seen_before is not None:\n"
            "            stmt = stmt.where(AgentHeartbeat.last_seen_at < seen_before)\n\n"
            "        result = await self._session.execute(stmt)\n\n"
            "        return list(result.scalars().all())\n")
        function = (
            "async def list_agent_heartbeats_for_firm(session, firm_id, seen_before=None):\n"
            "    query = select(AgentHeartbeat).where(AgentHeartbeat.firm_id == firm_id)\n\n"
            "    if seen_before is not None:\n"
            "        query = query.where(AgentHeartbeat.last_seen_at < seen_before)\n\n"
            "    rows = await session.execute(query)\n\n"
            "    return list(rows.scalars().all())\n")
        sim = pd.similarity(method, function)
        self.assertLess(sim["structural"], 0.8)
        self.assertGreaterEqual(sim["receiverless"], 0.8)
        self.assertEqual(sim["score"], sim["receiverless"])

    def test_ruby_instance_variables_match_locals(self):
        ivars = ("def total\n  @items.select { |item| item.active? }\n"
                 "        .sum { |item| item.price * item.quantity }\n"
                 "        .round(2)\nend\n")
        locals_ = ("def total(items)\n  items.select { |item| item.active? }\n"
                   "       .sum { |item| item.price * item.quantity }\n"
                   "       .round(2)\nend\n")
        self.assertGreater(pd.similarity(ivars, locals_)["receiverless"], pd.similarity(ivars, locals_)["structural"])

    def test_receiver_stripping_keeps_unrelated_code_apart(self):
        self.assertLess(pd.similarity("    def f(self):\n        return self._a + self._b\n", OTHER)["score"], 0.3)
        self.assertEqual(pd.RECEIVER.sub("", "self._session.execute(this.q, cls.x, @y, a@b, self.__z)"),
                         "session.execute(q, x, y, a@b, __z)")


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        Path(self.root, "app").mkdir()
        Path(self.root, "app/new.py").write_text(FUNC_A + "\n\ndef tiny(x):\n    return x + 1\n\n\n" + OTHER)
        self.graph = Path(self.root, "graph.json")
        self.graph.write_text(json.dumps({"nodes": [
            node("n_total", "total_price()", "app/new.py", 1),
            node("n_tiny", "tiny()", "app/new.py", 9),
            node("n_greet", "greet()", "app/new.py", 13),
        ], "links": []}))

    def select(self, added, **kwargs):
        diff = {"app/new.py": pd.FileDiff(added=set(added))}
        opts = dict(min_body_lines=4, min_added_fraction=0.5, max_query_chars=2000, min_tokens=20)
        return pd.select_candidates(self.graph, self.root, diff, **{**opts, **kwargs})

    def test_new_function_is_a_candidate_and_tiny_one_is_not(self):
        labels = [c.label for c in self.select(range(1, 20))]
        self.assertIn("total_price()", labels)
        self.assertIn("greet()", labels)
        self.assertNotIn("tiny()", labels)

    def test_rewritten_tiny_wrapper_still_retires_its_old_implementation(self):
        touched = set()
        candidates = self.select(range(1, 20), touched_ids=touched)
        self.assertNotIn("n_tiny", {candidate.id for candidate in candidates})
        self.assertIn("n_tiny", touched)
        candidate = candidates[0]
        old = pd.Hit("n_tiny", "tiny()", "app/new.py", 9, "function", "o/r",
                     candidate.body, 0.03, 1, 1)
        self.assertEqual(pd.evaluate(candidate, [old], repo="o/r", retired=set(),
                                    touched=touched, min_similarity=0.8), (None, False))

    def test_small_edit_to_a_long_function_is_not_a_candidate(self):
        self.assertEqual([c.label for c in self.select([3])], [])

    def test_ignored_paths_and_largest_first(self):
        self.assertEqual(self.select(range(1, 20), ignore=("app/*",)), [])
        ordered = self.select(range(1, 20))
        self.assertEqual(ordered[0].label, "total_price()")  # 6 lines before greet's 5

    def test_query_is_the_body_without_comments(self):
        candidate = self.select(range(1, 8))[0]
        self.assertTrue(candidate.query.startswith("def total_price"))


class EvaluateTests(unittest.TestCase):
    def setUp(self):
        self.cand = pd.Candidate("c1", "order_total()", "app/new.py", 1, FUNC_B, "", 6, 1.0, False)

    def evaluate(self, hits, **kw):
        base = dict(repo="o/r", retired=set(), touched={"c1"}, min_similarity=0.8, min_tokens=20)
        return pd.evaluate(self.cand, hits, **{**base, **kw})

    def hit(self, hit_id, path="lib/a.py", body=FUNC_A, **kw):
        return pd.Hit(hit_id, kw.get("label", "total_price()"), path, 1, kw.get("kind", "function"),
                      kw.get("repo", "o/r"), body, 0.03, 1, 1)

    def test_zero_one_and_several_matches(self):
        self.assertEqual(self.evaluate([self.hit("x", body=OTHER)]), (None, False))
        finding, _ = self.evaluate([self.hit("a")])
        self.assertEqual((finding.kind, len(finding.matches)), ("reuse", 1))
        finding, _ = self.evaluate([self.hit("a"), self.hit("b", path="lib/b.py")])
        self.assertEqual((finding.kind, len(finding.matches)), ("refactor", 2))
        many = [self.hit(f"m{i}", path=f"lib/{i}.py") for i in range(8)]
        finding, _ = self.evaluate(many, max_matches=3)
        self.assertEqual((finding.kind, len(finding.matches), finding.total), ("refactor", 3, 8))

    def test_self_other_repo_class_and_duplicates_are_ignored(self):
        hits = [self.hit("c1"), self.hit("o", repo="other/repo"), self.hit("k", kind="class"),
                self.hit("a"), self.hit("a")]
        finding, _ = self.evaluate(hits)
        self.assertEqual([m.hit.id for m in finding.matches], ["a"])
        finding, _ = self.evaluate([self.hit("o", repo="other/repo")], cross_repo=True)
        self.assertEqual(finding.kind, "reuse")

    def test_test_code_only_matches_test_code(self):
        self.assertEqual(self.evaluate([self.hit("t", path="spec/foo_spec.py")]), (None, False))
        self.cand.test = True
        finding, _ = self.evaluate([self.hit("t", path="spec/foo_spec.py")])
        self.assertEqual(finding.kind, "reuse")

    def test_retired_match_is_dropped_and_a_retired_near_copy_is_a_move(self):
        self.assertEqual(self.evaluate([self.hit("r")], retired={"r"}), (None, True))
        # even with another live match, relocating a function is not a duplicate
        self.assertEqual(self.evaluate([self.hit("r"), self.hit("a")], retired={"r"}), (None, True))
        # a retired function that is NOT similar is just dropped
        finding, moved = self.evaluate([self.hit("r", body=OTHER), self.hit("a")], retired={"r"})
        self.assertFalse(moved)
        self.assertEqual(finding.kind, "reuse")

    def test_another_rewritten_symbol_is_not_an_existing_match(self):
        self.assertEqual(self.evaluate([self.hit("other-candidate")], touched={"c1", "other-candidate"}), (None, False))

    def test_ignored_and_short_hits(self):
        self.assertEqual(self.evaluate([self.hit("m", path="db/migrate/1_x.py")], ignore=("db/migrate/*",)), (None, False))
        self.assertEqual(self.evaluate([self.hit("s")], min_tokens=500), (None, False))

    # Behavior matches: a body with no code overlap that the embeddings rank clearly closest.
    def vhit(self, hit_id, similarity, body=OTHER, **kw):
        h = self.hit(hit_id, path=kw.get("path", f"lib/{hit_id}.py"), body=body, **kw)
        h.vector_similarity = similarity
        return h

    def field(self, top, runner_up=0.45, rest=(0.40, 0.38, 0.35, 0.33)):
        return [self.vhit("top", top), self.vhit("second", runner_up)] + [
            self.vhit(f"r{i}", value) for i, value in enumerate(rest)]

    def test_a_standout_by_embedding_is_a_behavior_match(self):
        finding, _ = self.evaluate(self.field(0.70))
        self.assertEqual([(m.hit.id, m.basis) for m in finding.matches], [("top", "behavior")])
        self.assertEqual(finding.kind, "reuse")
        evidence = finding.matches[0].evidence()
        self.assertEqual((evidence["basis"], evidence["vector_similarity"]), ("behavior", 0.70))
        self.assertIn("structural", evidence)

    def test_existing_owner_use_does_not_hide_another_semantic_duplicate(self):
        records = []
        finding, _ = self.evaluate(self.field(0.95, runner_up=0.72),
                                   owner_calls={"top": "L2"}, existing_owner_calls=records)
        self.assertEqual([match.hit.id for match in finding.matches], ["second"])
        self.assertEqual(records[0][1].id, "top")

    def test_copy_still_blocks_even_when_the_candidate_also_calls_the_owner(self):
        copied = FUNC_A.replace("    return subtotal + tax", """    shipping = sum(item.quantity for item in items)
    discount = min(subtotal, shipping)
    total = subtotal + tax + shipping
    total -= discount
    adjusted = max(0, total)
    return adjusted""")
        self.cand.body = copied.replace("def total_price", "def order_total").replace(
            "    return adjusted", "    total_price(items, tax_rate)\n    return adjusted")
        self.assertGreaterEqual(pd.similarity(self.cand.body, copied)["score"], 0.8)
        records = []
        finding, _ = self.evaluate([self.hit("copy", body=copied)], owner_calls={"copy": "L10"},
                                   existing_owner_calls=records)
        self.assertEqual(finding.matches[0].basis, "code")
        self.assertEqual(records, [])

    def test_no_behavior_match_without_a_clear_standout(self):
        # A top hit that does not stand out from related code is no match.
        self.assertEqual(self.evaluate(self.field(0.48, runner_up=0.46, rest=(0.45, 0.44, 0.40, 0.38))), (None, False))
        # well above the median but not far enough ahead of the runner-up
        self.assertEqual(self.evaluate(self.field(0.70, runner_up=0.67)), (None, False))
        # ahead of the runner-up but too close to the median
        self.assertEqual(self.evaluate(self.field(0.50, runner_up=0.44, rest=(0.42, 0.42, 0.40, 0.40))), (None, False))
        # too few hits to judge what stands out
        self.assertEqual(self.evaluate([self.vhit("top", 0.95), self.vhit("b", 0.80)]), (None, False))
        # hits without a similarity (keyword-only, or a server without matches) never qualify
        self.assertEqual(self.evaluate(self.field(None, rest=(0.40,) * 5)), (None, False))
        self.assertEqual(self.evaluate(self.field(0.70), semantic=False), (None, False))

    def test_code_and_behavior_matches_count_together(self):
        finding, _ = self.evaluate(self.field(0.70) + [self.hit("copy", path="lib/copy.py")])
        self.assertEqual(finding.kind, "refactor")
        self.assertEqual([m.basis for m in finding.matches], ["code", "behavior"])

    def test_the_report_says_how_each_match_was_established(self):
        finding, _ = self.evaluate(self.field(0.70) + [self.hit("copy", path="lib/copy.py")])
        report = pd.Report(repo="o/r", head_sha="h" * 40, base_sha="b" * 40, ingested_commit="i" * 40, findings=[finding])
        comment = pd.render_comment(report)
        self.assertIn("100% similar", comment)
        self.assertIn("same behavior: closest by embedding similarity (0.70)", comment)
        matches = pd.report_json(report)["findings"][0]["matches"]
        self.assertEqual([m["basis"] for m in matches], ["code", "behavior"])
        self.assertEqual(matches[1]["vector_similarity"], 0.70)

    def test_a_retired_behavior_match_is_not_a_move(self):
        finding, moved = self.evaluate(self.field(0.70), retired={"top"})
        self.assertEqual((finding, moved), (None, False))


class HttpClientTests(unittest.TestCase):
    def test_ranked_score_floor_must_be_finite_and_positive(self):
        for floor in [None, 0, -0.01, True, float("nan"), float("inf"), "invalid"]:
            with self.subTest(floor=floor), self.assertRaises(ValueError):
                pd.NornicDB("http://localhost:7474", "u", "p", require_scores=True, min_rrf_score=floor)

    def test_bearer_search_request_shape(self):
        server = FakeServer(search_hits=[hit_json("a", "f()", "lib/a.py", FUNC_A, line=7)])
        try:
            db = pd.NornicDB(server.url, "u", "p", token="tok", database="mydb")
            hits = db.search("some code", limit=12)
        finally:
            server.close()
        request = server.requests[0]
        self.assertEqual(request["path"], "/nornicdb/search")
        self.assertEqual(request["auth"], "Bearer tok")
        self.assertEqual(request["body"]["database"], "mydb")
        self.assertEqual(request["body"]["labels"], ["Code"])
        self.assertEqual(request["body"]["limit"], 12)
        self.assertEqual(request["body"]["filters"], {"symbol_kind": ["function"]})
        self.assertIn("body", request["body"]["include_properties"])
        self.assertEqual((hits[0].id, hits[0].line, hits[0].source_file), ("a", 7, "lib/a.py"))

    def test_search_reads_chunk_similarity_ranks_and_terms(self):
        item = hit_json("a", "f()", "lib/a.py", FUNC_A, vector_rank=2, bm25_rank=5)
        item["matches"] = [{"kind": "chunk", "chunk": 0, "score": 0.81}, {"kind": "chunk", "chunk": 1, "score": 0.87},
                           {"kind": "term", "term": "price", "property": "body", "start": 4, "end": 9},
                           {"kind": "term", "term": "price", "property": "body", "start": 20, "end": 25},
                           {"kind": "term", "term": "total", "property": "body", "start": 0, "end": 5}]
        bare = hit_json("b", "g()", "lib/b.py", FUNC_A)
        server = FakeServer(search_hits=[item, bare])
        try:
            hits = pd.NornicDB(server.url, "u", "p", database="d").search("x", limit=2)
        finally:
            server.close()
        self.assertEqual((hits[0].vector_similarity, hits[0].vector_rank, hits[0].bm25_rank), (0.87, 2, 5))
        self.assertEqual(hits[0].terms, ["price", "total"])
        self.assertEqual((hits[1].vector_similarity, hits[1].terms), (None, []))

    def test_malformed_scores_and_ranks_cost_one_value_not_the_analysis(self):
        item = hit_json("a", "f()", "lib/a.py", FUNC_A)
        item.update({"rrf_score": "n/a", "vector_rank": None, "bm25_rank": 10 ** 400})
        item["matches"] = [{"kind": "chunk", "score": "high"}, {"kind": "chunk", "score": None},
                           {"kind": "chunk", "score": float("nan")}, {"kind": "chunk", "score": True},
                           {"kind": "chunk", "score": 10 ** 400}, {"kind": "chunk", "score": 0.83}, "not-a-match",
                           {"kind": "term", "term": 7}]
        server = FakeServer(search_hits=[item])
        try:
            hit = pd.NornicDB(server.url, "u", "p", database="d").search("x", limit=1)[0]
        finally:
            server.close()
        self.assertEqual((hit.vector_similarity, hit.score, hit.vector_rank, hit.bm25_rank, hit.terms), (0.83, 0.0, 0, 0, []))

    def test_the_analysis_never_loads_the_neo4j_driver(self):
        """Checked in a fresh interpreter: other tests in this process may import neo4j for the ingest."""
        server = FakeServer(search_hits=[hit_json("a", "f()", "lib/a.py", FUNC_A)],
                            cypher={"CodeRepository": [{"commit": "abc"}]})
        code = (
            "import sys; sys.path.insert(0, %r); import pr_duplicates as pd\n"
            "db = pd.NornicDB(%r, 'u', 'p', database='d')\n"
            "pd.ingested_commit(db, 'o/r'); db.search('x', limit=3)\n"
            "print('LOADED' if any(m == 'neo4j' or m.startswith('neo4j.') for m in sys.modules) else 'CLEAN')\n"
        ) % (str(Path(__file__).resolve().parents[1] / "scripts"), server.url)
        try:
            out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
        finally:
            server.close()
        self.assertEqual(out.stdout.strip(), "CLEAN", out.stderr)

    def test_basic_auth_for_cypher(self):
        server = FakeServer(cypher={"CodeRepository": [{"commit": "abc123"}]})
        try:
            db = pd.NornicDB(server.url, "viewer", "pw", database="d")
            self.assertEqual(pd.ingested_commit(db, "o/r"), "abc123")
        finally:
            server.close()
        self.assertTrue(server.requests[0]["auth"].startswith("Basic "))
        self.assertEqual(server.requests[0]["path"], "/db/d/tx/commit")

    def test_auth_failure_is_an_analysis_error_of_kind_auth(self):
        server = FakeServer(status=401)
        try:
            db = pd.NornicDB(server.url, "u", "p", database="d")
            with self.assertRaises(pd.AnalysisError) as raised:
                db.search("x", limit=5)
        finally:
            server.close()
        self.assertEqual(raised.exception.kind, "auth")

    def test_unreachable_server(self):
        db = pd.NornicDB("http://127.0.0.1:9", "u", "p", database="d", timeout=1)
        with mock.patch("pr_duplicates.time.sleep"):
            with self.assertRaises(pd.AnalysisError) as raised:
                db.search("x", limit=5)
        self.assertEqual(raised.exception.kind, "unreachable")


class AnalyzeTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        Path(self.root, "app").mkdir()
        Path(self.root, "app/new.py").write_text(FUNC_B)
        self.graph = Path(self.root, "graph.json")
        self.graph.write_text(json.dumps({"nodes": [node("c1", "order_total()", "app/new.py", 1)], "links": []}))
        self.diff = {"app/new.py": pd.FileDiff(added=set(range(1, 8)), created=True)}

    def run_analysis(self, server, **kw):
        db = pd.NornicDB(server.url, "u", "p", database="d")
        return pd.analyze(db, "o/r", self.graph, self.root, self.diff, head_sha="h" * 40, base_sha="b" * 40,
                          options={"min_tokens": 20, **kw.pop("options", {})}, **kw)

    def test_refactor_target_with_stale_warning(self):
        server = FakeServer(
            search_hits=[hit_json("a", "total_price()", "lib/a.py", FUNC_A), hit_json("b", "price_sum()", "lib/b.py", FUNC_A)],
            cypher={"CodeRepository": [{"commit": "c" * 40}]})
        try:
            report = self.run_analysis(server)
        finally:
            server.close()
        self.assertEqual(len(report.refactor_targets), 1)
        self.assertEqual(report.analyzed, 1)
        self.assertEqual(len(report.refactor_targets[0].matches), 2)
        self.assertTrue(any("merge base" in w for w in report.warnings))

    def test_only_extracted_calls_from_the_current_caller_are_proof_of_reuse(self):
        candidate = pd.select_candidates(self.graph, self.root, self.diff,
                                         min_body_lines=0, min_added_fraction=0, max_query_chars=2000)[0]
        valid = {"source": "c1", "target": "owner", "relation": "calls", "_origin": "ast",
                 "confidence": "EXTRACTED", "source_file": "app/new.py", "source_location": "L2"}
        for overrides in [{"_origin": "inferred"}, {"relation": "related_to"},
                          {"confidence": "INFERRED"}, {"source_file": "other.py"},
                          {"source": "different-owner"}]:
            with self.subTest(overrides=overrides):
                graph = json.loads(self.graph.read_text())
                graph["links"] = [{**valid, **overrides}]
                self.graph.write_text(json.dumps(graph))
                self.assertEqual(pd.resolved_owner_calls(self.graph, [candidate]), {})

    def test_old_body_of_new_forwarding_wrapper_is_not_a_duplicate(self):
        source = Path(self.root, "app/new.py")
        existing = source.read_text()
        line = len(existing.splitlines()) + 2
        source.write_text(existing + "\ndef old_api(items):\n    return order_total(items)\n")
        graph = json.loads(self.graph.read_text())
        graph["nodes"].append(node("old-api", "old_api()", "app/new.py", line))
        self.graph.write_text(json.dumps(graph))
        self.diff["app/new.py"].added.update(range(line, line + 2))
        server = FakeServer(search_hits=[hit_json("old-api", "old_api()", "app/new.py", FUNC_A)],
                            cypher={"CodeRepository": [{"commit": "b" * 40}]})
        try:
            report = self.run_analysis(server)
        finally:
            server.close()
        self.assertEqual(report.analyzed, 1)
        self.assertEqual(report.findings, [])

    def test_semantic_match_to_resolved_existing_call_is_existing_reuse(self):
        source = Path(self.root, "app/new.py")
        source.write_text(FUNC_B.replace("    net = 0", '    greet("user")\n    net = 0'))
        self.diff["app/new.py"].added.update(range(1, 10))
        graph = json.loads(self.graph.read_text())
        graph["links"] = [{"source": "c1", "target": "owner", "relation": "calls",
                           "_origin": "ast", "confidence": "EXTRACTED",
                           "source_file": "app/new.py", "source_location": "L2"}]
        self.graph.write_text(json.dumps(graph))
        hits = []
        for index, score in enumerate([0.95, 0.45, 0.40, 0.38, 0.35, 0.33]):
            h = hit_json("owner" if index == 0 else f"other-{index}", "greet()", f"lib/{index}.py", OTHER)
            h["matches"] = [{"kind": "chunk", "score": score}]
            hits.append(h)
        server = FakeServer(search_hits=hits, cypher={"CodeRepository": [{"commit": "b" * 40}]})
        try:
            report = self.run_analysis(server)
        finally:
            server.close()
        self.assertEqual(report.findings, [])
        self.assertIn("AST-resolved existing owner", pd.render_summary(report))
        self.assertEqual(pd.report_json(report)["existing_owner_calls"][0]["owner"]["id"], "owner")

    def test_repository_never_ingested(self):
        server = FakeServer()
        try:
            report = self.run_analysis(server)
        finally:
            server.close()
        self.assertIn("nothing was compared", report.nothing_compared)
        self.assertEqual([r for r in server.requests if r["path"] == "/nornicdb/search"], [])

    def test_removed_functions_are_retired(self):
        self.diff["lib/a.py"] = pd.FileDiff(removed={1, 2, 3}, deleted=True)
        server = FakeServer(
            search_hits=[hit_json("a", "total_price()", "lib/a.py", FUNC_A), hit_json("b", "price_sum()", "lib/b.py", FUNC_A)],
            cypher={"CodeRepository": [{"commit": "b" * 40}],
                    "n.symbol_kind = 'function'": [{"id": "a", "file": "lib/a.py", "loc": "L1"}]})
        try:
            report = self.run_analysis(server)
        finally:
            server.close()
        self.assertEqual((report.findings, report.moved), ([], 1))

    def test_caps_are_reported(self):
        server = FakeServer(cypher={"CodeRepository": [{"commit": "b" * 40}]}, search_hits=[])
        try:
            report = self.run_analysis(server, options={"max_candidates": 0})
            self.assertEqual(report.skipped_by_cap, 1)
            report = self.run_analysis(server, options={"deadline_seconds": 0}, clock=iter([0, 5]).__next__)
        finally:
            server.close()
        self.assertTrue(any("time limit" in w for w in report.warnings))


class RenderTests(unittest.TestCase):
    def report(self, matches=2):
        cand = pd.Candidate("c1", "order_total()", "app/new.py", 4, FUNC_B, "", 6, 1.0, False)
        hits = [pd.Hit(f"h{i}", f"total_{i}()", f"lib/{i}.py", 10 + i, "function", "o/r", FUNC_A, 0.03, 1, 1)
                for i in range(matches)]
        report = pd.Report("o/r", "h" * 40, "b" * 40, ingested_commit="c" * 40, analyzed=1)
        report.findings = [pd.Finding(cand, [pd.Match(h, {"score": 0.93, "lexical": 0.1, "structural": 0.93}) for h in hits],
                                      "refactor" if matches >= 2 else "reuse")]
        return report

    def test_comment_has_marker_links_and_basis(self):
        text = pd.render_comment(self.report())
        self.assertTrue(text.startswith(pd.COMMENT_MARKER))
        self.assertIn("https://github.com/o/r/blob/" + "h" * 40 + "/app/new.py#L4", text)
        self.assertIn("https://github.com/o/r/blob/" + "c" * 40 + "/lib/0.py#L10", text)
        self.assertIn("93% similar", text)
        self.assertIn("never blocks", text)

    def test_comment_states_the_true_number_of_matches_when_the_list_is_capped(self):
        report = self.report(matches=2)
        report.findings[0].total = 23
        text = pd.render_comment(report)
        self.assertIn("similar to 23 existing implementations (the closest 2 are listed)", text)
        self.assertIn("...and 21 more", pd.render_summary(report))

    def test_comment_when_nothing_is_left_and_omitted_count(self):
        self.assertIn("No refactor targets found", pd.render_comment(self.report(matches=1)))
        report = self.report()
        report.findings *= 3
        self.assertIn("2 more refactor targets", pd.render_comment(report, max_findings=1))

    def test_summary_lists_reuse_candidates(self):
        text = pd.render_summary(self.report(matches=1))
        self.assertIn("Reuse candidates", text)
        self.assertNotIn("Refactor targets", text)


class CommentTests(unittest.TestCase):
    def test_create_update_and_noop(self):
        server = FakeServer()
        try:
            gh = pd.GitHub(server.url, "ghtoken", "o/r", 7)
            self.assertEqual(gh.upsert(pd.COMMENT_MARKER + "one", create=False), "none")
            self.assertEqual(server.comments, [])
            self.assertEqual(gh.upsert(pd.COMMENT_MARKER + "one", create=True), "created")
            self.assertEqual(gh.upsert(pd.COMMENT_MARKER + "two", create=False), "updated")
            self.assertEqual(len(server.comments), 1)
            self.assertIn("two", server.comments[0]["body"])
            writes = len([r for r in server.requests if r["method"] in ("POST", "PATCH")])
            self.assertEqual(gh.upsert(pd.COMMENT_MARKER + "two", create=True), "updated")
            self.assertEqual(len([r for r in server.requests if r["method"] in ("POST", "PATCH")]), writes)  # unchanged: no write
            self.assertEqual(server.requests[0]["auth"], "Bearer ghtoken")
        finally:
            server.close()

    def test_fork_detection(self):
        fork = {"pull_request": {"head": {"repo": {"full_name": "x/r", "fork": True}}, "base": {"repo": {"full_name": "o/r"}}}}
        same = {"pull_request": {"head": {"repo": {"full_name": "o/r", "fork": False}}, "base": {"repo": {"full_name": "o/r"}}}}
        self.assertTrue(pd.is_fork(fork))
        self.assertFalse(pd.is_fork(same))
        self.assertFalse(pd.is_fork({}))


class MainTests(unittest.TestCase):
    """Runs the command line against a real git repository and fake servers."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        git = lambda *a: subprocess.run(["git", "-C", self.root, *a], check=True, capture_output=True, text=True)
        git("init", "-q", "-b", "main")
        git("config", "user.email", "t@example.com")
        git("config", "user.name", "T")
        Path(self.root, "lib").mkdir()
        Path(self.root, "lib/a.py").write_text(FUNC_A)
        Path(self.root, "lib/b.py").write_text(FUNC_A.replace("total_price", "price_sum"))
        git("add", "-A")
        git("commit", "-q", "-m", "base")
        self.base = git("rev-parse", "HEAD").stdout.strip()
        git("checkout", "-q", "-b", "pr")
        Path(self.root, "app").mkdir()
        Path(self.root, "app/new.py").write_text(FUNC_B)
        git("add", "-A")
        git("commit", "-q", "-m", "pr")
        self.graph = Path(self.root, "graph.json")
        self.graph.write_text(json.dumps({"nodes": [node("c1", "order_total()", "app/new.py", 1)], "links": []}))
        self.server = FakeServer(
            search_hits=[hit_json("a", "total_price()", "lib/a.py", FUNC_A), hit_json("b", "price_sum()", "lib/b.py", FUNC_A)],
            cypher={"CodeRepository": [{"commit": self.base}]})

    def tearDown(self):
        self.server.close()

    def run_main(self, *extra, env=None):
        argv = ["--graph", str(self.graph), "--repo-root", self.root, "--base", self.base, "--head", "pr",
                "--uri", self.server.url, "--user", "viewer", "--password", "pw", "--repo", "o/r",
                "--pr-number", "5", "--github-token", "gh", "--github-api-url", self.server.url,
                "--min-tokens", "20", *extra]
        # A NORNICDB_TOKEN in the developer's shell would replace the test's Basic auth; pin it empty.
        with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": "", "GITHUB_STEP_SUMMARY": "", "NORNICDB_TOKEN": "", **(env or {})}):
            with mock.patch("builtins.print"):
                return pd.main(argv)

    def test_end_to_end_posts_a_refactor_target_comment(self):
        self.assertEqual(self.run_main(), 0)
        self.assertEqual(len(self.server.comments), 1)
        self.assertIn("order_total()", self.server.comments[0]["body"])
        self.assertIn("2 existing implementations", self.server.comments[0]["body"])

    def test_fail_on_findings(self):
        self.assertEqual(self.run_main("--fail-on-findings"), 1)

    def test_no_comment_flag_and_json_report(self):
        out = Path(self.root, "report.json")
        self.assertEqual(self.run_main("--no-comment", "--json-out", str(out)), 0)
        self.assertEqual(self.server.comments, [])
        self.assertEqual(json.loads(out.read_text())["refactor_targets"], 1)

    def test_fork_pull_request_sends_nothing(self):
        event = Path(self.root, "event.json")
        event.write_text(json.dumps({"pull_request": {"head": {"repo": {"full_name": "x/r", "fork": True}},
                                                      "base": {"repo": {"full_name": "o/r"}}}}))
        self.assertEqual(self.run_main("--event-path", str(event)), 0)
        self.assertEqual(self.server.requests, [])

    def test_rejected_credentials_do_not_fail_the_job(self):
        self.server.status = 401
        self.assertEqual(self.run_main("--fail-on-findings"), 0)
        self.assertEqual(self.server.comments, [])

    def test_missing_credential_skips(self):
        argv = ["--graph", str(self.graph), "--repo-root", self.root, "--base", self.base, "--uri", self.server.url,
                "--repo", "o/r"]
        with mock.patch.dict(os.environ, {"NORNICDB_PASSWORD": "", "NORNICDB_TOKEN": ""}), mock.patch("builtins.print"):
            self.assertEqual(pd.main(argv), 0)
        self.assertEqual(self.server.requests, [])


if __name__ == "__main__":
    unittest.main()
