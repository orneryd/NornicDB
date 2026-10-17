"""A node's text is cut to fit the embedding model's input (stdlib unittest).

NornicDB does not yet split text for an OpenAI-compatible embedder (orneryd/NornicDB#1236), so a node
over the model's limit (Titan V2: 8,192 tokens, 50,000 characters) would never get a vector.
"""
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import graphify_local as gi  # noqa: E402


class CapTests(unittest.TestCase):
    def test_short_text_is_untouched(self):
        self.assertEqual(gi.cap_embedding_text("def f(): pass", "# hi", 1000), ("def f(): pass", "# hi", False))
        self.assertEqual(gi.cap_embedding_text("x" * 50, None, 0), ("x" * 50, None, False))

    def test_long_body_is_cut_with_a_marker(self):
        body, comments, cut = gi.cap_embedding_text("x" * 90000, None, 20000)
        self.assertTrue(cut)
        self.assertIsNone(comments)
        self.assertEqual(len(body), 20000)
        self.assertTrue(body.endswith(gi.TRUNCATION_MARKER))

    def test_comments_get_at_most_a_fifth(self):
        body, comments, cut = gi.cap_embedding_text("b" * 30000, "c" * 30000, 20000)
        self.assertTrue(cut)
        self.assertEqual(len(comments), 4000)
        self.assertLessEqual(len(body) + len(comments), 20000)
        self.assertTrue(body.endswith(gi.TRUNCATION_MARKER) and comments.endswith(gi.TRUNCATION_MARKER))
        # Short comments keep their whole length; the body gets the rest.
        body, comments, _ = gi.cap_embedding_text("b" * 30000, "short", 20000)
        self.assertEqual(comments, "short")
        self.assertEqual(len(body) + len(comments), 20000)

    def test_the_total_never_exceeds_the_cap(self):
        # Small but valid cap: neither field may push the total over it.
        for body_len, comment_len in [(950, 200), (5000, 5000), (999, 2), (10, 3000)]:
            body, comments, cut = gi.cap_embedding_text("b" * body_len, "c" * comment_len, gi.MIN_EMBED_CHARS)
            self.assertTrue(cut)
            self.assertLessEqual(len(body or "") + len(comments or ""), gi.MIN_EMBED_CHARS, (body_len, comment_len))

    def test_a_cap_too_small_for_the_marker_is_rejected(self):
        with self.assertRaises(ValueError):
            gi.cap_embedding_text("x" * 50, None, 10)
        with self.assertRaises(ValueError):
            gi.cap_embedding_text("x" * 50, None, -1)


class Session:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, **params):
        for row in params.get("rows") or []:
            if isinstance(row, dict) and "props" in row:
                self.rows.append(row["props"])
        return gi.HttpResult([])


class Driver:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def session(self, database=None):
        return Session(self.rows)


class IngestCapTests(unittest.TestCase):
    def ingest(self, **kwargs):
        root = tempfile.mkdtemp()
        lines = ["def big():"] + [f"    value_{i} = {i}" for i in range(4000)] + ["    return 1"]
        Path(root, "m.py").write_text("\n".join(lines) + "\n")
        graph = Path(root, "graph.json")
        graph.write_text(json.dumps({"nodes": [{"id": "big", "label": "big()", "file_type": "code", "source_file": "m.py",
                                                 "source_location": "L1", "_callable": True}], "links": []}))
        rows = []
        out = StringIO()
        with mock.patch.object(gi, "connect", lambda *a, **k: Driver(rows)), \
                mock.patch.object(gi, "ensure_database"), redirect_stdout(out):
            gi.import_graph(graph, "http://x", "u", "p", 100, root, "db", "o/r", sync=False, **kwargs)
        node = next(r for r in rows if r.get("id") == "big")
        return node, out.getvalue()

    def test_a_long_body_is_kept_whole_by_default(self):
        # NornicDB 1.4.3+ chunks long text itself, so there is no cap unless one is asked for.
        self.assertEqual(gi.DEFAULT_MAX_EMBED_CHARS, 0)
        node, out = self.ingest()
        self.assertGreater(len(node["body"]), 60000)
        self.assertNotIn(gi.TRUNCATION_MARKER, node["body"])
        self.assertNotIn("cut to", out)

    def test_a_cap_cuts_a_long_body(self):
        node, out = self.ingest(max_embed_chars=20000)
        self.assertLessEqual(len(node["body"]), 20000)
        self.assertTrue(node["body"].endswith(gi.TRUNCATION_MARKER))
        self.assertIn("1 cut to 20000 characters for the embedding model", out)

    def test_a_body_over_the_http_limit_is_cut_to_fit_not_dropped(self):
        with mock.patch.object(gi, "HTTP_MAX_BODY_BYTES", 30000), redirect_stderr(StringIO()) as err:
            # A cap below the HTTP limit is all that applies.
            node, out = self.ingest(max_embed_chars=20000)
            self.assertLessEqual(len(node["body"]), 20000)
            self.assertNotIn("HTTP request limit", out)
            # No cap: the body is cut to the transport limit, and kept.
            node, out = self.ingest()
        self.assertTrue(node["body"].endswith(gi.TRUNCATION_MARKER))
        self.assertLessEqual(len(node["body"].encode("utf-8")), 30000)
        self.assertGreater(len(node["body"]), 25000)
        self.assertIn("1 cut to fit the HTTP request limit", out)
        self.assertIn("Cut body of big", err.getvalue())

    def test_fit_http_body_counts_bytes_not_characters(self):
        body, comments, cut = gi.fit_http_body("é" * 40000, "ü" * 5000, 30000)  # 90,000 bytes
        self.assertTrue(cut)
        self.assertLessEqual(len(body.encode("utf-8")) + len(comments.encode("utf-8")), 30000)
        self.assertTrue(body.endswith(gi.TRUNCATION_MARKER))
        self.assertEqual(gi.fit_http_body("small", None, 30000), ("small", None, False))

    def test_fit_http_body_counts_json_escaping(self):
        # 20,000 control characters are 20,000 UTF-8 bytes but 120,000 once JSON-escaped.
        body, _, cut = gi.fit_http_body("\x01" * 20000, None, 30000)
        self.assertTrue(cut)
        self.assertLessEqual(len(json.dumps(body, ensure_ascii=False).encode("utf-8")), 30000)

    def test_the_whole_request_for_one_oversized_node_fits(self):
        # Escaping-heavy text that fits in raw bytes but not serialized: the request sent for the
        # node still fits under HTTP_MAX_REQUEST_BYTES, so HttpSession.run never needs to split it.
        sent = []
        session = gi.HttpSession("http://x", "Basic x", "db")
        def fake_urlopen(request, timeout=None):
            sent.append(len(request.data))
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.read.return_value = b'{"results": [{"columns": ["updated"], "data": [{"row": [1]}]}], "errors": []}'
            return response
        body, comments, _ = gi.fit_http_body('"\\' * 1_800_000 + "\x02" * 300_000, None, gi.HTTP_MAX_BODY_BYTES)
        props = {"id": "n", "repo": "o/r", "body": body}
        props["props_hash"] = gi.content_hash(props)
        with mock.patch.object(gi.urllib.request, "urlopen", fake_urlopen), \
                mock.patch.object(gi.json, "load", lambda response: json.loads(response.read())):
            session.run("UNWIND $rows AS row MERGE (n:Code {id: row.id, repo: $repo}) SET n += row.props",
                        rows=[{"id": "n", "props": props}], repo="o/r")
        self.assertEqual(len(sent), 1)
        self.assertLessEqual(sent[0], gi.HTTP_MAX_REQUEST_BYTES)
        self.assertEqual(gi.fit_http_body("x" * 50, None, None), ("x" * 50, None, False))


class CliTests(unittest.TestCase):
    def parse(self, argv, environ):
        environ = {"NORNICDB_URI": "http://localhost:7474", "NORNICDB_TOKEN": "test-token", "CODE_INTEL_MAX_EMBED_CHARS": "", **environ}  # never inherit the runner's cap
        with mock.patch.dict("os.environ", environ, clear=False), \
                mock.patch.object(sys, "argv", ["graphify_ingest.py", "--check", "--repo", "o/r", "--commit", "c1"] + argv), \
                mock.patch.object(gi, "last_ingested_state", self.state), \
                mock.patch.object(gi, "write_github_output"), redirect_stdout(StringIO()) as out:
            gi.main()
        return out.getvalue()

    def setUp(self):
        self.recorded = {"commit": "c1", "main_spec": "", "max_embed_chars": 20000}
        self.state = lambda *a, **k: self.recorded

    def test_empty_env_means_the_default_no_cap(self):
        self.recorded["max_embed_chars"] = 0
        out = self.parse([], {"CODE_INTEL_MAX_EMBED_CHARS": "", "CODE_INTEL_MAIN": ""})
        self.assertIn("needed=false", out)

    def test_dropping_the_old_cap_re_ingests(self):
        # A repo ingested with the old 20000 default gets its full bodies back on the next check.
        out = self.parse([], {"CODE_INTEL_MAIN": ""})
        self.assertIn("embedding cap changed", out)
        self.assertIn("needed=true", out)

    def test_a_changed_cap_needs_an_ingest(self):
        out = self.parse(["--max-embed-chars", "15000"], {"CODE_INTEL_MAIN": ""})
        self.assertIn("embedding cap changed", out)
        self.assertIn("needed=true", out)
        # A run from before the cap existed recorded none, i.e. no cap: the same as the default.
        self.recorded.pop("max_embed_chars")
        self.assertIn("needed=false", self.parse([], {"CODE_INTEL_MAIN": ""}))
        self.assertIn("needed=true", self.parse(["--max-embed-chars", "20000"], {"CODE_INTEL_MAIN": ""}))

    def test_negative_or_tiny_caps_are_rejected(self):
        for value in ["-1", "10"]:
            with self.assertRaises(SystemExit), mock.patch("sys.stderr", StringIO()):
                self.parse(["--max-embed-chars", value], {"CODE_INTEL_MAIN": ""})


if __name__ == "__main__":
    unittest.main()
