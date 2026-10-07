import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.graphify_local import (
    DEFAULT_DATABASE,
    DEFAULT_MAIN,
    MainSelectionError,
    body_for_node,
    comments_for_node,
    body_span,
    choose_main,
    collect_symbol_lines,
    comment_mask,
    comment_style,
    content_hash,
    emit_enriched_graph,
    import_graph,
    is_test_source,
    main,
    node_label,
    parse_location,
    relationship_type,
    scalar_properties,
    symbol_kind,
)

REPO = "orneryd/NornicDB"


class RecordingSession:
    """Stands in for a driver session: records queries, answers by query text."""

    def __init__(self):
        self.queries = []
        self.records = []          # default rows for any iterated query (SHOW DATABASES ...)
        self.responses = []        # [(substring of the query, rows)], first match wins
        self.database = None
        self._last = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, query, **params):
        self.queries.append((query, json.loads(json.dumps(params))))
        self._last = query
        return self

    def consume(self):
        return None

    def single(self):
        return None

    def __iter__(self):
        for fragment, rows in self.responses:
            if fragment in self._last:
                return iter(rows)
        return iter(self.records)


class RecordingDriver:
    def __init__(self):
        self.connection = RecordingSession()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def session(self, **kwargs):
        self.connection.database = kwargs.get("database")
        return self.connection


def ingest(graph, driver, root=".", batch_size=10, database="nornicdbcode", **kwargs):
    """import_graph against a recording driver, with the graph written to a temp file."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "graph.json"
        path.write_text(json.dumps(graph), encoding="utf-8")
        with patch("scripts.graphify_local.connect", return_value=driver):
            import_graph(path, "http://localhost:7474", "admin", "test", batch_size, root, database, REPO, **kwargs)


class GraphifyLocalTest(unittest.TestCase):
    def test_graphify_labels_and_scalar_properties(self):
        self.assertEqual(node_label({"file_type": "source-file"}), "Sourcefile")
        self.assertEqual(relationship_type({"relation": "calls-to"}), "CALLS_TO")
        self.assertEqual(scalar_properties({"name": "x", "weight": 1.25, "_origin": "ast", "metadata": {}}),
                         {"name": "x", "weight": 1.25})
        self.assertEqual(scalar_properties({"source": "a", "target": "b", "relation": "calls"}, edge=True),
                         {"relation": "calls"})

    def test_import_includes_implicit_endpoints_and_scopes_every_write_to_the_repo(self):
        graph = {
            "nodes": [{"id": "source", "label": "Source", "file_type": "code"}],
            "links": [{"source": "source", "target": "implicit", "relation": "calls", "confidence": "EXTRACTED"}],
        }
        driver = RecordingDriver()
        ingest(graph, driver)
        queries = driver.connection.queries
        self.assertEqual(driver.connection.database, "nornicdbcode")

        code_merge = [params for query, params in queries
                      if query.startswith("UNWIND $rows AS row MERGE (n:Code {id: row.id, repo: $repo}) ON CREATE")]
        self.assertEqual(len(code_merge), 1)
        self.assertEqual(code_merge[0]["repo"], REPO)
        props = code_merge[0]["rows"][0]["props"]
        self.assertEqual([row["id"] for row in code_merge[0]["rows"]], ["source"])
        self.assertEqual((props["label"], props["file_type"], props["repo"]), ("Source", "code", REPO))
        self.assertIn("updated_at", props)
        self.assertEqual(props["symbol_kind"], "external")  # no source file: referenced, not defined
        self.assertEqual(props["props_hash"], content_hash({k: v for k, v in props.items()}))

        entity_merge = [params for query, params in queries
                        if query.startswith("UNWIND $rows AS row MERGE (n:Entity {id: row.id, repo: $repo}) ON CREATE")]
        self.assertEqual([row["id"] for row in entity_merge[0]["rows"]], ["implicit"])
        self.assertEqual(entity_merge[0]["rows"][0]["props"]["symbol_kind"], "external")

        edge_props = {"relation": "calls", "confidence": "EXTRACTED",
                      "props_hash": content_hash({"relation": "calls", "confidence": "EXTRACTED"})}
        edge_merge = [(query, params) for query, params in queries if "MERGE (a)-[r:CALLS]" in query]
        self.assertEqual(len(edge_merge), 1)
        self.assertIn("MATCH (a:Code {id: row.src, repo: $repo}), (b:Entity {id: row.tgt, repo: $repo})", edge_merge[0][0])
        self.assertEqual(edge_merge[0][1]["rows"], [{"src": "source", "tgt": "implicit", "props": edge_props}])
        self.assertFalse(any("MATCH (a:Code {id: row.src})-[r:CALLS]->" in q for q, _ in queries),
                         "edge writes must stay on the single MERGE fast path")

    def test_import_flushes_full_and_partial_batches_and_indexes_first(self):
        graph = {
            "nodes": [{"id": node_id, "file_type": "code"} for node_id in ("a", "b", "c")],
            "edges": [
                {"source": "a", "target": "new1", "relation": "calls"},
                {"source": "b", "target": "new2", "relation": "calls"},
                {"source": "c", "target": "new1", "relation": "imports"},
            ],
        }
        driver = RecordingDriver()
        ingest(graph, driver, batch_size=2)
        queries = driver.connection.queries
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (n:Code" in query], [2, 1])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (n:Entity" in query], [2])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (a)-[r:CALLS]" in query], [2])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (a)-[r:IMPORTS]" in query], [1])
        # an id index and a repo index per label
        self.assertEqual(sum("CREATE INDEX graphify_" in query for query, _ in queries), 4)
        # Indexes precede the first MERGE for their label so lookups never scan the label.
        index_positions = {}
        for position, (query, _) in enumerate(queries):
            if "CREATE INDEX graphify_" in query:
                index_positions.setdefault(re.search(r"FOR \(n:(\w+)\)", query).group(1), position)
        for position, (query, _) in enumerate(queries):
            if "MERGE (n:" in query:
                label = query.split("MERGE (n:")[1].split(" ")[0]
                self.assertLess(index_positions[label], position, f"index for {label} must precede its first MERGE")

    def test_sync_deletes_only_stale_nodes_and_edges_this_importer_wrote(self):
        graph = {
            "nodes": [{"id": "a", "file_type": "code"}, {"id": "b", "file_type": "code"}],
            "links": [{"source": "a", "target": "b", "relation": "calls"}],
        }
        driver = RecordingDriver()
        driver.connection.responses = [
            ("RETURN n.id AS id", [{"id": "a"}, {"id": "b"}, {"id": "stale-node"}]),
            ("RETURN a.id AS src", [{"src": "a", "tgt": "b", "rel": "CALLS"},
                                    {"src": "a", "tgt": "stale-node", "rel": "CALLS"}]),
        ]
        ingest(graph, driver)
        queries = driver.connection.queries
        delete_nodes = [(q, params) for q, params in queries if "DETACH DELETE n" in q]
        self.assertEqual([params["ids"] for _, params in delete_nodes], [["stale-node"]])
        self.assertEqual(delete_nodes[0][1]["repo"], REPO)
        # hand-made nodes (no props_hash) and other repos are never candidates
        self.assertIn("n.props_hash IS NOT NULL", delete_nodes[0][0])
        fetch_ids = [q for q, _ in queries if "RETURN n.id AS id" in q][0]
        self.assertIn("n.repo = $repo", fetch_ids)
        self.assertIn("n.props_hash IS NOT NULL", fetch_ids)
        delete_edges = [params for q, params in queries if "MATCH (a {id: row.src, repo: $repo})-[r]->(b {id: row.tgt, repo: $repo})" in q]
        self.assertEqual([p["rows"] for p in delete_edges], [[{"src": "a", "tgt": "stale-node", "rel": "CALLS"}]])
        # the sync's own fetch (the last one) is restricted to nodes this importer wrote
        fetch_edges = [q for q, _ in queries if "RETURN a.id AS src, b.id AS tgt, type(r) AS rel" in q][-1]
        self.assertIn("a.props_hash IS NOT NULL AND b.props_hash IS NOT NULL", fetch_edges)

    def test_sync_refuses_a_mass_delete_before_deleting_anything(self):
        nodes = [{"id": f"n{i}", "file_type": "code"} for i in range(50)]
        graph = {"nodes": nodes, "links": []}
        existing = [{"id": f"n{i}"} for i in range(300)]  # 250 of 300 are missing from this graph
        driver = RecordingDriver()
        driver.connection.responses = [("RETURN n.id AS id", existing)]
        with self.assertRaises(SystemExit) as raised:
            ingest(graph, driver)
        self.assertIn("Refusing to sync", str(raised.exception))
        self.assertFalse(any("DELETE" in q for q, _ in driver.connection.queries))
        self.assertFalse(any("CodeRepository" in q for q, _ in driver.connection.queries),
                         "a refused run must not record its commit, so the next run retries")
        # an explicit allowance goes through
        driver = RecordingDriver()
        driver.connection.responses = [("RETURN n.id AS id", existing)]
        ingest(graph, driver, max_delete_fraction=1.0)
        deleted = [params["ids"] for q, params in driver.connection.queries if "DETACH DELETE n" in q]
        self.assertEqual(sum(len(batch) for batch in deleted), 250)

    def test_gentle_updates_compare_the_hash_inside_each_row(self):
        graph = {
            "nodes": [{"id": "a", "file_type": "code"}],
            "links": [{"source": "a", "target": "a", "relation": "calls"}],
        }
        driver = RecordingDriver()
        ingest(graph, driver)
        update_queries = [q for q, _ in driver.connection.queries if "props_hash <> row.props.props_hash" in q]
        self.assertEqual(len(update_queries), 1)  # nodes only; edges use the single MERGE fast path
        self.assertIn("n.props_hash IS NULL OR n.props_hash", update_queries[0])
        # rows are {id, props}: row.props_hash does not exist and is always NULL, which would
        # leave changed nodes unwritten forever
        self.assertFalse([q for q, _ in driver.connection.queries if "row.props_hash" in q])
        for q, _ in driver.connection.queries:
            self.assertNotIn("n.updated_at < row.updated_at", q)

    def test_unchanged_nodes_and_edges_are_not_sent_again(self):
        """A re-run, or one resuming an interrupted ingest, sends only what is new or different."""
        nodes = [{"id": name, "file_type": "code", "label": name} for name in ("a", "b", "c")]
        links = [{"source": "a", "target": "b", "relation": "calls"},
                 {"source": "b", "target": "c", "relation": "calls"},
                 {"source": "a", "target": "c", "relation": "imports"}]
        graph = {"nodes": nodes, "links": links}
        node_hash = lambda name: content_hash({"id": name, "file_type": "code", "label": name, "repo": REPO,
                                               "symbol_kind": "external"})
        edge_hash = lambda relation: content_hash({"relation": relation})
        driver = RecordingDriver()
        driver.connection.responses = [
            # a and b are already stored identically; c is stored with an outdated hash
            ("RETURN n.id AS id, n.props_hash AS hash",
             [{"id": "a", "hash": node_hash("a")}, {"id": "b", "hash": node_hash("b")}, {"id": "c", "hash": "stale"}]),
            # a->b is stored identically; b->c is missing; a->c has an outdated hash
            ("type(r) AS rel, r.props_hash AS hash",
             [{"src": "a", "tgt": "b", "rel": "CALLS", "hash": edge_hash("calls")},
              {"src": "a", "tgt": "c", "rel": "IMPORTS", "hash": "stale"}]),
        ]
        output = []
        with patch("builtins.print", side_effect=lambda *a, **k: output.append(" ".join(map(str, a)))):
            ingest(graph, driver, main=None, main_spec="none")
        queries = driver.connection.queries
        sent_nodes = [row["id"] for q, p in queries if "MERGE (n:Code" in q for row in p["rows"]]
        self.assertEqual(sent_nodes, ["c"])
        sent_edges = [(row["src"], row["tgt"]) for q, p in queries if "MERGE (a)-[r:" in q for row in p["rows"]]
        self.assertEqual(sorted(sent_edges), [("a", "c"), ("b", "c")])
        self.assertTrue(any("Skipped 2 nodes and 1 edges" in line for line in output), output)

    def test_rewrite_all_sends_everything(self):
        graph = {"nodes": [{"id": "a", "file_type": "code"}], "links": []}
        driver = RecordingDriver()
        driver.connection.responses = [("RETURN n.id AS id, n.props_hash AS hash", [])]
        ingest(graph, driver, main=None, main_spec="none", skip_unchanged=False)
        self.assertFalse([q for q, _ in driver.connection.queries if "RETURN n.id AS id, n.props_hash AS hash" in q],
                         "no existing-state fetch when everything is rewritten")
        self.assertEqual([row["id"] for q, p in driver.connection.queries if "MERGE (n:Code" in q for row in p["rows"]], ["a"])

    def test_content_hash_is_stable_and_ignores_metadata(self):
        self.assertEqual(content_hash({"a": "1", "b": "2"}), "be9380e87280e5d5")
        self.assertEqual(content_hash({"b": "2", "a": "1", "updated_at": 9.9, "props_hash": "zz"}),
                         "be9380e87280e5d5")
        self.assertNotEqual(content_hash({"a": "1"}), content_hash({"a": "2"}))

    def test_main_is_tagged_as_a_second_label_and_moves(self):
        graph = {
            "nodes": [{"id": "m", "label": "main()", "file_type": "code", "source_file": "cmd/x/main.go",
                       "source_location": "L3"},
                      {"id": "h", "label": "helper()", "file_type": "code", "source_file": "x.go", "source_location": "L1"}],
            "links": [{"source": "m", "target": "h", "relation": "calls"}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            chosen = choose_main(path, "cmd/x/main.go:main")
        driver = RecordingDriver()
        ingest(graph, driver, main=chosen, main_spec="cmd/x/main.go:main")
        queries = driver.connection.queries
        untag = [(q, p) for q, p in queries if "REMOVE n:Main" in q]
        tag = [(q, p) for q, p in queries if "SET n:Main" in q]
        self.assertEqual(len(untag), 1)
        self.assertIn("WHERE n.id <> $id", untag[0][0])
        self.assertEqual(untag[0][1], {"repo": REPO, "id": "m"})
        self.assertEqual([p for _, p in tag], [{"id": "m", "repo": REPO}])
        self.assertIn("MATCH (n:Code {id: $id, repo: $repo}) SET n:Main", tag[0][0])
        record = [p for q, p in queries if "MERGE (r:CodeRepository" in q][0]
        self.assertEqual((record["main_id"], record["main_spec"]), ("m", "cmd/x/main.go:main"))
        # labels are not part of the content hash, so tagging never rewrites a node
        node_props = [row["props"] for q, p in queries if "MERGE (n:Code" in q for row in p["rows"]]
        self.assertTrue(all("Main" not in json.dumps(props) for props in node_props))
        # no main: any previous :Main is removed and nothing is tagged
        driver = RecordingDriver()
        ingest(graph, driver, main=None, main_spec="none")
        self.assertFalse([q for q, _ in driver.connection.queries if "SET n:Main" in q])
        self.assertEqual([p["id"] for q, p in driver.connection.queries if "REMOVE n:Main" in q], [""])

    def test_import_reports_large_progress(self):
        driver = RecordingDriver()

        def items(_path, key):
            if key == "nodes":
                return iter([{"id": "source", "file_type": "code"}])
            return ({"source": "source", "target": "source", "relation": "calls"} for _ in range(10000))

        with patch("scripts.graphify_local.connect", return_value=driver), \
             patch("scripts.graphify_local.graph_items", side_effect=items), \
             patch("builtins.print") as output:
            import_graph(Path("unused"), "http://localhost:7474", "admin", "test", 250, ".", "nornicdbcode", REPO)

        output.assert_any_call("Imported 10000 edges", flush=True)
        self.assertEqual(sum(len(params["rows"]) for query, params in driver.connection.queries
                             if "MERGE (a)-[r:CALLS]" in query), 10000)

    def test_import_uses_links_key_and_writes_bodies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "demo.go").write_text("// Doc for A.\nfunc A() {}\n\nfunc B() {}\n", encoding="utf-8")
            graph = {
                "nodes": [
                    {"id": "a", "label": "A()", "file_type": "code",
                     "source_file": "demo.go", "source_location": "L2"},
                    {"id": "b", "label": "B()", "file_type": "code",
                     "source_file": "demo.go", "source_location": "L4"},
                ],
                "links": [{"source": "a", "target": "b", "relation": "calls", "confidence": "EXTRACTED"}],
            }
            driver = RecordingDriver()
            ingest(graph, driver, root=str(root))

        queries = driver.connection.queries
        self.assertIn(("CREATE DATABASE `nornicdbcode` IF NOT EXISTS", {}), queries)
        self.assertIn("SHOW DATABASES", [q for q, _ in queries][0])
        self.assertEqual(driver.connection.database, "nornicdbcode")
        node_rows = {row["id"]: row["props"]
                     for query, params in queries if "MERGE (n:" in query
                     for row in params["rows"]}
        # Comments live in their own property, apart from the symbol body.
        self.assertNotIn("// Doc for A.", node_rows["a"]["body"])
        self.assertIn("// Doc for A.", node_rows["a"]["comments"])
        self.assertTrue(node_rows["a"]["body"].rstrip().endswith("}"))
        self.assertEqual(node_rows["b"]["body"], "func B() {}")
        # Go has no _callable flag, but every function and method label ends in "()"
        self.assertEqual((node_rows["a"]["symbol_kind"], node_rows["b"]["symbol_kind"]), ("function", "function"))
        self.assertEqual(sum(len(params["rows"]) for query, params in queries
                             if "MERGE (a)-[r:CALLS]" in query), 1)

    def test_cli_defaults_and_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            graph = Path(directory) / "graph.json"
            graph.write_text('{"nodes":[],"edges":[]}', encoding="utf-8")
            with patch("sys.argv", ["graphify_local.py", "--batch-size", "0"]), self.assertRaises(SystemExit):
                main()
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph) + ".missing"]), \
                 self.assertRaises(SystemExit):
                main()
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph), "--database", "_x"]), \
                 self.assertRaises(SystemExit):
                main()
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph), "--main", "none", "--batch-size", "2"]), \
                 patch.dict("os.environ", {"NEO4J_PASSWORD": "test"}, clear=True), \
                 patch("scripts.graphify_local.repo_from_git", return_value=REPO), \
                 patch("scripts.graphify_local.git_output", return_value="abc123"), \
                 patch("scripts.graphify_local.import_graph") as importer:
                main()
            args, kwargs = importer.call_args
            self.assertEqual(args, (graph, "http://localhost:7474", "admin", "test", 2, ".", DEFAULT_DATABASE, REPO))
            self.assertEqual((kwargs["commit"], kwargs["branch"], kwargs["sync"], kwargs["main"], kwargs["main_spec"]),
                             ("abc123", "abc123", True, None, "none"))
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph), "--main", "none"]), \
                 patch.dict("os.environ", {}, clear=True), \
                 patch("scripts.graphify_local.import_graph") as importer:
                main()
            self.assertEqual(importer.call_args[0][1:4], ("http://localhost:7474", "admin", "password"))
        self.assertEqual(DEFAULT_MAIN, "cmd/nornicdb/main.go:main")

    def test_import_empty_graph_never_deletes(self):
        driver = RecordingDriver()
        driver.connection.records = [{"name": "neo4j"}, {"name": "nornicdbcode"}]
        ingest({"nodes": [], "edges": []}, driver, main=None, main_spec="none")
        for query, _ in driver.connection.queries:
            self.assertNotIn("DETACH DELETE", query)
            self.assertNotIn("DELETE r", query)

    def test_import_skips_create_when_database_exists(self):
        driver = RecordingDriver()
        driver.connection.records = [{"name": "neo4j"}, {"name": "graphify"}]
        ingest({"nodes": [], "links": []}, driver, database="graphify", main=None, main_spec="none")
        self.assertFalse(any("CREATE DATABASE" in q for q, _ in driver.connection.queries),
                         "existing database must not be recreated")
        self.assertEqual(driver.connection.database, "graphify")

    def test_parse_location(self):
        self.assertEqual(parse_location("L17"), 17)
        self.assertEqual(parse_location("L1"), 1)
        self.assertIsNone(parse_location(""))
        self.assertIsNone(parse_location("17"))
        self.assertIsNone(parse_location("Lx"))
        self.assertIsNone(parse_location(None))

    def test_emit_enriched_graph_embeds_bodies_and_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "demo.go").write_text("// Doc for A.\nfunc A() {}\n\nfunc B() {}\n", encoding="utf-8")
            graph = {
                "nodes": [
                    {"id": "a", "label": "A()", "file_type": "code",
                     "source_file": "demo.go", "source_location": "L2"},
                    {"id": "b", "label": "B()", "file_type": "code",
                     "source_file": "demo.go", "source_location": "L4"},
                ],
                "links": [{"source": "a", "target": "b", "relation": "calls"}],
                "directed": False,
            }
            graph_path = root / "graph.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            out_path = root / "enriched.json"
            emit_enriched_graph(graph_path, out_path, str(root))
            data = json.loads(out_path.read_text(encoding="utf-8"))
            by_id = {n["id"]: n for n in data["nodes"]}
            self.assertNotIn("// Doc for A.", by_id["a"]["body"])
            self.assertTrue(by_id["a"]["body"].rstrip().endswith("}"))
            self.assertEqual(by_id["b"]["body"], "func B() {}")
            self.assertEqual(data["links"], graph["links"])
            # CLI: --out-graph skips ingestion entirely.
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph_path),
                                    "--out-graph", str(out_path), "--repo-root", str(root)]), \
                 patch("scripts.graphify_local.emit_enriched_graph") as emitter, \
                 patch("scripts.graphify_local.import_graph") as importer:
                main()
                emitter.assert_called_once()
                importer.assert_not_called()

    def test_comment_style_and_mask(self):
        self.assertEqual(comment_style("main.py"), "hash")
        self.assertEqual(comment_style("run.sh"), "hash")
        self.assertEqual(comment_style("Dockerfile"), "hash")
        self.assertEqual(comment_style("query.sql"), "dash")
        self.assertEqual(comment_style("main.go"), "c")
        lines = [
            "// leading",
            "/* block",
            " * continuation",
            " */",
            "package demo",
            "// trailing",
            "var x = 1",
        ]
        self.assertEqual(comment_mask(lines, "c"), [True, True, True, True, False, True, False])
        self.assertEqual(comment_mask(["# one", "x = 1", "# two"], "hash"), [True, False, True])
        self.assertEqual(comment_mask(["-- note", "SELECT 1"], "dash"), [True, False])

    def test_body_span_attaches_comment_block_above(self):
        lines = [
            "// Copyright 2026",
            "package demo",
            "",
            "import \"fmt\"",
            "",
            "// Compute adds one.",
            "//",
            "// More detail.",
            "func Compute(x int) int {",
            "\treturn x + 1",
            "}",
        ]
        mask = comment_mask(lines, "c")
        # Doc block above Compute: lines 6-8, with one blank line (5) between.
        self.assertEqual(body_span(lines, 9, mask, "c"), 5)
        # The package clause adopts the file's license header directly above it
        # (this never affects symbol bodies: package/import code separates them).
        self.assertEqual(body_span(lines, 2, mask, "c"), 1)
        self.assertIsNone(body_span(lines, 9, mask, "none"))

    def test_body_span_stops_at_double_blank_and_code(self):
        lines = [
            "// header block",
            "//",
            "",
            "",
            "// detached doc",
            "func A() {}",
        ]
        mask = comment_mask(lines, "c")
        # Two blank lines separate the header block from the doc comment; the
        # walk tolerates one blank (line 4) but stops at the second (line 3).
        self.assertEqual(body_span(lines, 6, mask, "c"), 4)
        lines = [
            "# doc for f",
            "import os",
            "",
            "def f(): pass",
        ]
        mask = comment_mask(lines, "hash")
        self.assertIsNone(body_span(lines, 4, mask, "hash"))

    def test_body_for_node_full_symbol_bodies(self):
        go_source = """// Package demo does things.
package demo

import "fmt"

// Compute adds one.
//
// More detail.
func Compute(x int) int {
\t// inside comment
\treturn x + 1
}

// Other is second.
func Other() {}
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pkg" / "demo").mkdir(parents=True)
            (root / "pkg" / "demo" / "demo.go").write_text(go_source, encoding="utf-8")
            graph = {
                "nodes": [
                    {"id": "container", "label": "demo.go", "file_type": "code",
                     "source_file": "pkg/demo/demo.go", "source_location": "L1"},
                    {"id": "compute", "label": "Compute()", "file_type": "code",
                     "source_file": "pkg/demo/demo.go", "source_location": "L9"},
                    {"id": "other", "label": "Other()", "file_type": "code",
                     "source_file": "pkg/demo/demo.go", "source_location": "L15"},
                    {"id": "missing", "label": "Gone()", "file_type": "code",
                     "source_file": "pkg/demo/absent.go", "source_location": "L1"},
                ],
                "edges": [],
            }
            graph_path = root / "graph.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            symbols = collect_symbol_lines(graph_path)
            cache = {}
            state_cache = {}
            by_id = {n["id"]: n for n in graph["nodes"]}

            container_body = body_for_node(root, by_id["container"], symbols, cache, state_cache)
            self.assertIsNone(container_body, "file container nodes should not carry a whole-file body")

            compute_body = body_for_node(root, by_id["compute"], symbols, cache, state_cache)
            self.assertIsNotNone(compute_body)
            # Full, untruncated body without comment-only lines (those go to `comments`), and it
            # stops before Other()'s own doc comment.
            self.assertEqual(compute_body.splitlines()[0], "func Compute(x int) int {")
            self.assertIn("return x + 1", compute_body)
            self.assertNotIn("// Compute adds one.", compute_body)
            self.assertNotIn("// inside comment", compute_body)
            self.assertNotIn("// Other is second.", compute_body)
            self.assertTrue(compute_body.rstrip().endswith("}"))
            compute_comments = comments_for_node(root, by_id["compute"], symbols, {}, cache, state_cache)
            self.assertIn("// Compute adds one.", compute_comments)
            self.assertIn("// More detail.", compute_comments)
            self.assertIn("// inside comment", compute_comments)
            self.assertNotIn("// Other is second.", compute_comments)

            other_body = body_for_node(root, by_id["other"], symbols, cache, state_cache)
            self.assertEqual(other_body, "func Other() {}")
            self.assertEqual(comments_for_node(root, by_id["other"], symbols, {}, cache, state_cache), "// Other is second.")

            self.assertIsNone(body_for_node(root, by_id["missing"], symbols, cache, state_cache))

    def test_body_for_node_pages_and_headings(self):
        markdown = """# Title

Intro paragraph.

## Section One

Section body.

## Section Two

More body.
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "doc.md").write_text(markdown, encoding="utf-8")
            graph = {
                "nodes": [
                    {"id": "page", "label": "doc.md", "file_type": "document", "node_kind": "page",
                     "source_file": "doc.md", "source_location": "L1"},
                    {"id": "h1", "label": "Section One", "file_type": "document", "node_kind": "heading",
                     "source_file": "doc.md", "source_location": "L5"},
                    {"id": "h2", "label": "Section Two", "file_type": "document", "node_kind": "heading",
                     "source_file": "doc.md", "source_location": "L9"},
                ],
                "edges": [],
            }
            graph_path = root / "graph.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            symbols = collect_symbol_lines(graph_path)
            cache = {}
            state_cache = {}
            by_id = {n["id"]: n for n in graph["nodes"]}
            page_body = body_for_node(root, by_id["page"], symbols, cache, state_cache)
            self.assertEqual(page_body, markdown.rstrip("\n"))
            h1_body = body_for_node(root, by_id["h1"], symbols, cache, state_cache)
            self.assertIn("## Section One", h1_body)
            self.assertIn("Section body.", h1_body)
            self.assertNotIn("## Section Two", h1_body)
            h2_body = body_for_node(root, by_id["h2"], symbols, cache, state_cache)
            self.assertIn("## Section Two", h2_body)
            self.assertIn("More body.", h2_body)


if __name__ == "__main__":
    unittest.main()
