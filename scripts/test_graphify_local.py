import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.graphify_local import (
    body_for_node,
    body_span,
    collect_symbol_lines,
    comment_mask,
    comment_style,
    content_hash,
    emit_enriched_graph,
    import_graph,
    main,
    node_label,
    parse_location,
    relationship_type,
    scalar_properties,
)


class RecordingSession:
    def __init__(self):
        self.queries = []
        self.records = []
        self.record_sets = []
        self.database = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, query, **params):
        self.queries.append((query, json.loads(json.dumps(params))))
        return self

    def consume(self):
        return None

    def single(self):
        return None

    def __iter__(self):
        if self.record_sets:
            return iter(self.record_sets.pop(0))
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


class GraphifyLocalTest(unittest.TestCase):
    def test_graphify_labels_and_scalar_properties(self):
        self.assertEqual(node_label({"file_type": "source-file"}), "Sourcefile")
        self.assertEqual(relationship_type({"relation": "calls-to"}), "CALLS_TO")
        self.assertEqual(scalar_properties({"name": "x", "weight": 1.25, "_origin": "ast", "metadata": {}}),
                         {"name": "x", "weight": 1.25})
        self.assertEqual(scalar_properties({"source": "a", "target": "b", "relation": "calls"}, edge=True),
                 {"relation": "calls"})

    def test_import_includes_implicit_endpoints_and_batches(self):
        graph = {
            "nodes": [{"id": "source", "file_type": "code", "label": "Source", "_origin": "ast"}],
            "edges": [{"source": "source", "target": "implicit", "relation": "calls", "confidence": "EXTRACTED"}],
        }
        driver = RecordingDriver()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(path, "bolt://127.0.0.1:7687", "admin", "test", 1)

        queries = driver.connection.queries
        code_merge = [params for query, params in queries
                      if query.startswith("UNWIND $rows AS row MERGE (n:Code {id: row.id}) ON CREATE")]
        self.assertEqual(len(code_merge), 1)
        self.assertEqual([row["id"] for row in code_merge[0]["rows"]], ["source"])
        self.assertEqual(code_merge[0]["rows"][0]["props"]["label"], "Source")
        self.assertEqual(code_merge[0]["rows"][0]["props"]["file_type"], "code")
        self.assertIn("updated_at", code_merge[0]["rows"][0]["props"])
        self.assertEqual(code_merge[0]["rows"][0]["props"]["label"], "Source")
        self.assertEqual(code_merge[0]["rows"][0]["props"]["file_type"], "code")
        self.assertIn("updated_at", code_merge[0]["rows"][0]["props"])
        self.assertEqual(
            code_merge[0]["rows"][0]["props"]["props_hash"],
            content_hash({"id": "source", "label": "Source", "file_type": "code"}),
        )
        code_update = [params for query, params in queries
                       if "MATCH (n:Code {id: row.id})" in query and "props_hash" in query]
        self.assertEqual(len(code_update), 1)
        self.assertEqual([row["id"] for row in code_update[0]["rows"]], ["source"])
        self.assertIn("n.props_hash <> row.props_hash",
                      [q for q, _ in queries if "props_hash" in q][0])
        entity_merge = [params for query, params in queries
                        if query.startswith("UNWIND $rows AS row MERGE (n:Entity {id: row.id}) ON CREATE")]
        self.assertEqual([row["id"] for row in entity_merge[0]["rows"]], ["implicit"])
        edge_props = {"relation": "calls", "confidence": "EXTRACTED",
                      "props_hash": content_hash({"relation": "calls", "confidence": "EXTRACTED"})}
        edge_merge = [(query, params) for query, params in queries
                      if "MERGE (a)-[r:CALLS]" in query]
        self.assertIn(("UNWIND $rows AS row MATCH (a:Code {id: row.src}), (b:Entity {id: row.tgt}) "
                       "MERGE (a)-[r:CALLS]->(b) SET r += row.props",
                       {"rows": [{"src": "source", "tgt": "implicit", "props": edge_props}]}), edge_merge)
        self.assertFalse(any("MATCH (a:Code {id: row.src})-[r:CALLS]->" in q
                             for q, _ in queries),
                         "edge writes must stay on the single MERGE fast path")

    def test_import_sync_deletes_stale_nodes_and_edges(self):
        graph = {
            "nodes": [
                {"id": "a", "file_type": "code"},
                {"id": "b", "file_type": "code"},
            ],
            "links": [{"source": "a", "target": "b", "relation": "calls"}],
        }
        driver = RecordingDriver()
        driver.connection.record_sets = [
            [],  # SHOW DATABASES during ensure_database
            [{"id": "a"}, {"id": "b"}, {"id": "stale-node"}],
            [
                {"src": "a", "tgt": "b", "rel": "CALLS"},
                {"src": "a", "tgt": "stale-node", "rel": "CALLS"},
            ],
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(path, "bolt://127.0.0.1:7687", "admin", "test", 10)

        queries = driver.connection.queries
        delete_nodes = [params for query, params in queries if "DETACH DELETE n" in query]
        self.assertEqual(len(delete_nodes), 1)
        self.assertEqual(delete_nodes[0]["ids"], ["stale-node"])
        delete_edges = [params for query, params in queries
                        if "MATCH (a {id: row.src})-[r]->(b {id: row.tgt})" in query]
        self.assertEqual(len(delete_edges), 1)
        self.assertEqual(delete_edges[0]["rows"],
                         [{"src": "a", "tgt": "stale-node", "rel": "CALLS"}])
        edge_fetch = [q for q, _ in queries if "RETURN a.id AS src, b.id AS tgt, type(r) AS rel" in q]
        self.assertEqual(len(edge_fetch), 1)
        self.assertIn("AND (a:Code) AND (b:Code)", edge_fetch[0])
        self.assertNotIn("n.updated_at < row.updated_at", " ".join(q for q, _ in queries))

    def test_import_gentle_updates_never_rewrite_unchanged_nodes(self):
        graph = {
            "nodes": [{"id": "a", "file_type": "code"}],
            "links": [{"source": "a", "target": "a", "relation": "calls"}],
        }
        driver = RecordingDriver()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(path, "bolt://127.0.0.1:7687", "admin", "test", 10)
        update_queries = [q for q, _ in driver.connection.queries if "props_hash <> row.props_hash" in q]
        self.assertEqual(len(update_queries), 1)  # nodes only; edges use the single MERGE fast path
        self.assertIn("n.props_hash IS NULL OR n.props_hash", update_queries[0])
        for q, _ in driver.connection.queries:
            self.assertNotIn("n.updated_at < row.updated_at", q)
            self.assertNotIn("all(k IN keys(row.props)", q)

    def test_content_hash_is_stable_and_ignores_metadata(self):
        self.assertEqual(content_hash({"a": "1", "b": "2"}), "be9380e87280e5d5")
        self.assertEqual(content_hash({"b": "2", "a": "1", "updated_at": 9.9, "props_hash": "zz"}),
                         content_hash({"a": "1", "b": "2"}))
        self.assertEqual(content_hash({"id": "n", "count": 3, "flag": True, "none": None}),
                         content_hash({"id": "n"}))

    def test_import_flushes_full_and_partial_batches(self):
        graph = {
            "nodes": [{"id": node_id, "file_type": "code"} for node_id in ("a", "b", "c")],
            "edges": [
                {"source": "a", "target": "new1", "relation": "calls"},
                {"source": "b", "target": "new2", "relation": "calls"},
                {"source": "c", "target": "new1", "relation": "imports"},
            ],
        }
        driver = RecordingDriver()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(path, "bolt://127.0.0.1:7687", "admin", "test", 2)

        queries = driver.connection.queries
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (n:Code" in query], [2, 1])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (n:Entity" in query], [2])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (a)-[r:CALLS]" in query], [2])
        self.assertEqual([len(params["rows"]) for query, params in queries if "MERGE (a)-[r:IMPORTS]" in query], [1])
        self.assertEqual(sum("CREATE INDEX graphify_" in query for query, _ in queries), 2)
        # Indexes must be created before the first MERGE for their label so
        # id lookups never fall back to full-label scans.
        index_positions = {re.search(r"FOR \(n:(\w+)\)", query).group(1): position
                          for position, (query, _) in enumerate(queries)
                          if "CREATE INDEX graphify_" in query}
        merge_positions = {}
        for position, (query, _) in enumerate(queries):
            if "MERGE (n:" in query:
                label = query.split("MERGE (n:")[1].split(" ")[0]
                merge_positions.setdefault(label, position)
        for label, merge_position in merge_positions.items():
            self.assertLess(index_positions[label], merge_position,
                            f"index for {label} must precede its first MERGE")

    def test_import_reports_large_progress(self):
        driver = RecordingDriver()

        def items(_path, key):
            if key == "nodes":
                return iter([{"id": "source", "file_type": "code"}])
            return ({"source": "source", "target": "source", "relation": "calls"} for _ in range(10000))

        with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver), \
             patch("scripts.graphify_local.graph_items", side_effect=items), \
             patch("builtins.print") as output:
            import_graph(Path("unused"), "bolt://127.0.0.1:7687", "admin", "test", 250)

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
            graph_path = root / "graph.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            driver = RecordingDriver()
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(graph_path, "bolt://localhost:7687", "admin", "password", 10, str(root))

        queries = driver.connection.queries
        self.assertIn(("CREATE DATABASE `nornicdbcode`", {}), queries)
        self.assertIn("SHOW DATABASES", [q for q, _ in queries][0])
        self.assertEqual(driver.connection.database, "nornicdbcode")
        node_rows = {row["id"]: row["props"]
                     for query, params in queries if "MERGE (n:" in query
                     for row in params["rows"]}
        self.assertIn("// Doc for A.", node_rows["a"]["body"])
        self.assertTrue(node_rows["a"]["body"].rstrip().endswith("}"))
        self.assertEqual(node_rows["b"]["body"], "func B() {}")
        self.assertEqual(sum(len(params["rows"]) for query, params in queries
                             if "MERGE (a)-[r:CALLS]" in query), 1)

    def test_cli_rejects_invalid_inputs_and_forwards_options(self):
        with tempfile.TemporaryDirectory() as directory:
            graph = Path(directory) / "graph.json"
            graph.write_text('{"nodes":[],"edges":[]}', encoding="utf-8")
            with patch("sys.argv", ["graphify_local.py", "--batch-size", "0"]), \
                 self.assertRaises(SystemExit):
                main()
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph) + ".missing"]), \
                 self.assertRaises(SystemExit):
                main()
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph), "--batch-size", "2"]), \
                 patch.dict("os.environ", {"NEO4J_PASSWORD": "test"}), \
                 patch("scripts.graphify_local.import_graph") as importer:
                main()
                importer.assert_called_once_with(graph, "bolt://localhost:7687", "admin", "test", 2, ".", "nornicdbcode", sync=True)
            with patch("sys.argv", ["graphify_local.py", "--graph", str(graph)]), \
                 patch.dict("os.environ", {}, clear=True), \
                 patch("scripts.graphify_local.import_graph") as importer:
                main()
                importer.assert_called_once_with(graph, "bolt://localhost:7687", "admin", "password", 2000, ".", "nornicdbcode", sync=True)

    def test_import_empty_graph_never_deletes(self):
        graph = {"nodes": [], "edges": []}
        driver = RecordingDriver()
        driver.connection.records = [{"name": "neo4j"}, {"name": "nornicdbcode"}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "graph.json"
            path.write_text(json.dumps(graph), encoding="utf-8")
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(path, "bolt://127.0.0.1:7687", "admin", "test", 10, ".", "nornicdbcode")
        for query, _ in driver.connection.queries:
            self.assertNotIn("DETACH DELETE", query)
            self.assertNotIn("DELETE r", query)

    def test_import_skips_create_when_database_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            graph_path = root / "graph.json"
            graph_path.write_text('{"nodes":[],"links":[]}', encoding="utf-8")
            driver = RecordingDriver()
            driver.connection.records = [{"name": "neo4j"}, {"name": "graphify"}]
            with patch("scripts.graphify_local.GraphDatabase.driver", return_value=driver):
                import_graph(graph_path, "bolt://localhost:7687", "admin", "password", 10, str(root), "graphify")
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
            self.assertIn("// Doc for A.", by_id["a"]["body"])
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
            # Full, untruncated body including the doc comments above and the
            # comment inside — but stopping before Other()'s own doc comment.
            self.assertIn("// Compute adds one.", compute_body)
            self.assertIn("// More detail.", compute_body)
            self.assertIn("// inside comment", compute_body)
            self.assertIn("return x + 1", compute_body)
            self.assertNotIn("// Other is second.", compute_body)
            self.assertTrue(compute_body.rstrip().endswith("}"))

            other_body = body_for_node(root, by_id["other"], symbols, cache, state_cache)
            self.assertEqual(other_body, "// Other is second.\nfunc Other() {}")

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