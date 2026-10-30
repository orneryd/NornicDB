from graphify_divergence import analyze, compare_candidate_groups
"""Deterministic, isolated tests for the Graphify candidate report."""

import json
import tempfile
import unittest
from pathlib import Path

from graphify_divergence import analyze


class GraphifyDivergenceTest(unittest.TestCase):
    def test_candidates_only_share_cross_file_go_symbols(self):
        nodes = [
            {"id": "left", "label": ".Read()", "source_file": "pkg/cypher/left.go", "source_location": "L3"},
            {"id": "right", "label": ".Read()", "source_file": "pkg/cypher/right.go", "source_location": "L2"},
            {"id": "same", "label": ".Read()", "source_file": "pkg/cypher/left.go", "source_location": "L9"},
            {"id": "test", "label": ".Read()", "source_file": "pkg/cypher/read_test.go"},
            {"id": "other", "label": ".Read()", "source_file": "pkg/storage/other.go"},
            {"id": "different", "label": ".Write()", "source_file": "pkg/cypher/right.go"},
        ]
        edges = [
            {"source": "left", "target": "common", "relation": "calls"},
            {"source": "right", "target": "common", "relation": "calls"},
            {"source": "same", "target": "common", "relation": "calls"},
            {"source": "test", "target": "common", "relation": "calls"},
            {"source": "left", "target": "unrelated", "relation": "imports_from"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            graph = Path(directory) / "graph.json"
            graph.write_text(json.dumps({"nodes": nodes, "edges": edges}))
            first = analyze(graph, {"pkg/storage", "pkg/cypher"})
            graph.write_text(json.dumps({"nodes": list(reversed(nodes)), "edges": list(reversed(edges))}))
            self.assertEqual(first, analyze(graph, {"pkg/cypher", "pkg/storage"}))

        self.assertEqual(first["components"], [
            {"name": "pkg/cypher", "nodes": 4, "call_edges": 3},
            {"name": "pkg/storage", "nodes": 1, "call_edges": 0},
        ])
        self.assertEqual(len(first["candidates"]), 1)
        candidate = first["candidates"][0]
        self.assertEqual(candidate["label"], ".Read()")
        self.assertEqual([(pair["left"]["id"], pair["right"]["id"], pair["shared_callee_ids"])
                          for pair in candidate["pairs"]], [
            ("left", "right", ["common"]),
            ("same", "right", ["common"]),
        ])

    def test_compare_candidate_groups_is_sorted_and_revision_scoped(self):
        old = {"candidates": [
            {"component": "pkg/storage", "label": ".Old()"},
            {"component": "pkg/cypher", "label": ".Read()"},
        ]}
        current = {"candidates": [
            {"component": "pkg/storage", "label": ".New()"},
            {"component": "pkg/cypher", "label": ".Read()"},
        ]}
        expected = {
            "old_count": 2,
            "current_count": 2,
            "retained": 1,
            "old_only": ["pkg/storage|.Old()"],
            "current_only": ["pkg/storage|.New()"],
        }
        self.assertEqual(compare_candidate_groups(old, current), expected)
        self.assertEqual(compare_candidate_groups({"candidates": list(reversed(old["candidates"]))}, current), expected)


if __name__ == "__main__":
    unittest.main()