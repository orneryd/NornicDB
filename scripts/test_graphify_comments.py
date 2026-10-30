"""Comments kept apart from bodies, docstring boundaries, and community grouping (stdlib unittest)."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import graphify_local as gi  # noqa: E402

SOURCE = '''import os

# Reads the thing.
# Second line.
def read(path):
    """Read a file.

    Longer text.
    """
    # strip the newline
    data = open(path).read()  # trailing stays in the body
    return data.rstrip()


def other():
    return 1
'''


def graph_for(source, tmp):
    (Path(tmp) / "mod.py").write_text(source)
    nodes = [
        {"id": "read", "label": "read()", "file_type": "code", "source_file": "mod.py", "source_location": "L5", "_callable": True},
        {"id": "doc", "label": "Read a file.", "file_type": "rationale", "source_file": "mod.py", "source_location": "L6"},
        {"id": "other", "label": "other()", "file_type": "code", "source_file": "mod.py", "source_location": "L15", "_callable": True},
    ]
    links = [{"source": "doc", "target": "read", "relation": "rationale_for"}]
    path = Path(tmp) / "graph.json"
    path.write_text(json.dumps({"nodes": nodes, "links": links}))
    return path


class CommentsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.graph = graph_for(SOURCE, self.tmp)
        self.symbols = gi.collect_symbol_lines(self.graph)
        self.docs = gi.collect_docstring_lines(self.graph, "links")
        self.node = {n["id"]: n for n in gi.graph_items(self.graph, "nodes")}

    def body(self, node_id):
        return gi.body_for_node(self.tmp, self.node[node_id], self.symbols, {}, {}, self.docs)

    def comments(self, node_id):
        return gi.comments_for_node(self.tmp, self.node[node_id], self.symbols, self.docs, {}, {})

    def test_docstring_is_not_a_symbol_boundary(self):
        body = self.body("read")
        self.assertIn("return data.rstrip()", body)  # not cut off at the docstring

    def test_body_has_no_comment_only_lines_or_docstring(self):
        body = self.body("read")
        self.assertTrue(body.startswith("def read(path):"))
        self.assertNotIn("Reads the thing", body)
        self.assertNotIn("Longer text", body)
        self.assertNotIn("# strip the newline", body)
        self.assertIn("# trailing stays in the body", body)  # only comment-only lines move

    def test_comments_hold_leading_block_docstring_and_inner_comments_in_order(self):
        comments = self.comments("read")
        self.assertEqual(comments.splitlines()[:2], ["# Reads the thing.", "# Second line."])
        self.assertLess(comments.index("Read a file."), comments.index("# strip the newline"))
        self.assertIn("Longer text.", comments)

    def test_symbol_without_comments_has_none(self):
        self.assertIsNone(self.comments("other"))
        self.assertEqual(self.body("other"), "def other():\n    return 1")

    def test_rationale_node_body_is_its_docstring(self):
        self.assertEqual(self.body("doc").splitlines()[0].strip(), '"""Read a file.')
        self.assertIsNone(self.comments("doc"))

    def test_docstring_span(self):
        lines = ['    """one liner."""', "x", 'r"""a', "b", '"""']
        self.assertEqual(gi.docstring_span(lines, 1), (1, 1, "one liner."))
        self.assertEqual(gi.docstring_span(lines, 3), (3, 5, "a\nb"))
        self.assertIsNone(gi.docstring_span(lines, 2))


class CommunityTests(unittest.TestCase):
    def test_votes_one_name_per_community_and_reports_conflicts(self):
        communities = gi.Communities()
        communities.add("a", "Code", 1, "Alpha")
        communities.add("b", "Code", 1, "Alpha")
        communities.add("c", "Code", 1, "Stale")
        communities.add("d", "Code", 2, None)
        communities.add("e", "Code", None, "ignored")
        self.assertEqual(communities.name(1), "Alpha")
        self.assertEqual(communities.name(2), "")
        self.assertEqual(communities.ids(), [1, 2])
        self.assertEqual(communities.inconsistent(), 1)
        self.assertEqual(communities.unnamed(), 1)
        self.assertEqual(len(communities.members), 4)


if __name__ == "__main__":
    unittest.main()
