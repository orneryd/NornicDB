"""Rules for choosing the repository's main entry point (scripts/graphify_local.py).

Mirrors tests/test_main_selection.py in Soraban/code-intelligence."""
import json
import tempfile
import unittest
from pathlib import Path

from scripts import graphify_local as gi


def func(node_id, label, source_file, line=1, **extra):
    return {"id": node_id, "label": label, "file_type": "code", "source_file": source_file,
            "source_location": f"L{line}", "_callable": True, **extra}


def edge(source, target, relation="calls"):
    return {"source": source, "target": target, "relation": relation}


def write_graph(nodes, edges, key="edges"):
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump({"nodes": nodes, "links" if key == "links" else "edges": edges}, handle)
    handle.close()
    return Path(handle.name)


NODES = [
    {"id": "app_py", "label": "app.py", "file_type": "code", "source_file": "app.py", "source_location": "L1"},
    func("app_main", "main()", "app.py", 3),
    func("app_helper", "helper()", "app.py", 20),
    func("app_util", ".util()", "lib/util.py", 5),
    func("test_x", "test_everything()", "tests/test_app.py", 1),
    func("dup_a", "run()", "a/runner.py", 1),
    func("dup_b", "run()", "b/runner.py", 1),
    {"id": "klass", "label": "BigClass", "file_type": "code", "source_file": "lib/klass.py", "source_location": "L1",
     "_callable": True, "_callable_class": True},
    # a library symbol the code only references: no source_file
    {"id": "requests_get", "label": "get()", "file_type": "code", "source_file": "", "source_location": ""},
]
EDGES = [
    edge("app_py", "app_main", "contains"), edge("app_py", "app_helper", "contains"),
    edge("app_main", "app_helper"), edge("app_main", "app_util"), edge("app_helper", "app_util"),
    edge("test_x", "app_main"), edge("test_x", "app_helper"), edge("test_x", "app_util"),
    edge("test_x", "dup_a"), edge("test_x", "dup_b"),
    # the external symbol and the class are the most connected nodes in the graph
    *[edge(f"app_{n}", "requests_get") for n in ("main", "helper", "util")], edge("dup_a", "requests_get"),
    edge("klass", "app_main", "method"), edge("klass", "app_helper", "method"), edge("klass", "app_util", "method"),
    edge("klass", "dup_a", "method"), edge("klass", "dup_b", "method"),
    edge("app_util", "dup_b"),  # makes app_util strictly the best-connected function
]


class MainSelection(unittest.TestCase):
    def setUp(self):
        self.graph = write_graph(NODES, EDGES)

    def test_kinds(self):
        kinds = {n["id"]: gi.symbol_kind(n) for n in NODES}
        self.assertEqual(kinds["app_main"], "function")
        self.assertEqual(kinds["klass"], "class")
        self.assertEqual(kinds["requests_get"], "external")
        self.assertEqual(kinds["app_py"], "file")

    def test_go_style_functions_have_no_callable_flag_but_end_in_parens(self):
        go_func = {"id": "g_main", "label": "main()", "file_type": "code", "source_file": "cmd/x/main.go", "source_location": "L9"}
        go_method = dict(go_func, id="g_m", label=".Serve()")
        go_type = dict(go_func, id="g_t", label="Server")   # a struct type, not callable
        self.assertEqual(gi.symbol_kind(go_func), "function")
        self.assertEqual(gi.symbol_kind(go_method), "function")
        self.assertEqual(gi.symbol_kind(go_type), "other")
        # an external symbol still never counts, even with parens
        self.assertEqual(gi.symbol_kind(dict(go_func, source_file="")), "external")
        graph = write_graph([go_func, go_method, go_type], [edge("g_main", "g_m"), edge("g_m", "g_t")])
        self.assertEqual(gi.choose_main(graph, "cmd/x/main.go:main")["id"], "g_main")
        self.assertEqual(gi.choose_main(graph)["id"], "g_m")

    def test_default_is_the_most_connected_function_in_the_code(self):
        main = gi.choose_main(self.graph)
        # requests_get (external) and BigClass (class) are more connected, and test_everything
        # connects to as many nodes, but none of them may be chosen.
        self.assertEqual(main["id"], "app_util")  # 6 connected: app_main, app_helper, test_x, klass, dup_b, requests_get
        self.assertEqual(main["how"], "default")
        self.assertEqual(main["kind"], "function")
        self.assertFalse(gi.is_test_source(main["source_file"]))
        self.assertTrue(main["runner_ups"])

    def test_ties_break_by_file_then_line(self):
        graph = write_graph([func("b", "b()", "z.py", 1), func("a", "a()", "a.py", 9), func("c", "c()", "a.py", 2)],
                            [edge("a", "b"), edge("b", "c"), edge("c", "a")])
        self.assertEqual(gi.choose_main(graph)["id"], "c")

    def test_default_ignores_contains_edges(self):
        graph = write_graph(NODES[:3] + NODES[5:6], [edge("app_py", "app_main", "contains"),
                                                     edge("app_py", "app_helper", "contains"),
                                                     edge("app_py", "dup_a", "contains"),
                                                     edge("app_helper", "dup_a")])
        self.assertIn(gi.choose_main(graph)["id"], ("app_helper", "dup_a"))

    def test_links_key_is_supported(self):
        self.assertEqual(gi.choose_main(write_graph(NODES, EDGES, key="links"))["id"], "app_util")

    def test_explicit_forms(self):
        for spec in ("app_main", "app.py:main", "app.py:main()", "app.py:.main()"):
            self.assertEqual(gi.choose_main(self.graph, spec)["id"], "app_main", spec)
        self.assertEqual(gi.choose_main(self.graph, "main")["id"], "app_main")
        self.assertEqual(gi.choose_main(self.graph, ".util()")["id"], "app_util")
        self.assertEqual(gi.choose_main(self.graph, "lib/util.py:util")["id"], "app_util")
        self.assertEqual(gi.choose_main(self.graph, "a/runner.py:run")["id"], "dup_a")
        self.assertEqual(gi.choose_main(self.graph, "main")["how"], "explicit")

    def test_ambiguous_symbol_lists_candidates(self):
        with self.assertRaises(gi.MainSelectionError) as ctx:
            gi.choose_main(self.graph, "run")
        message = str(ctx.exception)
        self.assertIn("ambiguous", message)
        self.assertIn("a/runner.py", message)
        self.assertIn("b/runner.py", message)

    def test_external_symbols_cannot_be_main(self):
        for spec in ("requests_get", "get", "get()"):
            with self.assertRaises(gi.MainSelectionError, msg=spec):
                gi.choose_main(self.graph, spec)

    def test_files_cannot_be_main_and_unknown_names_suggest(self):
        with self.assertRaises(gi.MainSelectionError):
            gi.choose_main(self.graph, "app_py")
        with self.assertRaises(gi.MainSelectionError) as ctx:
            gi.choose_main(self.graph, "mian")
        self.assertIn("main", str(ctx.exception))
        with self.assertRaises(gi.MainSelectionError) as ctx:
            gi.choose_main(self.graph, "app.py:nope")
        self.assertIn("main()", str(ctx.exception))

    def test_classes_are_allowed_when_explicit(self):
        self.assertEqual(gi.choose_main(self.graph, "BigClass")["kind"], "class")

    def test_no_functions_means_no_default(self):
        graph = write_graph([n for n in NODES if n["id"] in ("app_py", "klass", "requests_get")], [])
        with self.assertRaises(gi.MainSelectionError) as ctx:
            gi.choose_main(graph)
        self.assertIn("none", str(ctx.exception))

    def test_tests_are_only_a_last_resort_for_the_default(self):
        only_tests = write_graph([func("t1", "test_a()", "tests/test_a.py"), func("t2", "test_b()", "tests/test_b.py")],
                                 [edge("t1", "t2")])
        self.assertIn(gi.choose_main(only_tests)["id"], ("t1", "t2"))

    def test_symbol_with_double_colon(self):
        nodes = [func("cls_m", "Foo::Bar", "lib/foo.rb", 4), func("other", ".x()", "lib/foo.rb", 9)]
        graph = write_graph(nodes, [edge("cls_m", "other")])
        self.assertEqual(gi.choose_main(graph, "lib/foo.rb:Foo::Bar")["id"], "cls_m")
        self.assertEqual(gi.choose_main(graph, "Foo::Bar")["id"], "cls_m")


if __name__ == "__main__":
    unittest.main()
