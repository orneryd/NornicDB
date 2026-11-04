"""Batched relationship inventory for the Graphify importer.

A release ingest failed after its node phase: the importer inventoried the repo's existing
relationships with one statement whose response carried all ~3.6M relationships, and the request
timed out through all twelve retries. The inventory now anchors one bounded batch of source nodes
per request on their physical ``elementId``.

These tests first reproduce that failure through a session that refuses an unbounded relationship
inventory, then prove the batched inventory covers exactly the same triples and hashes and that the
hash skip, repository isolation and stale-delete guards behave as before. The fake session answers
the importer's public statements over a fixed multi-repository graph; it raises the run's retryable
timeout for any relationship inventory that is not anchored on a bounded batch of source element
ids.

What batching does and does not bound: it bounds how many source nodes one request anchors. The
rows a request returns still grow with those sources' fan-out, so a graph with extreme fan-out needs
smaller batches (``--batch-size``), not a different inventory shape.
"""

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.graphify_local as graphify
from scripts.graphify_local import content_hash, import_graph

REPO = "orneryd/NornicDB"
OTHER_REPO = "someone/else"

# The statement the importer used to send: every relationship of the repo in one response.
OLD_EDGE_INVENTORY = (
    "MATCH (a)-[r]->(b) WHERE a.repo = $repo AND b.repo = $repo "
    "RETURN a.id AS src, b.id AS tgt, type(r) AS rel, r.props_hash AS hash"
)

CALLS_HASH = content_hash({"relation": "calls"})


def node_hash(node_id):
    """The content hash the importer stores for a bare code node of this repo."""
    return content_hash({"id": node_id, "file_type": "code", "repo": REPO, "symbol_kind": "external"})


class FakeDatabase:
    """Nodes and relationships of a small multi-repository database."""

    def __init__(self):
        self.nodes = []   # {element, id, repo, labels, props_hash}
        self.edges = []   # {src, tgt, rel, props_hash}, addressed by node element id

    def add_node(self, element, node_id, repo=REPO, labels=("Code",), props_hash="node-hash"):
        self.nodes.append({"element": element, "id": node_id, "repo": repo,
                           "labels": list(labels), "props_hash": props_hash})
        return self

    def add_edge(self, src, tgt, rel, props_hash="edge-hash"):
        self.edges.append({"src": src, "tgt": tgt, "rel": rel, "props_hash": props_hash})
        return self

    @property
    def by_element(self):
        return {node["element"]: node for node in self.nodes}


def release_database():
    """A repo whose graph mixes current, stale, unhashed and other-repository state."""
    database = FakeDatabase()
    # Nodes this importer wrote, in this repo.
    for element, node_id in (("e:a", "a"), ("e:b", "b"), ("e:c", "c")):
        database.add_node(element, node_id, props_hash=node_hash(node_id))
    # A node this importer has not hashed yet: another pipeline's, or an interrupted first run.
    database.add_node("e:legacy", "legacy", props_hash=None)
    # A node that has left the graph.
    database.add_node("e:gone", "gone", props_hash="gone-hash")
    # Another repository sharing the database.
    database.add_node("e:x", "x", repo=OTHER_REPO, props_hash="x-hash")
    database.add_edge("e:a", "e:b", "CALLS", CALLS_HASH)           # stored identically: skipped
    database.add_edge("e:b", "e:a", "CALLS", "outdated-hash")      # outdated: rewritten, not stale
    database.add_edge("e:c", "e:a", "CALLS", "old-calls")          # missing from the graph: stale
    database.add_edge("e:a", "e:gone", "CALLS", "old-calls")       # goes with its stale target node
    database.add_edge("e:legacy", "e:a", "CALLS", "legacy-calls")  # source has no props_hash
    database.add_edge("e:a", "e:legacy", "CALLS", "to-legacy")     # target has no props_hash
    database.add_edge("e:x", "e:a", "CALLS", "cross-src")          # another repository's source
    database.add_edge("e:a", "e:x", "CALLS", "cross-tgt")          # another repository's target
    return database


GRAPH = {
    "nodes": [{"id": name, "file_type": "code"} for name in ("a", "b", "c")],
    "links": [{"source": "a", "target": "b", "relation": "calls"},
              {"source": "b", "target": "a", "relation": "calls"}],
}


class FakeSession:
    """Answers the importer's statements over a FakeDatabase and records what it inventoried.

    A relationship inventory that is not a bounded batch of source element ids raises the error the
    release run produced (a retryable timeout), so a regression to one whole-repo response fails
    here instead of on the server.
    """

    def __init__(self, database):
        self.database = database
        self.queries = []
        self.inventoried = {"all": set(), "written": set()}  # (src, tgt, rel) triples returned

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, **params):
        # Snapshot the parameters: the importer reuses and clears its row batches after sending them.
        self.queries.append((query, json.loads(json.dumps(params))))
        if "MATCH (a)-[r]->(b)" in query:
            return graphify.HttpResult(self.inventory(query, params))
        if "elementId(n) AS element" in query:
            return graphify.HttpResult(self.node_inventory(query, params))
        return graphify.HttpResult([])

    def inventory(self, query, params):
        elements = params.get("elements")
        if "elementId(a) = element" not in query or not elements:
            raise graphify.RetryableError("HTTP timeout: one response would carry the whole repo")
        if len(elements) > graphify.EDGE_SOURCE_BATCH_SIZE:
            raise graphify.RetryableError(f"{len(elements)} sources in one inventory request")
        written_only = "a.props_hash IS NOT NULL AND b.props_hash IS NOT NULL" in query
        rows = []
        for element in elements:
            source = self.database.by_element.get(element)
            if source is None or source["repo"] != params["repo"]:
                continue                      # a.repo = $repo
            for edge in self.database.edges:
                if edge["src"] != element:
                    continue
                target = self.database.by_element[edge["tgt"]]
                if target["repo"] != params["repo"]:
                    continue                  # b.repo = $repo
                if written_only and (source["props_hash"] is None or target["props_hash"] is None):
                    continue                  # the stale-edge guard
                self.inventoried["written" if written_only else "all"].add(
                    (source["id"], target["id"], edge["rel"]))
                row = {"src": source["id"], "tgt": target["id"], "rel": edge["rel"]}
                if not written_only:
                    row["hash"] = edge["props_hash"]
                rows.append(row)
        return rows

    def node_inventory(self, query, params):
        label = re.search(r"MATCH \(n:(\w+) \{repo: \$repo\}", query)
        rows = []
        for node in self.database.nodes:
            if node["repo"] != params["repo"]:
                continue                      # n.repo = $repo
            if label:
                if label.group(1) not in node["labels"]:
                    continue
                if node["id"] is None or node["props_hash"] is None:
                    continue                  # WHERE n.id IS NOT NULL AND n.props_hash IS NOT NULL
            rows.append({"id": node["id"], "hash": node["props_hash"], "element": node["element"]})
        return rows


class FakeDriver:
    def __init__(self, database):
        self.database = database
        self.sessions = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def session(self, database=None):
        session = FakeSession(self.database)
        self.sessions.append(session)
        return session


def run(driver, graph, batch_size=10):
    """Run import_graph against ``driver`` and return the lines it printed."""
    output = []
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "graph.json"
        path.write_text(json.dumps(graph), encoding="utf-8")
        with patch("scripts.graphify_local.connect", return_value=driver), \
             patch("builtins.print", side_effect=lambda *args, **kw: output.append(" ".join(map(str, args)))):
            import_graph(path, "http://localhost:7474", "admin", "test", batch_size, ".", "nornicdbcode", REPO)
    return output


def ingest(database, graph, batch_size=10):
    """Run import_graph over ``database``; return (driver, printed lines)."""
    driver = FakeDriver(database)
    return driver, run(driver, graph, batch_size)


def inventory_queries(queries, written_only=None):
    """The relationship inventory statements the importer sent."""
    found = [(query, params) for query, params in queries if "MATCH (a)-[r]->(b)" in query]
    if written_only is None:
        return found
    guard = "a.props_hash IS NOT NULL AND b.props_hash IS NOT NULL"
    return [(query, params) for query, params in found if (guard in query) == written_only]


def sent_edges(queries):
    return [(row["src"], row["tgt"]) for query, params in queries
            if "MERGE (a)-[r:" in query for row in params["rows"]]


def deleted_edges(queries):
    return [row for query, params in queries if "DELETE r" in query for row in params["rows"]]


def deleted_nodes(queries):
    return [node_id for query, params in queries if "DETACH DELETE n" in query
            for node_id in params["ids"]]


def expected_inventory(database, written_only):
    """The triples a complete inventory of this repo must return, computed independently."""
    triples = set()
    for edge in database.edges:
        source = database.by_element[edge["src"]]
        target = database.by_element[edge["tgt"]]
        if source["repo"] != REPO or target["repo"] != REPO:
            continue
        if written_only and (source["props_hash"] is None or target["props_hash"] is None):
            continue
        triples.add((source["id"], target["id"], edge["rel"]))
    return triples


class WholeRepoInventoryFailureTest(unittest.TestCase):
    """The failure that stopped the release ingest, and the inventory shape that replaced it."""

    def test_the_previous_whole_repo_inventory_is_refused(self):
        """One statement asked for every relationship of the repo; its response could not be read
        before the request gave up, so all twelve retries were spent on it and the run failed after
        its node phase. Here the same statement is refused outright."""
        session = FakeSession(release_database())
        with self.assertRaises(graphify.RetryableError) as raised:
            session.run(OLD_EDGE_INVENTORY, repo=REPO)
        self.assertIn("whole repo", str(raised.exception))

    def test_the_importer_inventories_relationships_in_bounded_source_batches(self):
        database = release_database()
        driver, _ = ingest(database, GRAPH, batch_size=2)
        queries = driver.sessions[-1].queries

        inventories = inventory_queries(queries)
        self.assertTrue(inventories)
        for query, params in inventories:
            self.assertIn("MATCH (a) WHERE elementId(a) = element", query,
                          "each request must anchor on physical source ids")
            self.assertIn("a.repo = $repo AND b.repo = $repo", query,
                          "both endpoints stay scoped to this repo")
            self.assertNotIn("LIMIT", query, "batching must never truncate the inventory")
            self.assertLessEqual(len(params["elements"]), min(2, graphify.EDGE_SOURCE_BATCH_SIZE))

        # The hash inventory names every repo node exactly once, so no source and no edge is dropped.
        repo_elements = sorted(node["element"] for node in database.nodes if node["repo"] == REPO)
        self.assertEqual(sorted(element for _, params in inventory_queries(queries, written_only=False)
                                for element in params["elements"]), repo_elements)
        self.assertGreater(len(inventory_queries(queries, written_only=False)), 1, "inventory was batched")
        # The sync inventory names this importer's nodes, again once each.
        written_elements = sorted(node["element"] for node in database.nodes
                                  if node["repo"] == REPO and node["props_hash"] and "Code" in node["labels"])
        self.assertEqual(sorted(element for _, params in inventory_queries(queries, written_only=True)
                                for element in params["elements"]), written_elements)


class BatchedInventoryEquivalenceTest(unittest.TestCase):
    """The batched inventory sees the same relationships as a single whole-repo read would."""

    def test_every_batch_size_inventories_the_same_relationships(self):
        observations = {}
        for batch_size in (1, 2, 3, 4, 5, 200, 1000):
            database = release_database()
            driver, output = ingest(database, GRAPH, batch_size=batch_size)
            session = driver.sessions[-1]
            with self.subTest(batch_size=batch_size):
                self.assertEqual(session.inventoried["all"], expected_inventory(database, written_only=False))
                self.assertEqual(session.inventoried["written"], expected_inventory(database, written_only=True))
                skipped = [line for line in output if line.startswith("Skipped")]
                self.assertEqual(len(skipped), 1, output)
                observations[batch_size] = json.dumps(
                    [skipped[0], sorted(sent_edges(session.queries)),
                     deleted_edges(session.queries), deleted_nodes(session.queries)], sort_keys=True)
        # Same skips, same rewrites and same deletions at every batch size.
        self.assertEqual(len(set(observations.values())), 1, observations)
        skipped, rewritten, stale_edges, stale_nodes = json.loads(observations[1])
        self.assertIn("Skipped 3 nodes and 1 edges", skipped)  # a, b and c are current
        # b->a carries an outdated hash, so it is rewritten; a->b is current and is not sent.
        self.assertEqual(rewritten, [["b", "a"]])
        # c->a is stale; a->gone goes with its stale node; the edges of nodes without a props_hash
        # are not deletion candidates.
        self.assertEqual(stale_edges, [{"src": "c", "tgt": "a"}])
        self.assertEqual(stale_nodes, ["gone"])

    def test_the_batch_cap_splits_the_inventory_without_batch_size(self):
        """A large --batch-size cannot re-create one whole-repo response."""
        database = FakeDatabase()
        for index in range(graphify.EDGE_SOURCE_BATCH_SIZE + 50):
            database.add_node(f"e:n{index}", f"n{index}", props_hash=node_hash(f"n{index}"))
        driver, _ = ingest(database, {"nodes": [], "links": []}, batch_size=1000)
        queries = driver.sessions[-1].queries
        self.assertEqual(len(inventory_queries(queries, written_only=False)), 2)  # 250 sources / 200
        self.assertEqual(
            [len(params["elements"]) for _, params in inventory_queries(queries, written_only=False)],
            [graphify.EDGE_SOURCE_BATCH_SIZE, 50])


class EmptyRepoTest(unittest.TestCase):
    def test_an_empty_repo_inventories_no_relationships_and_deletes_nothing(self):
        driver, output = ingest(FakeDatabase(), GRAPH, batch_size=2)
        queries = driver.sessions[-1].queries
        self.assertEqual(inventory_queries(queries), [])
        self.assertEqual(deleted_edges(queries), [])
        self.assertEqual(deleted_nodes(queries), [])
        self.assertTrue(any("Skipped 0 nodes and 0 edges" in line for line in output), output)


class NullHashSourceTest(unittest.TestCase):
    def test_edges_from_a_source_without_a_props_hash_are_still_inventoried(self):
        """The old node inventory filtered ``props_hash IS NOT NULL``; its rows are now also the
        sources of the batched inventory, so that filter would silently drop those edges."""
        database = FakeDatabase()
        database.add_node("e:legacy", "legacy", props_hash=None)
        database.add_node("e:a", "a", props_hash=node_hash("a"))
        database.add_edge("e:legacy", "e:a", "CALLS", content_hash({"relation": "calls"}))
        graph = {"nodes": [{"id": "legacy", "file_type": "code"}, {"id": "a", "file_type": "code"}],
                 "links": [{"source": "legacy", "target": "a", "relation": "calls"}]}
        driver, output = ingest(database, graph, batch_size=1)
        session = driver.sessions[-1]
        self.assertEqual(session.inventoried["all"], {("legacy", "a", "CALLS")})
        self.assertEqual(session.inventoried["written"], set(), "the deletion guard still excludes it")
        self.assertEqual(sent_edges(session.queries), [], "the stored edge is recognised as current")
        self.assertTrue(any("Skipped 1 nodes and 1 edges" in line for line in output), output)


class DuplicateSourceTest(unittest.TestCase):
    def test_a_source_with_two_managed_labels_is_inventoried_once(self):
        database = FakeDatabase()
        # A node stored under two managed labels: the per-label inventory matches it twice.
        database.add_node("e:dup", "dup", labels=("Code", "Entity"), props_hash="dup-hash")
        database.add_node("e:a", "a", props_hash=node_hash("a"))
        database.add_edge("e:dup", "e:a", "CALLS", "old-calls")
        graph = {"nodes": [{"id": "dup"}, {"id": "a", "file_type": "code"}], "links": []}
        driver, _ = ingest(database, graph, batch_size=1)
        queries = driver.sessions[-1].queries
        self.assertEqual(sorted(element for _, params in inventory_queries(queries, written_only=True)
                                for element in params["elements"]), ["e:a", "e:dup"])
        self.assertEqual(driver.sessions[-1].inventoried["written"], {("dup", "a", "CALLS")})
        # The stale edge is deleted through the label the sync records for its source, as before.
        deletes = [query for query, _ in queries if "DELETE r" in query]
        self.assertEqual(len(deletes), 1, deletes)
        self.assertIn("MATCH (a:Entity {id: row.src, repo: $repo})-[r:CALLS]->"
                      "(b:Code {id: row.tgt, repo: $repo})", deletes[0])


class RepoIsolationTest(unittest.TestCase):
    def test_another_repository_is_never_inventoried_or_deleted(self):
        database = release_database()
        driver, _ = ingest(database, GRAPH, batch_size=2)
        session = driver.sessions[-1]
        for triples in session.inventoried.values():
            self.assertFalse([triple for triple in triples if "x" in triple[:2]], triples)
        self.assertFalse([row for row in deleted_edges(session.queries) if "x" in (row["src"], row["tgt"])])
        self.assertNotIn("x", deleted_nodes(session.queries))
        for query, _ in inventory_queries(session.queries):
            self.assertIn("a.repo = $repo AND b.repo = $repo", query)


class SyncGuardTest(unittest.TestCase):
    def test_the_stale_edge_guard_still_excludes_edges_without_a_props_hash(self):
        """An edge touching a node this importer did not write stays out of the delete candidates."""
        database = FakeDatabase()
        database.add_node("e:a", "a", props_hash=node_hash("a"))
        database.add_node("e:b", "b", props_hash=node_hash("b"))
        database.add_node("e:hand", "hand", props_hash=None)  # another pipeline's node
        database.add_edge("e:a", "e:b", "CALLS", "old-calls")
        database.add_edge("e:a", "e:hand", "CALLS", "old-calls")
        database.add_edge("e:hand", "e:a", "CALLS", "old-calls")
        graph = {"nodes": [{"id": "a", "file_type": "code"}, {"id": "b", "file_type": "code"}], "links": []}
        driver, _ = ingest(database, graph, batch_size=1)
        session = driver.sessions[-1]
        self.assertEqual(session.inventoried["all"],
                         {("a", "b", "CALLS"), ("a", "hand", "CALLS"), ("hand", "a", "CALLS")})
        self.assertEqual(session.inventoried["written"], {("a", "b", "CALLS")})
        self.assertEqual(deleted_edges(session.queries), [{"src": "a", "tgt": "b"}])
        self.assertEqual(deleted_nodes(session.queries), [])
        for query, _ in inventory_queries(session.queries, written_only=True):
            self.assertIn("a.props_hash IS NOT NULL AND b.props_hash IS NOT NULL", query)

    def test_the_mass_delete_guard_still_sees_the_whole_batched_inventory(self):
        """A truncated inventory would under-count and let a truncated extraction delete the graph."""
        database = FakeDatabase()
        for index in range(300):
            database.add_node(f"e:n{index}", f"n{index}", props_hash=node_hash(f"n{index}"))
        for index in range(299):
            database.add_edge(f"e:n{index}", f"e:n{index + 1}", "CALLS", "old-calls")
        graph = {"nodes": [{"id": f"n{index}", "file_type": "code"} for index in range(300)],
                 "links": [{"source": "n0", "target": "n1", "relation": "calls"}]}
        driver = FakeDriver(database)
        with self.assertRaises(SystemExit) as raised:
            run(driver, graph, 1000)
        self.assertIn("Refusing to sync", str(raised.exception))
        self.assertIn("existing edges", str(raised.exception))
        queries = driver.sessions[-1].queries
        self.assertFalse([query for query, _ in queries if "DELETE" in query],
                         "nothing may be deleted before the refusal")
        # The whole inventory was read, split at the source-batch cap.
        self.assertEqual(len(inventory_queries(queries, written_only=True)), 2)


if __name__ == "__main__":
    unittest.main()
