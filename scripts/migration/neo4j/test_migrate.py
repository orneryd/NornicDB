"""Client result-handling tests; no live source, target or driver required."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock

# The driver is not exercised by these unit tests. Import the real client with
# just its annotated driver names available; every session below is explicit.
sys.modules.setdefault("neo4j", types.SimpleNamespace(GraphDatabase=None, Driver=object, Session=object))
spec = importlib.util.spec_from_file_location("migration_client", Path(__file__).with_name("migrate.py"))
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


class MigrationClientTests(unittest.TestCase):
    def test_shared_queries_and_complete_result(self):
        queries = json.loads(Path(__file__).with_name("queries.json").read_text())
        self.assertEqual(queries["nodes"], client.NODE_UPSERT_CYPHER)
        self.assertEqual(queries["edges"].replace("{{type}}", "`KNOWS`"), client.edge_upsert_cypher("KNOWS"))
        result = Mock()
        result.single.return_value = {"applied": 2}
        session = Mock()
        session.run.return_value = result
        rows = [{"id": "1"}, {"id": "2"}]
        self.assertEqual(2, client.apply_batch(session, client.NODE_UPSERT_CYPHER, rows))
        session.run.assert_called_once_with(queries["nodes"], rows=rows)
        result.single.assert_called_once_with(strict=True)
        result.consume.assert_called_once_with()

    def test_incomplete_or_missing_counts_and_driver_errors(self):
        for record in ({"applied": 0}, {"applied": "1"}, {}):
            with self.subTest(record=record):
                session = Mock()
                session.run.return_value.single.return_value = record
                with self.assertRaises((RuntimeError, KeyError)):
                    client.apply_batch(session, client.NODE_UPSERT_CYPHER, [{"id": "1"}])
                session.run.return_value.consume.assert_called_once_with()
        for stage in ("run", "single", "consume"):
            with self.subTest(stage=stage):
                session = Mock()
                session.run.return_value.single.return_value = {"applied": 1}
                failing = session.run if stage == "run" else getattr(session.run.return_value, stage)
                failing.side_effect = RuntimeError("driver failed")
                with self.assertRaisesRegex(RuntimeError, "driver failed"):
                    client.apply_batch(session, client.NODE_UPSERT_CYPHER, [{"id": "1"}])

    def test_edge_loop_reports_only_target_confirmed_rows(self):
        rows = [{"id": "r1", "startId": "n1", "endId": "n2", "props": {}}]
        source = Mock()
        source.run.return_value = rows
        target = Mock()
        target.run.return_value.single.return_value = {"applied": 1}
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(1, client.replicate_edges(source, target, ["KNOWS"], set(), 500, False))
        self.assertIn("target relationship rows applied: 1", output.getvalue())
        target.run.assert_called_once_with(client.edge_upsert_cypher("KNOWS"), rows=rows)
        target.run.return_value.single.return_value = {"applied": 0}
        with contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, "target applied 0 of 1"):
                client.replicate_edges(source, target, ["KNOWS"], set(), 500, False)
        self.assertNotIn("target relationship rows applied", output.getvalue())
        target.reset_mock()
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(1, client.replicate_edges(source, target, ["KNOWS"], set(), 500, True))
        self.assertIn("source relationship rows read (dry run): 1", output.getvalue())
        target.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
