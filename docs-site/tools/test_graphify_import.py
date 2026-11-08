import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('graphify_import', Path(__file__).with_name('import-graphify.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ImportTests(unittest.TestCase):
    def test_repeated_init_ids_use_call_site_locations(self):
        declarations = [{'id': 'target', 'name': 'Target', 'file': 'pkg/a.go', 'line': 2, 'end': 2}, {'id': 'init@3', 'name': 'init', 'file': 'pkg/a.go', 'line': 3, 'end': 3}, {'id': 'init@4', 'name': 'init', 'file': 'pkg/a.go', 'line': 4, 'end': 4}]
        nodes = [{'id': 'target', 'label': 'Target()', 'source_file': 'pkg/a.go', 'source_location': 'L2'}, *[{'id': 'shared-init', 'label': 'init()', 'source_file': 'pkg/a.go', 'source_location': f'L{line}'} for line in (3, 4)]]
        edges = [{'source': 'shared-init', 'target': 'target', 'relation': 'calls', '_origin': 'ast', 'source_file': 'pkg/a.go', 'source_location': f'L{line}'} for line in (3, 4)]
        with tempfile.TemporaryDirectory() as temp:
            graph = Path(temp) / 'graph.json'
            graph.write_text(json.dumps({'nodes': nodes, 'edges': edges}))
            self.assertEqual(module.reduce_graph(graph, declarations), {('init@3', 'target'), ('init@4', 'target')})
