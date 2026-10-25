"""Run the actual importer with captured database readback; Go executes deletion."""
import contextlib
import io
import json
import pathlib
import sys
import types

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from peer_recording import install
install()
# The fixture stream is deterministic and small. Replace only the optional
# incremental JSON parser dependency; import_graph and its query writers run
# unchanged. Streaming-parser behavior is outside this name-emission subject.
parser = types.ModuleType("ijson")
parser.items = lambda stream, prefix, **kwargs: iter(json.load(stream).get(prefix.removesuffix(".item"), []))
sys.modules["ijson"] = parser
from scripts.test_graphify_local import RecordingDriver, ingest

driver = RecordingDriver()
driver.connection.responses = [
    ("RETURN n.id AS id", [{"id": "a"}, {"id": "b"}]),
    ("RETURN a.id AS src", [{"src": "a", "tgt": "b", "rel": rel} for rel in ("A", "B", "A|B")]),
]
graph = {"nodes": [{"id": n, "file_type": "code"} for n in ("a", "b")],
         "links": [{"source": "a", "target": "b", "relation": r} for r in ("a", "b")]}
with contextlib.redirect_stdout(io.StringIO()):
    ingest(graph, driver, main=None)
print(json.dumps([{"Text": q, "Parameters": p} for q, p in driver.connection.queries if " DELETE r" in q]))
