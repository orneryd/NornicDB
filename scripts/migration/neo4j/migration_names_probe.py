"""Lab test adapter: retain actual writers and replace only the Bolt driver."""
import json
import pathlib
import runpy
import sys
import types

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / "internal/testutil/cyphernames"))
from peer_recording import install
install()

driver = types.ModuleType("neo4j")
driver.GraphDatabase = driver.Driver = driver.Session = object
sys.modules["neo4j"] = driver
m = runpy.run_path(str(pathlib.Path(__file__).with_name("migrate.py")))
out = {}
for name in json.load(sys.stdin):
    statements = [{"Route": "create", "Text": m["edge_create_cypher"](name)}, {"Route": "props", "Text": m["edge_props_cypher"](name)}]
    for route, stream in [("node", "stream_nodes"), ("edge", "stream_edges")]:
        class Session:
            page = 0

            def run(self, text, **params):
                statements.append({"Route": route + ("-first" if self.page == 0 else "-cursor"), "Text": text})
                self.page += 1
                return [{"id": "cursor"}] if self.page == 1 else []

        session = Session()
        list(m[stream](session, name, 1))
        assert session.page == 2
    for kind in ["UNIQUENESS", "NODE_KEY", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE", "RELATIONSHIP_UNIQUENESS"]:
        row = dict(name=name, type=kind, entityType="RELATIONSHIP" if kind.startswith("RELATIONSHIP") else "NODE", labelsOrTypes=[name], properties=[name])
        statements.append(dict(Route=kind, Text=m["constraint_to_cypher"](row)))
        if kind in ("UNIQUENESS", "NODE_KEY"):
            row["properties"] = [name, "second`key"]
            statements.append(dict(Route=kind+"-composite", Text=m["constraint_to_cypher"](row)))
    for kind in ["RANGE", "BTREE", "FULLTEXT", "VECTOR"]:
        row = dict(name=name, type=kind, entityType="NODE", labelsOrTypes=[name], properties=[name], options={"indexConfig": {"vector.dimensions": 3}})
        statements.append(dict(Route=kind, Text=m["index_to_cypher"](row)))
        if kind != "VECTOR":
            row["properties"] = [name, "second`key"]
            statements.append(dict(Route=kind+"-composite", Text=m["index_to_cypher"](row)))
    out[name] = statements
json.dump(out, sys.stdout, ensure_ascii=False)
