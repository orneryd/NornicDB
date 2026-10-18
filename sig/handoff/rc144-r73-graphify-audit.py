"""Read-only Graphify graph audit. Run only in the local graphify container."""
import collections
import json
import sys
from pathlib import Path

for flavour in sys.argv[1:]:
    graph = json.loads(Path(f"/tmp/rc144-r73-graph-{flavour}/graphify-out/graph.json").read_text())
    nodes = {n["id"]: n for n in graph["nodes"]}
    def production(node):
        source = node.get("source_file", "")
        return source.endswith(".go") and not source.endswith("_test.go") and source.startswith(("pkg/", "cmd/")) and "/antlr/" not in source
    wanted = ("Embedder", "EmbedQuery", "ChunkQuery", "EmbedBatch", "EmbedDocument", "ProviderUse", "closeOwned", "releaseEmbedder", "relationshipEmbed", "embedRelationships", "embedPendingRelationships", "startEmbedder", "initEmbedder", "runEmbedWithTimeout", "BeginUse", "ShareEmbedder", "QueryEmbedding", "getOrCreateEmbedder", "startWarmup", "warmupLoop", "Foreground")
    focus = {key for key, n in nodes.items() if production(n) and (any(word.lower() in n.get("label", "").lower() for word in wanted) or (n.get("label") in (".Close()", ".Stop()", ".closeInternal()") and n["source_file"].startswith(("pkg/nornicdb/", "pkg/server/", "pkg/embed/"))))}
    selected = [{key: n.get(key) for key in ("id", "label", "source_file", "source_location")} for key, n in nodes.items() if key in focus]
    outgoing = []
    incoming = []
    relations = collections.Counter()
    for edge in graph["edges"]:
        relations[edge["relation"]] += 1
        src, dst = edge["source"], edge["target"]
        if src not in nodes or dst not in nodes:
            continue
        if not production(nodes[src]) or not production(nodes[dst]):
            continue
        if src in focus and dst in focus:
            outgoing.append(edge)
        if dst in focus and edge["relation"] in ("calls", "call", "invokes"):
            incoming.append({"caller": src, "target": dst, "relation": edge["relation"], "confidence": edge.get("confidence"), "file": nodes[src].get("source_file"), "line": nodes[src].get("source_location")})
    print(json.dumps({"head": flavour, "nodes": len(nodes), "edges": len(graph["edges"]), "relations": relations, "focus": selected, "focus_edges": outgoing, "incoming_calls": incoming}))
