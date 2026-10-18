"""Test-only analysis (fix145-1391): call-graph reachability over a graphify
graph.json, joined with the go/types loop inventory (inv.tsv).

python3 zz_callgraph.py graph.json inv.tsv > out.txt
"""
import collections
import json
import re
import sys

graph = json.load(open(sys.argv[1]))
inventory = [line.rstrip("\n").split("\t") for line in open(sys.argv[2]) if line.strip()]
nodes = {n["id"]: n for n in graph["nodes"]}
EDGE = {"calls", "indirect_call", "references", "method", "uses"}
succ = collections.defaultdict(set)
pred = collections.defaultdict(set)
relations = collections.Counter()
for link in graph["links"]:
    relations[link.get("relation")] += 1
    if link.get("relation") in EDGE:
        succ[link["source"]].add(link["target"])
        pred[link["target"]].add(link["source"])


def callable_node(node):
    return bool(node.get("_callable")) or (node.get("label") or "").endswith("()")


def loc(node):
    m = re.match(r"L(\d+)", node.get("source_location") or "")
    return int(m.group(1)) if m else 0


def name(node):
    return node.get("label", "").rstrip("()").lstrip(".")


funcs_by_file = collections.defaultdict(list)
for n in graph["nodes"]:
    f = n.get("source_file") or ""
    if f.endswith(".go") and callable_node(n):
        funcs_by_file[f].append((loc(n), n["id"]))
for f in funcs_by_file:
    funcs_by_file[f].sort()


def node_at(file, line):
    """The function node whose start line is the last one at or before line."""
    cands = funcs_by_file.get(file, [])
    best = None
    for start, nid in cands:
        if start <= line:
            best = nid
        else:
            break
    return best


def find(file_suffix, label):
    out = []
    for n in graph["nodes"]:
        if (n.get("source_file") or "").endswith(file_suffix) and name(n) == label:
            out.append(n["id"])
    return out


roots = {
    "Execute": find("pkg/cypher/executor_entry.go", "Execute"),
    "bolt": [n["id"] for n in graph["nodes"] if (n.get("source_file") or "").startswith("pkg/bolt/") and re.match(r"handle(Run|Begin|Commit|Pull|Discard)", name(n))],
    "http": [n["id"] for n in graph["nodes"] if (n.get("source_file") or "").startswith("pkg/server/") and re.match(r"handle(ImplicitTransaction|ExecuteInTransaction|OpenTransaction|CommitTransaction|Query)", name(n))],
    "procedures": [n["id"] for n in graph["nodes"] if (n.get("source_file") or "").startswith("pkg/cypher/") and re.match(r"(callProcedure|executeCall|dispatchProcedure|invokeProcedure|callBuiltinProcedure|executeCallProcedure)", name(n))],
}
print("== graph", sys.argv[1], "nodes", len(nodes), "links", len(graph["links"]))
print("relations", relations.most_common())
for k, v in roots.items():
    print("root", k, len(v), sorted((nodes[i].get("source_file"), nodes[i].get("source_location"), nodes[i].get("label")) for i in v)[:12])


def reach(starts):
    seen, stack = set(), list(starts)
    while stack:
        x = stack.pop()
        if x in seen:
            continue
        seen.add(x)
        stack.extend(succ[x] - seen)
    return seen


reached = {k: reach(v) for k, v in roots.items()}
allr = set().union(*reached.values())
cypher_funcs = [n for n in graph["nodes"] if (n.get("source_file") or "").startswith("pkg/cypher") and callable_node(n)]
print("pkg/cypher functions", len(cypher_funcs), "reached from any root", sum(1 for n in cypher_funcs if n["id"] in allr))
for k, s in reached.items():
    print("  reached from", k, sum(1 for n in cypher_funcs if n["id"] in s))
# Reached from bolt/http but not from Execute: alternate statement routes.
alt = [n for n in cypher_funcs if n["id"] in allr and n["id"] not in reached["Execute"]]
print("== pkg/cypher functions reached from bolt/http/procedures but NOT from Execute:", len(alt))
for n in sorted(alt, key=lambda n: (n.get("source_file"), loc(n)))[:80]:
    print("  ALT", n.get("source_file") + ":" + str(loc(n)), name(n))

# Join the loop inventory.
stats = collections.Counter()
graph_only, types_only = [], []
for row in inventory:
    kind, where, fn, domain, typ, observes = row[:6]
    types_reach = row[7] if len(row) > 7 else "?"
    if kind != "LOOP" or domain != "DATA":
        continue
    file, line = where.rsplit(":", 1)
    nid = node_at(file, int(line))
    g = nid is not None and nid in allr
    stats[(types_reach, g, observes)] += 1
    if g and types_reach == "false":
        graph_only.append((where, fn, typ, observes))
    if not g and types_reach == "true":
        types_only.append((where, fn, typ, observes, nid is not None))
print("== DATA loops: (go/types reachable, graph reachable, observes) -> count")
for k, v in sorted(stats.items()):
    print("  ", k, v)
print("== DATA loops the graph reaches but go/types refs did not:", len(graph_only))
for x in graph_only:
    print("  GRAPH+", *x)
print("== DATA loops go/types reaches but the graph does not:", len(types_only))
for x in types_only[:60]:
    print("  TYPES+", *x)

# Duplicate implementations.
labels = collections.defaultdict(list)
for n in cypher_funcs:
    labels[name(n)].append(n)
print("== same function name in several pkg/cypher places (reached):")
for lab, ns in sorted(labels.items()):
    rs = [n for n in ns if n["id"] in allr]
    if len(rs) > 1 and lab not in {"String", "Error", "Len", "Less", "Swap", "eval", "holds", "get", "put", "Value", "Err", "Done", "Deadline"}:
        print("  DUP", lab, "; ".join(n.get("source_file") + ":" + str(loc(n)) for n in rs))
print("== probed/plain twins (X and XWithCancellation / XProbed), reached:")
twins = 0
for lab, ns in sorted(labels.items()):
    for suffix in ("WithCancellation", "Probed"):
        if lab.endswith(suffix) and lab[: -len(suffix)] in labels:
            twins += 1
            plain = labels[lab[: -len(suffix)]]
            print("  TWIN", lab, ns[0].get("source_file") + ":" + str(loc(ns[0])), "plain", plain[0].get("source_file") + ":" + str(loc(plain[0])),
                  "probed-reached", any(n["id"] in allr for n in ns), "plain-reached", any(n["id"] in allr for n in plain))
print("twins", twins)
print("== traversal / path-search implementations reached from Execute:")
for n in sorted(cypher_funcs, key=lambda n: (n.get("source_file"), loc(n))):
    if n["id"] in reached["Execute"] and re.search(r"(?i)(travers|expand|varlength|variablelength|shortest|dfs|bfs|pathsearch|trails|walks|quantified)", name(n)):
        print("  PATH", n.get("source_file") + ":" + str(loc(n)), name(n))
