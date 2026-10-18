"""Rule 73 #1412: reachability and read->holder paths over graphify's call graph.

Run inside the shared graphify container:
  docker exec -i graphify python - /graphs/nornic-wt-fmem1412/cypher/graphify-out/graph.json \
      /repos/nornic-wt-fmem1412/zz-inventory.tsv < graphify_reach.py

Inputs: graphify's code-only graph of pkg/cypher and the typed inventory
(zz_rule73_inventory_test.go output). Output, tab-separated:
  REACH <function> <reachable 0/1> <depth> <one shortest call chain from an entry>
  FEEDS <holder function> <read functions it reaches through calls>
  DUP   <storage method> <functions reading it directly> (alternate routes to the same data)
  NAME  <name> <files> (same function name in several files: duplicate implementations)
"""
import json
import re
import sys
from collections import defaultdict, deque

graph = json.load(open(sys.argv[1]))
inventory = [line.rstrip("\n").split("\t") for line in open(sys.argv[2]) if line.strip()]

nodes = {n["id"]: n for n in graph["nodes"]}
def is_test(node_id):
    node = nodes.get(node_id)
    return node is None or node.get("source_file", "").endswith("_test.go")

# Names: "receiver.method" or "function", lower case, shared by both graphs.
def graphify_name(nid):
    node = nodes[nid]
    label = node.get("label", "")
    name = label.strip(".()")
    if label.startswith(".") and nid.startswith("cypher_") and nid.endswith("_" + name.lower()):
        return nid[len("cypher_"):-len(name) - 1] + "." + name.lower()
    return name.lower()

calls = defaultdict(set)
graphify_edges = typed_edges = 0
for edge in graph["edges"]:
    if edge["relation"] == "calls" and not is_test(edge["source"]) and not is_test(edge["target"]):
        calls[graphify_name(edge["source"])].add(graphify_name(edge["target"]))
        graphify_edges += 1
typed = set()
for row in inventory:
    if row[0] == "E":
        caller, callee = row[1].lower(), row[2].lower()
        typed.add((caller, callee))
        calls[caller].add(callee)
        typed_edges += 1
missing = sum(1 for caller, callee in typed if callee not in calls.get(caller, set()) or True) - 0
only_typed = 0
graph_pairs = set()
for edge in graph["edges"]:
    if edge["relation"] == "calls" and not is_test(edge["source"]) and not is_test(edge["target"]):
        graph_pairs.add((graphify_name(edge["source"]), graphify_name(edge["target"])))
only_typed = len(typed - graph_pairs)
only_graphify = len({p for p in graph_pairs if p[0] in {c for c, _ in typed}} - typed)
print("EDGES	graphify=%d typed=%d typed-not-in-graphify=%d graphify-not-in-typed=%d" % (len(graph_pairs), len(typed), only_typed, only_graphify))

def node_id(function):
    return function.lower()

# Statement entries: Execute (Bolt/HTTP/embedded), the transaction routes,
# and init-time registrations (procedures and functions registered as values).
entries = [n for n in ("storageexecutor.execute", "storageexecutor.executeintransaction", "storageexecutor.executeinternal",
                       "ensurebuiltinproceduresregistered") if n in calls]
for caller in list(calls):
    if caller.startswith("init") or caller.startswith("register"):
        entries.append(caller)
parent = {e: None for e in entries}
depth = {e: 0 for e in entries}
queue = deque(entries)
while queue:
    current = queue.popleft()
    for callee in calls[current]:
        if callee not in depth:
            depth[callee] = depth[current] + 1
            parent[callee] = current
            queue.append(callee)

def chain(nid):
    path = []
    while nid is not None:
        path.append(nid)
        nid = parent[nid]
    return " <- ".join(path)

functions = set()
reads_by_method = defaultdict(set)
read_functions = set()
for row in inventory:
    kind = row[0]
    if kind in ("R", "V", "P", "L"):
        functions.add(row[2])
    if kind in ("R", "V"):
        if "transactionStorageWrapper" not in row[2]:
            reads_by_method[row[3]].add(row[2])
            read_functions.add(row[2])
print("ENTRIES\t" + ",".join(entries))
ids = {}
for function in sorted(functions):
    nid = node_id(function)
    ids[function] = nid
    if nid is None:
        print("REACH\t%s\t?\t-\tnot in graphify graph" % function)
    elif nid in depth:
        print("REACH\t%s\t1\t%d\t%s" % (function, depth[nid], chain(nid)))
    else:
        print("REACH\t%s\t0\t-\tno call path from a statement entry" % function)

read_ids = {ids[f]: f for f in read_functions if ids.get(f)}
for function in sorted(functions):
    nid = ids.get(function)
    if nid is None:
        continue
    seen, stack, fed = {nid}, [nid], set()
    while stack:
        current = stack.pop()
        if current in read_ids and current != nid:
            fed.add(read_ids[current])
        for callee in calls[current]:
            if callee not in seen:
                seen.add(callee)
                stack.append(callee)
    if nid in read_ids:
        fed.add(function + "(itself)")
    if fed:
        print("FEEDS\t%s\t%s" % (function, ",".join(sorted(fed))))

for method, readers in sorted(reads_by_method.items()):
    if len(readers) > 1:
        print("DUP\t%s\t%d\t%s" % (method, len(readers), ",".join(sorted(readers))))

by_name = defaultdict(set)
for nid, node in nodes.items():
    if is_test(nid):
        continue
    label = node.get("label", "")
    if label.endswith("()"):
        by_name[label.strip(".()")].add(node.get("source_file", "") + ":" + node.get("source_location", ""))
inventory_names = {f.split(".")[-1] for f in functions}
for name in sorted(inventory_names):
    places = by_name.get(name, set())
    if len(places) > 1:
        print("NAME\t%s\t%s" % (name, ",".join(sorted(places))))
