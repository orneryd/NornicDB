"""Local code analysis only; run inside the authorised shared graphify container.

Graphify edges are static candidates, not proof a public query takes a route.
The separate Go coverage replay and live-oracle matrix supply runtime evidence.
"""
import collections
import json
import pathlib
import re
import sys

from tree_sitter import Language, Parser
import tree_sitter_go

root, graph_path, output = map(pathlib.Path, sys.argv[1:])
graph = json.loads(graph_path.read_text())
nodes = {n["id"]: n for n in graph["nodes"]}
edges = graph.get("edges", graph.get("links", []))
focus = re.compile(r"pattern|subquery|correlated|bound.*(?:Count|Rows)|pipeline.*Import|carry.*Import|graphReadingExpression|incidentEdge|undirected.*Edge|pipelineNodePredicateProperties", re.I)

def loc(node):
    file = node.get("source_file", "").replace(str(root) + "/", "")
    return file + ":" + node.get("source_location", "")

def production(node):
    file=node.get("source_file", "")
    in_repo = file.startswith(str(root)+"/") or (bool(file) and not file.startswith(("/","..")))
    return in_repo and not file.endswith("_test.go")

incoming=collections.defaultdict(list)
outgoing=collections.defaultdict(list)
for edge in edges:
    if edge.get("relation") == "calls" and edge.get("source") in nodes and edge.get("target") in nodes:
        incoming[edge["target"]].append(edge["source"])
        outgoing[edge["source"]].append(edge["target"])

def bfs(start, target):
    queue=collections.deque([start]);previous={start:None}
    while queue:
        current=queue.popleft()
        if current==target:
            route=[]
            while current is not None:
                route.append(current);current=previous[current]
            return list(reversed(route))
        for nxt in outgoing[current]:
            if nxt not in previous and production(nodes[nxt]):
                previous[nxt]=current;queue.append(nxt)
    return []

parser=Parser(Language(tree_sitter_go.language()))
definitions=[];calls=[];errors=[];excluded=[]
for path in sorted((root/"pkg/cypher").rglob("*.go")):
    if path.name.endswith("_test.go"):continue
    if path.name in {"cypher_parser.go","cypher_lexer.go","cypherparser_base_visitor.go","cypherparser_visitor.go","cypherparser_listener.go","cypherparser_base_listener.go"}:
        excluded.append(str(path.relative_to(root)));continue
    source=path.read_bytes();tree=parser.parse(source)
    if tree.root_node.has_error:errors.append(str(path.relative_to(root)))
    def text(node):return source[node.start_byte:node.end_byte].decode(errors="replace") if node else ""
    for declaration in tree.root_node.named_children:
        if declaration.type not in {"function_declaration","method_declaration"}:continue
        name=text(declaration.child_by_field_name("name"))
        receiver=text(declaration.child_by_field_name("receiver"))
        item=dict(file=str(path.relative_to(root)),line=declaration.start_point.row+1,name=name,receiver=receiver)
        definitions.append(item)
        stack=[declaration]
        while stack:
            node=stack.pop()
            if node.type=="call_expression":
                callee=node.child_by_field_name("function")
                calls.append(dict(caller=item,expression=text(callee),line=node.start_point.row+1))
            stack.extend(reversed(node.named_children))

targets={d["name"] for d in definitions if focus.search(d["name"])}
family_calls=[c for c in calls if re.split(r"[.\[]",c["expression"])[-1] in targets or focus.search(c["expression"])]
family_defs=[d for d in definitions if d["name"] in targets]
family_nodes=[n for n in nodes.values() if production(n) and focus.search(n.get("label","")) and n.get("source_file","").endswith(".go")]

target_names=["rowPatternPredicate","evaluatePatternPredicateValue","evaluatePatternComprehensionFromRow","evaluateExistsSubqueryValue","rowSubqueryValue","boundDegreeCount","boundChainCount","boundExpandCount","boundNodeLookupCount","boundPatternRows","executeCorrelatedSubqueryBody","carrySubqueryImports","evaluateRelationshipPatternInWhere","pipelineNodePredicateProperties","graphReadingExpression","patternPredicate","quantifiedGroup","executeApply","ExecuteQueryWithRecord","ExecuteRecordQuery","executeFragmentQuery"]
entries=[n["id"] for n in nodes.values() if production(n) and n.get("label")==".Execute()" and n.get("source_file","").endswith("pkg/cypher/executor_entry.go")]
routes={}
for target_name in target_names:
    candidates=[n for n in nodes.values() if production(n) and n.get("label","").lstrip(".")==target_name+"()"]
    records=[]
    for n in candidates:
        paths=[bfs(entry,n["id"]) for entry in entries]
        paths=[p for p in paths if p]
        records.append(dict(target=loc(n),callers=[dict(label=nodes[c].get("label"),location=loc(nodes[c]),production=production(nodes[c])) for c in incoming[n["id"]]],path=[dict(label=nodes[c].get("label"),location=loc(nodes[c])) for c in min(paths,key=len)] if paths else []))
    routes[target_name]=records

def ast_id(item):return item["file"]+":"+str(item["line"])+":"+item["name"]
by_name=collections.defaultdict(list)
for definition in definitions:by_name[definition["name"]].append(definition)
ast_edges=collections.defaultdict(list)
for call in calls:
    name=call["expression"].split(".")[-1]
    candidates=by_name[name]
    if not candidates:continue
    if "." in call["expression"] and call["expression"].split(".")[0] in {"fmt","strings","strconv","context","time","storage","sort","errors","maps","slices","reflect","sync","atomic","math","json","regexp","fabric"}:continue
    receiver=call["expression"].split(".")[0]
    if len(candidates)>1 and receiver=="e":
        typed=[d for d in candidates if "StorageExecutor" in d["receiver"]]
        if typed:candidates=typed
    for candidate in candidates:
        ast_edges[ast_id(call["caller"])].append(dict(target=ast_id(candidate),callLine=call["line"],expression=call["expression"],ambiguous=len(candidates)>1))
ast_defs={ast_id(d):d for d in definitions}
ast_entries=[ast_id(d) for d in definitions if d["name"]=="Execute" and "StorageExecutor" in d["receiver"]]
ast_routes={}
for target_name in target_names:
    for target in by_name[target_name]:
        queue=collections.deque(ast_entries);previous={entry:None for entry in ast_entries};steps={}
        while queue:
            current=queue.popleft()
            if current==ast_id(target):break
            for edge in ast_edges[current]:
                if edge["target"] not in previous:
                    previous[edge["target"]]=current;steps[edge["target"]]=edge;queue.append(edge["target"])
        route=[];current=ast_id(target)
        if current in previous:
            while current is not None:
                route.append(dict(symbol=ast_defs[current],incoming=steps.get(current)));current=previous[current]
            route.reverse()
        ast_routes[ast_id(target)]=route

report=dict(root=str(root),graph=str(graph_path),graphNodes=len(nodes),graphEdges=len(edges),relations=dict(collections.Counter(e.get("relation") for e in edges)),astFunctions=len(definitions),astCalls=len(calls),astParseErrors=errors,astExcludedGenerated=excluded,familyDefinitions=family_defs,familyCallsites=family_calls,graphFamilyNodes=family_nodes,entrypoints=[loc(nodes[n]) for n in entries],routes=routes,astCandidateRoutes=ast_routes)
output.write_text(json.dumps(report,indent=2))
print(json.dumps({k:report[k] for k in ["root","graphNodes","graphEdges","astFunctions","astCalls","astParseErrors","entrypoints"]},indent=2))
for name,records in routes.items():
    for record in records:
        print(name,record["target"],"callers",len(record["callers"]),"Execute-static-path", " -> ".join(x["label"] for x in record["path"]) or "NO RESOLVED PATH")
for target,route in ast_routes.items():
    print("AST-CANDIDATE",target," -> ".join(x["symbol"]["name"]+("?" if x["incoming"] and x["incoming"]["ambiguous"] else "") for x in route) or "NO RESOLVED PATH")
