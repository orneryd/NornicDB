"""Gate-only generated attack corpus; run only on TrueNAS, never product code."""
import gzip
import hashlib
import json
import os
import sys

cases = []
seen = set()

def add(member, state, query):
    if query in seen:
        return
    seen.add(query)
    cases.append(dict(id=hashlib.sha256(query.encode()).hexdigest()[:20],
                      family=member, position=state, query=query, rollback=False))

outer = "MATCH (a:Q) WITH a, a.id AS k, 1 AS weight"
bound = "MATCH (a:Q) OPTIONAL MATCH (a)-[r:R]->(b:Q) WITH a,r,b,a.id AS k,1 AS weight"
null = "OPTIONAL MATCH (a:Nope) WITH a,0 AS k,1 AS weight"

# The shared pattern reader admits complete relationship chains, quoted
# variables, direction/type alternatives, maps and inline predicates.
patterns = [
    ("bound-hop", "(a)-[:R]->(b)"),
    ("incoming", "(b)<-[:R]-(a)"),
    ("undirected", "(a)-[:R]-(b)"),
    ("bound-rel", "(a)-[r]->(b)"),
    ("anonymous-end", "(a)-[:R]->()"),
    ("rel-only", "()-[r]->(:Q)"),
    ("labels", "(a:Q)-[:R]->(b:Q)"),
    ("node-map", "(a)-[:R]->(b:Q {id:2})"),
    ("rel-map", "(a)-[:R {w:1}]->(b)"),
    ("row-map", "(a)-[:R {w:weight}]->(b)"),
    ("chain", "(a)-[:R]->()-[:R]->(b)"),
    ("var-length", "(a)-[:R*1..2]->(b)"),
    ("type-alternative", "(a)-[:R|U]->(b)"),
    ("inline-where", "(a)-[:R]->(b WHERE b.id > 1)"),
]
for state, pattern in patterns:
    for predicate in [pattern, "NOT " + pattern, pattern + " AND k > 1",
                      pattern + " OR k = 3", pattern + " XOR k = 2"]:
        add("P01-predicate", state+"/where", f"{bound} WHERE {predicate} RETURN k AS id ORDER BY id")
        add("P02-compiled-with", state, f"{bound} WITH a,r,b,k,weight WHERE {predicate} RETURN k AS id ORDER BY id")
    add("P03-exists-function", state, f"{bound} RETURN k AS id, exists({pattern}) AS v ORDER BY id")
    add("P04-comprehension", state, f"{bound} RETURN k AS id, size([{pattern} | 1]) AS v ORDER BY id")

for prefix, state in [(outer,"unbound"),(null,"null"),("WITH 'wrong' AS a,0 AS k,1 AS weight","wrong-type")]:
    for pattern in ["(a)-[:R]->(missing)", "(a)-[missing]->()", "(a)-->()"]:
        add("S01-local-admission", state+"/predicate", f"{prefix} RETURN exists({pattern}) AS v")
        add("S01-local-admission", state+"/comprehension", f"{prefix} RETURN size([{pattern} | 1]) AS v")
        for kind in ["EXISTS","COUNT"]:
            add("S01-local-admission", state+"/"+kind, f"{prefix} RETURN {kind} {{ {pattern} }} AS v")
        add("S01-local-admission",state+"/COLLECT",f"{prefix} RETURN COLLECT {{ MATCH {pattern} RETURN 1 AS v }} AS v")

horizons = [
    ("none",""), ("star","WITH *"), ("omit-import","WITH 1 AS local"),
    ("rename","WITH a AS renamed"),
    ("drop-readd","WITH 1 AS local WITH a AS restored"),
    ("rename-drop","WITH a AS renamed WITH 1 AS local"),
    ("distinct","WITH DISTINCT 1 AS local"),
    ("aggregate","WITH count(*) AS local"),
]
body_patterns = [("hop","(a)-[:R]->(b)"),("property","(a)-[:R {w:weight}]->(b)"),
                 ("endpoint-map","(a)-[:R]->(b:Q {id:2})"),("chain","(a)-[:R*1..2]->(b)")]
for prefix_state,prefix in [("bound",outer),("null",null)]:
    for horizon,projection in horizons:
        for shape,pattern in body_patterns:
            bodies = [("where-pattern",f"{projection} MATCH (b:Q) WHERE {pattern}"),
                      ("match-pattern",f"{projection} MATCH {pattern}")]
            for route,body in bodies:
                state=f"{prefix_state}/{horizon}/{shape}/{route}"
                for kind in ["EXISTS","COUNT","COLLECT"]:
                    ret = " RETURN b.id AS v ORDER BY v" if kind=="COLLECT" else ""
                    expr=f"{kind} {{ {body}{ret} }}"
                    add("S02-expression-imports",state+"/"+kind,f"{prefix} RETURN k AS id, {expr} AS v ORDER BY id")
                comp=f"size([{pattern} | b.id])"
                add("S03-comprehension-imports",state,f"{prefix} RETURN k AS id, COLLECT {{ {projection} RETURN {comp} AS v }} AS v ORDER BY id")

# Imported names cannot be reassigned; body locals disappear after WITH.
for body,state in [
    ("WITH null AS a MATCH (b:Q) WHERE (a)-[:R]->(b)","reassign-import"),
    ("WITH 1 AS local WITH 2 AS kept MATCH (b:Q) WHERE b.id = local","dropped-local"),
    ("WITH a AS renamed WITH 2 AS kept MATCH (b:Q) WHERE (renamed)-[:R]->(b)","dropped-alias"),
    ("WITH a AS renamed MATCH (b:Q) WHERE (renamed)-[:R]->(b)","read-alias"),
    ("WITH 1 AS local WITH a AS restored MATCH (b:Q) WHERE (restored)-[:R]->(b)","read-restored"),
]:
    for kind in ["EXISTS","COUNT","COLLECT"]:
        ret=" RETURN b.id AS v ORDER BY v" if kind=="COLLECT" else ""
        add("S04-scope-errors",state+"/"+kind,f"{outer} RETURN k AS id, {kind} {{ {body}{ret} }} AS v ORDER BY id")

for union in ["UNION","UNION ALL"]:
    for horizon,projection in horizons[:6]:
        left=f"{projection} MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v"
        right=f"{projection} MATCH (b:Q) WHERE (b)-[:R]->(a) RETURN b.id AS v"
        for kind in ["EXISTS","COUNT","COLLECT"]:
            # Sorted list length avoids unspecified UNION branch order.
            expr=f"{kind} {{ {left} {union} {right} }}"
            if kind=="COLLECT": expr=f"size({expr})"
            add("B01-union",union+"/"+horizon+"/"+kind,f"{outer} RETURN k AS id, {expr} AS v ORDER BY id")

calls = [
    ("scoped","CALL (a) { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v }"),
    ("wildcard","CALL (*) { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v }"),
    ("legacy-retain","CALL { WITH a MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v }"),
    ("legacy-drop","CALL { WITH a WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v }"),
    ("empty-local","CALL () { MATCH (x:Q)-[:R]->(b:Q) RETURN b.id AS v }"),
    ("empty-outer","CALL () { MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v }"),
]
for name,call in calls:
    add("B02-call",name+"/statement",f"{outer} {call} RETURN k AS id,v ORDER BY id,v")
    for kind in ["EXISTS","COUNT","COLLECT"]:
        for horizon,projection in horizons[:6]:
            body=f"{projection} {call} RETURN v ORDER BY v"
            add("B02-call",name+"/"+horizon+"/"+kind,f"{outer} RETURN k AS id,{kind} {{ {body} }} AS v ORDER BY id")

# Current-row iterator bindings, nested subqueries, path local shadowing,
# parser quoting, schema names and results that must not escape their scope.
for body,name in [
    ("EXISTS { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) }","exists"),
    ("COUNT { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) } > 0","count"),
    ("size(COLLECT { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id }) > 0","collect"),
    ("size([(a)-[:R]->(b) | b.id]) > 0","comprehension"),
]:
    for expression,state in [(f"[{body}]","list"),(f"CASE WHEN {body} THEN 1 ELSE 0 END","case"),
                             (f"any(a IN [a] WHERE {body})","quantifier"),
                             (f"[a IN [a] | {body}]","list-local"),
                             (f"reduce(v = 0, a IN [a] | v + CASE WHEN {body} THEN 1 ELSE 0 END)","reduce")]:
        add("N01-nesting",name+"/"+state,f"{outer} RETURN k AS id,{expression} AS v ORDER BY id")
add("N02-quoted", "quoted-hop", "MATCH (`a a`:Q)-[`r r`:R]->(`b b`:Q) WHERE (`a a`)-[`r r`]->(`b b`) RETURN `a a`.id AS id ORDER BY id")
add("N02-quoted", "schema-token", "WITH 'unused' AS Q,'unused' AS R MATCH (a:Q) WHERE (a)-[:R]->(:Q) RETURN a.id AS id ORDER BY id")
add("N03-path", "path-local", "MATCH p = (a:Q)-[:R]->(b:Q) RETURN a.id AS id,size([p = (a)-[:R]->(x) | length(p)]) AS v ORDER BY id")
for expression,local in [("EXISTS { MATCH (a:Q)-[:R]->(x) }","x"),
                         ("COUNT { MATCH (a:Q)-[:R]->(x) }","x"),
                         ("COLLECT { MATCH (a:Q)-[:R]->(x) RETURN x.id }","x"),
                         ("[(a:Q)-[:R]->(x) | x.id]","x")]:
    add("N04-no-escape","local",f"RETURN {expression} AS v,{local} AS leaked")

# General pipeline fallback shapes that no one-hop/degree kernel admits.
for clause in ["ORDER BY b.id SKIP 0 LIMIT 1", "ORDER BY b.id DESC LIMIT 1"]:
    for kind in ["EXISTS","COUNT","COLLECT"]:
        add("G01-general-body",kind+"/"+clause,f"{outer} RETURN k AS id,{kind} {{ WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v {clause} }} AS v ORDER BY id")
for query,state in [
    ("WITH 2 AS key MATCH (a:Q) WHERE (a)-[:R]->(:Q {id:key}) RETURN a.id AS id ORDER BY id","scalar-map"),
    ("MATCH (a:Q)-[r:R]->() WITH r WHERE ()-[r]->(:Q {id:2}) RETURN count(*) AS v","relationship-only"),
    ("MATCH (a:Q) RETURN a.id AS id,EXISTS { (a)-[:R]->() } AS v,COUNT { (a)-[:R]->() } AS c ORDER BY id","degree"),
    ("MATCH (a:Q) RETURN a.id AS id,EXISTS { (b:Q {id:a.id}) } AS v ORDER BY id","node-lookup"),
    ("MATCH (a:Q) SET a.flag = EXISTS { WITH 1 AS k MATCH (b:Q) WHERE (a)-[:R]->(b) } RETURN a.id AS id,a.flag AS v ORDER BY id","write-expression"),
]:
    add("G02-other-consumers",state,query)
    if state=="write-expression": cases[-1]["rollback"]=True

# Supplement: inventory-discovered context rewrites and public consumers.
for pattern in ["(a)-[:R]->()", "(a)-[:R]->(missing)", "(a)-[missing]->()"]:
    for expr,state in [(pattern,"bare-value"),(f"CASE WHEN {pattern} THEN 1 ELSE 0 END","case-pattern"),
                       (f"NOT {pattern}","not-value"),(f"exists({pattern})","function")]:
        add("U01-context-rewrites",state,f"{outer} RETURN k AS id,{expr} AS v ORDER BY id")
        add("U01-context-rewrites",state+"/set",f"{outer} SET a.gate={expr} RETURN k AS id,a.gate AS v ORDER BY id")
        cases[-1]["rollback"]=True

expr="EXISTS { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) }"
for query,state,write in [
    (f"MATCH (a:Q) CREATE (out:Gate {{key:a.id,found:{expr}}}) RETURN out.key AS id,out.found AS v ORDER BY id","create",True),
    (f"MATCH (a:Q) MERGE (out:Gate {{key:a.id}}) SET out.found={expr} RETURN out.key AS id,out.found AS v ORDER BY id","merge",True),
    (f"UNWIND [1,2,3] AS k MATCH (a:Q {{id:k}}) RETURN k AS id,{expr} AS v ORDER BY id","unwind",False),
    (f"{outer} WITH a,k,{expr} AS v WHERE v RETURN k AS id ORDER BY id","project-filter",False),
    (f"{outer} RETURN count(CASE WHEN {expr} THEN 1 END) AS v","aggregate",False),
    (f"{outer} RETURN k AS id ORDER BY {expr},id","order",False),
    (f"{outer} RETURN k AS id,{{found:{expr}}} AS v ORDER BY id","map",False),
]:
    add("U02-consumers",state,query)
    cases[-1]["rollback"]=write

for expr,state in [
    ("false AND EXISTS { RETURN 1/0 AS bad }","and-error"),
    ("true OR EXISTS { RETURN 1/0 AS bad }","or-error"),
    ("EXISTS { MATCH (b:Q) WHERE false AND (a)-[:R]->(b) }","short-circuit"),
    ("COUNT { MATCH (a)-[:R]->(b) WHERE EXISTS { WITH 1 AS local MATCH (c:Q) WHERE (b)-[:R]->(c) } }","nested-count"),
    ("COLLECT { MATCH (a)-[:R]->(b) RETURN COLLECT { WITH 1 AS local MATCH (c:Q) WHERE (b)-[:R]->(c) RETURN c.id ORDER BY c.id } AS v ORDER BY b.id }","nested-collect"),
    ("[(a)-[:R]->(b) WHERE EXISTS { WITH 1 AS local MATCH (c:Q) WHERE (b)-[:R]->(c) } | b.id]","comprehension-filter"),
]:
    add("U03-nested-errors",state,f"{outer} RETURN k AS id,{expr} AS v ORDER BY id")

for expr,state in [
    ("EXISTS { (a)-[:R]->(:$(lab)) }","dynamic-label"),
    ("EXISTS { (a)-[:$(typ)]->() }","dynamic-type"),
    ("exists((a)-[:R]->(:$(lab)))","dynamic-predicate"),
    ("size([(a)-[:R]->(:$(lab)) | 1])","dynamic-comprehension"),
    ("EXISTS { (a)-[:R]->(:$(missing)) }","undefined-dynamic"),
]:
    add("U04-dynamic-tokens",state,f"{outer} WITH a,k,'Q' AS lab,'R' AS typ RETURN k AS id,{expr} AS v ORDER BY id")

for kind in ["EXISTS","COUNT","COLLECT"]:
    for body,state in [("MATCH (x:Q) RETURN x.id AS v UNION MATCH (x:Q) RETURN x.id AS v","uncorrelated-union"),
                       ("MATCH (x:Q) RETURN x.id AS v UNION ALL MATCH (x:Q) RETURN x.id AS v","uncorrelated-union-all"),
                       ("WITH 1 AS local CALL (a) { WITH 1 AS nested MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id AS v } RETURN v","call-after-omit"),
                       ("CALL () { WITH 1 AS local RETURN local AS v } RETURN v","call-no-import"),
                       ("CREATE (:Gate) RETURN 1 AS v","write-body")]:
        expr=f"{kind} {{ {body} }}"
        if kind=="COLLECT" and "union" in state:expr=f"size({expr})"
        add("U05-body-admission",kind+"/"+state,f"{outer} RETURN k AS id,{expr} AS v ORDER BY id")
        if state=="write-body":cases[-1]["rollback"]=True

if os.environ.get("R73_BASE") == "1":
    # Reproduce the original 864-case base after supplements were added.
    cases=[case for case in cases if not case["family"].startswith("U")]

if os.environ.get("R73_SUPPLEMENT") == "1":
    cases=[case for case in cases if case["family"].startswith("U")]

if os.environ.get("R73_PARAMS") == "1":
    cases=[]
    seen=set()
    for horizon,projection in horizons[:6]:
        for kind in ["EXISTS","COUNT","COLLECT"]:
            ret=" RETURN b.id AS v ORDER BY v" if kind=="COLLECT" else ""
            add("V01-parameters",horizon+"/"+kind,f"{outer} RETURN k AS id,{kind} {{ {projection} MATCH (b:Q {{id:$target}}) WHERE (a)-[:R {{w:$w}}]->(b){ret} }} AS v ORDER BY id")
        add("V01-parameters",horizon+"/comprehension",f"{outer} RETURN k AS id,COLLECT {{ {projection} RETURN size([(a)-[:R {{w:$w}}]->(b:Q {{id:$target}}) | 1]) AS v }} AS v ORDER BY id")
    add("V01-parameters","null-parameter",f"{outer} RETURN k AS id,exists((a)-[:R {{w:$null}}]->()) AS v ORDER BY id")
    add("V01-parameters","missing-parameter",f"{outer} RETURN k AS id,EXISTS {{ (a)-[:R {{w:$absent}}]->() }} AS v ORDER BY id")
    add("V01-parameters","dynamic-label",f"{outer} RETURN k AS id,EXISTS {{ (a)-[:R]->(:$($lab)) }} AS v ORDER BY id")

if os.environ.get("R73_PROJECTION") == "1":
    cases=[]
    seen=set()
    for state,pattern in patterns:
        for projection in ["b.id", "r.w", "{id:b.id,w:r.w}",
                           "EXISTS { WITH 1 AS local MATCH (c:Q) WHERE (b)-[:R]->(c) }"]:
            add("W01-full-comprehension",state+"/"+projection,f"{bound} RETURN k AS id,[{pattern} | {projection}] AS v ORDER BY id")
    for projection in ["x.id", "length(p)", "{id:x.id,path:length(p)}"]:
        add("W01-full-comprehension","path-shadow/"+projection,f"MATCH p=(a:Q)-[:R]->(b:Q) RETURN a.id AS id,[p=(a)-[:R]->(x) | {projection}] AS v ORDER BY id")
    for kind in ["EXISTS","COUNT","COLLECT"]:
        for body,state in [
            ("MATCH (x:Q) WHERE x.id=a.id RETURN x.id AS v","correlated-filter"),
            ("MATCH (x:Q {id:a.id}) RETURN x.id AS v","correlated-node-map"),
            ("MATCH (x:Q),(y:Q) WHERE (a)-[:R]->(x) AND (x)-[:R]->(y) RETURN y.id AS v ORDER BY v","pattern-list"),
            ("MATCH (a)-[r:R]->(b) RETURN r.w AS v ORDER BY v","returned-rel-properties"),
        ]:
            add("W02-kernel-fallback",kind+"/"+state,f"{outer} RETURN k AS id,{kind} {{ {body} }} AS v ORDER BY id")

    for query,state in [
        ("WITH 1 AS k RETURN EXISTS { MATCH (a:Q) WHERE a.id=k } AS v","scalar-node-lookup"),
        ("WITH 9 AS k RETURN EXISTS { MATCH (a:Q) WHERE a.id=k } AS v","scalar-node-no-match"),
        ("MATCH (a:Q) WITH a,a.id AS k WHERE COUNT { (a)-[:R]->() } > 0 RETURN k AS id ORDER BY id","where-count"),
        ("MATCH (a:Q) WITH a,a.id AS k WHERE NOT EXISTS { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) } RETURN k AS id ORDER BY id","where-not-exists"),
        ("MATCH (a:Q) RETURN a.id AS id,EXISTS { MATCH (x:Q) WITH count(*) AS n WHERE n > a.id RETURN n } AS v ORDER BY id","implicit-import-grouping"),
        ("SHOW DATABASES YIELD name RETURN name,EXISTS { MATCH (x:Q) } AS v","admin-graph-classification"),
    ]:
        add("W03-other-admission",state,query)

if os.environ.get("R73_TX_STATE") == "1":
    cases=[]
    seen=set()
    for state,update in [("create",""),("delete","MATCH (:GateTx)-[r:R]->(:GateTx) DELETE r"),
                         ("update","MATCH (:GateTx)-[r:R]->(:GateTx) SET r.w=2")]:
        pattern="(a)-[:R {w:1}]->(b)"
        for expr,form in [(f"exists({pattern})","predicate"),(f"EXISTS {{ {pattern} }}","exists"),
                          (f"COUNT {{ {pattern} }}","count"),
                          (f"COLLECT {{ MATCH {pattern} RETURN 1 AS v }}","collect"),
                          (f"[{pattern} | 1]","comprehension")]:
            query=f"MATCH (a:GateTx {{id:1}}),(b:GateTx {{id:2}}) RETURN {expr} AS v"
            add("T01-transaction-state",state+"/"+form,query+" /* "+state+" */")
            statements=["CREATE (:GateTx {id:1})-[:R {w:1}]->(:GateTx {id:2})"]
            if update:statements.append(update)
            statements.append(query)
            cases[-1]["statements"]=statements
            cases[-1]["rollback"]=True

if os.environ.get("R73_REWRITES") == "1":
    cases=[]
    seen=set()
    rewrite_patterns=[
        ("node-label-expression","(a:Q&!P)-[:R]->(b)"),
        ("relationship-type-expression","(a)-[:R&!U]->(b)"),
        ("same-pattern-map","(a)-[:R {w:a.id}]->(b)"),
        ("relationship-inline-where","(a)-[r:R WHERE r.w=1]->(b)"),
        ("relationship-quantifier","(a)-[:R]->{1,2}(b)"),
        ("abbreviated-quantifier","(a)-->+(b)"),
        ("quantified-group","((a)-[:R]->(b)){1,2}"),
        ("zero-hop","(a)-[:R*0..0]->(a)"),
    ]
    for scope,prefix in [("bound",bound),("missing-end",outer),
                         ("null","WITH null AS a,null AS b,null AS r,0 AS k,1 AS weight")]:
        for shape,pattern in rewrite_patterns:
            for expression,form in [(f"exists({pattern})","function"),
                                    (f"[{pattern} | 1]","comprehension"),
                                    (f"EXISTS {{ MATCH {pattern} }}","EXISTS"),
                                    (f"COUNT {{ MATCH {pattern} }}","COUNT"),
                                    (f"COLLECT {{ MATCH {pattern} RETURN 1 AS v }}","COLLECT")]:
                add("Y01-normalisation",scope+"/"+shape+"/"+form,f"{prefix} RETURN k AS id,{expression} AS v ORDER BY id")
            add("Y01-normalisation",scope+"/"+shape+"/where",f"{prefix} WHERE {pattern} RETURN k AS id ORDER BY id")
    for horizon,projection in horizons[:6]:
        for kind in ["EXISTS","COUNT","COLLECT"]:
            ret=" RETURN b.id AS v ORDER BY v" if kind=="COLLECT" else ""
            add("Y02-normalised-imports",horizon+"/"+kind,f"{outer} RETURN k AS id,{kind} {{ {projection} MATCH (b:Q) WHERE (a:Q&!P)-[:R]->(b){ret} }} AS v ORDER BY id")

if os.environ.get("R73_CODEPATHS") == "1":
    cases=[]
    seen=set()
    for kind in ["EXISTS","COUNT","COLLECT"]:
        for body,state in [
            ("WITH 1 AS local CALL db.labels() YIELD label WHERE label=labels(a)[0] RETURN label AS v","procedure-labels"),
            ("WITH 1 AS local CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType AS v","procedure-types"),
            ("WITH 1 AS local OPTIONAL MATCH (a)-[:R]->(b) RETURN b.id AS v","optional"),
            ("WITH 1 AS local UNWIND [a] AS restored MATCH (restored)-[:R]->(b) RETURN b.id AS v","unwind-body"),
        ]:
            expr=f"{kind} {{ {body} }}"
            if kind=="COLLECT":expr=f"size({expr})"
            add("X01-codepath-bodies",kind+"/"+state,f"{outer} RETURN k AS id,{expr} AS v ORDER BY id")
    for expr,state in [
        ("EXISTS { MATCH (n:Q) }","exists"),
        ("COUNT { MATCH (n:Q) }","count"),
        ("size(COLLECT { MATCH (n:Q) RETURN n })","collect"),
        ("size([(a:Q)-->(b) | a])","comprehension"),
        ("exists(()-->())","function-pattern"),
        ("()-->()","bare-pattern"),
        ("size([(n) | n])","single-node-comprehension"),
    ]:
        add("X02-system-admission",state,f"SHOW DATABASES YIELD name RETURN name,{expr} AS v")
        cases[-1]["database"]="system"

corpus=dict(setup=["MATCH (x) DETACH DELETE x",
    "CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:R {w:2}]->(:Q {id:3}), (:P {id:4})"],cases=cases)
with gzip.open(sys.argv[1],"wt",encoding="utf-8") as out:
    json.dump(corpus,out,separators=(",",":"))
counts={}
for case in cases: counts[case["family"]]=counts.get(case["family"],0)+1
print("R73_GENERATED",len(cases),json.dumps(counts,sort_keys=True))
