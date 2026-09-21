# Isolated minimal repros, batch 3; each group starts from the same clean graph.
# python cypher_isolated3.py HOST [auto|tx]
import sys
from neo4j import GraphDatabase

d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
MODE = sys.argv[2] if len(sys.argv) > 2 else 'auto'


def run(q, **p):
    if MODE == 'tx':
        return [r.data() for r in d.execute_query(q, **p).records]
    with d.session() as s:
        return [r.data() for r in s.run(q, **p)]


SETUP = ["CREATE (:A {id:1, name:'n1', tags:['t1','all']}), (:A {id:2, name:'n2', tags:['t0','all']}), (:A {id:3, name:'n3', tags:['t1','all']}), (:A {id:4, name:'n4', tags:['t0','all']})",
         "MATCH (x:A {id:1}), (y:A {id:2}) CREATE (x)-[:NEXT {w:10}]->(y)",
         "MATCH (x:A {id:2}), (y:A {id:3}) CREATE (x)-[:NEXT {w:20}]->(y)"]
P = {'rows': [{'id': 1, 'name': 'n1'}, {'id': 2, 'name': 'n2'}], 'pairs': [[3, 4], [1, 2]], 'pm': [{'a': 3, 'b': 4}]}

GROUPS = {
    'W set-plus-equals-map-expression': [
        "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name + 'x'} RETURN a.id AS id, a.name AS n ORDER BY id",
        "MATCH (a:A {id: 3}) SET a += {name: a.name + 'y', id2: a.id * 2} RETURN a.name AS n, a.id2 AS id2",
        "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a.name = r.name + 'z' RETURN a.id AS id, a.name AS n ORDER BY id",
    ],
    'X list-property-subscript-in-where': [
        "MATCH (a:A) WHERE a.tags[0] = 't0' RETURN a.id AS id ORDER BY id",
        "MATCH (a:A) RETURN a.id AS id, a.tags[0] AS first ORDER BY id LIMIT 2",
        "MATCH (a:A) WHERE size(a.tags) = 2 RETURN count(a) AS c",
        "MATCH (a:A) WHERE head(a.tags) = 't0' RETURN count(a) AS c",
    ],
    'Y datetime-property-compare': [
        "MATCH (a:A) SET a.ts = datetime('2026-09-1' + toString(a.id) + 'T10:00:00Z') RETURN count(a) AS c",
        "MATCH (a:A) RETURN a.id AS id, a.ts AS ts ORDER BY id LIMIT 2",
        "MATCH (a:A) WHERE a.ts >= datetime('2026-09-13T00:00:00Z') RETURN count(a) AS c",
        "MATCH (a:A) WHERE a.ts >= '2026-09-13T00:00:00Z' RETURN count(a) AS c",
        "MATCH (a:A) RETURN a.id AS id ORDER BY a.ts DESC LIMIT 1",
    ],
    'Z min-max-on-strings': [
        "MATCH (a:A) RETURN min(a.name) AS lo, max(a.name) AS hi, min(a.id) AS lo_id",
    ],
    'AA unwind-subscript-match-merge-rel': [
        "UNWIND $pairs AS p MATCH (x:A {id: p[0]}), (y:A {id: p[1]}) MERGE (x)-[r:NEXT]->(y) SET r.w = p[0] * 10 RETURN count(r) AS c",
        "MATCH ()-[r:NEXT]->() RETURN count(r) AS c",
        "UNWIND $pm AS p MATCH (x:A {id: p.a}), (y:A {id: p.b}) MERGE (x)-[r:NEXT]->(y) RETURN count(r) AS c",
        "MATCH ()-[r:NEXT]->() RETURN count(r) AS c",
    ],
    'AB counter-increment-twice': [
        "MATCH (a:A) WHERE a.id <= 2 SET a.hits = coalesce(a.hits, 0) + 1 RETURN sum(a.hits) AS s",
        "MATCH (a:A) WHERE a.id <= 2 SET a.hits = coalesce(a.hits, 0) + 1 RETURN sum(a.hits) AS s",
        "MATCH (a:A) WHERE a.id <= 2 RETURN a.id AS id, a.hits AS h ORDER BY id",
    ],
    'AC set-multiple-labels': [
        "MATCH (a:A {id: 4}) SET a:Extra:Hot RETURN labels(a) AS l",
        "MATCH (a:A {id: 4}) SET a:Extra, a:Hot RETURN labels(a) AS l",
        "MATCH (a:A:Extra) RETURN count(a) AS c",
    ],
    'AD pattern-comprehension': [
        "MATCH (x:A) RETURN x.id AS id, size([(x)-[:NEXT]->(y) | y.id]) AS outdeg ORDER BY id",
        "MATCH (x:A {id: 1}) RETURN [(x)-[:NEXT]->(y) | y.name] AS names",
    ],
    'AE optional-match-count': [
        "MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->() RETURN x.id AS id, count(r) AS outdeg ORDER BY id",
        "MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->(y) RETURN x.id AS id, y.id AS y ORDER BY id",
    ],
    'AF variable-length-and-direction': [
        "MATCH (x:A {id: 1})-[:NEXT*]->(y:A) RETURN collect(y.id) AS ys",
        "MATCH (x:A)<-[:NEXT]-(y:A) WHERE x.id = 3 RETURN y.id AS y",
        "MATCH (x:A)-[:NEXT]-(y:A) WHERE x.id = 2 RETURN y.id AS y ORDER BY y",
        "MATCH ()-[r:NEXT]->() RETURN count(r) AS c, sum(r.w) AS s",
    ],
    'AG delete-node-with-relationships': [
        "MATCH (a:A {id: 1}) DELETE a",
        "MATCH (a:A {id: 1}) RETURN count(a) AS c",
        "MATCH ()-[r:NEXT]->() RETURN count(r) AS c",
    ],
}
for g, qs in GROUPS.items():
    with d.session() as s:
        s.run('MATCH (n) DETACH DELETE n').consume()
    for st in SETUP:
        d.execute_query(st)          # created inside explicit transactions (visible everywhere, see #461)
    print(f'== {g} [{MODE}]')
    for q in qs:
        try:
            print('  ', q, '\n      ->', run(q, **{k: v for k, v in P.items() if '$' + k in q}))
        except Exception as e:
            print('  ', q, '\n      -> ERROR', str(e)[-170:])
