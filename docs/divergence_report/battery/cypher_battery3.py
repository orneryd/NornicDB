# Cypher correctness battery 3: batch upserts, relationship CRUD, transactions/rollback, ids, datetime/list properties.
# python cypher_battery3.py HOST [auto|tx]
import sys, json
from neo4j import GraphDatabase

d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
MODE = sys.argv[2] if len(sys.argv) > 2 else 'auto'


def run(q, **p):
    if MODE == 'tx':
        return [r.data() for r in d.execute_query(q, **p).records]
    with d.session() as s:
        return [r.data() for r in s.run(q, **p)]


run('MATCH (n) DETACH DELETE n')
ROWS = [{'id': i, 'name': f'n{i}', 'grp': i % 3, 'tags': [f't{i % 2}', 'all'], 'ts': f'2026-09-{10 + i:02d}T10:00:00Z'} for i in range(1, 7)]
ERR = 'ERROR'
C = [
    # batch upsert with a list of maps
    ("UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a.name = r.name, a.grp = r.grp, a.tags = r.tags, a.ts = datetime(r.ts) RETURN count(a) AS c", {'rows': ROWS}, [{'c': 6}]),
    ("UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name + 'x'} RETURN count(a) AS c", {'rows': ROWS[:2]}, [{'c': 2}]),
    ("MATCH (a:A) RETURN count(a) AS c, count(DISTINCT a.id) AS ids", {}, [{'c': 6, 'ids': 6}]),
    ("MATCH (a:A) WHERE a.id IN [1, 2, 3] RETURN a.id AS id, a.name AS n ORDER BY id", {}, [{'id': 1, 'n': 'n1x'}, {'id': 2, 'n': 'n2x'}, {'id': 3, 'n': 'n3'}]),
    ("MATCH (a:A) WHERE a.id IN $ids RETURN count(a) AS c", {'ids': [2, 4, 99]}, [{'c': 2}]),
    ("MATCH (a:A) WHERE 't1' IN a.tags RETURN count(a) AS c", {}, [{'c': 3}]),
    ("MATCH (a:A) WHERE size(a.tags) = 2 AND a.tags[0] = 't0' RETURN count(a) AS c", {}, [{'c': 3}]),
    ("MATCH (a:A) WHERE a.ts >= datetime('2026-09-14T00:00:00Z') RETURN count(a) AS c", {}, [{'c': 3}]),
    ("MATCH (a:A) RETURN a.id AS id ORDER BY a.ts DESC LIMIT 2", {}, [{'id': 6}, {'id': 5}]),
    ("MATCH (a:A) RETURN a.grp AS g, collect(a.id) AS ids ORDER BY g", {}, 'GROUPS'),
    ("MATCH (a:A) RETURN a.grp AS g, max(a.id) AS hi, min(a.name) AS lo ORDER BY g", {}, [{'g': 0, 'hi': 6, 'lo': 'n3'}, {'g': 1, 'hi': 4, 'lo': 'n1x'}, {'g': 2, 'hi': 5, 'lo': 'n2x'}]),
    # relationships in batch
    ("UNWIND $pairs AS p MATCH (x:A {id: p[0]}), (y:A {id: p[1]}) MERGE (x)-[r:NEXT]->(y) SET r.w = p[0] * 10 RETURN count(r) AS c", {'pairs': [[1, 2], [2, 3], [3, 4], [1, 2]]}, [{'c': 4}]),
    ("MATCH ()-[r:NEXT]->() RETURN count(r) AS c, sum(r.w) AS s", {}, [{'c': 3, 's': 60}]),
    ("MATCH (x:A {id: 1})-[r:NEXT]->(y) RETURN y.id AS y, r.w AS w", {}, [{'y': 2, 'w': 10}]),
    ("MATCH (x:A)-[:NEXT]->(y:A) RETURN x.id AS x, y.id AS y ORDER BY x", {}, [{'x': 1, 'y': 2}, {'x': 2, 'y': 3}, {'x': 3, 'y': 4}]),
    ("MATCH (x:A {id: 1})-[:NEXT*]->(y:A) RETURN max(y.id) AS far, count(y) AS n", {}, [{'far': 4, 'n': 3}]),
    ("MATCH (x:A)<-[:NEXT]-(y:A) WHERE x.id = 3 RETURN y.id AS y", {}, [{'y': 2}]),
    ("MATCH (x:A)-[:NEXT]-(y:A) WHERE x.id = 2 RETURN y.id AS y ORDER BY y", {}, [{'y': 1}, {'y': 3}]),
    ("MATCH (x:A) RETURN x.id AS id, size([(x)-[:NEXT]->(y) | y.id]) AS outdeg ORDER BY id LIMIT 3", {}, [{'id': 1, 'outdeg': 1}, {'id': 2, 'outdeg': 1}, {'id': 3, 'outdeg': 1}]),
    ("MATCH (x:A) WHERE x.id <= 4 OPTIONAL MATCH (x)-[r:NEXT]->() RETURN x.id AS id, count(r) AS outdeg ORDER BY id", {}, [{'id': 1, 'outdeg': 1}, {'id': 2, 'outdeg': 1}, {'id': 3, 'outdeg': 1}, {'id': 4, 'outdeg': 0}]),
    ("MATCH (x:A {id: 2})-[r:NEXT]->(y) DELETE r RETURN count(*) AS c", {}, [{'c': 1}]),
    ("MATCH (x:A {id: 1})-[:NEXT*]->(y:A) RETURN collect(y.id) AS ys", {}, [{'ys': [2]}]),
    # conditional update / counters on nodes
    ("MATCH (a:A) WHERE a.grp = 0 SET a.hits = coalesce(a.hits, 0) + 1 RETURN count(a) AS c", {}, [{'c': 2}]),
    ("MATCH (a:A) WHERE a.grp = 0 SET a.hits = coalesce(a.hits, 0) + 1 RETURN sum(a.hits) AS s", {}, [{'s': 4}]),
    ("MATCH (a:A) SET a.flag = CASE WHEN a.id % 2 = 0 THEN 'even' ELSE 'odd' END RETURN count(a) AS c", {}, [{'c': 6}]),
    ("MATCH (a:A) RETURN a.flag AS f, count(*) AS c ORDER BY f", {}, [{'f': 'even', 'c': 3}, {'f': 'odd', 'c': 3}]),
    ("MATCH (a:A {id: 6}) SET a:Extra:Hot REMOVE a:Hot RETURN labels(a) AS l", {}, 'LABELS'),
    ("MATCH (a:A:Extra) RETURN count(a) AS c", {}, [{'c': 1}]),
    # ids
    ("MATCH (a:A {id: 1}) WITH elementId(a) AS eid MATCH (b) WHERE elementId(b) = eid RETURN b.id AS id", {}, [{'id': 1}]),
    ("MATCH (a:A {id: 1}) RETURN id(a) IS NOT NULL AS has_id, elementId(a) IS NOT NULL AS has_eid", {}, [{'has_id': True, 'has_eid': True}]),
    ("RETURN size(randomUUID()) AS n, timestamp() > 0 AS t", {}, [{'n': 36, 't': True}]),
    # delete with counts
    ("MATCH (a:A) WHERE a.id > 4 DETACH DELETE a RETURN count(*) AS c", {}, [{'c': 2}]),
    ("MATCH (a:A) RETURN count(a) AS c", {}, [{'c': 4}]),
    ("MATCH (a:A {id: 1}) DELETE a", {}, ERR),   # still has a relationship -> must fail
    ("MATCH (a:A {id: 1}) RETURN count(a) AS c", {}, [{'c': 1}]),
]


def norm(rows):
    return sorted(json.dumps(r, sort_keys=True, ensure_ascii=False, default=str) for r in rows)


fails = 0
for q, p, exp in C:
    try:
        got = run(q, **p)
    except Exception as e:
        got = 'ERROR ' + str(e)[-150:]
    if exp == ERR:
        ok = isinstance(got, str)
    elif isinstance(got, str):
        ok = False
    elif exp == 'GROUPS':
        ok = [(r['g'], sorted(r['ids'])) for r in got] == [(0, [3, 6]), (1, [1, 4]), (2, [2, 5])]
    elif exp == 'LABELS':
        ok = len(got) == 1 and sorted(got[0]['l']) == ['A', 'Extra']
    elif 'ORDER BY' in q.split('RETURN')[-1]:
        ok = [json.dumps(r, sort_keys=True, default=str) for r in got] == [json.dumps(r, sort_keys=True, default=str) for r in exp]
    else:
        ok = norm(got) == norm(exp)
    if not ok:
        fails += 1
        print('FAIL:', q, p if p and len(str(p)) < 80 else '', '\n   expected:', exp, '\n   got:     ', got)
print(f'[{MODE}] {len(C)} cases, {fails} failed')

# explicit transaction semantics (independent of MODE)
with d.session() as s:
    tx = s.begin_transaction(); tx.run("CREATE (:R {k: 1})").consume(); seen_inside = tx.run("MATCH (r:R) RETURN count(r) AS c").single()['c']; tx.rollback()
    after = s.run("MATCH (r:R) RETURN count(r) AS c").single()['c']
    print('rollback: visible inside tx =', seen_inside, '| after rollback =', after, '(expected 1 and 0)')
    tx = s.begin_transaction(); tx.run("CREATE (:R {k: 2})").consume()
    with d.session() as s2: other = s2.run("MATCH (r:R) RETURN count(r) AS c").single()['c']
    tx.commit(); final = s.run("MATCH (r:R) RETURN count(r) AS c").single()['c']
    print('isolation: other session sees uncommitted =', other, '| after commit =', final, '(expected 0 and 1)')
