# Cypher correctness battery: each case = (query, params, expected rows).
# Row order is compared only when the query has ORDER BY.
# python cypher_battery.py HOST [auto|tx]
import sys, json
from neo4j import GraphDatabase

d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
MODE = sys.argv[2] if len(sys.argv) > 2 else 'auto'


def run(q, **p):
    if MODE == 'tx':
        return [r.data() for r in d.execute_query(q, **p).records]
    with d.session() as s:
        return [r.data() for r in s.run(q, **p)]


run('MATCH (n) WHERE n:P OR n:C OR n:T DETACH DELETE n')
run("""CREATE (a:P {name:'Ann', age:30, tags:['x','y']}), (b:P {name:'Bob', age:25, tags:['y']}), (c:P {name:'Cid', age:35}), (d:P {name:'Dee'}),
       (x:C {name:'Acme', city:'Riga'}), (y:C {name:'Bolt', city:'Oslo'}),
       (a)-[:WORKS_AT {since:2020}]->(x), (b)-[:WORKS_AT {since:2022}]->(x), (c)-[:WORKS_AT {since:2019}]->(y),
       (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d)""")

C = [
    ("MATCH (p:P) RETURN p.name AS n ORDER BY n", {}, [{'n': 'Ann'}, {'n': 'Bob'}, {'n': 'Cid'}, {'n': 'Dee'}]),
    ("MATCH (p:P) RETURN p.name AS n ORDER BY p.age DESC, n LIMIT 2", {}, [{'n': 'Dee'}, {'n': 'Cid'}]),
    ("MATCH (p:P) WHERE p.age >= $a RETURN p.name AS n ORDER BY n", {'a': 30}, [{'n': 'Ann'}, {'n': 'Cid'}]),
    ("MATCH (p:P) WHERE p.age IS NULL RETURN p.name AS n", {}, [{'n': 'Dee'}]),
    ("MATCH (p:P) WHERE p.name STARTS WITH 'A' OR p.name ENDS WITH 'b' RETURN p.name AS n ORDER BY n", {}, [{'n': 'Ann'}, {'n': 'Bob'}]),
    ("MATCH (p:P) WHERE p.name IN $names RETURN count(p) AS c", {'names': ['Ann', 'Dee', 'Zed']}, [{'c': 2}]),
    ("MATCH (p:P) WHERE 'y' IN p.tags RETURN p.name AS n ORDER BY n", {}, [{'n': 'Ann'}, {'n': 'Bob'}]),
    ("MATCH (p:P) RETURN count(p) AS c, avg(p.age) AS a, min(p.age) AS lo, max(p.age) AS hi, sum(p.age) AS s", {}, [{'c': 4, 'a': 30.0, 'lo': 25, 'hi': 35, 's': 90}]),
    ("MATCH (p:P)-[:WORKS_AT]->(c:C) RETURN c.name AS c, count(p) AS n ORDER BY c", {}, [{'c': 'Acme', 'n': 2}, {'c': 'Bolt', 'n': 1}]),
    ("MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, p ORDER BY p.name RETURN c.name AS c, collect(p.name) AS ps ORDER BY c", {}, [{'c': 'Acme', 'ps': ['Ann', 'Bob']}, {'c': 'Bolt', 'ps': ['Cid']}]),
    ("MATCH (p:P)-[w:WORKS_AT]->(c:C) WHERE w.since < 2021 RETURN p.name AS n ORDER BY n", {}, [{'n': 'Ann'}, {'n': 'Cid'}]),
    ("MATCH (p:P) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:C) RETURN p.name AS n, c.name AS c ORDER BY n", {}, [{'n': 'Ann', 'c': 'Acme'}, {'n': 'Bob', 'c': 'Acme'}, {'n': 'Cid', 'c': 'Bolt'}, {'n': 'Dee', 'c': None}]),
    ("MATCH (p:P) WHERE NOT (p)-[:WORKS_AT]->() RETURN p.name AS n", {}, [{'n': 'Dee'}]),
    ("MATCH (p:P) WHERE EXISTS { (p)-[:KNOWS]->(:P {name:'Bob'}) } RETURN p.name AS n", {}, [{'n': 'Ann'}]),
    ("MATCH (a:P {name:'Ann'})-[:KNOWS*1..3]->(x:P) RETURN x.name AS n ORDER BY n", {}, [{'n': 'Bob'}, {'n': 'Cid'}, {'n': 'Dee'}]),
    ("MATCH (a:P {name:'Ann'})-[:KNOWS*2]->(x:P) RETURN x.name AS n", {}, [{'n': 'Cid'}]),
    ("MATCH p = shortestPath((a:P {name:'Ann'})-[:KNOWS*..5]->(d:P {name:'Dee'})) RETURN length(p) AS l", {}, [{'l': 3}]),
    ("MATCH (a:P)-[:KNOWS]->(b:P)-[:KNOWS]->(c:P) RETURN a.name AS a, c.name AS c ORDER BY a", {}, [{'a': 'Ann', 'c': 'Cid'}, {'a': 'Bob', 'c': 'Dee'}]),
    ("MATCH (p:P) WHERE p.age IS NOT NULL WITH p ORDER BY p.age SKIP 1 LIMIT 2 RETURN p.name AS n ORDER BY n", {}, [{'n': 'Ann'}, {'n': 'Cid'}]),
    ("MATCH (p:P) WITH p.age AS age WHERE age > 26 RETURN count(*) AS c", {}, [{'c': 2}]),
    ("MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, count(p) AS n WHERE n > 1 RETURN c.name AS c", {}, [{'c': 'Acme'}]),
    ("MATCH (p:P) RETURN p.name AS n, CASE WHEN p.age >= 30 THEN 'senior' WHEN p.age IS NULL THEN 'unknown' ELSE 'junior' END AS g ORDER BY n", {}, [{'n': 'Ann', 'g': 'senior'}, {'n': 'Bob', 'g': 'junior'}, {'n': 'Cid', 'g': 'senior'}, {'n': 'Dee', 'g': 'unknown'}]),
    ("MATCH (p:P) RETURN p.name AS n, coalesce(p.age, -1) AS a ORDER BY n SKIP 3 LIMIT 1", {}, [{'n': 'Dee', 'a': -1}]),
    ("UNWIND [3,1,2] AS x RETURN x ORDER BY x DESC", {}, [{'x': 3}, {'x': 2}, {'x': 1}]),
    ("UNWIND $rows AS r RETURN r.a + r.b AS s ORDER BY s", {'rows': [{'a': 1, 'b': 2}, {'a': 5, 'b': 5}]}, [{'s': 3}, {'s': 10}]),
    ("RETURN [x IN range(1,5) WHERE x % 2 = 1 | x * 10] AS l", {}, [{'l': [10, 30, 50]}]),
    ("RETURN size('привет') AS n, toUpper('abc') AS u, substring('hello', 1, 3) AS s, replace('a-b','-','+') AS r, split('a,b',',') AS sp", {}, [{'n': 6, 'u': 'ABC', 's': 'ell', 'r': 'a+b', 'sp': ['a', 'b']}]),
    ("RETURN toInteger('42') AS i, toFloat('1.5') AS f, toString(7) AS s, 7 / 2 AS d, 7 % 3 AS m, 2 ^ 3 AS p", {}, [{'i': 42, 'f': 1.5, 's': '7', 'd': 3, 'm': 1, 'p': 8.0}]),
    ("MATCH (p:P) RETURN DISTINCT size(coalesce(p.tags, [])) AS t ORDER BY t", {}, [{'t': 0}, {'t': 1}, {'t': 2}]),
    ("MATCH (:P {name:'Ann'})-[r]->() RETURN type(r) AS t ORDER BY t", {}, [{'t': 'KNOWS'}, {'t': 'WORKS_AT'}]),
    ("MERGE (t:T {k: 1}) ON CREATE SET t.created = true ON MATCH SET t.matched = true RETURN t.created AS c, t.matched AS m", {}, [{'c': True, 'm': None}]),
    ("MERGE (t:T {k: 1}) ON CREATE SET t.created2 = true ON MATCH SET t.matched = true RETURN t.created AS c, t.matched AS m, t.created2 AS c2", {}, [{'c': True, 'm': True, 'c2': None}]),
    ("UNWIND [1,2,2,3] AS k MERGE (t:T {k: k}) RETURN count(*) AS c", {}, [{'c': 4}]),
    ("MATCH (t:T) RETURN count(t) AS c", {}, [{'c': 3}]),
    ("MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 RETURN r.w AS w", {}, [{'w': 1}]),
    ("MATCH (:P {name:'Ann'})-[r:KNOWS]->() RETURN count(r) AS c", {}, [{'c': 2}]),
    ("MATCH (p:P {name:'Dee'}) SET p += {age: 40, city:'Riga'} RETURN p.age AS a, p.city AS c", {}, [{'a': 40, 'c': 'Riga'}]),
    ("MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.city AS c, labels(p) AS l", {}, [{'c': None, 'l': ['P', 'VIP']}]),
    ("MATCH (p:VIP) RETURN count(p) AS c", {}, [{'c': 1}]),
    ("MATCH (p:P) WHERE p.name =~ '(?i)^a.*' RETURN p.name AS n", {}, [{'n': 'Ann'}]),
    ("MATCH (p:P) RETURN p.name AS n ORDER BY toLower(p.name) DESC LIMIT 1", {}, [{'n': 'Dee'}]),
    ("MATCH (p:P) WITH collect(p.name) AS names RETURN size(names) AS n, reduce(s = 0, x IN [1,2,3] | s + x) AS r", {}, [{'n': 4, 'r': 6}]),
    ("MATCH (c:C) CALL { WITH c MATCH (p:P)-[:WORKS_AT]->(c) RETURN count(p) AS n } RETURN c.name AS c, n ORDER BY c", {}, [{'c': 'Acme', 'n': 2}, {'c': 'Bolt', 'n': 1}]),
    ("MATCH (p:P)-[:WORKS_AT]->(c:C) RETURN c.city AS city, p.name AS n ORDER BY city, n", {}, [{'city': 'Oslo', 'n': 'Cid'}, {'city': 'Riga', 'n': 'Ann'}, {'city': 'Riga', 'n': 'Bob'}]),
    ("MATCH (t:T) DETACH DELETE t RETURN count(*) AS c", {}, [{'c': 3}]),
    ("RETURN date('2026-09-19').year AS y, duration({days: 2}).days AS dd, datetime('2026-09-19T10:00:00Z') < datetime('2026-09-20T10:00:00Z') AS lt", {}, [{'y': 2026, 'dd': 2, 'lt': True}]),
]


def norm(rows):
    return sorted(json.dumps(r, sort_keys=True, ensure_ascii=False, default=str) for r in rows)


fails = 0
for q, p, exp in C:
    try:
        got = run(q, **p)
    except Exception as e:
        got = 'ERROR ' + str(e)[-150:]
    if isinstance(got, str):
        ok = False
    elif 'ORDER BY' in q.split('RETURN')[-1]:
        ok = [json.dumps(r, sort_keys=True, default=str) for r in got] == [json.dumps(r, sort_keys=True, default=str) for r in exp]
    else:
        ok = norm(got) == norm(exp)
    if not ok:
        fails += 1
        print('FAIL:', q, '\n   expected:', exp, '\n   got:     ', got)
print(f'[{MODE}] {len(C)} cases, {fails} failed')
