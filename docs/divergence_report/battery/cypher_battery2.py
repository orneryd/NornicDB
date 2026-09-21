# Cypher correctness battery 2: writes, constraints, functions, grouping, null handling.
# python cypher_battery2.py HOST [auto|tx]
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
for c in ('DROP CONSTRAINT u_sku IF EXISTS',):
    try:
        run(c)
    except Exception:
        pass
run("""CREATE (a:I {sku:'a1', name:'Alpha', price:10.5, qty:3, cat:'x'}), (b:I {sku:'b2', name:'Beta', price:20.0, qty:0, cat:'x'}),
       (c:I {sku:'c3', name:'Gamma', price:5.25, qty:7, cat:'y'}), (e:I {sku:'d4', name:'delta', qty:1, cat:'y'}),
       (o1:O {id:1}), (o2:O {id:2}),
       (o1)-[:HAS {n:2}]->(a), (o1)-[:HAS {n:1}]->(c), (o2)-[:HAS {n:5}]->(a)""")

ERR = 'ERROR'
C = [
    # --- reads: grouping / distinct / null handling
    ("MATCH (i:I) RETURN i.cat AS cat, count(*) AS n, sum(i.qty) AS q ORDER BY cat", {}, [{'cat': 'x', 'n': 2, 'q': 3}, {'cat': 'y', 'n': 2, 'q': 8}]),
    ("MATCH (i:I) RETURN count(DISTINCT i.cat) AS c, count(i.price) AS with_price", {}, [{'c': 2, 'with_price': 3}]),
    ("MATCH (i:I) RETURN DISTINCT i.cat AS cat ORDER BY cat", {}, [{'cat': 'x'}, {'cat': 'y'}]),
    ("MATCH (i:I) WHERE i.price IS NOT NULL RETURN min(i.price) AS lo, max(i.price) AS hi, round(avg(i.price) * 100) / 100 AS av", {}, [{'lo': 5.25, 'hi': 20.0, 'av': 11.92}]),
    ("MATCH (i:I) WHERE i.price > 6 AND (i.qty > 0 OR i.cat = 'y') RETURN i.sku AS s ORDER BY s", {}, [{'s': 'a1'}]),
    ("MATCH (i:I) WHERE NOT i.cat = 'x' RETURN i.sku AS s ORDER BY s", {}, [{'s': 'c3'}, {'s': 'd4'}]),
    ("MATCH (i:I) WHERE i.price < 100 RETURN count(*) AS c", {}, [{'c': 3}]),
    ("MATCH (i:I) WHERE i.name CONTAINS 'a' RETURN i.sku AS s ORDER BY s", {}, [{'s': 'a1'}, {'s': 'b2'}, {'s': 'c3'}, {'s': 'd4'}]),
    ("MATCH (i:I) WHERE toLower(i.name) STARTS WITH 'd' RETURN i.sku AS s", {}, [{'s': 'd4'}]),
    ("MATCH (i:I) RETURN i.sku AS s ORDER BY i.price ASC", {}, [{'s': 'c3'}, {'s': 'a1'}, {'s': 'b2'}, {'s': 'd4'}]),
    ("MATCH (i:I) RETURN i.sku AS s ORDER BY i.qty DESC SKIP $sk LIMIT $li", {'sk': 1, 'li': 2}, [{'s': 'a1'}, {'s': 'd4'}]),
    ("MATCH (o:O)-[h:HAS]->(i:I) RETURN o.id AS o, sum(h.n * i.price) AS total ORDER BY o", {}, [{'o': 1, 'total': 26.25}, {'o': 2, 'total': 52.5}]),
    ("MATCH (o:O)-[h:HAS]->(i:I) WITH i, sum(h.n) AS sold ORDER BY sold DESC RETURN i.sku AS s, sold", {}, [{'s': 'a1', 'sold': 7}, {'s': 'c3', 'sold': 1}]),
    ("MATCH (i:I) WHERE NOT EXISTS { (:O)-[:HAS]->(i) } RETURN i.sku AS s ORDER BY s", {}, [{'s': 'b2'}, {'s': 'd4'}]),
    ("MATCH (o:O {id: 1})-[h:HAS]->(i) RETURN type(h) AS t, startNode(h).id AS from_id, endNode(h).sku AS to ORDER BY to", {}, [{'t': 'HAS', 'from_id': 1, 'to': 'a1'}, {'t': 'HAS', 'from_id': 1, 'to': 'c3'}]),
    ("MATCH (i:I {sku:'a1'}) RETURN properties(i) AS p", {}, [{'p': {'sku': 'a1', 'name': 'Alpha', 'price': 10.5, 'qty': 3, 'cat': 'x'}}]),
    ("MATCH (i:I {sku:'a1'}) RETURN i {.sku, .price, double: i.qty * 2} AS m", {}, [{'m': {'sku': 'a1', 'price': 10.5, 'double': 6}}]),
    ("MATCH (n) WHERE n:O RETURN count(n) AS c", {}, [{'c': 2}]),
    ("MATCH (i:I) RETURN CASE i.cat WHEN 'x' THEN 1 WHEN 'y' THEN 2 ELSE 0 END AS k, count(*) AS c ORDER BY k", {}, [{'k': 1, 'c': 2}, {'k': 2, 'c': 2}]),
    # --- functions
    ("RETURN head([1,2,3]) AS h, last([1,2,3]) AS l, tail([1,2,3]) AS t, size([1,2,3]) AS s, [1,2,3][1] AS idx, [1,2,3,4][1..3] AS sl", {}, [{'h': 1, 'l': 3, 't': [2, 3], 's': 3, 'idx': 2, 'sl': [2, 3]}]),
    ("RETURN abs(-3) AS a, ceil(1.2) AS c, floor(1.8) AS f, round(2.5) AS r, sign(-2) AS sg, sqrt(16) AS q", {}, [{'a': 3, 'c': 2.0, 'f': 1.0, 'r': 3.0, 'sg': -1, 'q': 4.0}]),
    ("RETURN trim('  a ') AS t, left('hello', 2) AS l, right('hello', 2) AS r, reverse('abc') AS rv, toLower('AbC') AS lo", {}, [{'t': 'a', 'l': 'he', 'r': 'lo', 'rv': 'cba', 'lo': 'abc'}]),
    ("RETURN 'a' + 'b' AS s, 1 + 2.5 AS n, [1] + [2, 3] AS l, 'x' IN ['x', 'y'] AS isin, null IS NULL AS nn, coalesce(null, 'd') AS co", {}, [{'s': 'ab', 'n': 3.5, 'l': [1, 2, 3], 'isin': True, 'nn': True, 'co': 'd'}]),
    ("RETURN toBoolean('true') AS b, toInteger(3.9) AS i, toString(1.5) AS s, toFloat(2) AS f", {}, [{'b': True, 'i': 3, 's': '1.5', 'f': 2.0}]),
    ("RETURN any(x IN [1,2,3] WHERE x > 2) AS a, all(x IN [1,2,3] WHERE x > 0) AS al, none(x IN [1,2,3] WHERE x > 5) AS no, single(x IN [1,2,3] WHERE x = 2) AS si", {}, [{'a': True, 'al': True, 'no': True, 'si': True}]),
    ("WITH {a: 1, b: {c: 2}} AS m RETURN m.a AS a, m.b.c AS c, keys(m) AS k", {}, [{'a': 1, 'c': 2, 'k': ['a', 'b']}]),
    ("RETURN $m.x AS x, $l[0] AS first, size($l) AS n", {'m': {'x': 7}, 'l': [4, 5]}, [{'x': 7, 'first': 4, 'n': 2}]),
    ("UNWIND range(1, 3) AS i WITH i, i * i AS sq WHERE sq > 1 RETURN collect(sq) AS l", {}, [{'l': [4, 9]}]),
    ("MATCH (i:I) WITH i ORDER BY i.sku RETURN collect(i.sku)[0..2] AS firsttwo", {}, [{'firsttwo': ['a1', 'b2']}]),
    # --- writes
    ("UNWIND $rows AS r MERGE (i:I {sku: r.sku}) SET i.qty = r.qty, i.touched = true RETURN count(*) AS c", {'rows': [{'sku': 'a1', 'qty': 30}, {'sku': 'z9', 'qty': 9}]}, [{'c': 2}]),
    ("MATCH (i:I) WHERE i.touched RETURN i.sku AS s, i.qty AS q, i.name AS n ORDER BY s", {}, [{'s': 'a1', 'q': 30, 'n': 'Alpha'}, {'s': 'z9', 'q': 9, 'n': None}]),
    ("MATCH (i:I {sku:'z9'}) SET i = {sku:'z9', name:'Zeta'} RETURN properties(i) AS p", {}, [{'p': {'sku': 'z9', 'name': 'Zeta'}}]),
    ("MATCH (i:I {sku:'z9'}) SET i.qty = coalesce(i.qty, 0) + 1, i.price = $p RETURN i.qty AS q, i.price AS p", {'p': 1.5}, [{'q': 1, 'p': 1.5}]),
    ("MATCH (i:I {sku:'z9'}) SET i.price = null RETURN i.price AS p, keys(i) AS k", {}, [{'p': None, 'k': ['name', 'qty', 'sku']}]),
    ("MATCH (o:O {id: 2}), (i:I {sku:'z9'}) CREATE (o)-[h:HAS {n: 4}]->(i) RETURN h.n AS n", {}, [{'n': 4}]),
    ("MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = h.n + 1 RETURN i.sku AS s, h.n AS n ORDER BY s", {}, [{'s': 'a1', 'n': 6}, {'s': 'z9', 'n': 5}]),
    ("MATCH (:O {id: 2})-[h:HAS]->(:I {sku:'z9'}) DELETE h RETURN count(*) AS c", {}, [{'c': 1}]),
    ("MATCH (:O {id: 2})-[h:HAS]->() RETURN count(h) AS c", {}, [{'c': 1}]),
    ("MATCH (i:I {sku:'z9'}) DELETE i RETURN count(*) AS c", {}, [{'c': 1}]),
    ("MATCH (i:I) RETURN count(i) AS c", {}, [{'c': 4}]),
    ("FOREACH (k IN [1, 2, 3] | CREATE (:F {k: k}))", {}, []),
    ("MATCH (f:F) RETURN count(f) AS c, sum(f.k) AS s", {}, [{'c': 3, 's': 6}]),
    ("MATCH (f:F) WHERE f.k >= 2 DETACH DELETE f", {}, []),
    ("MATCH (f:F) RETURN collect(f.k) AS ks", {}, [{'ks': [1]}]),
    ("MATCH (o:O {id: 1}) OPTIONAL MATCH (o)-[:HAS]->(i:I) WHERE i.price > 100 RETURN o.id AS o, count(i) AS c", {}, [{'o': 1, 'c': 0}]),
    ("MATCH (o:O) OPTIONAL MATCH (o)-[h:HAS]->(i:I) WITH o, collect(i.sku) AS skus RETURN o.id AS o, size(skus) AS n ORDER BY o", {}, [{'o': 1, 'n': 2}, {'o': 2, 'n': 1}]),
    # --- constraints
    ("CREATE CONSTRAINT u_sku IF NOT EXISTS FOR (i:I) REQUIRE i.sku IS UNIQUE", {}, []),
    ("CREATE (:I {sku:'a1', name:'dup'})", {}, ERR),
    ("MATCH (i:I {sku:'a1'}) RETURN count(i) AS c", {}, [{'c': 1}]),
    ("MERGE (i:I {sku:'a1'}) ON MATCH SET i.seen = true RETURN i.seen AS s, i.name AS n", {}, [{'s': True, 'n': 'Alpha'}]),
]


def norm(rows):
    return sorted(json.dumps(r, sort_keys=True, ensure_ascii=False, default=str) for r in rows)


def canon(v):
    if isinstance(v, dict):
        return {k: canon(x) for k, x in v.items()}
    if isinstance(v, list):
        return [canon(x) for x in v]
    return v


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
    elif 'ORDER BY' in q.split('RETURN')[-1]:
        ok = [json.dumps(r, sort_keys=True, default=str) for r in got] == [json.dumps(r, sort_keys=True, default=str) for r in exp]
    else:
        ok = norm(got) == norm(exp)
    if not ok and 'keys(' in q and not isinstance(got, str):
        # keys() order is not defined: compare with sorted key lists
        srt = lambda rows: [{k: (sorted(v) if isinstance(v, list) and k in ('k',) else v) for k, v in r.items()} for r in rows]
        ok = norm(srt(got)) == norm(srt(exp))
    if not ok:
        fails += 1
        print('FAIL:', q, p if p else '', '\n   expected:', exp, '\n   got:     ', got)
print(f'[{MODE}] {len(C)} cases, {fails} failed')
