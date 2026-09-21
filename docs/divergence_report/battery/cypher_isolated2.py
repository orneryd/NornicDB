# Isolated minimal repros, batch 2; each group starts from the same clean graph.
# python cypher_isolated2.py HOST [auto|tx]
import sys
from neo4j import GraphDatabase

d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
MODE = sys.argv[2] if len(sys.argv) > 2 else 'auto'


def run(q, **p):
    if MODE == 'tx':
        return [r.data() for r in d.execute_query(q, **p).records]
    with d.session() as s:
        return [r.data() for r in s.run(q, **p)]


SETUP = """CREATE (a:I {sku:'a1', price:10.5, qty:3}), (b:I {sku:'b2', price:20.0, qty:0}), (c:I {sku:'c3', price:5.25, qty:7}),
       (o1:O {id:1}), (o2:O {id:2}),
       (o1)-[:HAS {n:2}]->(a), (o1)-[:HAS {n:1}]->(c), (o2)-[:HAS {n:5}]->(a)"""

GROUPS = {
    'M aggregate-inside-expression': [
        "MATCH (i:I) RETURN avg(i.price) AS av",
        "MATCH (i:I) RETURN round(avg(i.price) * 100) / 100 AS av",
        "MATCH (i:I) RETURN sum(i.qty) + 1 AS s1, sum(i.qty) * 2 AS s2, toInteger(avg(i.qty)) AS ai",
    ],
    'N with-aggregate-order-by': [
        "MATCH (o:O)-[h:HAS]->(i:I) WITH i, sum(h.n) AS sold ORDER BY sold DESC RETURN i.sku AS s, sold",
        "MATCH (o:O)-[h:HAS]->(i:I) WITH i, sum(h.n) AS sold RETURN i.sku AS s, sold ORDER BY sold DESC",
    ],
    'O property-of-function-result': [
        "MATCH (:O {id: 1})-[h:HAS]->(:I {sku:'a1'}) RETURN startNode(h).id AS from_id, endNode(h).sku AS to",
        "MATCH (:O {id: 1})-[h:HAS]->(:I {sku:'a1'}) WITH startNode(h) AS s, endNode(h) AS e RETURN s.id AS from_id, e.sku AS to",
    ],
    'P map-projection': [
        "MATCH (i:I {sku:'a1'}) RETURN i {.sku, .price} AS m",
        "MATCH (i:I {sku:'a1'}) RETURN i {.sku, double: i.qty * 2} AS m",
        "MATCH (i:I {sku:'a1'}) RETURN i {.*} AS m",
    ],
    'Q nested-map-access': [
        "WITH {a: 1, b: {c: 2}} AS m RETURN m.a AS a, m.b.c AS c",
        "WITH {a: 1, b: {c: 2}} AS m RETURN keys(m) AS k, m.b AS b",
        "RETURN $m.b.c AS c",
    ],
    'R slice-of-collect': [
        "MATCH (i:I) WITH i ORDER BY i.sku RETURN collect(i.sku)[0..2] AS firsttwo",
        "MATCH (i:I) WITH i ORDER BY i.sku WITH collect(i.sku) AS l RETURN l[0..2] AS firsttwo, l[0] AS first",
        "RETURN [1,2,3,4][1..3] AS sl",
    ],
    'S set-null-removes-property': [
        "MATCH (i:I {sku:'a1'}) SET i.price = null RETURN i.price AS p, keys(i) AS k",
        "MATCH (i:I {sku:'a1'}) RETURN keys(i) AS k, i.price IS NULL AS isnull",
        "MATCH (i:I) WHERE i.price IS NULL RETURN count(i) AS c",
    ],
    'T set-relationship-property-arithmetic': [
        "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = h.n + 1 RETURN i.sku AS s, h.n AS n",
        "MATCH (:O {id: 2})-[h:HAS]->(i:I) RETURN i.sku AS s, h.n AS n",
        "MATCH (:O {id: 1})-[h:HAS]->(i:I {sku:'c3'}) SET h.n = 10 RETURN h.n AS n",
        "MATCH (:O {id: 1})-[h:HAS]->(i:I) RETURN i.sku AS s, h.n AS n ORDER BY s",
    ],
    'U optional-match-collect': [
        "MATCH (o:O) OPTIONAL MATCH (o)-[h:HAS]->(i:I) WITH o, collect(i.sku) AS skus RETURN o.id AS o, size(skus) AS n, skus ORDER BY o",
        "MATCH (o:O) OPTIONAL MATCH (o)-[h:HAS]->(i:I) RETURN o.id AS o, collect(i.sku) AS skus ORDER BY o",
        "MATCH (o:O) MATCH (o)-[h:HAS]->(i:I) WITH o, collect(i.sku) AS skus RETURN o.id AS o, size(skus) AS n ORDER BY o",
    ],
    'V math-function-types': [
        "RETURN ceil(1.2) AS c, floor(1.8) AS f, round(2.5) AS r, round(2.567, 2) AS r2",
    ],
}
for g, qs in GROUPS.items():
    run('MATCH (n) DETACH DELETE n')
    run(SETUP)
    print(f'== {g} [{MODE}]')
    for q in qs:
        try:
            print('  ', q, '\n      ->', run(q, m={'b': {'c': 2}}) if '$m' in q else run(q))
        except Exception as e:
            print('  ', q, '\n      -> ERROR', str(e)[-170:])
