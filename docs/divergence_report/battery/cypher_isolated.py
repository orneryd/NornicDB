# Isolated minimal repros; each group starts from the same clean 6-node graph.
# python cypher_isolated.py HOST [auto|tx]
import sys
from neo4j import GraphDatabase

d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
MODE = sys.argv[2] if len(sys.argv) > 2 else 'auto'


def run(q, **p):
    if MODE == 'tx':
        return [r.data() for r in d.execute_query(q, **p).records]
    with d.session() as s:
        return [r.data() for r in s.run(q, **p)]


SETUP = """CREATE (a:P {name:'Ann', age:30}), (b:P {name:'Bob', age:25}), (c:P {name:'Cid', age:35}), (d:P {name:'Dee', city:'Riga'}),
       (x:C {name:'Acme'}), (y:C {name:'Bolt'}),
       (a)-[:WORKS_AT]->(x), (b)-[:WORKS_AT]->(x), (c)-[:WORKS_AT]->(y)"""

GROUPS = {
    'A order-by-unreturned-property': [
        "MATCH (p:P) WHERE p.age IS NOT NULL RETURN p.name AS n ORDER BY p.age DESC",
        "MATCH (p:P) WHERE p.age IS NOT NULL RETURN p.name AS n ORDER BY p.age DESC LIMIT 2",
        "MATCH (p:P) WHERE p.age IS NOT NULL RETURN p.name AS n, p.age AS age ORDER BY age DESC LIMIT 2",
        "MATCH (p:P) RETURN p.name AS n ORDER BY p.age DESC, n LIMIT 2",
    ],
    'B alias-equals-variable-after-with': [
        "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, count(p) AS n RETURN c.name AS c, n ORDER BY n DESC",
        "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, count(p) AS n RETURN c.name AS company, n ORDER BY n DESC",
        "MATCH (c:C) RETURN c.name AS c ORDER BY c",
    ],
    'C collect-after-with-order-by': [
        "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, p ORDER BY p.name RETURN c.name AS company, collect(p.name) AS people ORDER BY company",
        "MATCH (p:P)-[:WORKS_AT]->(c:C) RETURN c.name AS company, collect(p.name) AS people ORDER BY company",
    ],
    'D unwind-order-by': [
        "UNWIND [3,1,2] AS x RETURN x ORDER BY x DESC",
        "UNWIND [3,1,2] AS x RETURN x ORDER BY x",
        "UNWIND [3,1,2] AS x WITH x ORDER BY x RETURN collect(x) AS l",
    ],
    'E list-comprehension': [
        "RETURN [x IN range(1,5) WHERE x % 2 = 1 | x * 10] AS l",
        "RETURN [x IN range(1,5) WHERE x % 2 = 1] AS l",
        "RETURN [x IN range(1,5) | x * 10] AS l",
        "RETURN [x IN [1,2,3] WHERE x > 1 | x * 10] AS l",
    ],
    'F arithmetic': [
        "RETURN 7 / 2 AS int_div, 7.0 / 2 AS float_div, 7 % 3 AS m, 2 ^ 3 AS pow, 2 * 3 + 1 AS e",
        "RETURN 2 ^ 3 AS pow",
        "WITH 2 AS a, 3 AS b RETURN a ^ b AS pow, a / b AS int_div",
    ],
    'G merge-rel-on-create-set': [
        "MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 RETURN r.w AS w",
        "MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) RETURN r.w AS w, count(*) AS c",
    ],
    'H remove-then-set-label': [
        "MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.name AS n, p.city AS city, labels(p) AS l",
        "MATCH (p:VIP) RETURN count(p) AS vip",
        "MATCH (p:P) RETURN p.name AS n, p.city AS city ORDER BY n",
    ],
    'I reduce': [
        "RETURN reduce(s = 0, x IN [1,2,3] | s + x) AS r",
        "MATCH (p:P) WHERE p.age IS NOT NULL WITH collect(p.age) AS ages RETURN reduce(s = 0, x IN ages | s + x) AS total",
    ],
    'J call-subquery': [
        "MATCH (c:C) CALL { WITH c MATCH (p:P)-[:WORKS_AT]->(c) RETURN count(p) AS n } RETURN c.name AS company, n ORDER BY company",
    ],
    'K temporal-accessors': [
        "RETURN date('2026-09-19').year AS y, date('2026-09-19').month AS m",
        "RETURN duration({days: 2}).days AS dd",
        "WITH date('2026-09-19') AS dt RETURN dt.year AS y",
    ],
    'L order-by-function': [
        "MATCH (p:P) RETURN p.name AS n ORDER BY toLower(p.name) DESC LIMIT 1",
        "MATCH (p:P) RETURN p.name AS n ORDER BY size(p.name) DESC, n DESC LIMIT 1",
    ],
}
for g, qs in GROUPS.items():
    run('MATCH (n) DETACH DELETE n')
    run(SETUP)
    print(f'== {g} [{MODE}]')
    for q in qs:
        try:
            print('  ', q, '\n      ->', run(q))
        except Exception as e:
            print('  ', q, '\n      -> ERROR', str(e)[-170:])
