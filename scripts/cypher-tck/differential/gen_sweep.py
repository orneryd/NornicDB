"""Generate the differential sweep corpus (#907, #754).

usage: python gen_sweep.py [OUT]   (default: the corpus in
testing/cypher/tck/testdata/differential/sweep.json.gz)

Builds Cypher statements for every operator and clause family: each operand
type against each operator, unary operators and predicates, literals, every
function in Neo4j 5.26's catalogue with valid and invalid arguments,
aggregates, keywords as variable names, undefined variables, projection
shapes, comprehensions, patterns, writes, subqueries and clause semantics.
Expressions are placed in several positions (RETURN, WITH, WHERE, CASE, a
comprehension, UNWIND, ORDER BY, a statement with no prefix).

Every statement runs on SETUP's graph. Writes are marked "rollback": they run
in a transaction that is rolled back, or on a graph reset around them.

Statements whose answer isn't deterministic are left out: internal ids
(id(), elementId()) and min()/max() over nodes, which have no defined order.

The output is gzipped JSON: {"setup": [...], "cases": [{"id", "family",
"position", "query", "rollback"}]}. A case's id is a hash of its statement,
so ids don't change when the generator adds or reorders statements.
functions.json is Neo4j's SHOW FUNCTIONS output for the pinned image.
"""
import gzip
import hashlib
import itertools
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FUNCTIONS = os.path.join(HERE, "functions.json")
DEFAULT_OUT = os.path.join(HERE, "..", "..", "..", "testing", "cypher", "tck", "testdata", "differential", "sweep.json.gz")

SETUP = [
    "MATCH (x) DETACH DELETE x",
    "CREATE (n:Q {id: 1, i: 1, f: 2.5, s: 'ab', b: true, l: [1, 2, 3], big: 9007199254740993})"
    "-[:R {w: 1}]->(:Q {id: 2, i: 2, s: 'b'})-[:R {w: 2}]->(:Q {id: 3}), (:P {id: 4, s: 'x'})",
]

PREFIX = ("MATCH p = (n:Q {id: 1})-[r:R]->(o:Q) WITH n, r, p, o, 1 AS i, 2.5 AS f, 'ab' AS s, "
          "[1, 2, 3] AS l, {a: 1, b: 'x'} AS m, null AS z, true AS t, date('2020-01-02') AS d, "
          "duration('P1D') AS du")
VARS = ["n", "r", "p", "o", "i", "f", "s", "l", "m", "z", "t", "d", "du"]

# Positions an expression {e} is placed in.
POSITIONS = {
    "return": PREFIX + " RETURN {e} AS v",
    "with": PREFIX + " WITH {e} AS v RETURN v",
    "where": PREFIX + " WHERE {e} RETURN 1 AS v",
    "case": PREFIX + " RETURN CASE WHEN {e} THEN 1 ELSE 0 END AS v",
    "comprehension": PREFIX + " RETURN [x IN [1] | {e}] AS v",
    "unwind": PREFIX + " UNWIND [{e}] AS v RETURN v",
    "orderby": PREFIX + " RETURN i AS one ORDER BY {e}",
    "literal": "RETURN {e} AS v",
}

# Typed operands: variable and literal forms.
OPERANDS = {
    "int": ["i", "3"], "float": ["f", "0.5"], "string": ["s", "'x'"], "bool": ["t", "false"],
    "null": ["z", "null"], "list": ["l", "[1, 2]"], "map": ["m", "{a: 2}"], "node": ["n"],
    "rel": ["r"], "path": ["p"], "date": ["d", "date('2021-03-04')"], "duration": ["du", "duration('PT1H')"],
    "point": ["point({x: 1, y: 2})"], "bigint": ["n.big", "9007199254740993"], "bigfloat": ["9007199254740992.0"],
}

cases = []
seen = set()

# Answers that differ between two runs of the same server.
NONDETERMINISTIC = re.compile(r"\b(elementId|id)\((n|r)\)|MATCH \(x:Q\) RETURN (x IS NULL AS g, )?(min|max)\(|collect\(DISTINCT\)")


def add(family, position, q, mode="auto"):
    if q in seen or NONDETERMINISTIC.search(q):
        return
    seen.add(q)
    cases.append({"id": hashlib.sha1(q.encode()).hexdigest()[:12], "family": family, "position": position,
                  "query": q, "rollback": mode == "tx"})


def uses_vars(e):
    return any(re.search(r"(?<![\w.$'])" + re.escape(v) + r"(?![\w'(])", e) for v in VARS)


def place(family, e, positions):
    for position in positions:
        if position == "literal" and uses_vars(e):
            continue
        add(family, position, POSITIONS[position].replace("{e}", e))


# 1. Binary operators over every operand-type pair.
operand_list = [(kind, text) for kind, texts in OPERANDS.items() for text in texts]
for family, ops in [("arithmetic", ["+", "-", "*", "/", "%", "^"]),
                    ("comparison", ["=", "<>", "<", ">", "<=", ">="]),
                    ("boolean", ["AND", "OR", "XOR"]),
                    ("string-op", ["STARTS WITH", "ENDS WITH", "CONTAINS", "=~"]),
                    ("membership", ["IN"])]:
    for (_, left), (_, right), op in itertools.product(operand_list, operand_list, ops):
        positions = ["return", "literal"]
        if family in ("comparison", "boolean"):
            positions.append("where")
        place(family, f"{left} {op} {right}", positions)

# 2. Unary operators and predicates.
for _, x in operand_list:
    for e in [f"-{x}", f"+{x}", f"NOT {x}", f"{x} IS NULL", f"{x} IS NOT NULL", f"{x} IS :: INTEGER",
              f"{x} IS :: STRING NOT NULL", f"{x} IS NORMALIZED", f"{x} IS NOT NFKC NORMALIZED",
              f"{x}[0]", f"{x}[-1]", f"{x}[1..]", f"{x}[..2]", f"{x}['a']", f"{x}.a", f"{x}.id",
              f"{x}{{.a}}", f"{x}{{.*}}", f"{x}{{.id, k: 1}}"]:
        place("unary", e, ["return", "literal", "where"])

# 3. Literals.
for e in ["9223372036854775807", "-9223372036854775808", "9223372036854775808", "0x1F", "-0x1F", "0o17",
          "1e3", "1.5e-3", ".5", "5.", "1_000", "'a\\nb'", "'\\u00e9'", '"dq"', "'it''s'", "[]", "{}",
          "[1, [2, [3]]]", "{a: {b: 1}}", "{`x y`: 1}", "null", "true", "NaN", "0.0 / 0.0", "1 / 0", "1.0 / 0",
          "-0.0", "1 % 0", "2 ^ 63", "-2 ^ 63", "9223372036854775807 + 1", "-9223372036854775808 - 1",
          "-9223372036854775808 / -1", "9223372036854775807 * 2", "[1, 2] + [3]", "[1] + 2", "'a' + 1", "1 + 'a'"]:
    place("literal", e, ["return", "literal", "with"])

# 4. Functions from Neo4j's catalogue, valid and invalid argument counts/types.
SAMPLES = {
    "INTEGER": ["i", "-3"], "FLOAT": ["f", "-0.5"], "STRING": ["s", "'Ab c'"], "BOOLEAN": ["t"],
    "LIST<ANY>": ["l", "[1, 'a', null]"], "LIST<STRING>": ["['a', 'b']"], "LIST<INTEGER>": ["l"],
    "LIST<FLOAT>": ["[1.5, 2.5]"], "LIST<NODE>": ["[n, o]"], "LIST<RELATIONSHIP>": ["[r]"],
    "MAP": ["m", "{year: 2020, month: 2, day: 3}"], "NODE": ["n"], "RELATIONSHIP": ["r"], "PATH": ["p"],
    "ANY": ["i", "s", "l"], "DATE": ["d"], "DURATION": ["du"], "POINT": ["point({x: 1, y: 2})"],
    "NODE | RELATIONSHIP": ["n", "r"], "VECTOR": ["[1.0, 2.0]"],
}
SKIP_FUNCTIONS = re.compile(r"^(rand|randomUUID|timestamp|file|linenumber|graph\..*|.*\.(realtime|statement|transaction))$")
CURRENT_TIME = {"date", "datetime", "time", "localtime", "localdatetime"}
functions = json.load(open(FUNCTIONS, encoding="utf-8"))
for fn in functions:
    name = fn["name"]
    if SKIP_FUNCTIONS.match(name) or fn["aggregating"] or name in ("all", "any", "none", "single", "reduce", "exists"):
        continue
    types = [a["type"] for a in fn["argumentDescription"]]
    pools = []
    for t in types:
        pool = SAMPLES.get(t)
        if pool is None:
            base = [p for k, p in SAMPLES.items() if k in t.split(" | ")]
            pool = [x for p in base for x in p] or ["s"]
        pools.append(pool)
    calls = set()
    for combo in itertools.islice(itertools.product(*pools), 8):
        calls.add(f"{name}({', '.join(combo)})")
    if types:
        first = [p[0] for p in pools]
        for k in range(len(first)):
            calls.add(f"{name}({', '.join(first[:k] + ['null'] + first[k + 1:])})")
            wrong = "'zz'" if "STRING" not in types[k] else "1"
            calls.add(f"{name}({', '.join(first[:k] + [wrong] + first[k + 1:])})")
        calls.add(f"{name}({', '.join(first[:-1])})")
        calls.add(f"{name}({', '.join(first + ['1'])})")
    elif name not in CURRENT_TIME:
        calls.add(f"{name}()")
        calls.add(f"{name}(1)")
    for call in sorted(calls):
        if call.split("(")[0] in CURRENT_TIME and call.endswith("()"):
            continue
        place("function", call, ["return", "literal", "with"])

# 5. Aggregates: arguments, DISTINCT, nulls, grouping.
for agg in ["count", "sum", "avg", "min", "max", "collect", "stDev", "stDevP"]:
    for arg in ["x", "DISTINCT x", "x.k", "DISTINCT x.k", "*", "", "x, 1", "null", "DISTINCT"]:
        if arg == "*" and agg != "count":
            continue
        for source in ["UNWIND [1, 1, 2, null] AS x", "UNWIND [1.5, 'a', [1], null] AS x", "UNWIND [{k: 1}, {k: 1}, {k: 2}] AS x",
                       "MATCH (x:Q)", "MATCH (x:Nope)"]:
            add("aggregate", "return", f"{source} RETURN {agg}({arg}) AS v")
            add("aggregate", "grouped", f"{source} RETURN x IS NULL AS g, {agg}({arg}) AS v ORDER BY g")
for agg in ["percentileCont", "percentileDisc"]:
    for args in ["x, 0.5", "x, 0", "x, 1", "x, 1.5", "x", "DISTINCT x, 0.5"]:
        add("aggregate", "return", f"UNWIND [1, 2, 3, 4] AS x RETURN {agg}({args}) AS v")

# 6. Names: every keyword as a variable, in several positions.
KEYWORDS = ("ALL AND ANY AS ASC ASCENDING BY CALL CASE CONTAINS COUNT CREATE DELETE DESC DESCENDING DETACH DISTINCT "
            "ELSE END ENDS EXISTS FALSE FOREACH IN IS LIMIT MATCH MERGE NONE NOT NULL ON OPTIONAL OR ORDER REMOVE "
            "RETURN SET SINGLE SKIP STARTS THEN TRUE UNION UNWIND USE WHEN WHERE WITH XOR YIELD LOAD CSV FROM "
            "HEADERS SHORTEST PATH PATHS ANY KEY INDEX CONSTRAINT FOR REQUIRE UNIQUE NODE RELATIONSHIP TYPED "
            "NORMALIZED NFC FINISH INSERT").split()
SHAPES = ["MATCH ({k}:Q {{id: 1}}) RETURN {k}.id AS v", "WITH 1 AS {k} RETURN {k} AS v", "UNWIND [1] AS {k} RETURN {k} + 1 AS v",
          "WITH 1 AS {k} WITH {k} WHERE {k} = 1 RETURN {k} AS v", "WITH 1 AS {k}, 2 AS y RETURN y, {k} ORDER BY {k}",
          "WITH [1] AS {k} RETURN {k}[0] AS v", "WITH {{a: 1}} AS {k} RETURN {k}.a AS v", "RETURN 1 AS {k}",
          "WITH 1 AS {k} RETURN count({k}) AS v", "WITH 1 AS {k} RETURN [y IN [1] | {k}] AS v",
          "MATCH (a:Q {{id: 1}})-[{k}:R]->(b) RETURN type({k}) AS v", "WITH 1 AS y RETURN y AS {k} ORDER BY {k}"]
for keyword in sorted(set(KEYWORDS)):
    for case in (keyword.lower(), keyword):
        for shape in SHAPES:
            add("names", "shape", shape.format(k=case))

# 7. Undefined variables in every position.
for e in ["zz", "zz.a", "zz + 1", "zz IN [1]", "[zz]", "count(zz)", "zz{.a}", "size(zz)", "CASE zz WHEN 1 THEN 1 END",
          "[x IN [1] WHERE x = zz]", "{a: zz}", "zz[0]", "zz IS NULL", "NOT zz"]:
    place("undefined", e, ["return", "with", "where", "orderby", "unwind", "comprehension", "case"])

# 8. Projection / clause shapes.
for distinct in ["", "DISTINCT "]:
    for items in ["*", "x", "x, y", "x AS a", "*, x + 1 AS w", "x, count(*) AS c", "count(*) AS c", "collect(x) AS c", "x AS y, y AS x"]:
        for tail in ["", " ORDER BY x", " ORDER BY x DESC SKIP 1", " LIMIT 1", " SKIP 1 LIMIT 1", " WHERE x > 1"]:
            for kw in ["WITH", "RETURN"]:
                if kw == "RETURN" and "WHERE" in tail:
                    continue
                base = "UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y "
                q = base + f"{kw} {distinct}{items}{tail}"
                if kw == "WITH":
                    q += " RETURN *"
                add("projection", kw.lower(), q)

# 9. Comprehensions, quantifiers, CASE, reduce.
for e in ["[x IN l WHERE x > 1 | x * 2]", "[x IN l | x]", "[x IN l WHERE x > 1]", "[x IN [] | x]", "[x IN null | x]",
          "all(x IN l WHERE x > 0)", "any(x IN l WHERE x > 2)", "none(x IN l WHERE x > 3)", "single(x IN l WHERE x = 2)",
          "all(x IN [] WHERE false)", "any(x IN [null] WHERE x = 1)", "single(x IN [1, 1] WHERE x = 1)",
          "reduce(a = 0, x IN l | a + x)", "reduce(a = '', x IN ['a', 'b'] | a + x)", "reduce(a = 0, x IN [] | a + x)",
          "CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "CASE WHEN i > 0 THEN 'pos' END",
          "CASE z WHEN null THEN 1 ELSE 2 END", "CASE WHEN null THEN 1 ELSE 2 END", "CASE 1 WHEN 1.0 THEN 'eq' ELSE 'ne' END",
          "[(n)-[:R]->(b) | b.id]", "[(n)-[:R]->(b) WHERE b.id > 1 | b.id]", "size([(n)-->() | 1])",
          "EXISTS { MATCH (n)-->() }", "COUNT { MATCH (n)-->() }", "COLLECT { MATCH (n)-->(b) RETURN b.id }",
          "l[1..-1]", "l[-5..]", "l[10]", "l[null]", "m.missing", "n.missing", "keys(m)", "properties(n)",
          "s =~ 'a.*'", "s =~ '(?i)A.*'", "s STARTS WITH null", "'' CONTAINS ''", "[1, null] = [1, null]", "[1, 2] < [1, 3]",
          "{a: 1} = {a: 1.0}", "n = n", "n = o", "r = r", "p = p", "d < date('2021-01-01')", "du + du", "d + du", "d - du",
          "duration.between(d, date('2021-01-01'))", "date('2020-02-29') + duration('P1Y')", "datetime('2020-01-01T00:00:00Z') + duration('PT1S')"]:
    place("expression", e, ["return", "with", "where", "comprehension", "literal"])

# 10. Patterns: directions, lengths, labels, properties, joins, OPTIONAL, paths.
NODES = ["(a)", "(a:Q)", "(a:Q {id: 1})", "(a:Q|P)", "(a:!P)", "(a:%)", "(a {s: 'b'})", "(a:Nope)", "(:Q)"]
RELS = ["-->", "<--", "--", "-[r]->", "-[r:R]->", "-[r:R|S]->", "-[r:!S]->", "-[r:R {w: 2}]->", "-[*]->", "-[*0..1]->",
        "-[r*2]->", "-[r*1..2]-", "-[r:R*..3]->", "-[*0]->", "-[r:R]->{1,2}", "-[:R]->+", "-->{0,1}"]
ENDS = ["(b)", "(b:Q)", "(b:Q {id: 3})", "(a)"]
for node, rel, end in itertools.product(NODES, RELS, ENDS):
    pattern = f"{node}{rel}{end}"
    ret = "count(*) AS c"
    add("pattern", "count", f"MATCH {pattern} RETURN {ret}")
    if "(a" in node and "(b" in end:
        add("pattern", "ids", f"MATCH {pattern} RETURN a.id AS x, b.id AS y ORDER BY x, y")
    add("pattern", "optional", f"OPTIONAL MATCH {pattern} RETURN count(*) AS c")
    add("pattern", "path", f"MATCH p = {pattern} RETURN length(p) AS l, size(nodes(p)) AS n ORDER BY l, n")
for q in ["MATCH (a:Q), (b:Q) RETURN count(*) AS c", "MATCH (a:Q)-->(b), (b)-->(c) RETURN count(*) AS c",
          "MATCH (a)-[r]->(b)-[s]->(c) WHERE r = s RETURN count(*) AS c", "MATCH (a)-[r]->(b)<-[s]-(c) RETURN count(*) AS c",
          "MATCH (a:Q {id: 1}) MATCH (a)-->(b) RETURN b.id", "MATCH (a:Q {id: 1}) OPTIONAL MATCH (a)-->(b:P) RETURN a.id, b",
          "OPTIONAL MATCH (a:Nope) MATCH (b:Q) RETURN count(*) AS c", "MATCH (a:Q {id: 1}) OPTIONAL MATCH (a)-[r:Nope]->(b) RETURN r, b",
          "MATCH p = shortestPath((a:Q {id: 1})-[*]->(b:Q {id: 3})) RETURN length(p) AS l",
          "MATCH p = allShortestPaths((a:Q {id: 1})-[*]-(b:Q {id: 3})) RETURN length(p) AS l",
          "MATCH p = shortestPath((a:Q {id: 1})-[*]->(b:P)) RETURN p", "MATCH (a:Q) WHERE (a)-->(:Q {id: 3}) RETURN a.id",
          "MATCH (a:Q) WHERE NOT (a)-->() RETURN a.id", "MATCH (a:Q) RETURN a.id, [(a)-->(b) | b.id] AS out ORDER BY a.id",
          "MATCH (a)-[r]->(b) RETURN type(r) AS t, startNode(r).id AS s, endNode(r).id AS e ORDER BY s",
          "MATCH (a:Q)-[r]->(b) WHERE r.w > 1 RETURN a.id ORDER BY a.id", "MATCH (a:Q) WHERE a:Q AND NOT a:P RETURN count(*) AS c",
          "MATCH (a) WHERE a.missing IS NULL RETURN count(*) AS c", "MATCH (a:Q) WHERE a.id IN [1, 3] RETURN a.id ORDER BY a.id",
          "MATCH (a:Q) WHERE a.s STARTS WITH 'a' RETURN a.id", "MATCH (a) RETURN labels(a) AS l ORDER BY a.id",
          "MATCH (a:Q {id: 1})-[*0..]->(b) RETURN count(DISTINCT b) AS c", "MATCH (a)-[r*]->(b) RETURN size(r) AS l ORDER BY l DESC LIMIT 1",
          "MATCH (a:Q {id: 1})-[r*]->(b) RETURN [x IN r | x.w] AS w ORDER BY size(w)", "MATCH ()-[r]->() RETURN count(r) AS c",
          "MATCH (n) RETURN count(n) AS c", "MATCH (n:Q) WITH n ORDER BY n.id DESC LIMIT 2 RETURN collect(n.id) AS l",
          "MATCH (a:Q)--(b:Q) RETURN count(*) AS c", "MATCH (a:Q {id: 2})--(b) RETURN b.id ORDER BY b.id"]:
    add("pattern", "misc", q)

# 11. Writes (rolled back).
for q in ["CREATE (n:W {a: 1}) RETURN n.a", "CREATE (n:W {a: [1, 'x']}) RETURN n.a", "CREATE (n:W {a: {b: 1}}) RETURN n",
          "CREATE (n:W {a: null}) RETURN keys(n)", "CREATE (n:W)-[r:T {w: 1.5}]->(m:W) RETURN r.w, type(r)",
          "CREATE (n:W {a: date('2020-01-01')}) RETURN n.a", "CREATE (n:W {a: point({x: 1, y: 2})}) RETURN n.a.x",
          "CREATE (n:W {a: [1, [2]]}) RETURN n", "CREATE (n:W {a: []}) RETURN n.a", "CREATE (n:W:V) RETURN labels(n)",
          "CREATE (n:W) SET n.a = 1, n.b = n.a + 1 RETURN n.b", "MATCH (n:Q {id: 1}) SET n.i = n.i + 10 RETURN n.i",
          "MATCH (n:Q {id: 1}) SET n += {i: 5, z: 1} RETURN n.i, n.z, n.s", "MATCH (n:Q {id: 1}) SET n = {i: 5} RETURN keys(n)",
          "MATCH (n:Q {id: 1}) SET n.s = null RETURN n.s", "MATCH (n:Q {id: 1}) REMOVE n.s RETURN keys(n)",
          "MATCH (n:Q {id: 1}) SET n:X RETURN labels(n)", "MATCH (n:Q {id: 1}) REMOVE n:Q RETURN labels(n)",
          "MATCH (n:Q {id: 3}) DELETE n RETURN count(*) AS c", "MATCH (n:Q {id: 1}) DELETE n",
          "MATCH (n:Q {id: 1}) DETACH DELETE n RETURN count(*) AS c", "MATCH (n:Q {id: 1}) DETACH DELETE n WITH 1 AS x MATCH (m:Q) RETURN count(m) AS c",
          "MERGE (n:Q {id: 1}) RETURN n.s", "MERGE (n:Q {id: 9}) RETURN n.id", "MERGE (n:Q {id: 9}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 1 RETURN n.c, n.m",
          "MERGE (n:Q {id: 1}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 1 RETURN n.c, n.m",
          "MATCH (a:Q {id: 1}), (b:Q {id: 3}) MERGE (a)-[r:T]->(b) RETURN type(r)", "MATCH (a:Q {id: 1}), (b:Q {id: 2}) MERGE (a)-[r:R]->(b) RETURN r.w",
          "MERGE (a:Q {id: 1})-[:R]->(b:Q {id: 2}) RETURN b.s", "MERGE (n:Q {id: null}) RETURN n", "MERGE (n {id: 1}) RETURN labels(n)",
          "UNWIND [1, 2, 2] AS x MERGE (n:W {id: x}) RETURN count(*) AS c", "UNWIND [1, 2, 2] AS x CREATE (n:W {id: x}) RETURN count(*) AS c",
          "FOREACH (x IN [1, 2] | CREATE (:W {id: x})) WITH 1 AS one MATCH (w:W) RETURN count(w) AS c",
          "MATCH (n:Q) SET n.k = n.id * 2 RETURN sum(n.k) AS s", "CREATE (n:W {a: 1}) WITH n MATCH (m:W) RETURN count(m) AS c",
          "CREATE (n:W {a: 0.0 / 0.0}) RETURN n.a", "CREATE (n:W {a: 9223372036854775807}) RETURN n.a + 0", "CREATE (n:W {a: {b: 1}})",
          "CREATE (n:W {a: [1, null]})", "CREATE (n:W {a: [1, 'x']})", "MATCH (n:Q {id: 1}) SET n.l = n.l + [4] RETURN n.l",
          "MATCH (n:Q {id: 1}) SET n.big = n.big + 1 RETURN n.big", "CREATE (a)-[:T]->(a) RETURN 1 AS x", "CREATE ()-[:T]->() RETURN 1 AS x",
          "CREATE (a:W)-[:T]-(b:W)", "CREATE (a:W)-[:T|U]->(b:W)", "CREATE (a:W)-[*2]->(b:W)", "MERGE (a:W)-[:T*2]->(b:W)",
          "MATCH (n:Q {id: 1}) SET n.id = n.id RETURN n.id", "MATCH (n) DELETE n", "MATCH (n:P) DELETE n RETURN count(*) AS c"]:
    add("write", "tx", q, mode="tx")

# 12. Subqueries and UNION.
for q in ["CALL { RETURN 1 AS x } RETURN x", "UNWIND [1, 2] AS i CALL (i) { RETURN i * 2 AS d } RETURN d ORDER BY d",
          "UNWIND [1, 2] AS i CALL { WITH i RETURN i + 1 AS d } RETURN d ORDER BY d", "MATCH (a:Q) CALL (a) { MATCH (a)-->(b) RETURN count(b) AS c } RETURN a.id, c ORDER BY a.id",
          "MATCH (a:Q) CALL (a) { MATCH (a)-->(b) RETURN b } RETURN a.id, b.id ORDER BY a.id",
          "CALL { MATCH (a:Q) RETURN a.id AS x UNION MATCH (b:P) RETURN b.id AS x } RETURN x ORDER BY x",
          "RETURN 1 AS x UNION RETURN 1 AS x", "RETURN 1 AS x UNION ALL RETURN 1 AS x", "RETURN 1 AS x UNION RETURN 1.0 AS x",
          "RETURN 1 AS x UNION RETURN 2 AS y", "MATCH (a:Q) RETURN a.id AS x UNION ALL MATCH (b:P) RETURN b.id AS x",
          "MATCH (a:Q) RETURN EXISTS { MATCH (a)-->() } AS e ORDER BY a.id", "MATCH (a:Q) RETURN COUNT { (a)-->() } AS c ORDER BY a.id",
          "MATCH (a:Q) RETURN COLLECT { MATCH (a)-->(b) RETURN b.id } AS l ORDER BY a.id", "MATCH (a:Q) WHERE COUNT { (a)--() } > 1 RETURN a.id",
          "CALL { CREATE (n:W) RETURN n } RETURN count(n) AS c", "UNWIND [1, 2] AS i CALL (i) { CREATE (:W {id: i}) } RETURN count(*) AS c",
          "CALL { RETURN 1 AS x UNION RETURN 2 AS x } RETURN sum(x) AS s", "UNWIND [1] AS i CALL { RETURN i AS j } RETURN j",
          "MATCH (a:Q {id: 1}) RETURN EXISTS { (a)-[:R]->(:Q {id: 2}) } AS e", "RETURN EXISTS { MATCH (n:Nope) } AS e",
          "MATCH (a:Q) WITH a, COUNT { (a)-->() } AS c WHERE c > 0 RETURN a.id ORDER BY a.id", "CALL () { RETURN 1 AS x } RETURN x",
          "UNWIND [1, 2, 3] AS i CALL (i) { WITH i WHERE i > 1 RETURN i AS j } RETURN j ORDER BY j",
          "UNWIND [1, 2] AS i OPTIONAL CALL (i) { WITH i WHERE i > 1 RETURN i AS j } RETURN i, j ORDER BY i"]:
    add("subquery", "q", q, mode="tx" if "CREATE" in q else "auto")

# 13. Clause semantics: ordering, DISTINCT, grouping, nulls, SKIP/LIMIT.
VALUES = "[1, 1.0, '1', null, true, [1], {a: 1}, 2, -1, 'a', 'B', [], [null], date('2020-01-01'), duration('P1D'), 0.0 / 0.0]"
for q in [f"UNWIND {VALUES} AS x RETURN x ORDER BY x", f"UNWIND {VALUES} AS x RETURN x ORDER BY x DESC",
          f"UNWIND {VALUES} AS x RETURN DISTINCT x", f"UNWIND {VALUES} AS x RETURN count(DISTINCT x) AS c",
          f"UNWIND {VALUES} AS x RETURN collect(DISTINCT x) AS c", f"UNWIND {VALUES} AS x RETURN min(x) AS a, max(x) AS b",
          f"UNWIND {VALUES} AS x RETURN x, count(*) AS c", f"UNWIND {VALUES} AS x WITH x WHERE x = 1 RETURN count(*) AS c",
          f"UNWIND {VALUES} AS x WITH x WHERE x IS NULL RETURN count(*) AS c", f"UNWIND {VALUES} AS x RETURN valueType(x) AS t, count(*) AS c ORDER BY t",
          "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x SKIP 1 LIMIT 1", "UNWIND [3, 1, 2] AS x RETURN x SKIP 5", "UNWIND [3, 1, 2] AS x RETURN x LIMIT 0",
          "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x LIMIT 1 + 1", "UNWIND [3, 1, 2] AS x RETURN x SKIP -1", "UNWIND [3, 1, 2] AS x RETURN x LIMIT 1.5",
          "UNWIND [3, 1, 2] AS x RETURN x LIMIT -1", "UNWIND [3, 1, 2] AS x RETURN x LIMIT null", "UNWIND [3, 1, 2] AS x RETURN x LIMIT 'a'",
          "UNWIND [1, 2, 3, 4] AS x RETURN x % 2 AS k, collect(x) AS l ORDER BY k", "UNWIND [1, 2, 3, 4] AS x RETURN x % 2 AS k, sum(x) + 1 AS s ORDER BY k",
          "UNWIND [1, 2, 3, 4] AS x WITH x % 2 AS k, count(*) AS c WHERE c > 1 RETURN k, c ORDER BY k",
          "UNWIND [1, 2, 3, 4] AS x RETURN count(*) AS c ORDER BY c", "UNWIND [1, 2, 3] AS x RETURN x AS y ORDER BY x DESC",
          "UNWIND [1, 2, 3] AS x RETURN DISTINCT x % 2 AS y ORDER BY y", "UNWIND [1, 2, 3] AS x RETURN DISTINCT x % 2 AS y ORDER BY x",
          "UNWIND [1, 2, 3] AS x RETURN sum(x) AS s ORDER BY x", "UNWIND [] AS x RETURN count(*) AS c, sum(x) AS s, avg(x) AS a, collect(x) AS l, min(x) AS m",
          "UNWIND [] AS x RETURN x, count(*) AS c", "UNWIND null AS x RETURN x", "UNWIND [[1, 2], [3]] AS l UNWIND l AS x RETURN x",
          "UNWIND 5 AS x RETURN x", "UNWIND 'ab' AS x RETURN x", "UNWIND {a: 1} AS x RETURN x",
          "WITH 1 AS x, 2 AS x RETURN x", "WITH 1 AS x RETURN x, x", "WITH 1 AS x RETURN x AS y, 2 AS y", "WITH 1 AS x WITH x AS y RETURN x",
          "MATCH (n:Q) WITH n.s AS s, count(*) AS c RETURN s, c ORDER BY s", "MATCH (n:Q) RETURN n.s AS s, count(*) AS c ORDER BY s",
          "MATCH (n:Q) RETURN n.id AS id ORDER BY n.s, id", "MATCH (n:Q) WITH n ORDER BY n.id WITH collect(n.id) AS l RETURN l",
          "UNWIND [2, 1] AS x WITH x ORDER BY x RETURN collect(x) AS l", "UNWIND [1, 2] AS x RETURN avg(x) AS a, stDev(x) AS s",
          "UNWIND [1, 2, 3] AS x RETURN percentileDisc(x, 0.5) AS d, percentileCont(x, 0.4) AS c",
          "UNWIND ['b', 'a', 'B', 'A', null] AS x RETURN x ORDER BY x", "UNWIND [-0.0, 0.0, 0] AS x RETURN DISTINCT x",
          "UNWIND [1, 1.0] AS x RETURN DISTINCT x", "UNWIND [[1, 2], [1, 2.0]] AS x RETURN DISTINCT x", "UNWIND [{a: 1}, {a: 1.0}] AS x RETURN DISTINCT x"]:
    add("clause", "q", q)

# 15. Procedure arguments: a built-in procedure reads its evaluated
# arguments (#907), so a value bound by WITH or UNWIND, a computed value,
# null and a value of another type reach it as they would from a literal.
# Only procedures Neo4j 5.26 has (no APOC / GDS on the reference; NornicDB
# keeps db.index.fulltext.createNodeIndex / drop, which Neo4j removed), and none
# that leaves an index behind: names refer to missing indexes, and the
# index-creating calls fail on a null or mistyped argument.
PROCEDURE_CALLS = [
    ("db.index.fulltext.queryNodes", ["'missing_ft'", "'x'"], "node"),
    ("db.index.fulltext.queryRelationships", ["'missing_ft'", "'x'"], "relationship"),
    ("db.index.vector.queryNodes", ["'missing_vec'", "2", "[1.0, 2.0]"], "node"),
    ("db.index.vector.queryRelationships", ["'missing_vec'", "2", "[1.0, 2.0]"], "relationship"),
    ("db.awaitIndex", ["'missing_idx'", "1"], None),
    ("db.resampleIndex", ["'missing_idx'"], None),
]
FAILING_CREATES = [
    ("db.index.vector.createNodeIndex", ["'sweep_vec'", "'Q'", "'emb'", "3", "'cosine'"]),
]
ARGUMENT_REPLACEMENTS = ["null", "1.5", "'x'", "[1, 2]", "{a: 1}", "true"]


def procedure_call(name, args, yielded):
    tail = f" YIELD {yielded} RETURN {yielded}" if yielded else ""
    return f"CALL {name}({', '.join(args)}){tail}"


for name, args, yielded in PROCEDURE_CALLS:
    add("procedure-arguments", "literal", procedure_call(name, args, yielded))
    bound = ", ".join(f"{a} AS a{i}" for i, a in enumerate(args))
    names = [f"a{i}" for i in range(len(args))]
    add("procedure-arguments", "with", f"WITH {bound} " + procedure_call(name, names, yielded))
    add("procedure-arguments", "unwind", f"UNWIND [{args[0]}] AS a0 " + procedure_call(name, ["a0"] + args[1:], yielded))
    for i in range(len(args)):
        for value in ARGUMENT_REPLACEMENTS:
            changed = args[:i] + [f"v"] + args[i + 1:]
            add("procedure-arguments", "with-value", f"WITH {value} AS v " + procedure_call(name, changed, yielded))
for name, args in FAILING_CREATES:
    for i in range(len(args)):
        for value in ["null", "1.5", "[1, 2]", "{a: 1}"]:
            changed = args[:i] + ["v"] + args[i + 1:]
            add("procedure-arguments", "create-bad-value", f"WITH {value} AS v " + procedure_call(name, changed, None))


out_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=9) as out:
    json.dump({"setup": SETUP, "cases": cases}, out, separators=(",", ":"))
counts = {}
for case in cases:
    counts[case["family"]] = counts.get(case["family"], 0) + 1
print(len(cases), counts)
