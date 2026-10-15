"""Generate the differential sweep corpus (#907, #754).

usage: python gen_sweep.py [OUT]   (default: the corpus in
testing/cypher/tck/testdata/differential/sweep.json.gz)

Builds Cypher statements for every operator and clause family: each operand
type against each operator, unary operators and predicates, literals, every
function in Neo4j 5.26's catalogue with valid and invalid arguments,
aggregates, keywords as variable names (inside CASE too), map keys, properties and labels,
undefined variables, projection
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

# 6c. Names: every keyword as a variable inside CASE (both forms, every
# branch), a WHERE that holds a CASE, and a comprehension: CASE's scanners
# read their keywords where an operand stands (#1136).
CASE_VARIABLE_SHAPES = ["WITH {{id: 1}} AS {k} RETURN CASE WHEN {k}.id = 1 THEN {k}.id ELSE 0 END AS v",
                        "WITH 1 AS {k} RETURN CASE {k} WHEN 1 THEN {k} ELSE 0 END AS v",
                        "WITH 2 AS {k} RETURN CASE WHEN true THEN {k} END AS v",
                        "WITH 2 AS {k} RETURN CASE WHEN false THEN 0 ELSE {k} END AS v",
                        "WITH 2 AS {k} RETURN CASE WHEN {k} > 1 AND {k} < 3 THEN {k} * 2 ELSE 0 - {k} END AS v",
                        "UNWIND [1, 2] AS {k} WITH {k} WHERE CASE WHEN {k} > 1 THEN true ELSE false END RETURN {k} AS v",
                        "WITH 1 AS {k} RETURN [x IN [1, 2] | CASE WHEN x = {k} THEN {k} ELSE x END] AS v"]
for keyword in sorted(set(KEYWORDS)):
    for case in (keyword.lower(), keyword):
        for shape in CASE_VARIABLE_SHAPES:
            add("names", "case", shape.format(k=case))

# 6b. Names: every keyword as a map key, a property and a label, inside the
# expressions that scan for keywords (CASE in both forms, WHERE, a
# comprehension).
KEY_SHAPES = ["WITH {{{k}: 3}} AS m RETURN m.{k} AS v", "RETURN {{{k}: 1}}.{k} AS v",
              "WITH {{{k}: 3}} AS m RETURN CASE WHEN m.{k} = 3 THEN m.{k} ELSE 0 END AS v",
              "WITH {{{k}: 3}} AS m RETURN CASE m.{k} WHEN 3 THEN m.{k} ELSE 0 END AS v",
              "RETURN CASE WHEN true THEN {{{k}: 1}} ELSE {{{k}: 2}} END AS v",
              "WITH {{{k}: 3}} AS m WITH m WHERE m.{k} = 3 AND m.{k} > 1 RETURN m.{k} AS v",
              "WITH {{{k}: 3}} AS m RETURN [x IN [1, 2] WHERE x < m.{k} | {{{k}: x}}] AS v",
              "MATCH (n:Q {{id: 1}}) RETURN CASE WHEN n:{k} THEN 1 ELSE 0 END AS v"]
for keyword in sorted(set(KEYWORDS)):
    for case in (keyword.lower(), keyword):
        for shape in KEY_SHAPES:
            add("key-names", "shape", shape.format(k=case))

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

# 12b. Subquery expressions in every enclosing construct (#1055): each kind of
# subquery reads a scope variable (quantifier, comprehension, reduce), an outer
# scalar, an entity's property, a map member or a list element, wrapped in the
# expressions and clause positions that evaluate it.
SUBQUERY_BOOLS = ["EXISTS {{ MATCH (a:Q) WHERE a.id = {x} }}", "EXISTS {{ (a:Q {{id: {x}}}) }}",
                  "EXISTS {{ MATCH (a:Q {{id: {x}}})-[:R]->() }}", "EXISTS {{ MATCH (a:Q)-[:R]->(b) WHERE b.id = {x} }}",
                  "COUNT {{ MATCH (a:Q) WHERE a.id = {x} }} > 0",
                  "size(COLLECT {{ MATCH (a:Q) WHERE a.id = {x} RETURN a.id }}) > 0"]
SUBQUERY_VALUES = ["COUNT {{ MATCH (a:Q) WHERE a.id >= {x} }}",
                   "COLLECT {{ MATCH (a:Q) WHERE a.id >= {x} RETURN a.id ORDER BY a.id }}"]
SCOPED = ["any(v IN [1, 5] WHERE {b})", "all(v IN [1, 2] WHERE {b})", "all(v IN [1, 5] WHERE {b})",
          "none(v IN [5, 6] WHERE {b})", "single(v IN [1, 5] WHERE {b})", "[v IN [1, 2, 5] WHERE {b} | v]",
          "[v IN [1, 5] | {b}]", "reduce(s = 0, v IN [1, 2, 5] | s + CASE WHEN {b} THEN 1 ELSE 0 END)",
          "[w IN [1, 5] | any(v IN [w] WHERE {b})]", "any(v IN [1, 5] WHERE v > 1 OR {b})"]
SCOPED_VALUES = ["[v IN [1, 3] | {q}]", "reduce(s = 0, v IN [1, 3] | s + size([{q}]))"]
UNSCOPED = ["{b}", "NOT {b}", "{b} OR false", "false OR {b}", "true AND {b}", "{b} XOR false",
            "CASE WHEN {b} THEN 1 ELSE 0 END", "CASE {b} WHEN true THEN 'y' ELSE 'n' END", "coalesce({b}, false)",
            "[{b}]", "{{k: {b}}}", "toString({b})", "(i = 1 OR {b}) AND ({b} OR i = 2)"]
for construct, readers in [(SCOPED, ["v", "v + 0"]), (UNSCOPED, ["i", "n.id", "o.id", "m.a", "l[0]", "size(l)"])]:
    for wrap in construct:
        for sq in SUBQUERY_BOOLS:
            for reader in readers:
                e = wrap.format(b=sq.format(x=reader))
                positions = ["return", "with", "orderby"]
                if wrap.startswith(("any", "all", "none", "single", "NOT", "{b}", "false", "true", "(", "coalesce")) or wrap == "{b}":
                    positions.append("where")
                place("subquery-scope", e, positions)
for wrap in SCOPED_VALUES:
    for sq in SUBQUERY_VALUES:
        place("subquery-scope", wrap.format(q=sq.format(x="v")), ["return", "with"])
for sq in SUBQUERY_VALUES:
    for reader in ["i", "n.id", "o.id", "m.a", "l[0]"]:
        place("subquery-scope", sq.format(x=reader), ["return", "with", "orderby"])
        place("subquery-scope", sq.format(x=reader) + " = " + sq.format(x=reader), ["return", "where"])
# SET, and a quantified path's element WHERE reading the element's own variables.
for sq in SUBQUERY_BOOLS + [s + " = " + s for s in SUBQUERY_VALUES]:
    add("subquery-scope", "set", f"MATCH (n:Q {{id: 1}}) SET n.x = {sq.format(x='n.id')} RETURN n.x AS v", mode="tx")
    add("subquery-scope", "set", f"MATCH (n:Q {{id: 1}}) SET n.x = [v IN [1, 5] | {sq.format(x='v')}] RETURN n.x AS v", mode="tx")
for sq in SUBQUERY_BOOLS:
    for reader in ["x.id", "y.id", "e.w + 1"]:
        b = sq.format(x=reader)
        add("subquery-scope", "qpp", f"MATCH (s:Q {{id: 1}}) ((x)-[e:R WHERE {b}]->(y))+ (t) RETURN t.id AS v ORDER BY v")
        add("subquery-scope", "qpp", f"MATCH (s:Q {{id: 1}}) ((x)-[e:R]->(y) WHERE e.w = 2 OR {b})+ (t) RETURN t.id AS v ORDER BY v")

# Procedure YIELD predicates use the same correlated scope as WITH (#1226).
# Cover scalars independently of any particular search procedure.
YIELD_SCOPE_PREDICATES = [
    "EXISTS {{ MATCH (x) WHERE {v} IN labels(x) }}",
    "COUNT {{ MATCH (x) WHERE {v} IN labels(x) }} > 0",
    "size(COLLECT {{ MATCH (x) WHERE {v} IN labels(x) RETURN x.id }}) > 0",
    "size([(x:Q)-->(y) WHERE {v} IN labels(x) | y.id]) > 0",
    "EXISTS {{ MATCH (x) WHERE {v} IN labels(x) AND COUNT {{ (x)-->() }} > 0 }}",
    "EXISTS {{ WITH 1 AS {v} RETURN {v} }}",
    "EXISTS {{ UNWIND [1] AS {v} RETURN {v} }}",
    "EXISTS {{ RETURN 1 AS {v} }}",
    "EXISTS {{ WITH {v} AS {v} RETURN {v} }}",
    "EXISTS {{ WITH {v} RETURN {v} }}",
    "EXISTS {{ MATCH (x) WHERE missing IN labels(x) }}",
    "EXISTS {{ MATCH (x) WHERE label IN labels(x) }}",
]
# Parenthesized identity projections preserve the enclosing variable, both
# directly in YIELD's WHERE and after an explicit WITH.
for expression in ["({v})", "(({v}))", "( ( {v} ) )", "((`{v}`))"]:
    for body in ["WITH " + expression + " AS {v} RETURN {v}", "RETURN " + expression + " AS {v}"]:
        for keyword in ["EXISTS", "COUNT", "COLLECT"]:
            subquery = keyword + " {{ " + body + " }}"
            predicate = subquery if keyword == "EXISTS" else (
                subquery + " = 1" if keyword == "COUNT" else "size(" + subquery + ") = 1")
            YIELD_SCOPE_PREDICATES.append(predicate)
for prefix in ["", "WITH 'Q' AS wanted ", "MATCH (seed:Q {id: 1}) "]:
    for yielded, variable in [("label", "label"), ("label AS l", "l")]:
        for predicate in YIELD_SCOPE_PREDICATES:
            for position in ["yield-where", "yield-with-where"]:
                bridge = f" WITH {variable}" if position == "yield-with-where" else ""
                query = (prefix +
                         f"CALL db.labels() YIELD {yielded}{bridge} WHERE " +
                         predicate.format(v=variable) + f" RETURN {variable} AS v ORDER BY v")
                add("subquery-scope", position, query)
for prefix in ["WITH 'Q' AS wanted ", "MATCH (seed:Q {id: 1}) WITH seed, 'Q' AS wanted "]:
    add("subquery-scope", "yield-where", prefix + "CALL db.labels() YIELD label "
        "WHERE EXISTS { MATCH (x) WHERE label IN labels(x) AND label = wanted } RETURN label AS v ORDER BY v")

# The same declaration owner checks ordinary expression subqueries too.
for expression in ["n", "(n)", "((n))", "( ( n ) )", "((`n`))", "(n.x)", "((n.x))", "( ( n.x ) )", "(n.x) + 0"]:
    source = "{x: 7}" if ".x" in expression else "1"
    for body in ["WITH " + expression + " AS n RETURN n", "RETURN " + expression + " AS n"]:
        for keyword in ["EXISTS", "COUNT", "COLLECT"]:
            add("subquery-scope", "grouped-binding", "WITH " + source + " AS n RETURN " + keyword + " { " + body + " } AS v")

# 12b'. A quantified group's step variables (#1211): inside the group's
# predicates (an element's inline WHERE, the group's own WHERE) its node and
# relationship variables are one iteration's node and relationship in every
# expression form (a subscript's key, map and list indexing, function
# arguments, comprehensions, CASE, EXISTS / COUNT subqueries, property
# chains); after the pattern they are the group's lists. Each predicate runs
# under {1,2}, +, * and SHORTEST 1.
QPP_STEP_HEAD = "WITH {ab: 1, b: 2, `1`: true, R: 'r'} AS m, [10, 20, 30] AS l MATCH (s:Q {id: 1}) "
QPP_STEP_RELATIONSHIP = ["m[toString(e.w)] IS NULL", "m[toString(e.w)] = true", "l[e.w] > 10", "l[e.w - 1] IS NOT NULL",
                         "e['w'] > 0", "properties(e)['w'] >= 1", "{k: e}.k.w = 1", "coalesce(m[toString(e.w)], false)",
                         "[m[toString(e.w)]][0] IS NULL", "[v IN l WHERE v = e.w * 10] <> []",
                         "any(k IN keys(e) WHERE l[e[k]] > 10)", "reduce(a = 0, k IN keys(e) | a + e[k]) > 0",
                         "CASE WHEN l[e.w] > 10 THEN true ELSE false END", "CASE m[type(e)] WHEN 'r' THEN true ELSE false END",
                         "EXISTS { MATCH (u)-[e]->(v) WHERE l[e.w] > 0 }", "COUNT { MATCH ()-[q]->() WHERE q = e AND l[q.w] > 0 } = 1",
                         "size([(u)-[e]->(v) | v.id]) = 1"]
QPP_STEP_NODE = ["m[x.s] IS NULL", "m[coalesce(x.s, 'none')] IS NOT NULL", "x['id'] < 3", "l[x.id - 1] >= 10",
                 "[k IN keys(x) WHERE x[k] IS NULL] = []", "CASE WHEN m[x.s] = 1 THEN true ELSE x.id > 1 END",
                 "EXISTS { (x)-[:R]->() }", "COUNT { (x)-->() } >= 1"]
QPP_STEP_GROUP = ["l[e.w] > l[x.id - 1]", "m[x.s] IS NULL OR m[y.s] = 2", "x[toString(e.w)] IS NULL", "e[y.s] IS NULL"]
qpp_step_groups = ([f"((x)-[e:R WHERE {b}]->(y))" for b in QPP_STEP_RELATIONSHIP] +
                   [f"((x WHERE {b})-[e:R]->(y))" for b in QPP_STEP_NODE] +
                   [f"((x)-[e:R]->(y) WHERE {b})" for b in QPP_STEP_RELATIONSHIP + QPP_STEP_NODE + QPP_STEP_GROUP])
for group in qpp_step_groups:
    for selector, quantifier in [("", "{1,2}"), ("", "+"), ("", "*"), ("SHORTEST 1 ", "+")]:
        pattern = f"MATCH p = {selector}(s){group}{quantifier}(t)"
        add("qpp-step-variables", "count", QPP_STEP_HEAD + pattern + " RETURN count(p) AS c")
        add("qpp-step-variables", "rows", QPP_STEP_HEAD + pattern + " RETURN t.id AS v, length(p) AS n ORDER BY v, n")
# After the pattern: the group's lists, read whole or through a comprehension,
# and a singleton's use of a list, which Neo4j rejects.
for after in ["RETURN [k IN e | l[k.w]] AS v ORDER BY size(v)", "RETURN [k IN x | m[k.s]] AS v ORDER BY size(v)",
              "WHERE all(k IN e WHERE l[k.w] > 10) RETURN count(p) AS c", "WITH e, l RETURN size(e) AS v ORDER BY v",
              "RETURN l[e.w] AS v", "RETURN m[x.s] AS v", "WHERE l[e.w] > 10 RETURN count(p) AS c", "RETURN e.w AS v"]:
    add("qpp-step-variables", "after", QPP_STEP_HEAD + "MATCH p = (s)((x)-[e:R WHERE l[e.w] > 10]->(y))+(t) " + after)
for b in ["10 IN e", "e[0] IS NULL", "x[0] IS NULL", "m[e] IS NULL", "size(e) = 1", "e + 1 = 2"]:
    add("qpp-step-variables", "singleton", QPP_STEP_HEAD + f"MATCH p = (s)((x)-[e:R]->(y) WHERE {b})+(t) RETURN count(p) AS c")

# 12b''. Dynamic labels and types inside and outside quantified groups
# (#1227): a pattern's $(v) / $any(v) / $all(v) is read per step from a value
# bound before the MATCH; a value that can't name a label or type is a
# compile-time SyntaxError whether or not a row reaches it (the :Missing
# start), and a valid one matches as the written label would. The (v) is a
# value, not a pattern node. In Cypher 5 a dynamic label in a predicate
# (x:$(v) in WHERE) is a SyntaxError.
DYNAMIC_NODE_VALUES = ["'Q'", "['Q']", "['Q', 'P']", "'P'", "'Missing'", "[]", "1", "[1]", "true", "null"]
DYNAMIC_TYPE_VALUES = ["'R'", "['R']", "['R', 'S']", "'S'", "[]", "1", "[1]", "null"]
DYNAMIC_GROUPS = [("(s:Q {id: 1})", "(()-->(y:{tok}))", "+"), ("(s:Q {id: 1})", "((x:{tok})-->(y))", "{1,2}"),
                  ("(s:Q {id: 1})", "((x)-->(y:{tok}))", "*"), ("SHORTEST 1 (s:Q {id: 1})", "(()-->(y:{tok}))", "+"),
                  ("(:Missing)", "((x:{tok})-->(y))", "+")]
DYNAMIC_REL_GROUPS = [("(s:Q {id: 1})", "(()-[e:{tok}]->())", "+"), ("(s:Q {id: 1})", "((x)-[e:{tok}]->(y))", "{1,2}"),
                      ("(:Missing)", "(()-[e:{tok}]->())", "+")]
for head in ["WITH {v} AS v ", "UNWIND [{v}] AS v "]:
    for values, groups, outside in [
            (DYNAMIC_NODE_VALUES, DYNAMIC_GROUPS,
             ["MATCH (x:{tok}) RETURN count(x) AS c", "MATCH (s:Q {id: 1})-->(y:{tok}) RETURN y.id AS v ORDER BY v",
              "OPTIONAL MATCH (s:Q {id: 1})-->(y:{tok}) RETURN y.id AS v ORDER BY v"]),
            (DYNAMIC_TYPE_VALUES, DYNAMIC_REL_GROUPS,
             ["MATCH (s:Q {id: 1})-[e:{tok}]->(y) RETURN y.id AS v ORDER BY v", "MATCH ()-[e:{tok}]->() RETURN count(e) AS c"])]:
        for value in values:
            for token in ["$(v)", "$any(v)", "$all(v)"]:
                prefix = head.replace("{v}", value)
                for start, group, quantifier in groups:
                    if value == "null" and start == "(:Missing)":
                        # Neo4j reads a null token only where its plan
                        # reaches it: 0 or a TypeError, by its statistics.
                        continue
                    pattern = f"MATCH p = {start}{group.replace('{tok}', token)}{quantifier}(t)"
                    add("dynamic-labels", "group", prefix + pattern + " RETURN count(p) AS c")
                for template in outside:
                    add("dynamic-labels", "outside", prefix + template.replace("{tok}", token))
for value in ["'Q'", "1"]:
    for where in ["((x WHERE x:$(v))-->(y))+", "((x)-->(y) WHERE y:$any(v))+", "(()-[e WHERE e:$(v)]->())+"]:
        add("dynamic-labels", "predicate", f"WITH {value} AS v MATCH p = (s:Q {{id: 1}}){where}(t) RETURN count(p) AS c")
    add("dynamic-labels", "predicate", f"WITH {value} AS v MATCH (x:Q) WHERE x:$(v) RETURN count(x) AS c")
# A literal dynamic label is resolved before the statement runs.
for token in ["$('Q')", "$(['Q'])", "$any(['Q', 'P'])", "$(1)", "$('')", "$(null)"]:
    add("dynamic-labels", "literal", f"MATCH p = (s:Q {{id: 1}})(()-->(y:{token}))+(t) RETURN count(p) AS c")
    add("dynamic-labels", "literal", f"MATCH (y:{token}) RETURN count(y) AS c")

# 12b'''. A variable that all / any / none / single, a list comprehension (its
# WHERE too) or reduce binds shadows the clause's variable of the same name
# where it is bound, in a quantified group's predicates and anywhere else
# (#1227): its subscripts, slices, map keys and list operands are the local
# value's. The same name outside that scope is still the clause's, and a
# relationship or node there is still a type error.
SHADOW_CONTEXTS = {
    "group": "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})((x)-[e:R]->(y) WHERE {b})+(t) RETURN count(p) AS c",
    "inline": "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})((x)-[e:R WHERE {b}]->(y))+(t) RETURN count(p) AS c",
    "shortest": "WITH {k: 1} AS m MATCH p = SHORTEST 1 (s:Q {id: 1})((x WHERE {b})-[e:R]->(y))+(t) RETURN count(p) AS c",
    "where": "WITH {k: 1} AS m MATCH (x:Q {id: 1})-[e:R]->(y) WHERE {b} RETURN count(*) AS c",
    "return": "WITH {k: 1} AS m MATCH (x:Q {id: 1})-[e:R]->(y) RETURN {b} AS v",
    # x and m bound by earlier clauses: their values are written into the
    # WHERE, never over a local of the same name.
    "bound": "WITH {k: 1} AS m MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE {b} RETURN count(*) AS c",
}
SHADOW_PREDICATES = ["all(V IN [[true]] WHERE V[0])", "any(V IN [[true]] WHERE V[0])", "none(V IN [[false]] WHERE V[0])",
                     "single(V IN [[true]] WHERE V[0])", "all(V IN [{id: true}] WHERE V['id'])",
                     "all(V IN [{id: {k: true}}] WHERE V.id['k'])", "all(V IN [[true]] WHERE size(V[0..1]) = 1)",
                     "all(V IN [[1, 2]] WHERE V[-1] = 2)", "[V IN [[true]] | V[0]][0]", "[V IN [[true]] WHERE V[0] | V][0][0]",
                     "size([V IN [[1]] WHERE 1 IN V]) = 1", "[V IN [[1, 2]] | V[0..1]][0] = [1]",
                     "reduce(V = [[true]], k IN [1] | [s IN V | s])[0][0]", "reduce(a = true, V IN [[true]] | a AND V[0])",
                     "reduce(V = [], k IN [1, 2] | V + [k])[1] = 2", "all(V IN [[[true]]] WHERE all(w IN V[0] WHERE w))",
                     "[V IN [{k: [true]}] | V['k'][0]][0]",
                     "size([(V)-->(w) WHERE w.id > 0 | w.id]) >= 0", "size([(w)-->(z) WHERE w.id > 0 | [z.id][0]]) >= 0",
                     # Outside the local scope the name is the clause's.
                     "V[0] IS NULL AND all(V IN [[true]] WHERE V[0])", "1 IN V AND [V IN [[1]] | 1 IN V] = [true]",
                     "reduce(a = V[0], k IN [1] | a) IS NULL", "all(z IN [1] WHERE V[0] IS NULL)",
                     "[V IN [[1]] | V][0][0] = 1 AND V[0] IS NULL"]
for context, template in SHADOW_CONTEXTS.items():
    for variable in ["x", "e", "m"]:
        for b in SHADOW_PREDICATES:
            if variable != "x" and "(V)" in b:
                continue  # a pattern's (V) is a node: the step's x
            add("local-scope-shadowing", context, template.replace("{b}", b.replace("V", variable)))
# The same declarations however they are written (x IN[…], a comment
# before IN, a quoted name), a named pattern comprehension's path, and a
# subquery's pattern reading the local (#1227).
SHADOW_SPELLINGS = ["all(V IN[[true]] WHERE V[0])", "[V IN[[true]] | V[0]][0]", "all(V /* c */ IN [[true]] WHERE V[0])",
                    "size([V IN[{k: 1}] WHERE V.k = 1 | V]) = 1", "reduce(a = true, V IN[[true]] | a AND V[0])",
                    "all(`V V` IN [[true]] WHERE `V V`[0])", "all(V IN [y] WHERE EXISTS { (V)-->() })",
                    "all(V IN [y] WHERE COUNT { (V)-->() } > 0)", "all(V IN [y] WHERE size(COLLECT { MATCH (V)-->(z) RETURN z.id }) > 0)"]
for context, template in SHADOW_CONTEXTS.items():
    for variable in ["x", "e", "m"]:
        for b in SHADOW_SPELLINGS:
            add("local-scope-shadowing", context, template.replace("{b}", b.replace("V", variable)))
for head in ["WITH 7 AS p ", "WITH 'outer' AS p "]:
    for b in ["size([p = (y)-->(z) | length(p)]) = 1", "[p = (y)-->(z) | length(p)] = [1]", "size([p = (y)-->(z) | z.id]) = 1",
              "size([p = (y)-->(z) | p]) = 1", "all(v IN [true] WHERE v) AND size([p = (y)-->(z) | length(p)]) = 1"]:
        add("local-scope-shadowing", "named-path", head + "MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE " + b + " RETURN count(*) AS c")
        add("local-scope-shadowing", "named-path", head + "MATCH q = (:Q {id: 1})((x)-[e:R]->(y) WHERE " + b.replace("= 1", ">= 0") + ")+(t) RETURN count(q) AS c")
# A quoted group variable is the name it binds.
for step in ["((`x x` WHERE m[`x x`.s] IS NULL)-[e:R]->(y))", "((x)-[`e e`:R WHERE m[toString(`e e`.w)] IS NULL]->(y))",
             "((`x x`)-[e:R]->(y) WHERE `x x`.id > 0)"]:
    add("local-scope-shadowing", "quoted", "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})" + step + "+(t) RETURN count(p) AS c")
add("local-scope-shadowing", "quoted", "MATCH (`x x`:Q {id: 1}) MATCH p = (s:Q {id: 1})((`x x`)-[e:R]->(y))+(t) RETURN count(p) AS c")

# 12b''''. Slice bounds (#1227): a ".." nested in the key or quoted in it
# belongs to the key, in every evaluator and check.
SLICE_SUBSCRIPTS = ["WITH {v: true} AS m, {`a..b`: 'v'} AS r RETURN m[r['a..b']] AS v",
                    "WITH [10, 20] AS l RETURN l[head([0, 1][0..1])] AS v",
                    "WITH {v: true} AS m, ['v'] AS l RETURN m[head(l[0..1])] AS v",
                    "WITH {`a..b`: 7} AS m RETURN m[['a..b'][0]] AS v",
                    "WITH [1, 2, 3] AS l RETURN l[size(l[0..2])..] AS v",
                    "WITH [1, 2, 3] AS l RETURN l[[0, 2][0]..[0, 2][1]] AS v",
                    "RETURN {`..`: 1}['..'] AS v",
                    "MATCH (n:Q {id: 1}) RETURN n.l[head([0, 1][0..1])] AS v",
                    "MATCH (n:Q {id: 1}) RETURN n.l[size(n.l[1..])..] AS v",
                    "MATCH (n:Q {id: 1}) WITH n, {`a..b`: 'l'} AS k RETURN n[k['a..b']] AS v",
                    "MATCH (n:Q {id: 1}) RETURN [x IN [n.l] | x[head([1, 2][0..1])..]] AS v",
                    "WITH {v: true} AS m, {`a..b`: 'v'} AS r WHERE m[r['a..b']] RETURN 1 AS v",
                    "WITH {v: true} AS m, {`a..b`: 'v'} AS r WHERE m[r['a..b']] IS NULL RETURN 1 AS v",
                    "WITH [true, false] AS l WHERE l[head([0, 1][0..1])] RETURN 1 AS v",
                    "UNWIND [[1, 2, 3]] AS l RETURN l[head([1, 2][0..1])..] AS v",
                    "WITH ['a..b'] AS l RETURN {`a..b`: 1}[l[0]] AS v"]
for q in SLICE_SUBSCRIPTS:
    add("slice-bounds", "return", q)

# 12b'''''. References and locals (#1227): a name read outside the local that
# binds it is the clause's variable (an implicit grouping key, an undefined
# variable); inside, the local's. A pattern comprehension's WHERE binds
# nothing.
for q in ["UNWIND [1, 2] AS x RETURN x + size(collect([x IN [1] | x])) AS v",
          "UNWIND [1, 2] AS x RETURN x, size(collect([x IN [1] | x])) AS v ORDER BY x",
          "UNWIND [1, 2] AS x RETURN size(collect([x IN [x] | x])) AS v",
          "UNWIND [1, 2] AS x RETURN count(*) + size([x IN [1] | x]) AS v",
          "UNWIND [1, 2] AS x RETURN x + size(collect(x)) AS v",
          "RETURN all(z IN [x] WHERE z) OR all(x IN [true] WHERE x) AS v",
          "RETURN CASE WHEN true THEN all(z IN [x] WHERE z) ELSE all(x IN [true] WHERE x) END AS v",
          "RETURN x + size([x IN [1] | x]) AS v",
          "RETURN size([x IN [1] | x]) + size([x IN [2] | x]) AS v",
          "WITH 1 AS x RETURN all(x IN[{k: 7}] WHERE x.k = 7) AS v",
          "WITH 1 AS x RETURN CASE WHEN true THEN all(x IN [{k: 7}] WHERE x.k = 7) ELSE x.k = 1 END AS v",
          "WITH 7 AS v UNWIND [v IN[1, 2] | v * 10] AS u RETURN collect(u) AS c",
          "WITH 7 AS v RETURN reduce(v = v, z IN [1] | v + z) AS r",
          "MATCH (n:Q {id: 1}) RETURN [(n)-->(z) WHERE z.id IN (l) | z.id] AS v",
          "MATCH (n:Q {id: 1}) RETURN [(n)-->(z) WHERE z.id IN (l) | l] AS v",
          "WITH [2] AS l MATCH (n:Q {id: 1}) RETURN [(n)-->(z) WHERE z.id IN (l) | z.id] AS v",
          "MATCH (n:Q {id: 1}) RETURN [(n)-->(z) WHERE z.id > 0 | [y IN [z.id] | y]] AS v"]:
    add("local-scope-references", "return", q)
# An aggregate inside reduce stays its list or initial value; a local
# inside an EXISTS / COUNT / COLLECT body may not redeclare an outer
# variable (it may in CALL { } and at the top level) and gives the body's
# patterns its own type; CASE in a named comprehension sees the path; a
# backquoted named path is one (#1227).
for q in ["UNWIND [1, 2] AS x RETURN reduce(acc = 0, y IN collect(x) | acc + y) AS v",
          "UNWIND [1, 2] AS x RETURN reduce(acc = count(*), y IN [1, 2] | acc + y) AS v",
          "UNWIND [1, 2] AS x WITH reduce(acc = 0, y IN collect(x) | acc + y) AS v RETURN v",
          "UNWIND [1, 2] AS x RETURN reduce(acc = [], y IN collect(x) | acc + [y * 2]) AS v",
          "UNWIND [1, 2] AS x RETURN count(*) AS c ORDER BY reduce(acc = 0, y IN collect(x) | acc + y)",
          "UNWIND [1, 2] AS x RETURN [y IN collect(x) WHERE y > 1 | y] AS v",
          "UNWIND [1, 2] AS x RETURN all(y IN collect(x) WHERE y > 0) AS v",
          "WITH 7 AS p RETURN COLLECT { MATCH (n:Q {id: 1}) RETURN [p = (n)-->(z) | length(p)] AS v } AS v",
          "WITH 7 AS x RETURN COLLECT { MATCH (n:Q {id: 1}) RETURN [x IN [1] | x] AS v } AS v",
          "WITH 7 AS x RETURN COUNT { MATCH (n:Q) WHERE all(x IN [1] WHERE x = 1) } AS v",
          "WITH 7 AS x RETURN COLLECT { MATCH (n:Q {id: 1}) RETURN reduce(x = 0, y IN [1] | x + y) AS v } AS v",
          "WITH 7 AS p MATCH (y:Q {id: 1}) WHERE EXISTS { MATCH (y) WHERE size([p = (y)-->(z) | p]) > 0 } RETURN count(*) AS c",
          "MATCH (r:Q {id: 1}) RETURN COUNT { MATCH (n:Q) WHERE all(r IN [1] WHERE r = 1) } AS v",
          "WITH 7 AS x CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v",
          "WITH 7 AS p CALL (p) { MATCH (n:Q {id: 1}) RETURN [p = (n)-->(z) | length(p)] AS v } RETURN v",
          "WITH 7 AS x RETURN COLLECT { MATCH (n:Q {id: 1}) RETURN x AS v } AS v",
          "WITH 7 AS x RETURN COLLECT { MATCH (n:Q {id: 1}) RETURN [y IN [1] | y + x] AS v } AS v",
          "MATCH (x:Q {id: 1})-[e:R]->(y) WHERE all(e IN [1] WHERE EXISTS { (e)-->() }) RETURN count(*) AS c",
          "MATCH (x:Q {id: 1})-[e:R]->(y) WHERE all(e IN ['s'] WHERE COUNT { (e)-->() } > 0) RETURN count(*) AS c",
          "MATCH (x:Q {id: 1})-[e:R]->(y) WHERE all(e IN [y] WHERE EXISTS { (e)-->() }) RETURN count(*) AS c",
          "MATCH p = (:Q {id: 1})((x)-[e:R]->(y) WHERE all(e IN [1] WHERE EXISTS { (e)-->() }))+(t) RETURN count(p) AS c",
          "WITH 7 AS p MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE [p = (y)-->(z) | CASE WHEN length(p) = 1 THEN z.id ELSE -1 END] = [3] RETURN count(*) AS c",
          "MATCH (y:Q {id: 2}) RETURN [p = (y)-->(z) | CASE WHEN length(p) = 1 THEN z.id ELSE -1 END] AS v",
          "MATCH (y:Q {id: 2}) RETURN [x IN[1, 2] | CASE WHEN x = 1 THEN 'one' ELSE 'other' END] AS v",
          "RETURN [`p p` = (a:Q)-->(b:Q) | length(`p p`)] AS v",
          "MATCH (y:Q {id: 2}) RETURN [`p p` = (y)-->(z) WHERE length(`p p`) = 1 | z.id] AS v",
          "WITH 7 AS `p p` MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE size([`p p` = (y)-->(z) | length(`p p`)]) = 1 RETURN count(*) AS c",
          "RETURN [`x x` IN[1, 2] | `x x` * 10] AS v"]:
    add("local-scope-references", "round3", q)

# A CALL body starts its own scope from its imports; an EXISTS / COUNT /
# COLLECT body sees the enclosing query, its own MATCH and WITH bindings,
# and checks nested subqueries from them; a local is typed with the locals
# around it resolved (#1227).
for q in ["WITH 7 AS x RETURN COLLECT { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v",
          "WITH 7 AS x RETURN COUNT { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v } AS v",
          "WITH 7 AS x RETURN EXISTS { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v } AS v",
          "WITH 7 AS x RETURN COLLECT { CALL () { RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v",
          "WITH 7 AS x RETURN COLLECT { CALL { WITH x RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v",
          "WITH 7 AS x CALL () { RETURN COLLECT { RETURN [x IN [1] | x] AS v } AS v } RETURN v",
          "WITH 7 AS x CALL (x) { RETURN COLLECT { RETURN [x IN [1] | x] AS v } AS v } RETURN v",
          "WITH 7 AS x CALL (x) { RETURN COUNT { MATCH (n:Q) WHERE all(x IN [1] WHERE x = 1) } AS v } RETURN v",
          "MATCH (a:Q {id: 1})-[x:R]->(y) WHERE all(x IN [y] WHERE all(r IN [x] WHERE EXISTS { (r)-->() })) RETURN count(*) AS v",
          "MATCH (r:Q {id: 99}) WHERE all(x IN [1] WHERE all(r IN [x] WHERE EXISTS { (r)-->() })) RETURN count(*) AS v",
          "MATCH (a:Q {id: 1})-[x:R]->(y) WHERE all(x IN [y] WHERE all(r IN [x, x] WHERE COUNT { (r)-->() } > 0)) RETURN count(*) AS v",
          "RETURN COUNT { WITH 7 AS x WHERE EXISTS { WITH [x IN [1] | x] AS v RETURN v } } AS v",
          "RETURN COUNT { MATCH (x:Q {id: 99}) WHERE EXISTS { MATCH (n) WHERE all(x IN [1] WHERE x = 1) } } AS v",
          "RETURN COUNT { MATCH (x:Q) WHERE EXISTS { MATCH (x)-->(n) } } AS v",
          "RETURN COUNT { UNWIND [1, 2] AS x WITH x WHERE EXISTS { WITH x AS y RETURN y } } AS v",
          "WITH 1 AS g RETURN COLLECT { UNWIND [1] AS x WITH count(*) AS c RETURN c + g } AS v",
          "MATCH (a:Q) RETURN a.id AS id, EXISTS { MATCH (x:Q) MATCH (y:Q {id: x.id}) } AS x ORDER BY id",
          "MATCH (n:Q) WITH n, n.id AS k WHERE COUNT { MATCH (o:Q) WHERE o.id > k } > 0 RETURN n.id AS v ORDER BY v",
          "MATCH (n:Q) RETURN [(n)-[:R]->(m) WHERE EXISTS { MATCH (o:Q) WHERE o.id = m.id + 1 } | m.id] AS v ORDER BY v",
          "WITH 1 AS x RETURN COLLECT { WITH x AS y MATCH (n:Q) WHERE n.id = y + x RETURN n.id } AS v",
          "RETURN COUNT { MATCH (x:Q) WITH x WHERE EXISTS { MATCH (x)-[r:R]->() } } AS v"]:
    add("local-scope-references", "round4", q)

# A CALL (x) body sees its imports through the whole body, after a WITH in
# it too, and may declare them again: Neo4j rejects that ("is shadowing an
# imported variable"), NornicDB keeps it as an extension (known_mismatches,
# intended, #907). An expression subquery may not (#1227).
for q in ["WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v } RETURN v",
          "WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v UNION WITH 2 AS y RETURN x AS v } RETURN v",
          "WITH 7 AS x CALL (x) { WITH x, 1 AS y WITH y RETURN x AS v } RETURN v",
          "WITH 7 AS x, 8 AS z CALL (x) { WITH 1 AS z RETURN x + z AS v } RETURN v",
          "WITH 7 AS x CALL (x) { WITH x AS x RETURN x AS v } RETURN v",
          "WITH 7 AS x CALL (x) { WITH 1 AS x RETURN x AS v } RETURN v",
          "WITH 7 AS x CALL (x) { UNWIND [1] AS x RETURN x AS v } RETURN v",
          "WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v UNION WITH 1 AS x RETURN x AS v } RETURN v",
          "WITH 7 AS x RETURN EXISTS { WITH 1 AS x RETURN x } AS v",
          "WITH 7 AS x RETURN COUNT { UNWIND [1] AS x RETURN x } AS v",
          "WITH 7 AS x RETURN COLLECT { WITH x AS x RETURN x } AS v"]:
    add("local-scope-references", "call-imports", q)
# ... and when it runs, the imports stay bound through the body's WITH
# clauses: a MATCH, OPTIONAL MATCH, WHERE pattern, MERGE or SET after a WITH
# reads the imported variable (#1227).
for q in ["MATCH (x:Q {id: 1}) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN v",
          "MATCH (x:Q {id: 3}) CALL (x) { WITH 1 AS y OPTIONAL MATCH (x)-->(m) RETURN m.id AS v } RETURN v",
          "MATCH (x:Q {id: 1}) CALL (x) { WITH 1 AS y MATCH (m:Q) WHERE (x)-->(m) RETURN m.id AS v } RETURN v",
          "MATCH (x:Q) CALL (x) { WITH 1 AS y WHERE (x)-->() RETURN x.id AS v } RETURN v ORDER BY v",
          "MATCH (x:Q) CALL (x) { WITH 1 AS y WHERE EXISTS { (x)-->() } RETURN x.id AS v } RETURN v ORDER BY v",
          "MATCH (x:Q {id: 1}) CALL (x) { OPTIONAL MATCH (n:Nope) WITH count(n) AS c MATCH (x)-->(m) RETURN c, m.id AS v } RETURN c, v",
          "MATCH (x:Q {id: 1}) CALL (x) { MATCH (n:Nope) WITH count(*) AS c RETURN x.id AS i, c } RETURN i, c",
          "MATCH (x:Q {id: 1}) CALL (*) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN v",
          "MATCH (x:Q {id: 1}) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v UNION WITH 2 AS y MATCH (m)-->(x) RETURN m.id AS v } RETURN v",
          "MATCH (x:Q) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN x.id AS i, v ORDER BY i",
          "MATCH (x:Q {id: 1}) CALL (x) { UNWIND [1, 2] AS k WITH k MATCH (x)-->(m) RETURN k, m.id AS v } RETURN k, v ORDER BY k",
          "MATCH (x:Q {id: 1}) CALL (x) { MATCH (m:Q {id: 3}) WITH m MATCH (x)-->(k) RETURN m.id AS mm, k.id AS v } RETURN mm, v"]:
    add("local-scope-references", "call-imports", q)
for q in ["MATCH (x:Q {id: 3}) CALL (x) { WITH 1 AS y MERGE (x)-[:S]->(t:T {id: 'z'}) RETURN t.id AS w } WITH count(*) AS c MATCH (n:Q)-[:S]->(:T) RETURN collect(n.id) AS v",
          "MATCH (x:Q {id: 2}) CALL (x) { WITH 1 AS y SET x.p = y RETURN x.p AS w } WITH count(*) AS c MATCH (n:Q) WHERE n.p = 1 RETURN collect(n.id) AS v",
          "MATCH (x:Q {id: 2}) CALL (x) { MATCH (n:Nope) WITH count(*) AS c SET x.q = c RETURN x.q AS w } WITH count(*) AS z MATCH (n:Q) WHERE n.q = 0 RETURN collect(n.id) AS v"]:
    add("local-scope-references", "call-imports", q, mode="tx")

# A list comprehension's elements are its projection's, typed with its
# local as an element of the list it iterates (#1227).
for q in ["UNWIND [at IN range(0, 1) | [at]] AS e RETURN collect(e[0]) AS v",
          "UNWIND [at IN range(0, 1) | [at, 'p']] AS e WITH DISTINCT e[1] AS v RETURN collect(v) AS v",
          "WITH [{reading: {party_ids: ['a', 'b'], party_versions: [1, 2]}}] AS readings UNWIND readings AS item WITH item.reading AS r UNWIND [at IN range(0, size(coalesce(r.party_ids, [])) - 1) | ['party', r.party_ids[at], r.party_versions[at]]] AS entry WITH DISTINCT entry[1] AS element RETURN collect(element) AS v",
          "UNWIND [[0, 'p'], [1, 'p']] AS e WITH DISTINCT e[1] AS v RETURN collect(v) AS v",
          "UNWIND [at IN range(0, 1) | [at]] + [at IN range(2, 3) | [at]] AS e RETURN collect(e[0]) AS v",
          "UNWIND [at IN range(0, 3) WHERE at > 1] AS e RETURN collect(e + 1) AS v",
          "UNWIND [at IN [[1], [2]] WHERE size(at) > 0] AS e RETURN collect(e[0]) AS v",
          "UNWIND [at IN range(0, 1) | at > 0] AS e RETURN collect(NOT e) AS v",
          "UNWIND [at IN range(0, 1) | {k: at}] AS e RETURN collect(e.k) AS v",
          "WITH [at IN range(0, 1) | [at]] AS l RETURN l[0][0] AS v",
          "RETURN [at IN range(0, 1) | [at]][1][0] AS v",
          "UNWIND [at IN range(0, 1) | [at]] AS e RETURN collect(e.k) AS v"]:
    add("unwind-source", "comprehension", q)
add("unwind-source", "comprehension", "CREATE (r:Tmp {ids: ['a'], vs: [1]}) WITH r UNWIND [at IN range(0, size(r.ids) - 1) | ['p', r.ids[at], r.vs[at]]] AS entry WITH DISTINCT entry[1] AS e RETURN collect(e) AS v", mode="tx")

# 12b''''''. A subscript on a map variable's property (#1227): v.h[k],
# v.a.b[k] and v.l[i] read the row's map in a MATCH's own WHERE, a
# variable-length or quantified step's, an EXISTS / COUNT / CALL
# subquery's, and after WITH, as h[k] does; v comes from WITH, a CALL
# result or UNWIND.
MAP_HEADS = ["WITH {h: {`1`: true, `2`: false}, l: [2, 3], a: {b: {k: 2}}} AS v ",
             "CALL () { RETURN {`1`: true, `2`: false} AS hh } WITH {h: hh, l: [2, 3], a: {b: {k: 2}}} AS v ",
             "UNWIND [{h: {`1`: true, `2`: false}, l: [2, 3], a: {b: {k: 2}}}] AS v "]
MAP_FORMS = ["MATCH (n:Q) WHERE v.h[toString(n.id)] IS NULL RETURN n.id AS v ORDER BY v",
             "MATCH (n:Q) WHERE v.h[toString(n.id)] RETURN n.id AS v ORDER BY v",
             "MATCH (s:Q {id: 1})-[rs:R*1..2]->(e) WHERE all(x IN rs WHERE v.h[toString(startNode(x).id)] IS NULL) RETURN e.id AS v ORDER BY v",
             "MATCH (s:Q {id: 1})-[rs:R*1..2]->(e) WHERE any(x IN rs WHERE v.h[toString(x.w)] IS NOT NULL) RETURN e.id AS v ORDER BY v",
             "MATCH (s:Q {id: 1})(()-[r:R WHERE v.h[toString(r.w)] IS NULL]->()){1,2}(e) RETURN e.id AS v ORDER BY v",
             "MATCH (s:Q {id: 1})((x)-[r:R]->(y) WHERE v.h[toString(x.id)] IS NOT NULL)+(e) RETURN e.id AS v ORDER BY v",
             "MATCH (s:Q)-[r:R]->(e)-[r2:R]->(f) WHERE any(k IN [r.w, r2.w] WHERE v.h[toString(k)] IS NOT NULL) RETURN count(*) AS v",
             "MATCH (n:Q) WHERE EXISTS { MATCH (n)-[r:R]->() WHERE v.h[toString(r.w)] } RETURN n.id AS v ORDER BY v",
             "MATCH (n:Q) WHERE NOT EXISTS { MATCH (n)-[r:R]->() WHERE v.h[toString(r.w)] IS NOT NULL } RETURN n.id AS v ORDER BY v",
             "MATCH (n:Q) RETURN n.id AS id, COUNT { MATCH (n)-[r:R]->() WHERE v.h[toString(r.w)] IS NULL } AS v ORDER BY id",
             "MATCH (n:Q) CALL (n, v) { MATCH (n)-[r:R]->(m) WHERE v.h[toString(r.w)] IS NULL RETURN count(m) AS c } RETURN n.id AS id, c AS v ORDER BY id",
             "MATCH (n:Q) WITH v, n WHERE v.h[toString(n.id)] IS NULL RETURN n.id AS v ORDER BY v",
             "WITH v.h AS h MATCH (n:Q) WHERE h[toString(n.id)] IS NULL RETURN n.id AS v ORDER BY v",
             "MATCH (n:Q) WHERE v.a.b['k'] = n.id RETURN n.id AS v",
             "MATCH (n:Q) WHERE v.a.b[toString(n.id)] IS NULL RETURN count(n) AS v",
             "MATCH (n:Q) WHERE v.l[0] = n.id RETURN n.id AS v",
             "MATCH (n:Q) WHERE v.l[n.id - 2] = n.id RETURN n.id AS v ORDER BY v",
             "MATCH (n:Q) WHERE n.id IN v.l[0..1] RETURN n.id AS v",
             "MATCH (n:Q) WHERE v.h[toString(n.id)] = true AND n.id IN v.l RETURN n.id AS v",
             "MATCH (n:Q) RETURN n.id AS id, {b: true, `1`: 7}[toString(n.id)] AS v ORDER BY id"]
for head in MAP_HEADS:
    for form in MAP_FORMS:
        add("map-property-subscript", "where", head + form)

# 12c. Subquery bodies that start from the row's own variables (#1055): the
# bound node, relationship and path fast paths against the pipeline. Bodies
# read a bound relationship, a row value in a property map or WHERE, a path,
# and use shortestPath, CALL and UNION; each is wrapped like 12b's.
BOUND_BODIES = ["EXISTS {{ (n)-[:R]->() }}", "COUNT {{ (n)-->() }}", "COUNT {{ (n)-[s]->() }}",
                "COUNT {{ (n)-[r]->() }}", "COUNT {{ (n)-[r:R]->() }}", "EXISTS {{ (n)-[r]->(o) }}",
                "COUNT {{ (n)<-[:R]-() }}", "COUNT {{ (n)--() }}", "COUNT {{ (o)-[:R]->() }}",
                "EXISTS {{ (n)-[:R]->(:Q {{id: o.id}}) }}", "EXISTS {{ (n)-[:R]->(:Q {{id: i + 1}}) }}",
                "EXISTS {{ MATCH (n)-[:R]->(x) WHERE x.id = i + 1 }}", "EXISTS {{ MATCH (n)-[:R]->(x) WHERE x.id = m.a + 1 }}",
                "COUNT {{ (n)-[:R]->()-[:R]->() }}", "EXISTS {{ MATCH (n)-[:R]->(x) WHERE x.id IN l }}",
                "EXISTS {{ MATCH q = (n)-[:R]->() WHERE length(q) = 1 }}", "EXISTS {{ MATCH (n)-[:R]->(x) WHERE length(p) = 1 }}",
                "EXISTS {{ MATCH (x)-[e:R]->(y) WHERE e = r }}", "EXISTS {{ MATCH sp = shortestPath((n)-[*]->(o)) }}",
                "COUNT {{ MATCH sp = shortestPath((n)-[*]->(x:Q)) WHERE x <> n }}",
                "EXISTS {{ CALL (n) {{ MATCH (n)-->(x) RETURN x }} RETURN x }}",
                "COUNT {{ MATCH (n)-->(x) RETURN x UNION MATCH (n)<--(x) RETURN x }}",
                "COLLECT {{ MATCH (n)-[e]->(x) RETURN type(e) }}"]
for wrap in ["{b}", "NOT {b}", "CASE WHEN {b} THEN 1 ELSE 0 END", "[{b}]", "coalesce({b}, false)",
             "any(v IN [1, 5] WHERE {b})", "[v IN [1, 5] | {b}]"]:
    for body in BOUND_BODIES:
        b = body.format()
        if wrap not in ("{b}", "[{b}]") and not b.startswith("EXISTS"):
            b = "(" + b + ") > 0" if b.startswith("COUNT") else "size(" + b + ") > 0"
        positions = ["return", "with", "orderby"]
        if wrap in ("{b}", "NOT {b}") and b.startswith("EXISTS"):
            positions.append("where")
        place("subquery-scope", wrap.format(b=b), positions)
# A pattern comprehension as the enclosing construct: its subquery reads the
# comprehension's own variables.
for wrap in ["[(n)-[:R]->(x) WHERE {b} | x.id]", "size([(n)-[:R]->(x) WHERE {b} | x])", "[(n)-[:R]->(x) | {b}]",
             "[(n)-[e:R]->(x) WHERE {b} | e.w]"]:
    for sq in SUBQUERY_BOOLS:
        for reader in ["x.id", "x.id + i"]:
            place("subquery-scope", wrap.format(b=sq.format(x=reader)), ["return", "with"])
# Relationship and path readers, outside any scope.
for wrap in ["{b}", "NOT {b}", "CASE WHEN {b} THEN 1 ELSE 0 END", "[{b}]"]:
    for sq in SUBQUERY_BOOLS:
        for reader in ["r.w", "length(p)", "size(nodes(p))"]:
            positions = ["return", "with"] + (["where"] if wrap in ("{b}", "NOT {b}") else [])
            place("subquery-scope", wrap.format(b=sq.format(x=reader)), positions)

# 12d. Projected expressions used where a type is needed (#1055): each
# expression projected by WITH, a second WITH, UNWIND of it or of a list of it,
# or a CALL subquery's RETURN, then used as a node or relationship in a
# pattern, a property owner, a function argument or an operand.
SRC = "MATCH p = (n:Q {id: 1})-[r:R]->(o:Q)"
PROJECTED = ["n", "r", "p", "n.id", "r.w", "1", "'x'", "null", "[n, o]", "[r]", "[n, 1]", "collect(n)", "collect(r)",
             "collect(n.id)", "count(n)", "coalesce(n, o)", "coalesce(n, null)", "coalesce(1, 2)",
             "CASE WHEN true THEN n ELSE o END", "CASE WHEN true THEN 1 ELSE 2 END", "{node: n}.node", "n{.id}",
             "[x IN [n] | x]", "[n, o][0]", "[n, o][0..1]", "nodes(p)", "nodes(p)[0]", "startNode(r)", "n = o",
             "EXISTS { (n)-->() }"]
SHAPES = {"with": SRC + " WITH {e} AS v {u}",
          "with-chain": SRC + " WITH {e} AS v WITH v {u}",
          "unwind-item": SRC + " UNWIND [{e}] AS v {u}",
          "unwind-list": SRC + " WITH [{e}] AS l UNWIND l AS v {u}",
          "call-return": SRC + " CALL (n, r, p, o) {{ RETURN {e} AS v }} {u}"}
USES = ["MATCH (v) RETURN count(*) AS c", "MATCH (v)-[:R]->(x) RETURN count(x) AS c",
        "OPTIONAL MATCH (v)-->(x) RETURN count(x) AS c", "MATCH ()-[v]->() RETURN count(*) AS c",
        "RETURN v.id AS x", "RETURN labels(v) AS x", "RETURN type(v) AS x", "RETURN length(v) AS x",
        "RETURN toUpper(v) AS x", "RETURN v + 1 AS x", "RETURN 1 IN v AS x", "RETURN v[0] AS x",
        "WITH v WHERE v RETURN 1 AS x"]
for e in PROJECTED:
    for shape, template in SHAPES.items():
        if shape.startswith("unwind") and e.startswith(("collect", "count")):
            continue
        for use in USES:
            add("projection-types", shape, template.replace("{e}", e).replace("{u}", use).replace("{{", "{").replace("}}", "}"))
for use in USES:
    add("projection-types", "yield", "CALL db.labels() YIELD label AS v " + use)

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


# 14. Predicate operands: AND / OR / XOR / NOT over pattern predicates,
# 12c. Pattern comprehensions (#1109): the WHERE takes any predicate, IN and
# NOT IN included, and the projection any expression, a list comprehension
# with its own IN included; each comprehension is a value in every position.
COMPREHENSION_PATTERNS = ["(n)-[:R]->(b)", "(b)<-[:R]-(n)", "(n)-[:R*1..2]->(b)", "(n)-->(b:Q)", "q = (n)-[:R]->(b)"]
COMPREHENSION_WHERES = ["", " WHERE b.id IN [2, 3]", " WHERE NOT b.id IN [2]", " WHERE b.id IN l",
                        " WHERE b.s IN ['b', 'x']", " WHERE b.id IN [x IN l WHERE x > 1]", " WHERE b.id = 2 OR b.id IN [3]",
                        " WHERE b:Q AND b.id IN [2, 3]", " WHERE b.s STARTS WITH 'b'", " WHERE any(x IN [2] WHERE x = b.id)",
                        " WHERE b.missing IS NULL AND 2 IN [b.id]"]
COMPREHENSION_PROJECTIONS = ["b.id", "[x IN [b.id] WHERE x IN [2, 3] | x * 10]", "b.id IN [2]"]
for pattern in COMPREHENSION_PATTERNS:
    for where in COMPREHENSION_WHERES:
        for projection in COMPREHENSION_PROJECTIONS + (["length(q)"] if pattern.startswith("q =") else []):
            e = f"[{pattern}{where} | {projection}]"
            place("pattern-comprehension", e, ["return", "with", "unwind"])
            place("pattern-comprehension", f"size({e}) > 0", ["where", "case"])

# EXISTS / COUNT subqueries and plain conditions. An operand is a pattern
# only when it is one whole relationship chain (#907): a parenthesized
# conjunction of subqueries that contains "-[" is not one.
PREDICATE_ATOMS = ["(a)-[:R]->(b)", "(a)-->()", "(b)-[:R]->(:Q)", "NOT (a)<--()",
                   "EXISTS { MATCH (a)-[:R]->(x) }", "NOT EXISTS { MATCH (b)-[:R]->(y) }",
                   "COUNT { (a)--() } > 1", "a.id = 1", "false", "true", "(b.id > 1)"]
PREDICATE_POSITIONS = {
    "match": "MATCH (a:Q)-[r:R]->(b:Q) WHERE {e} RETURN a.id AS x, b.id AS y ORDER BY x, y",
    "optional": "MATCH (a:Q) OPTIONAL MATCH (a)-[r:R]->(b:Q) WHERE {e} RETURN a.id AS x, b.id AS y ORDER BY x, y",
    "with": "MATCH (a:Q)-[r:R]->(b:Q) WITH a, b WHERE {e} RETURN a.id AS x ORDER BY x",
}
for left, right in itertools.product(PREDICATE_ATOMS, PREDICATE_ATOMS):
    for op in ["AND", "OR", "XOR"]:
        for e in [f"{left} {op} {right}", f"false OR ({left} {op} {right})", f"NOT ({left} {op} {right})"]:
            for position, template in PREDICATE_POSITIONS.items():
                add("predicate-operands", position, template.replace("{e}", e))
# The same chains over plain boolean operands, as a projected value.
BOOLEAN_ATOMS = ["true", "false", "null", "NOT true", "NOT null", "a.id = 1", "(b.id > 1)"]
for left, right in itertools.product(BOOLEAN_ATOMS, BOOLEAN_ATOMS):
    for op in ["AND", "OR", "XOR"]:
        for e in [f"NOT ({left} {op} {right})", f"NOT (NOT {left} {op} {right})", f"NOT ({left} {op} NOT {right})"]:
            add("predicate-operands", "return", f"MATCH (a:Q)-[r:R]->(b:Q) RETURN {e} AS v ORDER BY a.id")

# 13. UNWIND of an expression as written, not wrapped in a list: a property,
# a property list joined with another list, a slice, a function's list, a
# missing property, a map member, a variable.
UNWIND_SOURCES = ["n.l", "n.l + n.l", "n.l + [4]", "[0] + n.l", "(n.l + n.l)", "n.l + l", "l + n.l",
                  "n.l[0..2] + n.l", "n.l[1..]", "coalesce(n.l, []) + [9]", "reverse(n.l + [4])",
                  "n.missing", "n.l + n.missing", "n.s", "o.l", "m.a", "m.a + 1", "n.`l`", "n.`l` + [5]",
                  "l", "l + l", "[x IN n.l | x * 2] + n.l", "CASE WHEN true THEN n.l ELSE [] END + [7]"]
for e in UNWIND_SOURCES:
    add("unwind-source", "unwind", PREFIX + f" UNWIND {e} AS v RETURN v")
    add("unwind-source", "unwind-count", PREFIX + f" UNWIND {e} AS v RETURN count(v) AS c")

# 16. Selected shortest paths (#1212): SHORTEST k, ANY SHORTEST, ALL SHORTEST
# and SHORTEST k GROUPS over quantified path patterns, quantified and
# variable-length relationships, with a bound or unbound end, both ends
# bound, a cycle back to the start, filters on the end, length bounds, and
# no path. Each statement creates its own small graph (a directed triangle
# 1->2->3->1 with the chord 1->3, a tail 3->4, a cycle 4->5->6->4, a :U
# shortcut 2->5 and the isolated 7) and is rolled back. A SHORTEST k may take
# any of the paths tied at its last length, so only the lengths and end
# nodes of its paths are returned; ALL SHORTEST and GROUPS take every path
# of their lengths, so their nodes are returned too.
SHORTEST_GRAPH = ("CREATE (s1:S {id: 1}), (s2:S {id: 2}), (s3:S {id: 3}), (s4:S {id: 4}), (s5:S {id: 5}), "
                  "(s6:S {id: 6}), (s7:S {id: 7}), (s1)-[:T {w: 1}]->(s2), (s2)-[:T {w: 2}]->(s3), "
                  "(s3)-[:T {w: 3}]->(s1), (s1)-[:T {w: 2}]->(s3), (s3)-[:T {w: 1}]->(s4), (s4)-[:T {w: 2}]->(s5), "
                  "(s5)-[:T {w: 3}]->(s6), (s6)-[:T {w: 1}]->(s4), (s2)-[:U {w: 5}]->(s5) WITH count(*) AS created ")
SHORTEST_SELECTORS = ["ANY SHORTEST", "SHORTEST 1", "SHORTEST 2", "SHORTEST 3", "ALL SHORTEST", "SHORTEST 2 GROUPS",
                      "SHORTEST 1 PATHS"]
SHORTEST_PATTERNS = ["(()-[:T]->())+", "(()-[:T]-())+", "(()<-[:T]-())+", "(()-[:T]->()){2,}", "(()-[:T]-())*",
                     "(()-[:T|U]->())+", "(()-[r:T WHERE r.w > 1]-())+", "((x)-[:T]->(y) WHERE x.id < y.id)+",
                     "(()-[:T]-(x WHERE x.id <> 3))+", "-[:T]->+", "-[:T*]-", "-[:T]->{1,3}",
                     "-[:T]->(m)(()-[:T]-())+", "(()-[:T]->()-[:T]->())+"]
SHORTEST_ENDS = {
    "start": ("(a:S {id: 1})", "(b)", "b.id AS e"),
    "end": ("(a)", "(b:S {id: 5})", "a.id AS e"),
    "pair": ("(a:S {id: 1})", "(b:S {id: 5})", "a.id AS e"),
    "cycle": ("(a:S {id: 1})", "(a)", "a.id AS e"),
    "none": ("(a:S {id: 4})", "(b:S {id: 7})", "b.id AS e"),
    "filtered": ("(a:S {id: 2})", "(b:S WHERE b.id >= 4)", "b.id AS e"),
}
for selector in SHORTEST_SELECTORS:
    whole = "GROUP" in selector or selector.startswith("ALL")
    for pattern in SHORTEST_PATTERNS:
        for position, (start, end, projected) in SHORTEST_ENDS.items():
            match = f"MATCH p = {selector} {start}{pattern}{end}"
            add("shortest-selector", position, SHORTEST_GRAPH + match + f" RETURN {projected}, length(p) AS l ORDER BY e, l", mode="tx")
            if whole:
                add("shortest-selector", position + "-nodes", SHORTEST_GRAPH + match
                    + " RETURN [n IN nodes(p) | n.id] AS ns ORDER BY ns", mode="tx")
# Length bounds on the clause's WHERE, a bound start from an earlier MATCH,
# an end filter in the WHERE, and a parameter-free bound expression.
for selector in ["SHORTEST 1", "SHORTEST 2", "ALL SHORTEST", "SHORTEST 2 GROUPS"]:
    for pattern in ["(()-[:T]-())+", "(()-[:T]->())+", "-[:T]-+", "(()-[:T|U]-())*"]:
        for where in ["length(p) <= 2", "length(p) < 2", "2 >= length(p)", "length(p) <= 1 + 1", "length(p) <= 0",
                      "length(p) <= 2.5", "b.id > 2", "b.id > 2 AND length(p) <= 3", "length(p) > 2"]:
            add("shortest-selector", "where", SHORTEST_GRAPH + f"MATCH p = {selector} (a:S {{id: 1}}){pattern}(b) WHERE {where}"
                " RETURN b.id AS e, length(p) AS l ORDER BY e, l", mode="tx")
            add("shortest-selector", "bound-start", SHORTEST_GRAPH + f"MATCH (a:S {{id: 3}}) MATCH p = {selector} (a){pattern}(b:S)"
                f" WHERE {where} RETURN count(*) AS c, sum(length(p)) AS l", mode="tx")
# Pre-filters on the whole path (a parenthesised path's WHERE) select among
# the paths that hold them; the clause's WHERE filters the selected ones.
for selector in ["SHORTEST 1", "ALL SHORTEST", "SHORTEST 2 GROUPS"]:
    for inner, outer in [("((a:S {id: 1})(()-[:T]-(n))+(b:S {id: 4}) WHERE none(x IN n WHERE x.id = 3))", ""),
                         ("(a:S {id: 1})(()-[:T]-(n))+(b:S {id: 4})", " WHERE none(x IN n WHERE x.id = 3)"),
                         ("((a:S {id: 1})(()-[r:T]-())+(b) WHERE size(r) > 2)", ""),
                         ("(a:S {id: 1})(()-[r:T]-())+(b)", " WHERE size(r) > 2")]:
        add("shortest-selector", "path-filter", SHORTEST_GRAPH + f"MATCH p = {selector} {inner}{outer}"
            " RETURN length(p) AS l ORDER BY l", mode="tx")
# System statement policy and administration transactions share one owner.
# Database-scoped statements cover session and USE routes. Sequences retain
# one transaction, so routing and incompatible statement types are compared.
for schema in ["CREATE INDEX review_sweep FOR (n:Review) ON (n.x)",
               "CREATE CONSTRAINT review_sweep FOR (n:Review) REQUIRE n.x IS UNIQUE",
               "DROP INDEX review_sweep IF EXISTS", "MATCH (n:Review) RETURN count(n) AS n"]:
    for mode in ["", "EXPLAIN ", "PROFILE "]:
        query = mode + schema
        add("system-administration", "session", query, "tx")
        cases[-1]["database"] = "system"
        add("system-administration", "use", "USE system " + query, "tx")
for statements in [
        ["SHOW USERS", "CALL dbms.components()"],
        ["SHOW USERS", "RETURN 1 AS n"],
        ["SHOW USERS", "EXPLAIN CREATE (:Review)"],
        ["RETURN 1 AS n", "SHOW USERS"],
        ["SHOW DATABASES YIELD name WHERE name = 'system' RETURN count(*) AS n"] * 2]:
    query = "; ".join(statements)
    add("system-administration", "transaction", query, "tx")
    cases[-1]["statements"] = statements

# TemporalBinding: a named-zone overlap must retain the selected offset through every
# mutation source and action. WITH-bound maps exercise the same expression
# paths as parameter maps; the Bolt regression supplies driver temporals.
for text in ["2026-10-25T01:30:00+01:00[Europe/London]",
             "2026-10-25T01:30:00.123456789+01:00[Europe/London]",
             "2026-10-25T01:30:00Z[Europe/London]",
             "2026-11-01T01:30:00.123456789-04:00[America/New_York]",
             "2026-11-01T01:30:00.123456789-05:00[America/New_York]",
             "2026-10-25T01:30:00[Europe/London]",
             "2026-03-29T01:30:00[Europe/London]",
             "2026-11-01T01:30:00[America/New_York]",
             "2026-03-08T02:30:00[America/New_York]"]:
    dt = f"datetime('{text}')"
    add("temporal-map-offset", "constructor", f"WITH {dt} AS d RETURN d.epochSeconds AS e, toString(d) AS s")
    for relationship in [False, True]:
        pattern = "(n:TemporalBinding {id:'v'})"
        lead = ""
        if relationship:
            pattern = "(a)-[n:TemporalBinding {id:'v'}]->(b)"
            lead = "MATCH (a:Q {id:1}), (b:Q {id:2}) "
        for source in ["m", "{t:d, ts:[d]}", f"{{t:{dt}, ts:[{dt}]}}"]:
            binding = f"WITH {dt} AS d WITH d, {{t:d, ts:[d]}} AS m "
            for route in ["create", "property", "replace", "create-add", "merge", "where-merge", "on-create", "on-match"]:
                prefix = binding + lead
                if route == "create":
                    body = "CREATE " + ("(a)-[n:TemporalBinding {id:'v', t:d, ts:[d]}]->(b)" if relationship else "(n:TemporalBinding {id:'v', t:d, ts:[d]})")
                elif route == "property":
                    body = "CREATE " + pattern + " SET n.t=d, n.ts=[d]"
                elif route == "replace":
                    body = "CREATE " + pattern + f" SET n={source}, n.id='v'"
                elif route == "create-add":
                    body = "CREATE " + pattern + f" SET n += {source}"
                elif route == "merge":
                    body = "MERGE " + pattern + f" SET n += {source}"
                elif route == "where-merge":
                    prefix = binding + "WITH d, m WHERE m.t IS NOT NULL " + lead
                    body = "MERGE " + pattern + f" SET n += {source}"
                elif route == "on-create":
                    body = "MERGE " + pattern + f" ON CREATE SET n += {source}"
                else:
                    body = "CREATE " + pattern + " WITH " + ("a, b, " if relationship else "") + "d, m MERGE " + pattern + f" ON MATCH SET n += {source}"
                query = prefix + body + " RETURN n.t.epochSeconds AS e, toString(n.t) AS s, n.ts[0].epochSeconds AS le, toString(n.ts[0]) AS ls"
                add("temporal-map-offset", ("rel-" if relationship else "node-") + route, query, "tx")


for text in ["2026-10-25T01:30:00+02:00[Europe/London]",
             "2026-03-29T01:30:00Z[Europe/London]",
             "2026-03-29T01:30:00+01:00[Europe/London]",
             "2026-03-08T02:30:00-05:00[America/New_York]",
             "2026-03-08T02:30:00-04:00[America/New_York]",
             "2026-11-01T01:30:00-06:00[America/New_York]",
             "2026-07-01T12:00:00Z[Europe/London]",
             "2026-01-01T12:00:00+01:00[Europe/London]"]:
    add("temporal-map-offset", "conflicting-zone-offset", f"RETURN datetime('{text}')")

for value in ["point({x:1.25,y:2.5})", "[true,false]"]:
    for relationship in [False, True]:
        pattern = "(n:TypedBinding)"
        read = "MATCH (n:TypedBinding)"
        if relationship:
            pattern = "(a:TypedStart)-[n:TYPED_BINDING]->(b:TypedEnd)"
            read = "MATCH (:TypedStart)-[n:TYPED_BINDING]->(:TypedEnd)"
        add("temporal-map-offset", "typed-predicate", f"WITH {value} AS p CREATE {pattern} SET n.v=p WITH p {read} WHERE n.v=p MERGE (h:TypedHit) RETURN count(h)", "tx")
        add("temporal-map-offset", "typed-case", f"WITH {value} AS p MERGE {pattern} SET n.v=CASE WHEN true THEN p ELSE null END RETURN n.v=p", "tx")

# Correlated subquery bodies (#1317 slice 7): every body shape a statement
# shortcut accepts (label count, typed relationship count, LIMIT projection,
# filtered count) over an imported node (a) or scalar (k), as EXISTS, COUNT,
# COLLECT and CALL, plus uncorrelated controls that keep the shortcuts.
CORRELATED_OUTER = "MATCH (a:Q) WITH a, a.id AS k"
for match, ret in [("MATCH (a:Q)", "RETURN count(a) AS c"),
                   ("MATCH (a)-[r:R]->()", "RETURN count(r) AS c"),
                   ("MATCH (a)<-[r:R]-()", "RETURN count(r) AS c"),
                   ("MATCH (a)", "RETURN a.id AS c LIMIT 5"),
                   ("MATCH (b:Q) WHERE b.id > k", "RETURN count(b) AS c"),
                   ("MATCH (b:Q) WHERE b.id > a.id", "RETURN count(b) AS c"),
                   ("MATCH (b:Q {id: k})", "RETURN count(b) AS c"),
                   ("MATCH (b:Q {id: a.id})", "RETURN b.id AS c LIMIT 5"),
                   ("MATCH (b:Q)", "RETURN count(b) AS c"),
                   ("MATCH ()-[r:R]->()", "RETURN count(r) AS c"),
                   ("MATCH (b:Q)", "RETURN b.id AS c ORDER BY c LIMIT 2")]:
    add("correlated-body", "exists", f"{CORRELATED_OUTER} RETURN k AS id, EXISTS {{ {match} }} AS v ORDER BY id")
    add("correlated-body", "count", f"{CORRELATED_OUTER} RETURN k AS id, COUNT {{ {match} }} AS v ORDER BY id")
    add("correlated-body", "collect", f"{CORRELATED_OUTER} RETURN k AS id, COLLECT {{ {match} {ret} }} AS v ORDER BY id")
    add("correlated-body", "where", f"{CORRELATED_OUTER} WHERE COUNT {{ {match} }} > 0 RETURN k AS id ORDER BY id")
    add("correlated-body", "call", f"{CORRELATED_OUTER} CALL (a, k) {{ {match} {ret} }} RETURN k AS id, c AS v ORDER BY id, v")

out_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=9) as out:
    json.dump({"setup": SETUP, "cases": cases}, out, separators=(",", ":"))
counts = {}
for case in cases:
    counts[case["family"]] = counts.get(case["family"], 0) + 1
print(len(cases), counts)
