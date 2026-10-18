CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C {id:9});
MATCH (n:A) CALL (n) { MATCH (m:B) WHERE EXISTS { (m)-->(n) } WITH count(m) AS t RETURN t + n.id AS c } WITH n.id AS k, c ORDER BY k RETURN 'R14' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { MATCH (m:A) WHERE EXISTS { (m {id: n.id}) } RETURN count(m) AS c } WITH n.id AS k, c ORDER BY k RETURN 'R15' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN COLLECT { MATCH (m:A) WHERE m.id = n.id RETURN [n IN [5] | n][0] + m.id AS v }[0] AS c } WITH n.id AS k, c ORDER BY k RETURN 'R16' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN COLLECT { UNWIND [1] AS k RETURN n.id AS v }[0] AS c } WITH n.id AS k, c ORDER BY k RETURN 'R17' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN EXISTS { MATCH (m:A) WHERE m.id = n.id } AS c } WITH n.id AS k, c ORDER BY k RETURN 'R18' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { MATCH (m:A) RETURN sum(COUNT { MATCH (m) WHERE m = n }) AS c } WITH n.id AS k, c ORDER BY k RETURN 'R19' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN COLLECT { MATCH (n:C) RETURN 10 AS v UNION ALL RETURN n.id AS v }[0] AS c } WITH n.id AS k, c ORDER BY k RETURN 'R20' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
