CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C {id:9});
MATCH (n:A) CALL (n) { MATCH (m:A) WHERE m.id = `n`.id RETURN count(m) AS c } WITH n.id AS k, c ORDER BY k RETURN 'R21' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN COUNT { MATCH (m:A) WHERE m.id = `n`.id } AS c } WITH n.id AS k, c ORDER BY k RETURN 'R22' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
MATCH (n:A) CALL (n) { RETURN COUNT { MATCH (m:A {id: `n`.id}) } AS c } WITH n.id AS k, c ORDER BY k RETURN 'R23' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
