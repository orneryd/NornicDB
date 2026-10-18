CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C {id:9});
MATCH (n:A) CALL () { RETURN size([(n)-->() | 1]) AS c } WITH n.id AS k, c ORDER BY k RETURN 'R09' AS q, collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r;
