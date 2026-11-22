// Close-out lane L8, round 2 public statements on v1.5.1 (copied from the pin branch). Test-only, not for merge.
package cypher

import "testing"

func TestL8_H4_014_ExactG3(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, l8SetupG3...)
		q := "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN n.id AS e0, substring(toString(m.x), 1) AS e1, m.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 ORDER BY e0, e1, e2 SKIP 3 LIMIT 2"
		o := l8Run(t, exec, q, nil)
		// Neo4j 5.26.30 (lead): [[1, "", null], [1, "", null]].
		if o.Err {
			t.Errorf("Neo4j gives 2 rows with null; NornicDB refused: %s %s", o.Code, o.Msg)
		}
		// Diag: the divisor and its parts per row.
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN DISTINCT n.w AS w, coalesce(false, n.w) AS c, toInteger(coalesce(false, n.w)) AS ti, o.id AS oid, toInteger(o.id) AS toid", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN count(*) AS rows, count(o) AS withO", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN m.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN toInteger(o.id) % 0 AS v LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C) OPTIONAL MATCH (n)<-[q:R]-(o:C) WITH n, o WHERE o IS NOT NULL RETURN toInteger(o.id) % 0 AS v LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C {id: 3}) RETURN coalesce(false, n.w) AS c, toInteger(coalesce(false, n.w)) AS ti", nil)
	})
}

func TestL8_H4_015_ExactG2(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, l8SetupG2...)
		q := "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0, '' AS e1"
		o := l8Run(t, exec, q, nil)
		// Neo4j 5.26.30 (lead): [] (no row has m.w = 'a').
		if o.Err {
			t.Errorf("Neo4j gives []; NornicDB refused: %s %s", o.Code, o.Msg)
		}
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE m.w = 'a' RETURN count(*) AS c", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) RETURN count(*) AS c", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + 1) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WITH n, m WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (m:B)<-[r:S]-(n) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) AND (m.w = 'a') RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE m.w = 'a' AND n.x + true RETURN n.x AS e0", nil)
	})
}

func TestL8_H4_018_Member01075(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "UNWIND range(1, 10) AS i CREATE ({nr: i})")
		// Neo4j 5.26.30 (lead): ArgumentError "SKIP: Invalid input. '-2' is not a valid value".
		l8WantCode(t, exec, "MATCH (n) WITH n SKIP toInteger(rand()*-9) WITH count(*) AS count RETURN count > 0 AS nonEmpty", "Neo.ClientError.Statement.ArgumentError")
	})
}

