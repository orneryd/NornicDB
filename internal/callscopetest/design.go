package callscopetest

// DesignCase pins public results independently of the production scope owner.
// These include Cypher 25 consumers that are outside the Neo4j 5.26 corpus.
type DesignCase struct {
	Name, Query string
	Columns     []string
	Rows        [][]interface{}
}

func DesignCases() []DesignCase {
	return []DesignCase{
		{"create_export", "CALL () { RETURN 1 AS a } CREATE (:ScopeDesign {n:a}) RETURN a", []string{"a"}, [][]interface{}{{int64(1)}}},
		{"merge_export", "CALL () { RETURN 1 AS a } MERGE (n:ScopeDesign {n:a}) SET n.seen=a RETURN n.seen AS seen", []string{"seen"}, [][]interface{}{{int64(1)}}},
		{"delete_export", "CREATE (n:ScopeDesign) WITH n CALL (n) { RETURN n AS victim } DELETE victim RETURN 1 AS kept", []string{"kept"}, [][]interface{}{{int64(1)}}},
		{"sequential_let", "CYPHER 25 WITH 1 AS a CALL (a) { LET b=a, c=b+1 RETURN c } RETURN c", []string{"c"}, [][]interface{}{{int64(2)}}},
		// Both aliases are new body bindings, including a name that initially
		// denoted a weak implicit import. Later scalars must retain that identity.
		{"let_new_local", "CYPHER 25 CREATE (left:LetScopeNode), (right:LetScopeNode) WITH left AS selected CALL { LET n=selected, c=COUNT { MATCH (n:LetScopeNode) RETURN n } RETURN c } RETURN c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"let_replaces_weak", "CYPHER 25 CREATE (left:LetScopeNode), (right:LetScopeNode) WITH left AS selected, 9 AS n CALL { LET n=selected, c=COUNT { MATCH (n:LetScopeNode) RETURN n } RETURN c } RETURN c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"fused_with", "WITH 1 AS a CALL (a) { UNWIND [0] AS z WITH 2 AS x WITH * RETURN * UNION RETURN 3 AS x } RETURN a,x ORDER BY x", []string{"a", "x"}, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}}},
		{"conditional", "CYPHER 25 WITH 1 AS a CALL (a) { WHEN true THEN WITH a AS x RETURN * ELSE WITH a AS x RETURN * } RETURN a,x", []string{"a", "x"}, [][]interface{}{{int64(1), int64(1)}}},
		{"lexical_source", "WITH 1 AS a CALL (a) { RETURN [a IN [a] | a] AS xs } RETURN a,xs", []string{"a", "xs"}, [][]interface{}{{int64(1), []interface{}{int64(1)}}}},
		{"explicit_after_with", "WITH 11 AS x CALL (x) { WITH 2 AS local RETURN x AS saved } RETURN saved", []string{"saved"}, [][]interface{}{{int64(11)}}},
		{"distinct_order", "WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x RETURN x*y AS a } RETURN a ORDER BY a", []string{"a"}, [][]interface{}{{int64(22)}, {int64(33)}}},
		{"rebound_horizon", "WITH 1 AS a CALL (a) { UNWIND [2] AS a WITH a AS v RETURN * } RETURN a,v", []string{"a", "v"}, [][]interface{}{{int64(1), int64(2)}}},
		{"nested_export_shadow", "WITH 1 AS a CALL { CALL { RETURN 2 AS a } RETURN a AS b } RETURN a,b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
	}
}
