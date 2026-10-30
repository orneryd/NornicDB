package querytextmatrix

// BraceCourseChangeCases keeps the rewrite regressions identical on public routes.
func BraceCourseChangeCases(visit func(MapBraceCase)) {
	for _, value := range []string{"a.k", "coalesce(a.k, 1)", "a{.k}"} {
		for _, sibling := range []string{",", ",, other: 2", ", other:"} {
			inner := "UNWIND [] AS x MATCH (a)-->(b {k: " + value + sibling + "}) RETURN b AS out"
			for _, query := range []string{
				inner, "EXPLAIN " + inner,
				inner + " UNION RETURN null AS out",
				"CYPHER 25 " + inner + " NEXT RETURN out",
				"CYPHER 25 { " + inner + " } UNION { RETURN null AS out }",
				"CYPHER 25 CALL { { " + inner + " } } RETURN out",
			} {
				visit(MapBraceCase{Name: query, Query: query, SyntaxError: true})
			}
		}
	}
	for _, query := range []string{
		"UNWIND [] AS x MATCH (a)-->(b {k: a.k,}) RETURN b AS out",
		"EXPLAIN MATCH (a)-->(b {k: a.k,}) RETURN b AS out",
		"UNWIND [1] AS row CREATE (n {k: 1,}) RETURN n AS out",
	} {
		visit(MapBraceCase{Name: query, Query: query, SyntaxError: true})
	}
	for _, name := range []string{"count", "exists", "collect"} {
		query := "WITH {k: 1} AS " + name + " RETURN " + name + "{.k} AS out"
		visit(MapBraceCase{Name: name, Query: query, Alias: "out", Want: map[string]interface{}{"k": int64(1)}})
	}
}
