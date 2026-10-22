package callscopetest

func ResponsibilityCases() []Case {
	var cases []Case
	add := func(name, query string) {
		cases = append(cases, Case{Name: "responsibility/" + name, Query: query, Reference: query})
	}
	for _, header := range []string{"CALL (i)", "CALL (*)", "CALL"} {
		prefix := ""
		if header == "CALL" {
			prefix = "WITH i "
		}
		add("optional-filter/"+header, "UNWIND [1,2] AS i OPTIONAL "+header+" { "+prefix+"WITH i WHERE i>1 RETURN i AS j } RETURN i,j")
	}
	add("optional-unwind", "UNWIND [1,2] AS i OPTIONAL CALL (i) { UNWIND range(1,i) AS k RETURN k } RETURN i,k")
	add("optional-aggregate", "UNWIND [1,2] AS i OPTIONAL CALL (i) { RETURN count(*) AS c } RETURN i,c")
	for _, expression := range []string{"abs(-7)", "size('1234567')", "toInteger('7')", "coalesce(null,7)"} {
		add("computed-match/"+expression, "MATCH (node:CallScopeOracleTarget {n:"+expression+",oracleRun:$oracleRun}) CALL { WITH node RETURN node.n AS wanted } RETURN wanted")
	}
	add("return-names", "CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate")
	add("nested-return-names", "CALL { CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate } RETURN duplicate")
	add("quoted-return-names", "CALL { RETURN 1 AS `a a`,2 AS `a a` } RETURN `a a`")
	add("dynamic-return-names", "CALL { CALL db.labels() YIELD * RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate")
	add("dynamic-nested-return-names", "CALL { CALL db.labels() YIELD * CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate } RETURN duplicate")
	add("first-union-import-filter", "WITH 1 AS imported CALL { WITH imported WHERE imported>0 RETURN imported+1 AS v UNION WITH imported RETURN imported+2 AS v } RETURN v")
	for _, projection := range []string{"WITH imported WHERE imported>0 RETURN imported+2 AS v", "WITH imported AS renamed RETURN renamed+2 AS v"} {
		query := "WITH 1 AS imported CALL { WITH imported RETURN imported+1 AS v UNION " + projection + " } RETURN v ORDER BY v"
		cases = append(cases, Case{Name: "responsibility/later-union/" + projection, Query: query, Reference: query, Compatibility: "WITH 1 AS imported CALL (imported) { WITH imported RETURN imported+1 AS v UNION " + projection + " } RETURN v ORDER BY v", Extension: "907-later-union-import"})
	}
	add("quoted-reference", "WITH 1 AS `outer name` CALL (`outer name`) { RETURN `outer name`+1 AS value } RETURN value")
	add("string-is-not-reference", "CALL { RETURN 'not a variable: `missing`' AS value } RETURN value")
	return cases
}
