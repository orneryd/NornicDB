// Package statementfactstest defines the public scope/header correction matrix.
// Native execution, Bolt and HTTP replay the same shapes and expected statuses.
package statementfactstest

import "fmt"

type Case struct {
	Name, Query, Code string
	Columns           []string
}

func Cases() []Case {
	var cases []Case
	add := func(name, query string, columns []string, valid bool) {
		code := ""
		if !valid {
			code = "Neo.ClientError.Statement.SyntaxError"
		}
		cases = append(cases, Case{Name: name, Query: query, Columns: columns, Code: code})
	}
	for i, alias := range []struct{ text, name string }{
		{"kind", "kind"}, {"`kind`", "kind"}, {"`a,b`", "a,b"}, {"`a AS b`", "a AS b"}, {"`a``b`", "a`b"}, {"`UNION`", "UNION"}, {"`écho`", "écho"},
	} {
		for gap, space := range []string{" ", " /* UNION RETURN */ ", " // UNION RETURN\n "} {
			query := "CALL db.labels() YIELD" + space + "`label` AS" + space + alias.text + " RETURN *"
			add(fmt.Sprintf("yield_%d_%d", i, gap), query, []string{alias.name}, true)
			for _, union := range []string{"UNION", "UNION ALL"} {
				add(fmt.Sprintf("yield_%d_%d_%s", i, gap, union), query+" "+union+" "+query, []string{alias.name}, true)
			}
		}
	}
	add("union_in_comment", "RETURN 'UNION RETURN * CALL { }' AS x /* UNION RETURN y */", []string{"x"}, true)
	add("union_line_comment", "RETURN 1 AS x // UNION RETURN missing\n", []string{"x"}, true)
	add("quoted_import", "WITH 1 AS `x,y` CALL (`x,y`) { WITH 2 AS y RETURN `x,y` + y AS r } RETURN r", []string{"r"}, true)
	add("with_star_known_empty", "CALL { CREATE (:CanonicalEmptyScope) } WITH * RETURN 1 AS value", []string{"value"}, true)
	add("with_star_known_empty_added", "CALL { CREATE (:CanonicalEmptyScope) } WITH *, 2 AS item RETURN item AS value", []string{"value"}, true)
	tails := []string{
		"UNWIND [x] AS r RETURN r",
		"MATCH (n:HeaderBoundMissing) WHERE n.value = x RETURN y AS r",
		"CALL db.labels() YIELD label WHERE x = 1 RETURN y AS r",
		"RETURN y AS r ORDER BY x",
		"MATCH (n:HeaderBoundMissing) SET n.value = x RETURN y AS r",
		"CREATE (n:HeaderBoundCreated {value: x}) RETURN y AS r",
		"MATCH (n:HeaderBoundMissing) DELETE x RETURN y AS r",
		"FOREACH (item IN [x] | CREATE (:HeaderBoundCreated {value:item})) RETURN y AS r",
	}
	for seed, prefix := range []string{"WITH 1 AS x", "UNWIND [] AS x"} {
		for mode, importing := range []string{"WITH x ", "WITH * ", "(x)", "(*)"} {
			for read, tail := range tails {
				call := "CALL { " + importing + "WITH 2 AS y " + tail + " }"
				valid := mode >= 2
				if valid {
					call = "CALL " + importing + " { WITH 2 AS y " + tail + " }"
				}
				// DELETE of a bound integer is a runtime type error. Its retained control
				// checks the same scope through a harmless arithmetic projection instead.
				if valid && read == 6 {
					call = "CALL " + importing + " { WITH 2 AS y RETURN x + y AS r }"
				}
				add(fmt.Sprintf("binding_%d_%d_%d", seed, mode, read), prefix+" "+call+" RETURN x, r", []string{"x", "r"}, valid)
			}
		}
	}
	for index, query := range []string{
		"WITH 1 AS outerValue RETURN EXISTS { WITH 2 AS innerValue RETURN outerValue } AS value",
		"WITH 1 AS outerValue RETURN COUNT { WITH 2 AS innerValue RETURN outerValue } AS value",
		"RETURN [item IN [1,2] | COUNT { WITH 2 AS hidden RETURN item }] AS value",
		"RETURN reduce(total=0,item IN [1,2] | total + COUNT { RETURN item }) AS value",
		"RETURN EXISTS { (local:CanonicalPattern)-[:R]->() } AS value",
		"RETURN COUNT { (local:CanonicalPattern)-[:R]->() } AS value",
	} {
		add(fmt.Sprintf("expression_scope_%d", index), query, []string{"value"}, true)
	}
	for index, query := range []string{
		"MATCH (node:CanonicalPattern) RETURN [(node)-->(d) | d.id] AS value",
		"MATCH (node:CanonicalPattern) RETURN [p = (node)-->(d) WHERE d.id IS NOT NULL | length(p)] AS value",
		"RETURN [item IN [1,2] WHERE item > 0 | item] AS value",
		"WITH 9 AS item RETURN [item IN [item] | item] AS value",
		"RETURN reduce(total=1,item IN [1,2] | total + item) AS value",
		"WITH 1 AS item RETURN any(item IN [item] WHERE item > 0) AS value",
	} {
		add(fmt.Sprintf("local_scope_%d", index), query, []string{"value"}, true)
	}
	for index, query := range []string{
		"RETURN [item IN missing | item] AS value",
		"RETURN [item IN [1] | item] AS value, item",
		"MATCH (node:CanonicalPattern) RETURN [(node)-->(d) | d.id] AS value, d",
		"RETURN reduce(total=0,item IN [1] | total + item) AS value, total",
	} {
		add(fmt.Sprintf("local_scope_undefined_%d", index), query, nil, false)
	}
	return cases
}
