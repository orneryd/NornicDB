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
	for index, query := range []string{
		"RETURN 1 AS ``",
		"WITH 1 AS `` RETURN ``",
		"WITH 1 AS `` RETURN *",
		"UNWIND [1] AS `` RETURN ``",
		"CALL db.labels() YIELD label AS `` RETURN ``",
		"CALL db.labels() YIELD label AS ``",
		"CALL { RETURN 1 AS `` } RETURN *",
		"MATCH (n:EmptyNameMissing) RETURN count(n) AS ``",
		"RETURN 1 AS `` UNION RETURN 2 AS ``",
	} {
		add(fmt.Sprintf("empty_name_%d", index), query, []string{""}, true)
	}
	add("empty_name_scoped_import", "WITH 1 AS `` CALL (``) { RETURN `` AS value } RETURN value", []string{"value"}, true)
	for index, sample := range []struct {
		query   string
		columns []string
		valid   bool
	}{
		{"UNWIND [1,2,2] AS x WITH DISTINCT x AS y WHERE x > 1 RETURN y", []string{"y"}, true},
		{"UNWIND [] AS x WITH DISTINCT x AS y WHERE x > 1 RETURN y", []string{"y"}, true},
		{"UNWIND [1,2,2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c", []string{"y", "c"}, true},
		{"UNWIND [1,2,2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x + 1", []string{"y", "c"}, true},
		{"UNWIND [1,2,2] AS x RETURN count(DISTINCT x) AS c ORDER BY COUNT(distinct x)", []string{"c"}, true},
		{"UNWIND [1,2,2] AS x WITH x AS y WHERE x > 1 RETURN y", []string{"y"}, true},
		{"UNWIND [1,2,2] AS x WITH count(*) AS c WHERE x > 0 RETURN c", nil, false},
		{"UNWIND [1,2,2] AS x WITH DISTINCT x + 1 AS y WHERE x > 0 RETURN y", nil, false},
		{"UNWIND [1,2,2] AS x RETURN count(*) AS c ORDER BY x", nil, false},
		{"UNWIND [1,2,2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x", nil, false},
		{"UNWIND [1,2,2] AS x RETURN count(*) AS c ORDER BY sum(x)", nil, false},
		{"UNWIND [1,2,2] AS x WITH DISTINCT x + 1 AS y ORDER BY x RETURN y", nil, false},
		{"WITH 1 AS threshold, [1,2] AS values WITH [x IN values WHERE x > threshold] AS filtered RETURN filtered", []string{"filtered"}, true},
		{"UNWIND [x IN [1] WHERE missing > x] AS missing RETURN missing", nil, false},
	} {
		add(fmt.Sprintf("aggregation_scope_%d", index), sample.query, sample.columns, sample.valid)
	}
	add("quoted_import", "WITH 1 AS `x,y` CALL (`x,y`) { WITH 2 AS y RETURN `x,y` + y AS r } RETURN r", []string{"r"}, true)
	add("with_star_known_empty", "CALL { CREATE (:CanonicalEmptyScope) } WITH * RETURN 1 AS value", []string{"value"}, true)
	add("with_star_known_empty_added", "CALL { CREATE (:CanonicalEmptyScope) } WITH *, 2 AS item RETURN item AS value", []string{"value"}, true)
	for index, sample := range []struct{ prefix, expression string }{
		{"", "1 /* column */ + 2"},
		{"", "(1 /* column */ + 2)"},
		{"", "size([1, /* column */ 2])"},
		{"", "[1, /* column */ 2]"},
		{"", "{key: 1 /* column */}"},
		{"", "1 // column\n + 2"},
		{"WITH 1 AS `a b` ", "`a b` /* column */ + 2"},
	} {
		query := sample.prefix + "RETURN " + sample.expression
		add(fmt.Sprintf("source_column_%d", index), query, []string{sample.expression}, true)
		for _, union := range []string{"UNION", "UNION ALL"} {
			add(fmt.Sprintf("source_column_%d_%s", index, union), query+" "+union+" "+query, []string{sample.expression}, true)
		}
	}
	for _, union := range []string{"UNION", "UNION ALL"} {
		add("source_column_mismatch_"+union, "RETURN 1 /* column */ + 2 "+union+" RETURN 1   + 2", nil, false)
	}
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
