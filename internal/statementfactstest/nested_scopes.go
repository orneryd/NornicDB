package statementfactstest

import "fmt"

// NestedScopeCases exercises the same transfer at every nested scope: inputs
// are dependencies, exported names bind after the construct, and private names
// cannot retain a containing CALL's otherwise unused input.
func NestedScopeCases() []ProjectionPermutation {
	return append(append(NestedScopePrivateCases(), NestedScopeReadCases()...), CallImportAdmissionCases()...)
}

func NestedScopePrivateCases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	constructs := []struct{ name, private, export string }{
		{"call_empty", "CALL () { WITH 2 AS value RETURN 1 AS kept }", "CALL () { RETURN 2 AS value }"},
		{"call_unscoped", "CALL { WITH 2 AS value RETURN 1 AS kept }", "CALL { RETURN 2 AS value }"},
		{"call_all", "CALL (*) { WITH 2 AS value RETURN 1 AS kept }", "CALL (*) { RETURN 2 AS value }"},
		{"call_listed", "WITH 1 AS seed CALL (seed) { WITH 2 AS value RETURN 1 AS kept }", "WITH 1 AS seed CALL (seed) { RETURN 2 AS value }"},
		{"call_wildcard", "CALL () { WITH 2 AS value RETURN 1 AS kept }", "CALL () { WITH 2 AS value RETURN * }"},
		{"exists", "WITH EXISTS { WITH 2 AS value RETURN value AS private } AS kept", ""},
		{"count", "WITH COUNT { WITH 2 AS value RETURN value AS private } AS kept", ""},
		{"collect", "WITH COLLECT { WITH 2 AS value RETURN value AS private } AS kept", ""},
		{"list", "WITH [value IN [2] | value] AS kept", ""},
		{"pattern_path", "WITH [value = ()-->() | length(value)] AS kept", ""},
		{"foreach", "FOREACH (value IN [2] | CREATE (:NestedScope {n:value})) WITH 1 AS bridge", ""},
		{"foreach_private_node", "FOREACH (item IN [1] | CREATE (value:NestedScope)) WITH 1 AS bridge", ""},
		{"all", "WITH all(value IN [2] WHERE value=2) AS kept", ""},
		{"any", "WITH any(value IN [2] WHERE value=2) AS kept", ""},
		{"none", "WITH none(value IN [2] WHERE value=3) AS kept", ""},
		{"single", "WITH single(value IN [2] WHERE value=2) AS kept", ""},
		{"reduce_accumulator", "WITH reduce(value=0, item IN [2] | value+item) AS kept", ""},
		{"reduce_iterator", "WITH reduce(total=0, value IN [2] | total+value) AS kept", ""},
		{"nested_expression_call", "WITH EXISTS { CALL () { RETURN 2 AS value } RETURN value } AS kept", ""},
	}
	for _, construct := range constructs {
		for _, empty := range []bool{false, true} {
			prefix := "WITH 7 AS value "
			if empty {
				prefix = "UNWIND [] AS value "
			}
			add := func(name, body string, invalid bool, value int64) {
				query := prefix + "CALL { " + body + " } RETURN x"
				code := ""
				rows := [][]any{{value}}
				if invalid {
					code = "Neo.ClientError.Statement.SyntaxError"
				}
				if invalid || empty {
					rows = nil
				}
				samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("nested_scope/%s/%s/empty=%v", construct.name, name, empty), Query: query, Columns: []string{"x"}, Code: code}, OracleQuery: query, Rows: rows})
			}
			add("private", construct.private+" RETURN 1 AS x", false, 1)
			add("shadow", construct.private+" UNWIND [3] AS value RETURN value AS x", false, 3)
			add("drop", construct.private+" UNWIND [3] AS value WITH 1 AS keep RETURN value AS x", true, 0)
			if construct.export != "" {
				add("export", construct.export+" RETURN value AS x", false, 2)
				add("export_drop", construct.export+" WITH 1 AS keep RETURN value AS x", true, 0)
			}
		}
	}
	return samples
}

// The native queries use implicit imports; the reference explicitly scopes the
// same inputs. Neither query changes the nested expression being tested.
func NestedScopeReadCases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	for _, construct := range []struct {
		name, body string
		node       bool
		value      int64
	}{
		{"call_scoped", "CALL (value) { RETURN value AS kept } RETURN kept AS x", false, 7},
		{"call_all", "CALL (*) { RETURN value AS kept } RETURN kept AS x", false, 7},
		{"call_legacy", "CALL { WITH value RETURN value AS kept } RETURN kept AS x", false, 7},
		{"call_implicit", "CALL { RETURN value AS kept } RETURN kept AS x", false, 7},
		{"exists_pattern", "RETURN CASE WHEN EXISTS { MATCH (value) } THEN 1 ELSE 0 END AS x", true, 1},
		{"count_pattern", "RETURN COUNT { MATCH (value) } AS x", true, 1},
		{"collect", "RETURN size(COLLECT { RETURN value AS v }) AS x", false, 1},
		{"pattern_node", "RETURN size([(value)-->() | 1]) AS x", true, 0},
		{"pattern_expression", "RETURN CASE WHEN (value)-->() THEN 1 ELSE 0 END AS x", true, 0},
		{"list_source", "RETURN head([value IN [value] | value]) AS x", false, 7},
		{"all", "RETURN CASE WHEN all(item IN [value] WHERE item=7) THEN 1 ELSE 0 END AS x", false, 1},
		{"any", "RETURN CASE WHEN any(item IN [value] WHERE item=7) THEN 1 ELSE 0 END AS x", false, 1},
		{"none", "RETURN CASE WHEN none(item IN [value] WHERE item=0) THEN 1 ELSE 0 END AS x", false, 1},
		{"single", "RETURN CASE WHEN single(item IN [value] WHERE item=7) THEN 1 ELSE 0 END AS x", false, 1},
		{"reduce_initial", "RETURN reduce(value=value, item IN [2] | value+item) AS x", false, 9},
		{"reduce_source", "RETURN reduce(total=0, value IN [value] | total+value) AS x", false, 7},
		{"foreach_source", "FOREACH (item IN [value] | CREATE (:NestedScope {n:item})) RETURN 1 AS x", false, 1},
	} {
		for _, empty := range []bool{false, true} {
			prefix := "WITH 7 AS value "
			if construct.node {
				prefix = "MATCH (value:ProjectionSemanticSentinel) WITH value LIMIT 1 "
			}
			if empty {
				prefix += "UNWIND [] AS emptyInput "
			}
			body, referenceBody := construct.body, construct.body
			if construct.name == "call_legacy" || construct.name == "call_implicit" {
				// NornicDB accepts the implicit outer input too. Neo4j needs
				// an explicit input at both unscoped CALL boundaries.
				referenceBody = "WITH value " + body
				if construct.name == "call_implicit" {
					referenceBody = "WITH value CALL { WITH value RETURN value AS kept } RETURN kept AS x"
				}
			}
			query := prefix + "CALL { " + body + " } RETURN x"
			oracle := prefix + "CALL (value) { " + referenceBody + " } RETURN x"
			rows := [][]any{{construct.value}}
			if empty {
				rows = nil
			}
			samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("nested_scope/%s/read/empty=%v", construct.name, empty), Query: query, Columns: []string{"x"}}, OracleQuery: oracle, Rows: rows})
		}
	}
	return samples
}

// CallImportAdmissionCases varies the enclosing import form independently of
// the innermost read. The implicit outer/legacy inner form is the admission
// regression; explicit forms are controls for the same branch binding owner.
func CallImportAdmissionCases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	for _, enclosing := range []struct{ name, open string }{
		{"implicit", "CALL { "},
		{"legacy", "CALL { WITH value "},
		{"listed", "CALL (value) { "},
		{"all", "CALL (*) { "},
		{"with_star", "CALL { WITH * "},
	} {
		for _, leaf := range []struct{ name, open string }{
			{"legacy", "CALL { WITH value "},
			{"implicit", "CALL { "},
		} {
			for _, depth := range []int{2, 3} {
				for _, empty := range []bool{false, true} {
					prefix := "WITH 7 AS value "
					if empty {
						prefix += "UNWIND [] AS emptyInput "
					}
					body := leaf.open + "RETURN value AS kept }"
					reference := "CALL { WITH value RETURN value AS kept }"
					for level := 1; level < depth; level++ {
						returned := "kept"
						if level == depth-1 {
							returned += " AS x"
						}
						body = enclosing.open + body + " RETURN " + returned + " }"
						reference = "CALL { WITH value " + reference + " RETURN " + returned + " }"
					}
					rows := [][]any{{int64(7)}}
					if empty {
						rows = nil
					}
					samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("call_import_admission/%s/%s/depth=%d/empty=%v", enclosing.name, leaf.name, depth, empty), Query: prefix + body + " RETURN x", Columns: []string{"x"}}, OracleQuery: prefix + reference + " RETURN x", Rows: rows})
				}
			}
		}
	}
	return samples
}

// allReduce is a Cypher 25 scope. It has the same accumulator/iterator windows
// as REDUCE; its live reference is Neo4j 25 rather than the 5.26 reference.
func NestedScope25Cases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	for _, local := range []struct{ name, expression string }{
		{"", "allReduce(value=0, item IN [2] | value+item, value>0)"},
		{"/iterator", "allReduce(total=0, value IN [2] | total+value, total>0)"},
	} {
		for _, empty := range []bool{false, true} {
			for _, drop := range []bool{false, true} {
				prefix := "CYPHER 25 WITH 7 AS value "
				if empty {
					prefix = "CYPHER 25 UNWIND [] AS value "
				}
				body := "WITH " + local.expression + " AS kept UNWIND [3] AS value "
				if drop {
					body += "WITH 1 AS keep "
				}
				query := prefix + "CALL { " + body + "RETURN value AS x } RETURN x"
				code := ""
				rows := [][]any{{int64(3)}}
				if drop {
					code = "Neo.ClientError.Statement.SyntaxError"
				}
				if drop || empty {
					rows = nil
				}
				samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("nested_scope/allReduce%s/drop=%v/empty=%v", local.name, drop, empty), Query: query, Columns: []string{"x"}, Code: code}, OracleQuery: query, Rows: rows})
			}
		}
	}
	for _, input := range []struct{ name, expression string }{
		{"read_initial", "allReduce(value=value, item IN [2] | value+item, value>0)"},
		{"read_source", "allReduce(total=0, value IN [value] | total+value, total>0)"},
	} {
		for _, empty := range []bool{false, true} {
			prefix := "CYPHER 25 WITH 7 AS value "
			if empty {
				prefix = "CYPHER 25 UNWIND [] AS value "
			}
			body := "RETURN CASE WHEN " + input.expression + " THEN 1 ELSE 0 END AS x"
			query := prefix + "CALL { " + body + " } RETURN x"
			oracle := prefix + "CALL (value) { " + body + " } RETURN x"
			rows := [][]any{{int64(1)}}
			if empty {
				rows = nil
			}
			samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("nested_scope/allReduce/%s/empty=%v", input.name, empty), Query: query, Columns: []string{"x"}}, OracleQuery: oracle, Rows: rows})
		}
	}
	return samples
}
