package statementfactstest

import (
	"fmt"
	"strings"
)

// CallResultCase includes an independent local control and exact ordered rows.
type CallResultCase struct {
	Case
	Local, Standard string
	Rows            [][]any
}

// CallRoleCases crosses the three input roles and the lookup shortcut family.
// The review case uses the reset reference's empty Item population. Generated
// lookups use an isolated fixture label seeded identically on every graph.
func CallRoleCases() []CallResultCase {
	cases := []CallResultCase{
		{Case: Case{Name: "roles/bound_plan/review/empty_union", Query: "USE comp UNWIND [] AS a CALL () { USE comp.one RETURN 2 AS x UNION USE comp.two RETURN 3 AS x } RETURN count(*) AS n", Columns: []string{"n"}}, Rows: [][]any{{int64(0)}}},
		// Cypher: this unscoped CALL declares a new local value with UNWIND.
		// WITH scratch does not project it, so RETURN value reads a dropped
		// variable. TCK WithOrderBy1.feature [46] covers the WITH scoping rule;
		// the bundled TCK has no CALL-subquery/import-shadow counterpart.
		{Case: Case{Name: "roles/bound_plan/review/drop_between_calls", Query: "USE comp WITH 7 AS value CALL { UNWIND [value] AS first UNWIND [2,3] AS value CALL { USE comp.remote UNWIND [0] AS k RETURN k } WITH 0 AS scratch CALL { USE comp.remote UNWIND [0] AS j RETURN j } RETURN value AS x } RETURN x", Code: "Neo.ClientError.Statement.SyntaxError"}},
		{Case: Case{Name: "roles/bound_plan/review/mid_use_count", Query: "USE comp WITH 7 AS value CALL (value) { UNWIND [1,2] AS k USE comp.remote RETURN count(*) AS c } RETURN c", Columns: []string{"c"}}, Rows: [][]any{{int64(2)}}},
		{Case: Case{Name: "roles/bound_plan/review/remote_prefix", Query: "USE comp WITH 7 AS value CALL (value) { USE comp.remote UNWIND [1,2] AS k CALL { USE comp.one UNWIND [0] AS j RETURN j } RETURN count(*) AS c } RETURN c", Columns: []string{"c"}}, Rows: [][]any{{int64(2)}}},
		{Case: Case{Name: "roles/rebinding/review", Query: "USE comp WITH 7 AS value CALL { UNWIND [value] AS first UNWIND [2,3] AS value CALL { USE comp.remote UNWIND [0] AS k RETURN k } RETURN value AS x } RETURN x", Columns: []string{"x"}}, Rows: [][]any{{int64(2)}, {int64(3)}}},
		{Case: Case{Name: "roles/review/continuation", Query: "USE comp WITH 7 AS value CALL (value) { CALL { USE comp.remote UNWIND [1,2] AS k RETURN k } RETURN count(*) AS c } RETURN c", Columns: []string{"c"}}, Rows: [][]any{{int64(2)}}},
		{Case: Case{Name: "roles/review/routing", Query: "USE comp WITH 'comp.remote' AS g CALL { USE graph.byName(g) UNWIND [0] AS seed RETURN 7 AS x } RETURN x", Columns: []string{"x"}}, Rows: [][]any{{int64(7)}}},
		{Case: Case{Name: "roles/review/lookup", Query: "USE comp WITH 7 AS key CALL { USE comp.remote WITH key MATCH (n:Item) WHERE n.id = key RETURN count(key) AS c } RETURN c", Columns: []string{"c"}}, Rows: [][]any{{int64(0)}}},
	}
	for _, mode := range []string{"listed", "implicit"} {
		for _, graph := range []string{"one", "remote"} {
			for _, input := range []string{"[]", "[1]", "[2,1,2]"} {
				for _, tail := range []struct {
					name, text string
					rows       [][]any
				}{
					{"count", "RETURN count(*) AS x", [][]any{{int64(map[string]int{"[]": 0, "[1]": 1, "[2,1,2]": 3}[input])}}},
					{"distinct", "RETURN DISTINCT k AS x ORDER BY x", nil},
					{"window", "RETURN k AS x ORDER BY x DESC SKIP 1 LIMIT 1", nil},
					{"retained", "WITH count(*) AS c RETURN c + value AS x", [][]any{{int64(7 + map[string]int{"[]": 0, "[1]": 1, "[2,1,2]": 3}[input])}}},
				} {
					rows := tail.rows
					if tail.name == "distinct" && input != "[]" {
						rows = [][]any{{int64(1)}}
						if input == "[2,1,2]" {
							rows = append(rows, []any{int64(2)})
						}
					}
					if tail.name == "window" && input == "[2,1,2]" {
						rows = [][]any{{int64(2)}}
					}
					frame := "CALL (value) { "
					if mode == "implicit" {
						frame = "CALL { "
					}
					query := "USE comp WITH 7 AS value " + frame + "CALL { USE comp." + graph + " UNWIND " + input + " AS k RETURN k } " + tail.text + " } RETURN x"
					cases = append(cases, CallResultCase{Case: Case{Name: fmt.Sprintf("roles/continuation/%s/%s/%s/%s", mode, graph, input, tail.name), Query: query, Columns: []string{"x"}}, Rows: rows})
				}
			}
		}
	}
	for _, function := range []string{"byName", "byElementId"} {
		for _, scoped := range []bool{false, true} {
			for _, bodyReads := range []bool{false, true} {
				for _, size := range []int{0, 1, 2} {
					first, second := "comp.one", "comp.remote"
					if function == "byElementId" {
						first, second = "4:one:one", "4:neo4j:unused"
					}
					values, rows := "[]", [][]any{}
					if size >= 1 {
						values = "['" + first + "']"
						rows = append(rows, []any{int64(7)})
					}
					if size == 2 {
						values = "['" + first + "','" + second + "']"
						rows = append(rows, []any{int64(7)})
					}
					expression, frame := "7", "CALL { "
					if scoped {
						frame = "CALL () { "
					}
					if bodyReads {
						expression = "g"
						if scoped {
							frame = "CALL (g) { "
						}
						if size >= 1 {
							rows[0][0] = first
						}
						if size == 2 {
							rows[1][0] = second
						}
					}
					query := "USE comp UNWIND " + values + " AS g " + frame + "USE graph." + function + "(g) UNWIND [0] AS seed RETURN " + expression + " AS x } RETURN x"
					cases = append(cases, CallResultCase{Case: Case{Name: fmt.Sprintf("roles/routing/%s/scoped=%v/body=%v/size=%d", function, scoped, bodyReads, size), Query: query, Columns: []string{"x"}}, Rows: rows})
				}
			}
		}
	}
	for _, graph := range []string{"one", "remote"} {
		for _, keys := range []string{"[]", "[7]", "[7,8,7,null]"} {
			for _, shape := range []struct{ name, projection string }{
				{"count_key", "count(key)"}, {"count_node", "count(n)"},
				{"collect_key", "collect(key)"}, {"collect_prop", "collect(n.v)"},
				{"rows", "n.v"}, {"rows_key", "key"},
			} {
				var rows [][]any
				values := []any{}
				if keys == "[7]" {
					values = []any{int64(7)}
				}
				if keys == "[7,8,7,null]" {
					values = []any{int64(7), int64(8), int64(7), nil}
				}
				for _, key := range values {
					value := any(int64(11))
					if key == int64(8) {
						value = int64(22)
					}
					if strings.HasSuffix(shape.name, "key") {
						value = key
					}
					switch {
					case strings.HasPrefix(shape.name, "count"):
						value = int64(1)
						if key == nil {
							value = int64(0)
						}
					case strings.HasPrefix(shape.name, "collect"):
						value = []any{value}
						if key == nil {
							value = []any{}
						}
					case key == nil:
						continue
					}
					rows = append(rows, []any{key, value})
				}
				query := "USE comp UNWIND " + keys + " AS key CALL { USE comp." + graph + " WITH key MATCH (n:CallRoleItem) WHERE n.id = key RETURN " + shape.projection + " AS x } RETURN key, x"
				cases = append(cases, CallResultCase{Case: Case{Name: fmt.Sprintf("roles/lookup/%s/%s/%s", graph, keys, shape.name), Query: query, Columns: []string{"key", "x"}}, Rows: rows})
			}
		}
	}
	cases = append(cases, callRebindingCases()...)
	for i := range cases {
		local := cases[i].Query
		for _, use := range []string{"USE comp.remote ", "USE comp.one ", "USE comp.two ", "USE graph.byName(g) ", "USE graph.byElementId(g) ", "USE comp "} {
			local = strings.ReplaceAll(local, use, "")
		}
		cases[i].Local = local
		if !strings.Contains(cases[i].Name, "/implicit/") && !strings.Contains(cases[i].Name, "routing") && !strings.Contains(cases[i].Name, "/rebinding/") && cases[i].Name != "roles/bound_plan/review/drop_between_calls" {
			cases[i].Standard = local
		}
	}
	return cases
}

// Read first so implicit imports have a retained lifetime, then replace that
// binding before or between fragment boundaries. Rebinding imported names is a
// NornicDB extension: the USE-free local execution is its differential oracle.
func callRebindingCases() []CallResultCase {
	var cases []CallResultCase
	for _, graph := range []string{"one", "remote"} {
		for _, input := range []struct {
			text string
			rows [][]any
		}{
			{"[]", nil}, {"[2]", [][]any{{int64(2)}}}, {"[2,3]", [][]any{{int64(2)}, {int64(3)}}},
		} {
			query := "USE comp WITH 7 AS value CALL { UNWIND [value] AS first UNWIND " + input.text + " AS value USE comp." + graph + " RETURN value AS x } RETURN x"
			cases = append(cases, CallResultCase{Case: Case{Name: fmt.Sprintf("roles/rebinding/use/%s/%s", graph, input.text), Query: query, Columns: []string{"x"}}, Rows: input.rows})
		}
	}
	for _, function := range []string{"byName", "byElementId"} {
		for _, mode := range []string{"implicit", "listed"} {
			for _, size := range []int{0, 1, 2} {
				entry, first, second := "comp.one", "comp.one", "comp.remote"
				if function == "byElementId" {
					entry, first, second = "4:one:entry", "4:one:one", "4:neo4j:unused"
				}
				values, rows := "[]", [][]any{}
				if size >= 1 {
					values = "['" + first + "']"
					rows = append(rows, []any{first})
				}
				if size == 2 {
					values = "['" + first + "','" + second + "']"
					rows = append(rows, []any{second})
				}
				frame := "CALL { "
				if mode == "listed" {
					frame = "CALL (g) { "
				}
				query := "USE comp WITH '" + entry + "' AS g " + frame + "UNWIND [g] AS first UNWIND " + values + " AS g CALL { USE graph." + function + "(g) UNWIND [0] AS k RETURN k } RETURN g AS x } RETURN x"
				cases = append(cases, CallResultCase{Case: Case{Name: fmt.Sprintf("roles/rebinding/routing/%s/%s/size=%d", function, mode, size), Query: query, Columns: []string{"x"}}, Rows: rows})
			}
		}
	}
	for _, mode := range []string{"implicit", "listed"} {
		for _, graph := range []string{"one", "remote"} {
			for _, position := range []string{"before", "between", "both"} {
				for _, input := range []struct {
					text   string
					values []int64
				}{
					{"[]", nil}, {"[2]", []int64{2}}, {"[2,3]", []int64{2, 3}},
				} {
					for _, tail := range []string{"identity", "projection", "drop", "aggregate"} {
						frame := "CALL { "
						if mode == "listed" {
							frame = "CALL (value) { "
						}
						body := "UNWIND [value] AS first "
						call := func(name string) string {
							return "CALL { USE comp." + graph + " UNWIND [0] AS " + name + " RETURN " + name + " } "
						}
						if position == "between" {
							body += call("k")
						}
						body += "UNWIND " + input.text + " AS value "
						if position == "before" || position == "both" {
							body += call("k")
						}
						if position == "both" {
							body += "UNWIND [value + 10] AS value "
						}
						if position == "between" || position == "both" {
							body += call("j")
						}
						projection := "RETURN value AS x"
						switch tail {
						case "projection":
							projection = "WITH value + 1 AS value RETURN value AS x"
						case "drop":
							projection = "WITH 0 AS scratch RETURN value AS x"
						case "aggregate":
							projection = "WITH sum(value) AS total RETURN total AS x"
						}
						var rows [][]any
						var total int64
						for _, value := range input.values {
							if position == "both" {
								value += 10
							}
							if tail == "projection" {
								value++
							}
							total += value
							rows = append(rows, []any{value})
						}
						if tail == "aggregate" {
							rows = [][]any{{total}}
						}
						query := "USE comp WITH 7 AS value " + frame + body + projection + " } RETURN x"
						item := CallResultCase{Case: Case{Name: fmt.Sprintf("roles/rebinding/%s/%s/%s/%s/%s", mode, graph, position, input.text, tail), Query: query, Columns: []string{"x"}}, Rows: rows}
						// Same Cypher WITH scoping: the local shadow is not
						// projected, so its later read is undefined even on empty input.
						if mode == "implicit" && tail == "drop" {
							item.Code = "Neo.ClientError.Statement.SyntaxError"
							item.Columns = nil
							item.Rows = nil
						}
						cases = append(cases, item)
					}
				}
			}
		}
	}
	return cases
}
