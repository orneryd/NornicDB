package statementfactstest

import (
	"fmt"
	"sort"
	"strings"
)

// ProjectionCases is a Cartesian population, not a list of selected examples:
// six item forms, four grouping depths, five name kinds and eight clause sites.
// Grouping changes anonymous presentation text, but never a variable identity.
func ProjectionCases() []Case {
	var cases []Case
	for _, name := range []struct{ kind, written, decoded string }{
		{"plain", "x", "x"}, {"quoted", "`long name`", "long name"},
		{"empty", "``", ""}, {"keyword", "`WHERE`", "WHERE"}, {"escaped", "`a``b`", "a`b"},
	} {
		seed := "WITH 'value' AS " + name.written + ", 7 AS anchor"
		for _, form := range []string{"variable", "alias", "literal", "compound", "property", "function"} {
			base := name.written
			switch form {
			case "literal":
				base = "1"
			case "compound":
				base += " + ''"
			case "property":
				base = "{value: " + base + "}.value"
			case "function":
				base = "toString(" + base + ")"
			}
			for depth := 0; depth <= 3; depth++ {
				expression := strings.Repeat("(", depth) + base + strings.Repeat(")", depth)
				item, bare, column := expression, base, expression
				if form == "variable" || form == "alias" {
					column = name.decoded
				}
				if form == "alias" {
					item += " AS " + name.written
					bare += " AS " + name.written
				}
				for _, site := range []string{"return", "with", "with_star", "union", "union_all", "yield", "call_export", "return_star"} {
					query := seed + " RETURN " + item
					columns := []string{column}
					boundItem, boundColumn := item, column
					if form != "variable" && form != "alias" {
						boundItem += " AS projected"
						boundColumn = "projected"
					}
					switch site {
					case "with":
						query, columns = seed+" WITH "+boundItem+" RETURN *", []string{boundColumn}
					case "with_star":
						query, columns = seed+" WITH *, "+boundItem+" RETURN *", []string{name.decoded, "anchor"}
						if boundColumn != name.decoded {
							columns = append(columns, boundColumn)
						}
						sort.Strings(columns)
					case "union", "union_all":
						operator := " UNION "
						if site == "union_all" {
							operator = " UNION ALL "
						}
						// Variables agree even when only one branch is grouped. Anonymous
						// expressions agree only when their original display spans agree.
						other := item
						if form == "variable" || form == "alias" {
							other = bare
						}
						query += ", 9 AS tail" + operator + seed + " RETURN 9 AS tail, " + other
						columns = append(columns, "tail")
					case "yield":
						query = "CALL db.labels() YIELD label AS " + name.written + " WHERE " + name.written + " = 'ProjectionSemanticSentinel' RETURN " + item
					case "call_export":
						query, columns = "CALL { "+seed+" RETURN "+boundItem+" } RETURN *", []string{boundColumn}
					case "return_star":
						query, columns = seed+" RETURN *, "+expression+" AS extra", []string{name.decoded, "anchor"}
						sort.Strings(columns)
						columns = append(columns, "extra")
					}
					cases = append(cases, Case{Name: fmt.Sprintf("%s_%s_depth%d_%s", name.kind, form, depth, site), Query: query, Columns: columns})
				}
			}
		}
	}
	for index, query := range []string{"RETURN (1)", "WITH 1 AS x RETURN (x+1)", "WITH 1 AS x RETURN (x) AS y"} {
		cases = append(cases, Case{Name: fmt.Sprintf("review_control_%d", index), Query: query, Columns: []string{"(1)", "(x+1)", "y"}[index : index+1]})
	}
	for _, rows := range []string{"[]", "[{kept:7,value:9}]"} {
		for property, written := range []string{"r.kept AS kept, r.value AS value", "r.`kept` AS kept, r.`value` AS value", "r.kept AS `kept`, r.value AS `value`", "r.`kept` AS `kept`, r.`value` AS `value`"} {
			cases = append(cases, Case{Name: fmt.Sprintf("mixed_star_rows%s_property%d", rows, property), Query: "UNWIND " + rows + " AS r WITH " + written + " RETURN *, 2 AS extra", Columns: []string{"kept", "value", "extra"}})
		}
	}
	return cases
}
