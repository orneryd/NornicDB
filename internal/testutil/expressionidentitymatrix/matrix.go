// Package expressionidentitymatrix generates expression-subquery identity cases.
package expressionidentitymatrix

import "fmt"

type Case struct{ ID, Query, Rows string }

// Cases covers both projection clauses and all three expression subqueries.
// Each query uses a distinct binding so cached validation of a clean control
// cannot conceal an incorrectly rejected comment-bearing identity.
func Cases() []Case {
	expressions := []string{
		"%s", "(%s)", "((%s))", "((`%s`))",
		"( /* before */ %s )", "(%s /* after */)", "((/* inside */ %s))",
		"(/* between */ (%s) /* after */)",
		"/* leading */ (%s) /* trailing */",
		"(// before ' \" , (\n %s)",
		"(%s // after ' \" , )\n)",
		"((/* ' \" ` , ) ( [ ] { } */ %s))",
		"/* before */ %s /* after */",
		"((%s /* after */))",
	}
	var cases []Case
	for _, clause := range []string{"WITH", "RETURN"} {
		for _, keyword := range []string{"EXISTS", "COUNT", "COLLECT"} {
			for index, expression := range expressions {
				name := fmt.Sprintf("identity_%s_%s_%d", clause, keyword, index)
				expression = fmt.Sprintf(expression, name)
				body := clause + " " + expression + " AS " + name
				if clause == "WITH" {
					body += " RETURN " + name
				}
				rows := "[[true]]"
				if keyword == "COUNT" {
					rows = "[[1]]"
				} else if keyword == "COLLECT" {
					rows = "[[[7]]]"
				}
				cases = append(cases, Case{
					fmt.Sprintf("expression-identity/%s/%s/%d", clause, keyword, index),
					"WITH 7 AS " + name + " RETURN " + keyword + " { " + body + " } AS v",
					rows,
				})
			}
		}
	}
	return cases
}
