// Package caseinputs holds shared reproduction inputs for tests and reference checks.
package caseinputs

import (
	"fmt"
	"math"
)

// Case is one statement of the reproduction matrix.
type Case struct {
	ID    string
	Class string // "trigger" or "control"
	Query string
	// Params are the statement parameters.
	Params map[string]interface{}
	// Setup statements run first, in order, against the same engine.
	Setup []string
	// Want is the single-row single-column contract value.
	Want interface{}
	// WantRows, when set, is the whole result of a multi-row statement.
	WantRows [][]interface{}
}

// nor440OuterOperator builds the outer arithmetic statement around a CASE text
// and states the Neo4j result of left <op> case for the forms whose CASE
// yields c.
type nor440OuterOperator struct {
	name   string
	left   int64
	render func(left int64, caseText string) string
	Want   func(left, c int64) interface{}
}

var nor440OuterOperators = []nor440OuterOperator{
	{name: "plus", left: 10,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d + %s", left, caseText) },
		Want:   func(left, c int64) interface{} { return left + c }},
	{name: "minus", left: 10,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d - %s", left, caseText) },
		Want:   func(left, c int64) interface{} { return left - c }},
	{name: "multiply", left: 10,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d * %s", left, caseText) },
		Want:   func(left, c int64) interface{} { return left * c }},
	{name: "divide", left: 12,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d / %s", left, caseText) },
		Want:   func(left, c int64) interface{} { return left / c }},
	{name: "modulo", left: 10,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d %% %s", left, caseText) },
		Want:   func(left, c int64) interface{} { return left % c }},
	{name: "power", left: 2,
		render: func(left int64, caseText string) string { return fmt.Sprintf("%d ^ %s", left, caseText) },
		Want: func(left, c int64) interface{} {
			// Cypher exponentiation always yields a float.
			return math.Pow(float64(left), float64(c))
		}},
}

// nor440CaseForm is one CASE body shape from the measured trigger family. c is
// the value the CASE yields for its selected branch.
type nor440CaseForm struct {
	name    string
	caseVal int64
	text    string
}

var nor440CaseForms = []nor440CaseForm{
	{name: "then-additive", caseVal: 3, text: "CASE WHEN true THEN 5 - 2 ELSE 0 END"},
	{name: "when-additive", caseVal: 3, text: "CASE WHEN 1 + 1 > 1 THEN 3 ELSE 0 END"},
	{name: "else-additive", caseVal: 3, text: "CASE WHEN false THEN 0 ELSE 5 - 2 END"},
	{name: "subject-additive", caseVal: 3, text: "CASE 1 + 2 WHEN 3 THEN 3 ELSE 0 END"},
	{name: "nested-case-additive", caseVal: 3, text: "CASE WHEN true THEN 2 + CASE WHEN true THEN 1 ELSE 0 END ELSE 0 END"},
	{name: "unary-minus-leading", caseVal: -3, text: "CASE WHEN true THEN -3 ELSE 0 END"},
	{name: "unary-minus-after-operator", caseVal: 3, text: "CASE WHEN true THEN 5 + -2 ELSE 0 END"},
}

// Cases is the complete deterministic matrix: the whole trigger
// cross product (CASE body shape x outer operator), the tier-coincidence rows,
// the expression contexts the issue enumerates, and the healthy controls.
func Cases() []Case {
	cases := make([]Case, 0, 128)
	for _, form := range nor440CaseForms {
		for _, outer := range nor440OuterOperators {
			cases = append(cases, Case{
				ID:    fmt.Sprintf("trigger-%s-%s", outer.name, form.name),
				Class: "trigger",
				Query: "RETURN " + outer.render(outer.left, form.text) + " AS r",
				Want:  outer.Want(outer.left, form.caseVal),
			})
		}
	}

	cases = append(cases,
		// Tier coincidence: the outer operator is the tier that is split, and
		// the CASE holds an operator of that same tier.
		Case{ID: "trigger-multiply-inside-multiply", Class: "trigger",
			Query: "RETURN 10 * CASE WHEN true THEN 2 * 2 ELSE 1 END AS r", Want: int64(40)},
		Case{ID: "trigger-divide-inside-divide", Class: "trigger",
			Query: "RETURN 12 / CASE WHEN true THEN 6 / 3 ELSE 1 END AS r", Want: int64(6)},
		Case{ID: "trigger-modulo-inside-modulo", Class: "trigger",
			Query: "RETURN 10 % CASE WHEN true THEN 7 % 4 ELSE 1 END AS r", Want: int64(1)},
		Case{ID: "trigger-power-inside-power", Class: "trigger",
			Query: "RETURN 2 ^ CASE WHEN true THEN 2 ^ 2 ELSE 1 END AS r", Want: math.Pow(2, 4)},
		// A tighter-tier CASE operator under a looser outer operator is still a
		// trigger: the +- tier is scanned before * / % and ^, so the split lands
		// inside the CASE whatever the outer operator is.
		Case{ID: "trigger-multiply-inner-plus", Class: "trigger",
			Query: "RETURN 10 * CASE WHEN true THEN 5 + 2 ELSE 0 END AS r", Want: int64(70)},
		Case{ID: "trigger-multiply-inner-modulo", Class: "trigger",
			Query: "RETURN 10 * CASE WHEN true THEN 7 % 4 ELSE 0 END AS r", Want: int64(30)},
		Case{ID: "trigger-divide-inner-plus", Class: "trigger",
			Query: "RETURN 20 / CASE WHEN true THEN 2 + 2 ELSE 1 END AS r", Want: int64(5)},
		Case{ID: "trigger-floats-subtract", Class: "trigger",
			Query: "RETURN 1.5 + CASE WHEN true THEN 2.5 - 0.5 ELSE 0 END AS r", Want: 3.5},
		Case{ID: "trigger-strings-concat", Class: "trigger",
			Query: "RETURN 'a' + CASE WHEN true THEN 'b' + 'c' ELSE 'd' END AS r", Want: "abc"},

		// The issue's statements, with the Neo4j results stated in it.
		Case{ID: "trigger-issue-return-scalar", Class: "trigger",
			Query: "RETURN 1 + CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END AS r", Want: int64(4)},
		Case{ID: "trigger-issue-list-comprehension", Class: "trigger",
			Query: "RETURN [x IN [1, 5, 9] | x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END] AS r",
			Want:  []interface{}{int64(1), int64(9), int64(17)}},
		Case{ID: "trigger-issue-comprehension-map", Class: "trigger",
			Query:  "RETURN [x IN [1, 5, 9] | {v: x + CASE WHEN $start > x THEN $start - x ELSE 0 END}] AS r",
			Params: map[string]interface{}{"start": int64(4)},
			Want: []interface{}{
				map[string]interface{}{"v": int64(4)},
				map[string]interface{}{"v": int64(5)},
				map[string]interface{}{"v": int64(9)},
			}},
		Case{ID: "trigger-issue-comprehension-unary", Class: "trigger",
			Query: "RETURN [x IN [1, 5, 9] | x + CASE WHEN x > 1 THEN -1 ELSE 0 END] AS r",
			Want:  []interface{}{int64(1), int64(4), int64(8)}},
		Case{ID: "trigger-issue-comprehension-when-arithmetic", Class: "trigger",
			Query: "RETURN [x IN [1, 5, 9] | x + CASE WHEN x - 1 > 0 THEN 1 ELSE 0 END] AS r",
			Want:  []interface{}{int64(1), int64(6), int64(10)}},
		Case{ID: "trigger-issue-comprehension-multiply", Class: "trigger",
			Query: "RETURN [x IN [1, 5, 9] | x * CASE WHEN x > 1 THEN x * 2 ELSE 1 END] AS r",
			Want:  []interface{}{int64(1), int64(50), int64(162)}},
		Case{ID: "trigger-issue-comprehension-strings", Class: "trigger",
			Query: "RETURN [x IN [1, 5, 9] | toString(x) + CASE WHEN x > 1 THEN 'a' + 'b' ELSE 'c' END] AS r",
			Want:  []interface{}{"1c", "5ab", "9ab"}},
		Case{ID: "trigger-issue-unwind-sum", Class: "trigger",
			Query: "UNWIND [1, 5, 9] AS x RETURN sum(x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END) AS r",
			Want:  int64(27)},
		Case{ID: "trigger-issue-match-set-property", Class: "trigger",
			Setup: []string{"CREATE (:D {k: 5})"},
			Query: "MATCH (d:D) SET d.p = d.k + CASE WHEN d.k > 0 THEN d.k - 1 ELSE 0 END RETURN d.p AS r",
			Want:  int64(9)},

		// The remaining expression contexts the issue enumerates.
		Case{ID: "trigger-with-projection", Class: "trigger",
			Query: "WITH 3 AS x RETURN x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END AS r",
			Want:  int64(5)},
		Case{ID: "trigger-where-predicate", Class: "trigger",
			Query: "WITH 3 AS x WHERE x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END = 5 RETURN x AS r",
			Want:  int64(3)},
		Case{ID: "trigger-order-by-key", Class: "trigger",
			Query:    "UNWIND [3, 1] AS x RETURN x AS r ORDER BY x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END DESC",
			WantRows: [][]interface{}{{int64(3)}, {int64(1)}}},
		Case{ID: "trigger-create-property", Class: "trigger",
			Query: "CREATE (n:F {v: 2 + CASE WHEN true THEN 3 - 1 ELSE 0 END}) RETURN n.v AS r",
			Want:  int64(4)},
		Case{ID: "trigger-set-merge", Class: "trigger",
			Setup: []string{"CREATE (:E {k: 4})"},
			Query: "MATCH (d:E) SET d += {p: d.k + CASE WHEN d.k > 0 THEN d.k - 1 ELSE 0 END} RETURN d.p AS r",
			Want:  int64(7)},
		Case{ID: "trigger-map-literal", Class: "trigger",
			Query: "RETURN {v: 10 + CASE WHEN true THEN 5 - 2 ELSE 0 END} AS r",
			Want:  map[string]interface{}{"v": int64(13)}},
		Case{ID: "trigger-map-projection", Class: "trigger",
			Setup: []string{"CREATE (:G {k: 2})"},
			Query: "MATCH (d:G) RETURN d {.k, v: d.k + CASE WHEN d.k > 0 THEN d.k - 1 ELSE 0 END} AS r",
			Want:  map[string]interface{}{"k": int64(2), "v": int64(3)}},
		Case{ID: "trigger-pattern-comprehension", Class: "trigger",
			Setup: []string{"CREATE (:H {k: 10})-[:R]->(:I {w: 3})"},
			Query: "MATCH (d:H) RETURN [(d)-[:R]->(o) | d.k + CASE WHEN o.w > 1 THEN o.w - 1 ELSE 0 END] AS r",
			Want:  []interface{}{int64(12)}},
		Case{ID: "trigger-reduce-accumulator", Class: "trigger",
			Query: "RETURN reduce(acc = 0, x IN [1, 5] | acc + x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END) AS r",
			Want:  int64(10)},
		Case{ID: "trigger-function-argument", Class: "trigger",
			Query: "RETURN toString(10 + CASE WHEN true THEN 5 - 2 ELSE 0 END) AS r",
			Want:  "13"},
		Case{ID: "trigger-aggregate-argument", Class: "trigger",
			Query: "UNWIND [1, 5] AS x RETURN sum(x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END) AS r",
			Want:  int64(10)},
		Case{ID: "trigger-property-left-operand", Class: "trigger",
			Setup: []string{"CREATE (:K {k: 6})"},
			Query: "MATCH (d:K) RETURN d.k + CASE WHEN d.k > 1 THEN d.k - 1 ELSE 0 END AS r",
			Want:  int64(11)},
		Case{ID: "trigger-map-property-left-operand", Class: "trigger",
			Query: "WITH {k: 2} AS m RETURN m.k + CASE WHEN true THEN 1 - 1 ELSE 5 END AS r",
			Want:  int64(2)},
	)

	cases = append(cases,
		// Healthy controls the issue records as working. They must stay green.
		Case{ID: "control-parenthesized-case", Class: "control",
			Query: "RETURN 1 + (CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END) AS r", Want: int64(4)},
		Case{ID: "control-case-first", Class: "control",
			Query: "RETURN CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END + 1 AS r", Want: int64(4)},
		Case{ID: "control-case-first-unary", Class: "control",
			Query: "RETURN CASE WHEN true THEN -1 ELSE 0 END + 10 AS r", Want: int64(9)},
		Case{ID: "control-right-of-comparison", Class: "control",
			Query: "RETURN 1 = CASE WHEN true THEN 1 ELSE 0 END AS r", Want: true},
		Case{ID: "control-right-of-and", Class: "control",
			Query: "RETURN true AND CASE WHEN true THEN true ELSE false END AS r", Want: true},
		Case{ID: "control-right-of-or", Class: "control",
			Query: "RETURN false OR CASE WHEN true THEN true ELSE false END AS r", Want: true},
		Case{ID: "control-operators-inside-function-call", Class: "control",
			Query: "RETURN 1 + CASE WHEN true THEN toInteger('2' + '3') ELSE 0 END AS r", Want: int64(24)},
		Case{ID: "control-operators-inside-function-call-string", Class: "control",
			Query: "RETURN 'a' + CASE WHEN true THEN toUpper('b' + '') ELSE '' END AS r", Want: "aB"},
		Case{ID: "control-operators-inside-brackets", Class: "control",
			Query: "RETURN 1 + CASE WHEN true THEN [1 + 2][0] ELSE 0 END AS r", Want: int64(4)},
		Case{ID: "control-case-without-arithmetic", Class: "control",
			Query: "RETURN 1 + CASE 2 WHEN 2 THEN 10 ELSE 0 END AS r", Want: int64(11)},
		Case{ID: "control-case-with-comparison-only", Class: "control",
			Query: "RETURN 1 + CASE WHEN 4 > 1 THEN 4 ELSE 0 END AS r", Want: int64(5)},
		Case{ID: "control-outer-minus-inner-multiply", Class: "control",
			Query: "RETURN 1 - CASE WHEN true THEN 5 * 2 ELSE 0 END AS r", Want: int64(-9)},
		Case{ID: "control-outer-plus-inner-multiply", Class: "control",
			Query: "RETURN 1 + CASE WHEN true THEN 2 * 3 ELSE 0 END AS r", Want: int64(7)},
		Case{ID: "control-outer-minus-inner-power", Class: "control",
			Query: "RETURN 12 - CASE WHEN true THEN 2 ^ 2 ELSE 0 END AS r", Want: float64(8)},
		Case{ID: "control-parenthesized-tier-coincidence", Class: "control",
			Query: "RETURN 2 * (CASE WHEN true THEN 3 * 2 ELSE 1 END) AS r", Want: int64(12)},
		Case{ID: "control-null-branch", Class: "control",
			Query: "RETURN 1 + CASE WHEN false THEN 1 ELSE null END AS r", Want: nil},
		Case{ID: "control-comparison-with-arithmetic-left", Class: "control",
			Query: "RETURN 5 - 2 = CASE WHEN true THEN 3 ELSE 0 END AS r", Want: true},
		Case{ID: "control-where-parenthesized", Class: "control",
			Query: "WITH 3 AS x WHERE x + (CASE WHEN x > 1 THEN x - 1 ELSE 0 END) = 5 RETURN x AS r",
			Want:  int64(3)},
		Case{ID: "control-where-case-first", Class: "control",
			Query: "WITH 3 AS x WHERE CASE WHEN x > 1 THEN x - 1 ELSE 0 END + x = 5 RETURN x AS r",
			Want:  int64(3)},
		Case{ID: "control-comprehension-parenthesized", Class: "control",
			Query: "RETURN [x IN [1, 5] | x + (CASE WHEN x > 1 THEN x - 1 ELSE 0 END)] AS r",
			Want:  []interface{}{int64(1), int64(9)}},
	)
	return cases
}
