package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Which subqueries run for a row before the expression around them (Neo4j's
// Apply) and which only where the expression reaches them, and so whether a
// deciding operand discards a subquery's error (applyOperand,
// semiApplyPredicate, liftsValueSubquery, wherePredicatePlan with its
// selective filters, localSubqueryScope, logicalNotAnd, logicalNotOr,
// constant folding). Every subquery below divides by x, which is 0, so it
// raises / by zero when it runs. Expected values and errors are Neo4j
// 5.26.30's, the plans' order read with EXPLAIN.
func TestSubqueryEvaluationOrderMatchesNeo4j(t *testing.T) {
	subqueries := strings.NewReplacer(
		"EXISTS_X", "EXISTS { WITH x WHERE 1/x=1 RETURN x }",
		"COUNT_X", "COUNT { WITH x WHERE 1/x=1 RETURN x }",
		"COLLECT_X", "COLLECT { WITH x WHERE 1/x=1 RETURN x }",
		"EXISTS_N", "EXISTS { WITH n WHERE 1/n.v=1 RETURN n }",
	)
	const divisionByZero = "/ by zero"
	zeros := [][]interface{}{{int64(0)}, {int64(0)}}
	falses := [][]interface{}{{false}, {false}}
	trues := [][]interface{}{{true}, {true}}
	none := [][]interface{}{}
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
		message     string
	}{
		// RETURN: an AND's or a chain's subqueries run first, negated or not.
		{"return_exists_and", "UNWIND [0,0] AS x RETURN EXISTS_X AND x <> 0 AS v", nil, divisionByZero},
		{"return_and_count", "UNWIND [0,0] AS x RETURN x <> 0 AND COUNT_X > 0 AS v", nil, divisionByZero},
		{"return_and_exists", "UNWIND [0,0] AS x RETURN x <> 0 AND EXISTS_X AS v", nil, divisionByZero},
		{"return_and_exists_beside_case", "UNWIND [0,0] AS x RETURN x<>0 AND (EXISTS_X = CASE WHEN x=0 THEN true ELSE false END) AS v", nil, divisionByZero},
		{"return_and_exists_body_case", "UNWIND [0,0] AS x RETURN x<>0 AND EXISTS { WITH x WHERE CASE WHEN 1/x=1 THEN true ELSE false END RETURN x } AS v", nil, divisionByZero},
		{"return_and_coalesce_exists", "UNWIND [0,0] AS x RETURN x<>0 AND coalesce(EXISTS_X, false) AS v", falses, ""},
		{"return_or_coalesce_count", "UNWIND [0,0] AS x RETURN x=0 OR coalesce(COUNT_X,0)>0 AS v", trues, ""},
		{"return_and_quantifier_input", "UNWIND [0,0] AS x RETURN x<>0 AND any(y IN COLLECT_X WHERE y=0) AS v", nil, divisionByZero},
		{"return_and_comprehension_input", "UNWIND [0,0] AS x RETURN x<>0 AND [y IN COLLECT_X | y]=[] AS v", nil, divisionByZero},
		{"return_and_reduce_input", "UNWIND [0,0] AS x RETURN x<>0 AND reduce(a=0,y IN COLLECT_X | a+y)>0 AS v", nil, divisionByZero},
		{"return_not_exists_and", "UNWIND [0,0] AS x RETURN NOT EXISTS_X AND x <> 0 AS v", nil, divisionByZero},
		{"return_and_not_exists", "UNWIND [0,0] AS x RETURN x <> 0 AND NOT EXISTS_X AS v", nil, divisionByZero},
		{"return_and_size_collect", "UNWIND [0,0] AS x RETURN x <> 0 AND size(COLLECT_X) > 0 AS v", nil, divisionByZero},
		{"return_and_or", "UNWIND [0,0] AS x RETURN x <> 0 AND (EXISTS_X OR x = 1) AS v", nil, divisionByZero},
		{"return_xor", "UNWIND [0,0] AS x RETURN x <> 0 XOR EXISTS_X AS v", nil, divisionByZero},
		{"return_list_of_and", "UNWIND [0,0] AS x RETURN [x <> 0 AND EXISTS_X] AS v", nil, divisionByZero},
		{"return_tostring_and", "UNWIND [0,0] AS x RETURN toString(x <> 0 AND EXISTS_X) AS v", nil, divisionByZero},
		{"return_and_in_or", "UNWIND [0,0] AS x RETURN (x <> 0 AND EXISTS_X) OR x = 1 AS v", nil, divisionByZero},
		{"return_not_or", "UNWIND [0,0] AS x RETURN NOT (x = 0 OR EXISTS_X) AS v", nil, divisionByZero},
		{"order_by_and", "UNWIND [0,0] AS x RETURN x AS v ORDER BY x <> 0 AND EXISTS_X", nil, divisionByZero},
		{"return_selective_and", "UNWIND [0,0] AS x RETURN x = 1 AND EXISTS_X AS v", nil, divisionByZero},
		{"return_chain_count_first", "UNWIND [0,0] AS x RETURN COUNT_X < x < -1 AS v", nil, divisionByZero},
		{"return_chain_size_collect", "UNWIND [0,0] AS x RETURN x < -1 < size(COLLECT_X) AS v", nil, divisionByZero},
		{"return_and_chain", "UNWIND [0,0] AS x RETURN x <> 0 AND x < -1 < COUNT_X AS v", nil, divisionByZero},
		{"return_long_chain", "UNWIND [0,0] AS x RETURN x < -1 < COUNT_X < 5 AS v", nil, divisionByZero},
		{"return_equality_chain", "UNWIND [0,0] AS x RETURN x = 1 = COUNT_X AS v", nil, divisionByZero},
		{"return_descending_chain", "UNWIND [0,0] AS x RETURN -1 > x > COUNT_X AS v", nil, divisionByZero},
		{"return_chain_arithmetic", "UNWIND [0,0] AS x RETURN x < -1 < (COUNT_X + 1) AS v", nil, divisionByZero},
		{"return_and_null", "UNWIND [0,0] AS x RETURN EXISTS_X AND null AS v", nil, divisionByZero},
		{"return_or_false", "UNWIND [0,0] AS x RETURN EXISTS_X OR false AS v", nil, divisionByZero},
		{"return_and_true", "UNWIND [0,0] AS x RETURN EXISTS_X AND true AS v", nil, divisionByZero},
		{"case_when_exists", "UNWIND [0,0] AS x RETURN CASE WHEN EXISTS_X THEN 1 ELSE 0 END AS v", nil, divisionByZero},
		// Neo4j folds constant predicates before it plans subqueries.
		{"return_and_false", "UNWIND [0,0] AS x RETURN x <> 0 AND EXISTS_X AND false AS v", falses, ""},
		{"return_and_not_true", "UNWIND [0,0] AS x RETURN EXISTS_X AND NOT true AS v", falses, ""},
		{"return_and_constant_comparison", "UNWIND [0,0] AS x RETURN EXISTS_X AND (1 = 2) AS v", falses, ""},
		{"return_constant_chain_link", "UNWIND [0,0] AS x RETURN 1 < 0 < COUNT_X AS v", falses, ""},
		{"return_promoted_true_chain", "RETURN 9007199254740993 = 9007199254740992.0 > COUNT { RETURN 1 } AS v", [][]interface{}{{true}}, ""},
		{"return_promoted_false_chain", "UNWIND [0,0] AS x RETURN 9007199254740993 > 9007199254740992.0 > COUNT_X AS v", falses, ""},
		{"return_folded_and_or", "UNWIND [0,0] AS x RETURN (EXISTS_X AND false) OR x = 0 AS v", trues, ""},
		{"return_not_folded_and", "UNWIND [0,0] AS x RETURN NOT (EXISTS_X AND false) AS v", trues, ""},
		{"where_exists_and_false", "UNWIND [0,0] AS x WITH x WHERE EXISTS_X AND false RETURN x", none, ""},
		{"where_folded_or", "UNWIND [0,0] AS x WITH x WHERE false AND x <> 0 OR EXISTS_X RETURN x", nil, divisionByZero},
		// RETURN: an OR's disjunct runs only when the others don't decide;
		// NOT (a AND b) is NOT a OR NOT b.
		{"return_exists_or", "UNWIND [0,0] AS x RETURN EXISTS_X OR x = 0 AS v", trues, ""},
		{"return_and_or_decided", "UNWIND [0,0] AS x RETURN (EXISTS_X AND x <> 0) OR x = 0 AS v", trues, ""},
		{"return_count_or", "UNWIND [0,0] AS x RETURN COUNT_X > 0 OR x = 0 AS v", trues, ""},
		{"return_or_count_and", "UNWIND [0,0] AS x RETURN x = 0 OR (COUNT_X > 0 AND x <> 0) AS v", trues, ""},
		{"return_not_and", "UNWIND [0,0] AS x RETURN NOT (x <> 0 AND EXISTS_X) AS v", trues, ""},
		{"return_or_exists", "UNWIND [0,0] AS x RETURN x = 0 OR EXISTS_X AS v", trues, ""},
		{"return_not_exists_or", "UNWIND [0,0] AS x RETURN NOT EXISTS_X OR x = 0 AS v", trues, ""},
		// RETURN: a subquery in a list scope or a coalesce argument runs only
		// where reached.
		{"return_and_any_correlated", "UNWIND [0,0] AS x RETURN x <> 0 AND any(y IN [1] WHERE EXISTS { WITH x, y WHERE 1/x=y RETURN x }) AS v", falses, ""},
		{"return_and_size_comprehension", "UNWIND [0,0] AS x RETURN x <> 0 AND size([y IN [1] | COUNT { WITH x, y WHERE 1/x=y RETURN x }]) > 0 AS v", falses, ""},
		{"return_and_any", "UNWIND [0,0] AS x RETURN x <> 0 AND any(y IN [1] WHERE EXISTS_X) AS v", falses, ""},
		{"return_coalesce_count", "UNWIND [0,0] AS x RETURN coalesce(1, COUNT_X) AS v", [][]interface{}{{int64(1)}, {int64(1)}}, ""},
		{"return_coalesce_and", "UNWIND [0,0] AS x RETURN coalesce(x <> 0 AND EXISTS_X, false) AS v", falses, ""},
		{"return_coalesce_later_and", "UNWIND [0,0] AS x RETURN coalesce(null, x <> 0 AND EXISTS_X) AS v", falses, ""},
		{"return_coalesce_single_and", "UNWIND [0,0] AS x RETURN coalesce(x <> 0 AND EXISTS_X) AS v", falses, ""},
		{"return_tostring_coalesce_and", "UNWIND [0,0] AS x RETURN toString(coalesce(x <> 0 AND EXISTS_X, false)) AS v", [][]interface{}{{"false"}, {"false"}}, ""},
		{"return_any_and", "UNWIND [0,0] AS x RETURN any(y IN [1] WHERE x <> 0 AND EXISTS_X) AS v", falses, ""},
		{"return_comprehension_and", "UNWIND [0,0] AS x RETURN [y IN [1] | x <> 0 AND EXISTS_X] AS v", [][]interface{}{{[]interface{}{false}}, {[]interface{}{false}}}, ""},
		{"return_comprehension_where_and", "UNWIND [0,0] AS x RETURN [y IN [1] WHERE x <> 0 AND EXISTS_X] AS v", [][]interface{}{{[]interface{}{}}, {[]interface{}{}}}, ""},
		{"return_reduce_and", "UNWIND [0,0] AS x RETURN reduce(a = true, y IN [1] | a AND x <> 0 AND EXISTS_X) AS v", falses, ""},
		{"where_coalesce_and", "UNWIND [0,0] AS x WITH x WHERE coalesce(x <> 0 AND EXISTS_X, false) RETURN x", none, ""},
		// WHERE: positive subquery conjuncts run first.
		{"where_and_exists", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND x <> 1 AND EXISTS_X RETURN x", nil, divisionByZero},
		{"where_exists_and", "UNWIND [0,0] AS x WITH x WHERE EXISTS_X AND x <> 0 RETURN x", nil, divisionByZero},
		{"where_and_count", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT_X > 0 RETURN x", nil, divisionByZero},
		{"where_and_collect", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COLLECT_X = [] RETURN x", nil, divisionByZero},
		{"where_and_exists_true", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND EXISTS_X = true RETURN x", nil, divisionByZero},
		{"where_and_count_exists", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT_X > 0 AND EXISTS_X RETURN x", nil, divisionByZero},
		{"where_not_not_exists", "UNWIND [0,0] AS x WITH x WHERE NOT NOT EXISTS_X AND x <> 0 RETURN x", nil, divisionByZero},
		{"where_and_not_not_exists", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND NOT (NOT EXISTS_X) RETURN x", nil, divisionByZero},
		{"where_nested_and", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND (x <> 1 AND EXISTS_X) RETURN x", nil, divisionByZero},
		{"where_not_exists_and_count", "UNWIND [0,0] AS x WITH x WHERE NOT EXISTS_X AND x <> 0 AND COUNT_X > 0 RETURN x", nil, divisionByZero},
		{"where_xor", "UNWIND [0,0] AS x WITH x WHERE x = 0 XOR EXISTS_X RETURN x", nil, divisionByZero},
		{"match_where_and_exists", "CREATE (:T {v:0}), (:T {v:0}) ;; MATCH (n:T) WHERE n.v <> 0 AND EXISTS_N RETURN n.v AS v", nil, divisionByZero},
		{"match_where_exists_and", "CREATE (:T {v:0}), (:T {v:0}) ;; MATCH (n:T) WHERE EXISTS_N AND n.v <> 0 RETURN n.v AS v", nil, divisionByZero},
		{"where_and_count_minus_one", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND -1 < COUNT_X RETURN x", nil, divisionByZero},
		{"where_and_count_greater_minus_one", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT_X > -1 RETURN x", nil, divisionByZero},
		{"where_and_count_at_least_zero", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT_X >= 0 RETURN x", nil, divisionByZero},
		{"where_is_null_and_exists", "UNWIND [0,0] AS x WITH x WHERE x IS NULL AND EXISTS_X RETURN x", nil, divisionByZero},
		{"where_chain_or", "UNWIND [0,0] AS x WITH x WHERE x < -1 < COUNT_X OR x = 0 RETURN x", nil, divisionByZero},
		// WHERE: an equality or range filter runs before the subqueries.
		{"where_equality_and_exists", "UNWIND [0,0] AS x WITH x WHERE x = 1 AND EXISTS_X RETURN x", none, ""},
		{"where_exists_and_equality", "UNWIND [0,0] AS x WITH x WHERE EXISTS_X AND x = 1 RETURN x", none, ""},
		{"where_range_and_exists", "UNWIND [0,0] AS x WITH x WHERE x < -1 AND EXISTS_X RETURN x", none, ""},
		{"where_range_and_count", "UNWIND [0,0] AS x WITH x WHERE x > 5 AND COUNT_X > 0 RETURN x", none, ""},
		{"where_range_and_count_reversed", "UNWIND [0,0] AS x WITH x WHERE x < -1 AND COUNT_X > 0 RETURN x", none, ""},
		{"where_equality_and_count", "UNWIND [0,0] AS x WITH x WHERE x = 1 AND COUNT_X > 0 RETURN x", none, ""},
		{"where_chain", "UNWIND [0,0] AS x WITH x WHERE x < -1 < COUNT_X RETURN x", none, ""},
		{"where_not_or_and_equality", "UNWIND [0,0] AS x WITH x WHERE NOT (x = 0 OR EXISTS_X) AND x = 1 RETURN x", none, ""},
		// WHERE: negated conjuncts and disjunctions run after the filters.
		{"where_and_not_exists", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND NOT EXISTS_X RETURN x", none, ""},
		{"where_not_exists_and", "UNWIND [0,0] AS x WITH x WHERE NOT EXISTS_X AND x <> 0 RETURN x", none, ""},
		{"where_not_count_and", "UNWIND [0,0] AS x WITH x WHERE NOT COUNT_X > 0 AND x <> 0 RETURN x", none, ""},
		{"where_and_not_count", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND NOT (COUNT_X > 0) RETURN x", none, ""},
		{"where_not_parenthesized_exists", "UNWIND [0,0] AS x WITH x WHERE NOT (EXISTS_X) AND x <> 0 RETURN x", none, ""},
		{"match_where_not_exists_and", "CREATE (:T {v:0}), (:T {v:0}) ;; MATCH (n:T) WHERE NOT EXISTS_N AND n.v <> 0 RETURN n.v AS v", none, ""},
		{"where_and_or", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND (EXISTS_X OR x = 1) RETURN x", none, ""},
		{"where_and_in_or", "UNWIND [0,0] AS x WITH x WHERE (x <> 0 AND EXISTS_X) OR x = 1 RETURN x", none, ""},
		{"where_and_count_or", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT_X > 0 OR x = 1 RETURN x", none, ""},
		{"where_and_in_or_decided", "UNWIND [0,0] AS x WITH x WHERE (x <> 0 AND EXISTS_X) OR x = 0 RETURN x", zeros, ""},
		{"where_or_and_decided", "UNWIND [0,0] AS x WITH x WHERE x = 0 OR (x <> 0 AND EXISTS_X) RETURN x", zeros, ""},
		{"where_and_or_precedence", "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND EXISTS_X OR x = 0 RETURN x", zeros, ""},
		{"where_not_and", "UNWIND [0,0] AS x WITH x WHERE NOT (x <> 0 AND EXISTS_X) RETURN x", zeros, ""},
		{"where_not_and_reached", "UNWIND [0,0] AS x WITH x WHERE NOT (x = 0 AND EXISTS_X) RETURN x", nil, divisionByZero},
		{"where_not_or", "UNWIND [0,0] AS x WITH x WHERE NOT (x = 0 OR EXISTS_X) RETURN x", none, ""},
		// CASE: a condition's subqueries run where reached, but a subquery's
		// own body plans its WHERE again.
		{"case_and_exists", "UNWIND [0,0] AS x RETURN CASE WHEN x <> 0 AND EXISTS_X THEN 1 ELSE 0 END AS v", zeros, ""},
		{"case_exists_and", "UNWIND [0,0] AS x RETURN CASE WHEN EXISTS_X AND x <> 0 THEN 1 ELSE 0 END AS v", zeros, ""},
		{"case_later_when", "UNWIND [0,0] AS x RETURN CASE WHEN x = 0 THEN 0 WHEN EXISTS_X THEN 1 END AS v", zeros, ""},
		{"case_simple_exists", "UNWIND [0,0] AS x RETURN CASE EXISTS_X WHEN true THEN 1 ELSE 0 END AS v", nil, divisionByZero},
		{"case_chain", "UNWIND [0,0] AS x RETURN CASE WHEN x < -1 < COUNT_X THEN 1 ELSE 0 END AS v", zeros, ""},
		{"case_not_exists_and", "UNWIND [0,0] AS x RETURN CASE WHEN NOT EXISTS_X AND x <> 0 THEN 1 ELSE 0 END AS v", zeros, ""},
		{"case_unreached_then", "UNWIND [0,0] AS x RETURN CASE WHEN x <> 0 THEN COUNT_X ELSE 0 END AS v", zeros, ""},
		{"where_case", "UNWIND [0,0] AS x WITH x WHERE CASE WHEN EXISTS_X AND x <> 0 THEN true ELSE false END RETURN x", none, ""},
		{"and_case", "UNWIND [0,0] AS x RETURN x <> 0 AND CASE WHEN EXISTS_X THEN true ELSE false END AS v", falses, ""},
		{"case_tostring_and", "UNWIND [0,0] AS x RETURN CASE WHEN trim(toString(EXISTS_X AND x<>0)) = 'false' THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}, {int64(1)}}, ""},
		{"case_subquery_body_where", "UNWIND [0,0] AS x RETURN CASE WHEN COUNT { WITH x WHERE x<>0 AND EXISTS_X RETURN x } > 0 THEN 1 ELSE 0 END AS v", nil, divisionByZero},
		{"case_then_text", "UNWIND [0,0] AS x RETURN CASE WHEN x = 0 THEN 'z' ELSE toString(COUNT_X) END AS v", [][]interface{}{{"z"}, {"z"}}, ""},
		{"case_simple_else", "UNWIND [0,0] AS x RETURN CASE x WHEN 0 THEN 0 ELSE COUNT_X END AS v", zeros, ""},
		{"case_and_in_or", "UNWIND [0,0] AS x RETURN CASE WHEN (x <> 0 AND EXISTS_X) OR x = 0 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}, {int64(1)}}, ""},
		{"match_case_exists_and", "CREATE (:T {v:0}), (:T {v:0}) ;; MATCH (n:T) RETURN CASE WHEN EXISTS_N AND n.v <> 0 THEN 1 ELSE 0 END AS v", zeros, ""},
		{"with_case", "UNWIND [0,0] AS x WITH x, CASE WHEN EXISTS_X AND x <> 0 THEN 1 ELSE 0 END AS v RETURN v", zeros, ""},
		{"case_then_and", "UNWIND [0,0] AS x RETURN CASE WHEN true THEN x <> 0 AND EXISTS_X END AS v", falses, ""},
		{"case_reached_then_and", "UNWIND [0,0] AS x RETURN CASE WHEN x = 0 THEN x <> 0 AND EXISTS_X ELSE false END AS v", falses, ""},
		{"case_simple_then_and", "UNWIND [0,0] AS x RETURN CASE x WHEN 0 THEN x <> 0 AND EXISTS_X END AS v", falses, ""},
	} {
		query := subqueries.Replace(tc.query)
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rows, err := runPerRowRound5Statements(t, query)
				if tc.message != "" {
					requireRound3Neo4jError(t, err, tc.message)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, rows)
			})
		}
	}
}

